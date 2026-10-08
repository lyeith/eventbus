// Opt-in Python wait evidence. This adapter owns only a bounded private channel;
// native process cancellation, response policy and cleanup remain authoritative.
package lambda

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

//go:embed dev_python_stacks.py
var pythonStackCollector string

const maxPythonStackBytes = 64 << 10
const pythonStackReadBudget = 250 * time.Millisecond

type DevPythonStacksConfig struct {
	SnapshotAfter time.Duration `yaml:"snapshot_after,omitempty"`
	DeadlineLead  time.Duration `yaml:"deadline_lead,omitempty"`
}

func validatePythonStacks(config *DevPythonStacksConfig) error {
	if config.SnapshotAfter < 0 || config.SnapshotAfter > 900*time.Second {
		return errors.New("python_stacks.snapshot_after must be 0..900s")
	}
	if config.DeadlineLead < 0 || config.DeadlineLead > 5*time.Second {
		return errors.New("python_stacks.deadline_lead must be 0..5s (0 selects 200ms)")
	}
	return nil
}

type pythonStackFrame struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}
type pythonThreadStack struct {
	ID        uint64             `json:"id"`
	State     string             `json:"state"`
	Frames    []pythonStackFrame `json:"frames"`
	Truncated bool               `json:"truncated"`
}
type pythonTaskStack struct {
	ID        uint64             `json:"id"`
	State     string             `json:"state"`
	Reason    string             `json:"reason,omitempty"`
	Frames    []pythonStackFrame `json:"frames"`
	Truncated bool               `json:"truncated"`
}
type pythonLoopStack struct {
	ID        uint64            `json:"id"`
	State     string            `json:"state"`
	Reason    string            `json:"reason,omitempty"`
	Tasks     []pythonTaskStack `json:"tasks"`
	Truncated bool              `json:"truncated"`
}
type pythonStackSnapshot struct {
	Status    string              `json:"status"`
	Reason    string              `json:"reason,omitempty"`
	Truncated bool                `json:"truncated"`
	Threads   []pythonThreadStack `json:"threads"`
	Loops     []pythonLoopStack   `json:"loops"`
}
type pythonStackRecord struct {
	SchemaVersion string    `json:"schema_version"`
	RequestID     string    `json:"request_id"`
	FunctionName  string    `json:"function_name"`
	FunctionARN   string    `json:"function_arn"`
	Attempt       int       `json:"attempt"`
	Trigger       string    `json:"trigger"`
	RequestedAt   time.Time `json:"requested_at,omitzero"`
	CapturedAt    time.Time `json:"captured_at"`
	pythonStackSnapshot
}
type pythonStackSummary struct {
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Trigger string `json:"trigger"`
}

type pythonStackSession struct {
	service                      *Service
	identity                     InvocationMetadata
	scheduled                    time.Time
	scheduledTrigger             string
	started                      time.Time
	config                       DevPythonStacksConfig
	reschedule                   chan struct{}
	request                      chan string
	stop, done                   chan struct{}
	mu                           sync.Mutex
	requested, attached, stopped bool
	reader, writer               *os.File
	record                       pythonStackRecord
	ownershipErr                 error
	readDeadline, stopDeadline   time.Time
	unavailableReason            string
}

func newPythonStackSession(service *Service, identity InvocationMetadata, started, deadline time.Time, config DevPythonStacksConfig) *pythonStackSession {
	scheduled, trigger := deadline.Add(-config.DeadlineLead), "deadline"
	if config.SnapshotAfter > 0 && started.Add(config.SnapshotAfter).Before(scheduled) {
		scheduled, trigger = started.Add(config.SnapshotAfter), "snapshot_after"
	}
	return &pythonStackSession{service: service, identity: identity, scheduled: scheduled, scheduledTrigger: trigger, started: started, config: config, reschedule: make(chan struct{}, 1), request: make(chan string, 1), stop: make(chan struct{}), done: make(chan struct{})}
}

// Readiness selects the real Invoke deadline. Reschedule only an unrequested
// collector; explicit/admission snapshots remain single-shot and never renew.
func (session *pythonStackSession) updateDeadline(deadline time.Time) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.requested || session.stopped {
		return
	}
	session.scheduled, session.scheduledTrigger = deadline.Add(-session.config.DeadlineLead), "deadline"
	if session.config.SnapshotAfter > 0 && session.started.Add(session.config.SnapshotAfter).Before(session.scheduled) {
		session.scheduled, session.scheduledTrigger = session.started.Add(session.config.SnapshotAfter), "snapshot_after"
	}
	select {
	case session.reschedule <- struct{}{}:
	default:
	}
}

// RequestPythonSnapshot requests one best-effort private snapshot of the exact
// admitted attempt. true means accepted, not captured or joined. Obtain identity
// through ExecuteObserved admission; request while alive before canceling it.
// No native management API or handler code is involved.
func (service *Service) RequestPythonSnapshot(identity InvocationMetadata) bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	for _, owner := range service.active {
		if owner.metadata == identity && owner.pythonStacks != nil {
			return owner.pythonStacks.requestOnce("explicit")
		}
	}
	return false
}

func (session *pythonStackSession) requestOnce(trigger string) bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.requested || session.stopped {
		return false
	}
	session.requested = true
	session.request <- trigger
	return true
}

// attach transfers parent pipe ownership only after child launch succeeds.
func (session *pythonStackSession) attach(reader, writer *os.File) {
	session.mu.Lock()
	session.attached, session.reader, session.writer = true, reader, writer
	session.mu.Unlock()
	go session.collect()
}

func (session *pythonStackSession) collect() {
	defer close(session.done)
	defer func() {
		session.mu.Lock()
		if session.reader != nil {
			session.ownershipErr = errors.Join(session.ownershipErr, session.reader.Close())
			session.reader = nil
		}
		session.mu.Unlock()
	}()
	session.mu.Lock()
	timer := time.NewTimer(max(0, time.Until(session.scheduled)))
	session.mu.Unlock()
	defer timer.Stop()
	var trigger string
	waiting := true
	for waiting {
		select {
		case trigger = <-session.request:
			waiting = false
		case <-session.reschedule:
			session.mu.Lock()
			timer.Reset(max(0, time.Until(session.scheduled)))
			session.mu.Unlock()
		case <-timer.C:
			session.mu.Lock()
			// A readiness transition may have replaced a timer just as it fired.
			due, scheduledTrigger := !time.Now().Before(session.scheduled), session.scheduledTrigger
			if !due {
				timer.Reset(time.Until(session.scheduled))
			}
			if due && !session.requested && !session.stopped {
				session.requested = true
				session.request <- scheduledTrigger
			}
			session.mu.Unlock()
			if !due {
				continue
			}
			select {
			case trigger = <-session.request:
				waiting = false
			default:
				session.publish("not_requested", pythonStackSnapshot{Status: "capture_unavailable", Reason: "process_completed"}, time.Time{})
				return
			}
		case <-session.stop:
			session.publish("not_requested", pythonStackSnapshot{Status: "capture_unavailable", Reason: "process_completed"}, time.Time{})
			return
		}
	}
	requestedAt := time.Now()
	_ = session.writer.SetWriteDeadline(requestedAt.Add(10 * time.Millisecond))
	_, writeErr := session.writer.Write([]byte("snapshot\n"))
	_ = session.writer.Close()
	if writeErr != nil {
		session.publish(trigger, pythonStackSnapshot{Status: "capture_unavailable", Reason: "request_unavailable"}, requestedAt)
		return
	}
	session.mu.Lock()
	readDeadline := requestedAt.Add(pythonStackReadBudget)
	if session.stopped {
		readDeadline = minStackDeadline(readDeadline, session.stopDeadline)
	}
	session.readDeadline = readDeadline
	deadlineErr := session.reader.SetReadDeadline(readDeadline)
	session.mu.Unlock()
	if deadlineErr != nil {
		session.publish(trigger, pythonStackSnapshot{Status: "capture_failed", Reason: "channel_failed"}, requestedAt)
		return
	}
	// Live replies are one JSONL frame: a configured launcher can retain its
	// duplicate child descriptor until invocation exit, so EOF is not a live
	// message boundary. Retention after actual cleanup remains a dirty join.
	data, err := bufio.NewReaderSize(session.reader, maxPythonStackBytes+1).ReadSlice('\n')
	if errors.Is(err, io.EOF) {
		err = nil
	}
	snapshot := pythonStackSnapshot{Status: "capture_unavailable", Reason: "process_completed"}
	switch {
	case len(data) > maxPythonStackBytes || errors.Is(err, bufio.ErrBufferFull):
		snapshot = pythonStackSnapshot{Status: "capture_failed", Reason: "response_too_large", Truncated: true}
	case errors.Is(err, os.ErrDeadlineExceeded):
		snapshot.Reason = "collection_timeout"
		session.mu.Lock()
		if session.stopped {
			session.ownershipErr = errors.Join(session.ownershipErr, err, errors.New("private Python stack pipe retained after process cleanup"))
		}
		session.mu.Unlock()
	case err != nil:
		snapshot = pythonStackSnapshot{Status: "capture_failed", Reason: "channel_failed"}
	case len(data) > 0:
		snapshot = pythonStackSnapshot{}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		decodeErr := decoder.Decode(&snapshot)
		var extra any
		if decodeErr != nil || decoder.Decode(&extra) != io.EOF || !validPythonStack(snapshot) {
			snapshot = pythonStackSnapshot{Status: "capture_failed", Reason: "invalid_response"}
		}
	default:
		session.mu.Lock()
		if !session.stopped {
			snapshot.Reason = "collector_unavailable"
		}
		session.mu.Unlock()
	}
	session.publish(trigger, snapshot, requestedAt)
}

func (session *pythonStackSession) publish(trigger string, snapshot pythonStackSnapshot, requested time.Time) {
	record := pythonStackRecord{SchemaVersion: "eventbus.lambda.python-stack.v1", RequestID: session.identity.RequestID, FunctionName: session.identity.FunctionName, FunctionARN: session.identity.FunctionARN, Attempt: session.identity.Attempt, Trigger: trigger, RequestedAt: requested.UTC(), CapturedAt: time.Now().UTC(), pythonStackSnapshot: snapshot}
	err := session.service.appendDiagnostic(record)
	session.mu.Lock()
	session.record = record
	session.ownershipErr = errors.Join(session.ownershipErr, err)
	session.mu.Unlock()
}

// finish is called only after actual process-group cleanup, or before a process
// was launched. It joins all collector work before invocation release. A missing
// optional snapshot is not a substitute for child/pipe ownership evidence.
func (session *pythonStackSession) finish() error {
	session.mu.Lock()
	if !session.stopped {
		session.stopped = true
		session.stopDeadline = time.Now().Add(100 * time.Millisecond)
		close(session.stop)
		if session.reader != nil {
			session.readDeadline = minStackDeadline(session.readDeadline, session.stopDeadline)
			session.ownershipErr = errors.Join(session.ownershipErr, session.reader.SetReadDeadline(session.readDeadline))
		}
	}
	attached := session.attached
	session.mu.Unlock()
	if !attached {
		// Admission observers may prevent launch; no collector goroutine exists.
		session.mu.Lock()
		missing := session.record.SchemaVersion == ""
		session.mu.Unlock()
		if missing {
			reason := session.unavailableReason
			if reason == "" {
				reason = "process_not_started"
			}
			session.publish("not_requested", pythonStackSnapshot{Status: "capture_unavailable", Reason: reason}, time.Time{})
		}
	} else {
		<-session.done
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.reader != nil {
		session.ownershipErr = errors.Join(session.ownershipErr, session.reader.Close())
		session.reader = nil
	}
	if session.writer != nil {
		_ = session.writer.Close()
		session.writer = nil
	}
	return session.ownershipErr
}

func minStackDeadline(first, second time.Time) time.Time {
	if first.IsZero() || second.Before(first) {
		return second
	}
	return first
}

func (session *pythonStackSession) summary() *pythonStackSummary {
	session.mu.Lock()
	defer session.mu.Unlock()
	return &pythonStackSummary{Status: session.record.Status, Reason: session.record.Reason, Trigger: session.record.Trigger}
}

func validPythonStack(snapshot pythonStackSnapshot) bool {
	if snapshot.Status != "captured" && snapshot.Status != "capture_failed" {
		return false
	}
	if !validStackReason(snapshot.Reason) || len(snapshot.Threads) > 32 || len(snapshot.Loops) > 8 {
		return false
	}
	for _, thread := range snapshot.Threads {
		if thread.State != "alive" || !validStackFrames(thread.Frames) {
			return false
		}
	}
	tasks := 0
	for _, loop := range snapshot.Loops {
		if loop.State != "running" && loop.State != "unavailable" || !validStackReason(loop.Reason) {
			return false
		}
		tasks += len(loop.Tasks)
		for _, task := range loop.Tasks {
			if task.State != "pending" && task.State != "done" && task.State != "canceled" || !validStackReason(task.Reason) || !validStackFrames(task.Frames) {
				return false
			}
		}
	}
	return tasks <= 64
}
func validStackReason(value string) bool {
	switch value {
	case "", "collection_failed", "collection_timeout", "loop_unavailable", "loop_closed", "unsupported_task_type", "opaque_awaiter", "opaque_awaitable", "budget_exhausted", "await_cycle", "thread_frames_unavailable", "task_collection_failed", "unsupported_loop_type", "callback_timeout", "frames_unavailable", "reply_size_limit", "invalid_request", "collector_failed":
		return true
	}
	return false
}
func validStackFrames(frames []pythonStackFrame) bool {
	if len(frames) > 32 {
		return false
	}
	for _, frame := range frames {
		if frame.Line < 0 {
			return false
		}
		for _, text := range []string{frame.Function, frame.File} {
			if len(text) > 256 || !utf8.ValidString(text) {
				return false
			}
			for _, character := range text {
				if unicode.IsControl(character) {
					return false
				}
			}
		}
	}
	return true
}
