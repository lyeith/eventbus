package lambda

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devcapture"
)

type pythonStackFaultFixture struct {
	session *pythonStackSession
	service *Service
	log     *bytes.Buffer
	command *os.File
	reply   *os.File
}

func newPythonStackFaultFixture(t *testing.T, sink io.Writer) *pythonStackFaultFixture {
	t.Helper()
	log := &bytes.Buffer{}
	if sink == nil {
		sink = log
	}
	service := &Service{diagnosticCapture: devcapture.NewWriter(sink, "private stack fault fixture")}
	now := time.Now()
	session := newPythonStackSession(service, InvocationMetadata{
		RequestID: "actual-stack-request", FunctionName: "wait:live",
		FunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:wait:live", Attempt: 2,
	}, now, now.Add(time.Hour), DevPythonStacksConfig{DeadlineLead: 200 * time.Millisecond})
	command, parentWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	parentReader, reply, err := os.Pipe()
	if err != nil {
		command.Close()
		parentWriter.Close()
		t.Fatal(err)
	}
	fixture := &pythonStackFaultFixture{session: session, service: service, log: log, command: command, reply: reply}
	t.Cleanup(func() {
		// These fake peers belong only to this protocol fixture. Close them
		// before joining a collector which may be waiting for its bounded EOF.
		command.Close()
		reply.Close()
		_ = session.finish()
		_ = service.diagnosticCapture.Close()
	})
	session.attach(parentReader, parentWriter)
	if !session.requestOnce("explicit") {
		t.Fatal("initial exact-attempt snapshot request was refused")
	}
	return fixture
}

func (fixture *pythonStackFaultFixture) respond(t *testing.T, data []byte) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.ReadAll(fixture.command)
		_, _ = fixture.reply.Write(data)
		_ = fixture.reply.Close()
	}()
	t.Cleanup(func() {
		fixture.command.Close()
		fixture.reply.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("owned fake response peer did not join")
		}
	})
}

func (fixture *pythonStackFaultFixture) record(t *testing.T) pythonStackRecord {
	t.Helper()
	select {
	case <-fixture.session.done:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded private stack collector did not join")
	}
	if err := fixture.session.finish(); err != nil {
		t.Fatalf("joined protocol fault unexpectedly dirtied ownership: %v", err)
	}
	var record pythonStackRecord
	decoder := json.NewDecoder(bytes.NewReader(fixture.log.Bytes()))
	if err := decoder.Decode(&record); err != nil {
		t.Fatal(err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("snapshot attempt published more than one private record: %v", err)
	}
	if record.SchemaVersion != "eventbus.lambda.python-stack.v1" ||
		record.RequestID != fixture.session.identity.RequestID ||
		record.FunctionARN != fixture.session.identity.FunctionARN ||
		record.FunctionName != fixture.session.identity.FunctionName ||
		record.Attempt != 2 || record.Trigger != "explicit" ||
		record.RequestedAt.IsZero() || record.CapturedAt.Before(record.RequestedAt) {
		t.Fatalf("snapshot lost actual attempt metadata: %#v", record)
	}
	return record
}

func pythonStackFaultValidSnapshot() pythonStackSnapshot {
	return pythonStackSnapshot{
		Status: "captured",
		Threads: []pythonThreadStack{{
			ID: 1, State: "alive", Frames: []pythonStackFrame{{Function: "receive", File: "handler.py", Line: 42}},
		}},
		Loops: []pythonLoopStack{{
			ID: 2, State: "running", Tasks: []pythonTaskStack{{
				ID: 3, State: "pending", Reason: "opaque_awaitable",
				Frames: []pythonStackFrame{{Function: "wait_for_body", File: "handler.py", Line: 51}},
			}},
		}},
	}
}

func marshalPythonStackFault(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestPythonStackChannelRejectsMalformedPrivateEvidence(t *testing.T) {
	valid := marshalPythonStackFault(t, pythonStackFaultValidSnapshot())
	secret := "private-fault-secret-must-not-be-retained"
	cases := []struct {
		name   string
		data   []byte
		reason string
	}{
		{"partial", []byte("{\"status\":\"captured\",\"threads\":["), "invalid_response"},
		{"trailing-object", append(append([]byte(nil), valid...), []byte("{}")...), "invalid_response"},
		{"unknown-source", marshalPythonStackFault(t, map[string]any{
			"status": "captured", "source": secret,
		}), "invalid_response"},
		{"unknown-locals", marshalPythonStackFault(t, map[string]any{
			"status": "captured", "threads": []any{map[string]any{
				"id": 1, "state": "alive", "frames": []any{map[string]any{
					"function": "receive", "file": "handler.py", "line": 42, "locals": secret,
				}},
			}},
		}), "invalid_response"},
		{"raw-failure", marshalPythonStackFault(t, pythonStackSnapshot{
			Status: "capture_failed", Reason: secret,
		}), "invalid_response"},
		{"oversized", bytes.Repeat([]byte("x"), maxPythonStackBytes+1), "response_too_large"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := newPythonStackFaultFixture(t, nil)
			fixture.respond(t, test.data)
			record := fixture.record(t)
			if record.Status != "capture_failed" || record.Reason != test.reason ||
				len(record.Threads) != 0 || len(record.Loops) != 0 {
				t.Fatalf("malformed private response was retained as evidence: %#v", record)
			}
			if record.Truncated != (test.name == "oversized") {
				t.Fatalf("wire truncation was not explicit: %#v", record)
			}
			if bytes.Contains(fixture.log.Bytes(), []byte(secret)) {
				t.Fatal("rejected private wire leaked source, locals or raw failure text")
			}
		})
	}
	t.Run("valid-safe-await-chain", func(t *testing.T) {
		fixture := newPythonStackFaultFixture(t, nil)
		fixture.respond(t, valid)
		record := fixture.record(t)
		if record.Status != "captured" || len(record.Loops) != 1 ||
			record.Loops[0].Tasks[0].Reason != "opaque_awaitable" {
			t.Fatalf("actual helper await reason was rejected: %#v", record)
		}
	})
}

func TestPythonStackChannelFramedLiveReplyDoesNotWaitForLauncherEOF(t *testing.T) {
	fixture := newPythonStackFaultFixture(t, nil)
	framed := append(marshalPythonStackFault(t, pythonStackFaultValidSnapshot()), '\n')
	written := make(chan error, 1)
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		command, err := io.ReadAll(fixture.command)
		if err == nil && string(command) != "snapshot\n" {
			err = errors.New("unexpected private snapshot command")
		}
		if err == nil {
			_, err = fixture.reply.Write(framed)
		}
		// A launcher may retain its copy of fd5 until invocation cleanup.
		// Deliberately leave this descriptor open after the complete frame.
		written <- err
	}()
	t.Cleanup(func() {
		fixture.command.Close()
		fixture.reply.Close()
		select {
		case <-peerDone:
		case <-time.After(time.Second):
			t.Error("owned fake live collector did not join")
		}
	})
	select {
	case err := <-written:
		if err != nil {
			t.Fatalf("fake collector could not publish its live frame: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("owned fake collector did not join its frame write")
	}
	select {
	case <-fixture.session.done:
	case <-time.After(time.Second):
		t.Fatal("complete live JSONL response waited for launcher EOF")
	}
	if _, err := fixture.reply.Stat(); err != nil {
		t.Fatalf("fixture lost its intentionally held launcher descriptor: %v", err)
	}
	fixture.session.mu.Lock()
	stopped := fixture.session.stopped
	fixture.session.mu.Unlock()
	if stopped {
		t.Fatal("live framing proof crossed invocation cleanup")
	}
	record := fixture.record(t)
	if record.Status != "captured" || record.Reason != "" || len(record.Loops) != 1 ||
		record.Loops[0].Tasks[0].Reason != "opaque_awaitable" {
		t.Fatalf("complete live frame was replaced by unavailable evidence: %#v", record)
	}
}

func TestPythonStackChannelUnavailableDoesNotInventCapture(t *testing.T) {
	t.Run("collector-eof-while-alive", func(t *testing.T) {
		fixture := newPythonStackFaultFixture(t, nil)
		fixture.respond(t, nil)
		record := fixture.record(t)
		if record.Status != "capture_unavailable" || record.Reason != "collector_unavailable" {
			t.Fatalf("early collector EOF claimed completion or capture: %#v", record)
		}
	})
	t.Run("live-collection-timeout", func(t *testing.T) {
		fixture := newPythonStackFaultFixture(t, nil)
		// A live process may have no cooperative snapshot (for example, the
		// interpreter is busy). This is separate from a failed post-stop join.
		record := fixture.record(t)
		if record.Status != "capture_unavailable" || record.Reason != "collection_timeout" {
			t.Fatalf("missing best-effort live snapshot claimed capture: %#v", record)
		}
		if fixture.service.DevEvidence() != nil {
			t.Fatal("optional live snapshot absence invented ownership uncertainty")
		}
	})
}

func TestPythonStackChannelRetainedPipeFailsJoinedOwnership(t *testing.T) {
	fixture := newPythonStackFaultFixture(t, nil)
	commandAccepted := make(chan struct{})
	go func() {
		_, _ = io.ReadAll(fixture.command)
		close(commandAccepted)
	}()
	select {
	case <-commandAccepted:
	case <-time.After(time.Second):
		t.Fatal("snapshot request did not reach its fake peer")
	}
	// finish denotes actual process-group cleanup. A writing descriptor still
	// held after that boundary cannot become clean merely by closing our reader.
	started := time.Now()
	err := fixture.session.finish()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("retained post-cleanup pipe was certified joined: %v; summary=%#v", err, fixture.session.summary())
	}
	if time.Since(started) > time.Second {
		t.Fatal("retained private descriptor prolonged the bounded join")
	}
	select {
	case <-fixture.session.done:
	default:
		t.Fatal("finish returned before its owned collector exited")
	}
	if fixture.session.requestOnce("explicit") {
		t.Fatal("joined attempt accepted a late snapshot request")
	}
	if again := fixture.session.finish(); !errors.Is(again, os.ErrDeadlineExceeded) {
		t.Fatalf("repeated finish erased original pipe uncertainty: %v", again)
	}
}

func TestPythonStackChannelAppendFailureRemainsSticky(t *testing.T) {
	unavailable := errors.New("fixture private append unavailable")
	fixture := newPythonStackFaultFixture(t, unavailableDiagnosticWriter{unavailable})
	fixture.respond(t, marshalPythonStackFault(t, pythonStackFaultValidSnapshot()))
	select {
	case <-fixture.session.done:
	case <-time.After(time.Second):
		t.Fatal("failed private append did not join")
	}
	if err := fixture.session.finish(); !errors.Is(err, unavailable) {
		t.Fatalf("collector finish erased private sink failure: %v", err)
	}
	if !errors.Is(fixture.service.DevEvidence(), unavailable) {
		t.Fatalf("private append uncertainty was not sticky: %v", fixture.service.DevEvidence())
	}
	if fixture.session.requestOnce("explicit") {
		t.Fatal("failed attempt launched another collector")
	}
}

type pythonStackFaultGateWriter struct {
	entered, release chan struct{}
	once             sync.Once
}

func (writer *pythonStackFaultGateWriter) Write(data []byte) (int, error) {
	writer.once.Do(func() { close(writer.entered) })
	<-writer.release
	return len(data), nil
}

func TestPythonStackFinishJoinsPrivateAppendBeforeRelease(t *testing.T) {
	writer := &pythonStackFaultGateWriter{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	fixture := newPythonStackFaultFixture(t, writer)
	// Cleanup runs in reverse registration order: release the blocked append
	// before the fixture joins its collector, including after a failed assertion.
	t.Cleanup(func() { release.Do(func() { close(writer.release) }) })
	fixture.respond(t, marshalPythonStackFault(t, pythonStackFaultValidSnapshot()))
	select {
	case <-writer.entered:
	case <-time.After(time.Second):
		t.Fatal("private append never entered")
	}
	finished := make(chan error, 1)
	go func() { finished <- fixture.session.finish() }()
	select {
	case err := <-finished:
		t.Fatalf("collector finish preceded private append completion: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(writer.release) })
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("joined private append was unhealthy: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("collector did not join its released private append")
	}
}

func TestPythonStackWireBoundsAndHelperReasons(t *testing.T) {
	for _, reason := range []string{
		"await_cycle", "opaque_awaitable", "unsupported_task_type", "task_collection_failed",
		"thread_frames_unavailable", "loop_closed", "unsupported_loop_type", "callback_timeout",
		"frames_unavailable", "reply_size_limit", "invalid_request", "collector_failed",
	} {
		t.Run("helper-"+reason, func(t *testing.T) {
			if !validPythonStack(pythonStackSnapshot{Status: "capture_failed", Reason: reason}) {
				t.Fatalf("actual fixed helper reason rejected: %s", reason)
			}
		})
	}
	cases := []struct {
		name   string
		mutate func(*pythonStackSnapshot)
	}{
		{"thread-limit", func(s *pythonStackSnapshot) {
			s.Threads = make([]pythonThreadStack, 33)
			for i := range s.Threads {
				s.Threads[i].State = "alive"
			}
		}},
		{"loop-limit", func(s *pythonStackSnapshot) {
			s.Loops = make([]pythonLoopStack, 9)
			for i := range s.Loops {
				s.Loops[i].State = "running"
			}
		}},
		{"total-task-limit", func(s *pythonStackSnapshot) {
			s.Loops = []pythonLoopStack{{State: "running", Tasks: make([]pythonTaskStack, 32)}, {State: "running", Tasks: make([]pythonTaskStack, 33)}}
			for i := range s.Loops {
				for j := range s.Loops[i].Tasks {
					s.Loops[i].Tasks[j].State = "pending"
				}
			}
		}},
		{"frame-limit", func(s *pythonStackSnapshot) { s.Threads[0].Frames = make([]pythonStackFrame, 33) }},
		{"string-byte-limit", func(s *pythonStackSnapshot) { s.Threads[0].Frames[0].File = strings.Repeat("界", 86) }},
		{"invalid-utf8", func(s *pythonStackSnapshot) { s.Threads[0].Frames[0].Function = string([]byte{0xff}) }},
		{"control-character", func(s *pythonStackSnapshot) { s.Threads[0].Frames[0].File = "handler.py\nrequest-secret" }},
		{"negative-line", func(s *pythonStackSnapshot) { s.Threads[0].Frames[0].Line = -1 }},
		{"thread-state", func(s *pythonStackSnapshot) { s.Threads[0].State = "request-secret" }},
		{"task-state", func(s *pythonStackSnapshot) { s.Loops[0].Tasks[0].State = "request-secret" }},
		{"unknown-reason", func(s *pythonStackSnapshot) { s.Loops[0].Tasks[0].Reason = "request-secret" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			snapshot := pythonStackFaultValidSnapshot()
			test.mutate(&snapshot)
			if validPythonStack(snapshot) {
				t.Fatal("out-of-contract private stack data accepted")
			}
		})
	}
}

func TestPythonStackConfigurationOwnsFlagAndExactAttempt(t *testing.T) {
	entry := executableFunction{name: "wait", environment: map[string]string{"EVENTBUS_DEV_PYTHON_STACKS": "1"}}
	input := invocation{name: "wait", requestID: "original-request"}
	for _, value := range environment(entry, input, "") {
		if strings.HasPrefix(value, "EVENTBUS_DEV_PYTHON_STACKS=") {
			t.Fatal("fixture environment enabled an unowned private stack channel")
		}
	}
	service := &Service{active: make(map[uint64]invocationOwner)}
	identity := InvocationMetadata{RequestID: "request", FunctionName: "wait", FunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:wait", Attempt: 2}
	now := time.Now()
	session := newPythonStackSession(service, identity, now, now.Add(time.Hour), DevPythonStacksConfig{})
	service.active[1] = invocationOwner{metadata: identity, pythonStacks: session}
	wrong := identity
	wrong.Attempt = 1
	if service.RequestPythonSnapshot(wrong) {
		t.Fatal("stale attempt selected current process")
	}
	if !service.RequestPythonSnapshot(identity) || service.RequestPythonSnapshot(identity) {
		t.Fatal("exact admitted attempt did not enforce one snapshot")
	}
	input.pythonStacks = session
	enabled := false
	for _, value := range environment(entry, input, "") {
		if value == "EVENTBUS_DEV_PYTHON_STACKS=1" {
			enabled = true
		}
	}
	if !enabled {
		t.Fatal("owned configuration did not select its private channel")
	}
}
