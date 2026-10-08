//go:build performance && (linux || darwin)

package lambda

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/testperf"
)

const performanceAsyncSamples = 20

type performanceAsyncJoin struct {
	RequestID  string
	PID        int
	Waited     bool
	CleanupErr error
}

type performanceAsyncOwner struct {
	service *Service
	mu      sync.Mutex
	joins   []performanceAsyncJoin
}

func newPerformanceAsyncOwner(t *testing.T, directory string, capture *DevAsyncConfig, activity DevActivity) *performanceAsyncOwner {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	function := Function{Runtime: "command", Command: []string{executable, "-test.run=^TestPerformanceReviewCommandChild$"},
		Environment: map[string]string{"EVENTBUS_PERFORMANCE_COMMAND_CHILD": "1"}, Timeout: 5 * time.Second}
	service, err := NewService(&Config{Functions: map[string]Function{"performance-async:live": function},
		DevAsync: capture, DevActivity: activity}, directory)
	if err != nil {
		t.Fatal(err)
	}
	owner := &performanceAsyncOwner{service: service}
	cleanup := service.processCleanup
	service.processCleanup = func(command *exec.Cmd) error {
		err := cleanup(command)
		var requestID string
		for _, value := range command.Env {
			if strings.HasPrefix(value, "EVENTBUS_LAMBDA_REQUEST_ID=") {
				requestID = strings.TrimPrefix(value, "EVENTBUS_LAMBDA_REQUEST_ID=")
				break
			}
		}
		point := performanceAsyncJoin{RequestID: requestID, Waited: command.ProcessState != nil, CleanupErr: err}
		if command.Process != nil {
			point.PID = command.Process.Pid
		}
		owner.mu.Lock()
		owner.joins = append(owner.joins, point)
		owner.mu.Unlock()
		return err
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("performance async owner close: %v", err)
		}
	})
	return owner
}

func (owner *performanceAsyncOwner) assertJoined(t *testing.T, admissions []Admission) {
	t.Helper()
	owner.mu.Lock()
	joins := append([]performanceAsyncJoin(nil), owner.joins...)
	owner.mu.Unlock()
	if len(joins) != len(admissions) {
		t.Fatalf("launches=%d admissions=%d", len(joins), len(admissions))
	}
	expected := make(map[string]bool, len(admissions))
	for _, admitted := range admissions {
		if admitted.RequestID == "" || expected[admitted.RequestID] {
			t.Fatalf("native admission identity: %#v", admissions)
		}
		expected[admitted.RequestID] = true
	}
	seen := make(map[string]bool, len(joins))
	for _, point := range joins {
		if !expected[point.RequestID] || seen[point.RequestID] || point.PID <= 0 ||
			!point.Waited || point.CleanupErr != nil || processAlive(point.PID) {
			t.Fatalf("actual async child identity/join failed: %#v", point)
		}
		if err := syscall.Kill(-point.PID, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("native group remains after Wait/cleanup: pid=%d err=%v", point.PID, err)
		}
		seen[point.RequestID] = true
		t.Logf("PERFORMANCE_ASYNC_JOIN request_id=%s pid=%d native_wait_joined=true", point.RequestID, point.PID)
	}
	history := owner.service.AsyncSnapshot()
	if len(history) != len(admissions) {
		t.Fatalf("terminal history=%d admissions=%d", len(history), len(admissions))
	}
	for _, record := range history {
		if !expected[record.RequestID] || record.State != "succeeded" || record.Attempts != 1 || record.CompletedAt.IsZero() {
			t.Fatalf("accepted native event changed: %#v", record)
		}
	}
	if err := owner.service.DevEvidence(); err != nil {
		t.Fatalf("joined async evidence dirty: %v", err)
	}
}

func assertPerformanceAsyncCapture(t *testing.T, records []AsyncRecord, admissions []Admission) {
	t.Helper()
	states := make(map[string][]string, len(admissions))
	for _, record := range records {
		if record.SchemaVersion != "eventbus.lambda.async.v1" {
			t.Fatalf("capture schema: %#v", record)
		}
		if record.State == "queued" && record.Attempts != 0 ||
			record.State != "queued" && record.Attempts != 1 ||
			record.State == "succeeded" && record.CompletedAt.IsZero() {
			t.Fatalf("capture attempt/terminal facts: %#v", record)
		}
		states[record.RequestID] = append(states[record.RequestID], record.State)
	}
	if len(records) != 3*len(admissions) || len(states) != len(admissions) {
		t.Fatalf("capture transitions=%d identities=%d admissions=%d", len(records), len(states), len(admissions))
	}
	for _, admitted := range admissions {
		if strings.Join(states[admitted.RequestID], ",") != "queued,running,succeeded" {
			t.Fatalf("capture/admission atomicity/order changed: id=%s states=%v", admitted.RequestID, states[admitted.RequestID])
		}
	}
}

// Discard removes external I/O; JSON encoding and the async ledger remain on.
// Durable uses the ordinary owned file and its per-record Sync. Both cases use
// the same fresh native command child, payload, four workers and task count.
func TestPerformanceAuditLambdaAsyncCapture(t *testing.T) {
	for _, mode := range []string{"discard", "durable"} {
		for _, producers := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/producers_%d", mode, producers), func(t *testing.T) {
				directory := t.TempDir()
				path := filepath.Join(directory, "async.jsonl")
				config := &DevAsyncConfig{Workers: 4, Capacity: 64, HistoryLimit: 64, RetryDelays: []time.Duration{0, 0}}
				if mode == "discard" {
					config.LogWriter = io.Discard
				} else {
					config.LogPath = path
				}
				owner := newPerformanceAsyncOwner(t, directory, config, nil)
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				start := make(chan struct{})
				quitMetadata := make(chan struct{})
				metadataDone := make(chan struct{})
				var metadataMS []float64
				var metadataErr error
				go func() {
					defer close(metadataDone)
					<-start
					for range performanceAsyncSamples {
						before := time.Now()
						if _, err := owner.service.DescribeTarget("performance-async:live", ""); err != nil {
							metadataErr = err
							return
						}
						metadataMS = append(metadataMS, float64(time.Since(before))/float64(time.Millisecond))
						timer := time.NewTimer(time.Millisecond)
						select {
						case <-quitMetadata:
							timer.Stop()
							return
						case <-timer.C:
						}
					}
				}()
				admissions := make([]Admission, performanceAsyncSamples)
				admissionMS := make([]float64, performanceAsyncSamples)
				failures := make(chan error, performanceAsyncSamples)
				var producersDone sync.WaitGroup
				for producer := range producers {
					producersDone.Add(1)
					go func() {
						defer producersDone.Done()
						<-start
						for index := producer; index < performanceAsyncSamples; index += producers {
							before := time.Now()
							admitted, err := owner.service.Admit(ctx, InvokeInput{FunctionName: "performance-async:live", Payload: []byte("{}")})
							admissionMS[index] = float64(time.Since(before)) / float64(time.Millisecond)
							admissions[index] = admitted
							if err != nil {
								failures <- err
							}
						}
					}()
				}
				before := time.Now()
				close(start)
				producersDone.Wait()
				drainErr := owner.service.DrainAsync(ctx)
				batchMS := float64(time.Since(before)) / float64(time.Millisecond)
				close(quitMetadata)
				<-metadataDone
				close(failures)
				for err := range failures {
					t.Error(err)
				}
				if drainErr != nil {
					t.Errorf("accepted task drain: %v", drainErr)
				}
				if metadataErr != nil {
					t.Errorf("unrelated metadata during async batch: %v", metadataErr)
				}
				if t.Failed() {
					t.FailNow()
				}
				owner.assertJoined(t, admissions)
				if mode == "durable" {
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					var records []AsyncRecord
					decoder := json.NewDecoder(bytes.NewReader(data))
					for {
						var record AsyncRecord
						err := decoder.Decode(&record)
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							t.Fatal(err)
						}
						records = append(records, record)
					}
					assertPerformanceAsyncCapture(t, records, admissions)
				}
				name := fmt.Sprintf("lambda_async_%s_producers_%d_workers_4", mode, producers)
				testperf.Report(t, name, "admission_ms", admissionMS)
				testperf.Report(t, name, "describe_target_ms", metadataMS)
				testperf.Report(t, name, "accepted_batch_joined_ms", []float64{batchMS})
				if err := owner.service.Close(ctx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

type performanceAsyncBlockedWriter struct {
	state       string
	entered     chan AsyncRecord
	release     chan struct{}
	once        sync.Once
	unblockOnce sync.Once
	mu          sync.Mutex
	records     []AsyncRecord
}

func (writer *performanceAsyncBlockedWriter) unblock() {
	writer.unblockOnce.Do(func() { close(writer.release) })
}

func (writer *performanceAsyncBlockedWriter) Write(data []byte) (int, error) {
	var record AsyncRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return 0, err
	}
	if record.State == writer.state {
		writer.once.Do(func() {
			writer.entered <- record
			<-writer.release
		})
	}
	writer.mu.Lock()
	writer.records = append(writer.records, record)
	writer.mu.Unlock()
	return len(data), nil
}

// This diagnostic fixture records coupling rather than asserting the old lock
// layout. Capture is deliberately backpressured at each ownership boundary;
// the retained barrier must never approve or resume before actual completion.
func TestPerformanceAuditLambdaAsyncBlockedCapture(t *testing.T) {
	for _, state := range []string{"queued", "running", "succeeded"} {
		t.Run(state, func(t *testing.T) {
			writer := &performanceAsyncBlockedWriter{state: state, entered: make(chan AsyncRecord, 1), release: make(chan struct{})}
			var service *Service
			retained := devquiescence.New(func() error { return service.DevEvidence() })
			owner := newPerformanceAsyncOwner(t, t.TempDir(), &DevAsyncConfig{Workers: 1, Capacity: 4,
				HistoryLimit: 4, RetryDelays: []time.Duration{0, 0}, LogWriter: writer}, retained)
			service = owner.service
			// Unblock before the registered owner Close on every failure path.
			t.Cleanup(writer.unblock)
			type admittedResult struct {
				admission Admission
				err       error
			}
			admittedDone := make(chan admittedResult, 1)
			go func() {
				admission, err := service.Admit(context.Background(), InvokeInput{FunctionName: "performance-async:live", Payload: []byte("{}")})
				admittedDone <- admittedResult{admission, err}
			}()
			var blocked AsyncRecord
			select {
			case blocked = <-writer.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("capture did not enter controlled block")
			}
			stateLockAvailable := service.mu.TryLock()
			if stateLockAvailable {
				service.mu.Unlock()
			}
			t.Logf("PERFORMANCE_ASYNC_BLOCK state=%s request_id=%s lambda_state_lock_available=%t",
				state, blocked.RequestID, stateLockAvailable)
			metadataDone := make(chan error, 1)
			var metadataMS float64
			go func() {
				before := time.Now()
				_, err := service.DescribeTarget("performance-async:live", "")
				metadataMS = float64(time.Since(before)) / float64(time.Millisecond)
				metadataDone <- err
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			pending, err := retained.Quiesce(ctx)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) || pending.FixtureSafe || pending.WorkCount != 1 ||
				pending.State != devquiescence.Draining {
				t.Fatalf("blocked capture lost accepted-task custody: snapshot=%#v err=%v", pending, err)
			}
			if _, err := retained.Resume(pending.Generation); !errors.Is(err, devquiescence.ErrNotSafe) {
				t.Fatalf("blocked evidence allowed generation resume: %v", err)
			}
			writer.unblock()
			var admitted admittedResult
			select {
			case admitted = <-admittedDone:
			case <-time.After(5 * time.Second):
				t.Fatal("admission did not complete after capture release")
			}
			if admitted.err != nil || admitted.admission.RequestID != blocked.RequestID {
				t.Fatalf("native admission changed: %#v", admitted)
			}
			select {
			case err := <-metadataDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("unrelated metadata did not resume after capture release")
			}
			joinedCtx, joinedCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer joinedCancel()
			held, err := retained.Quiesce(joinedCtx)
			if err != nil || !held.FixtureSafe || held.WorkCount != 0 || held.Generation != pending.Generation {
				t.Fatalf("actual async/capture join did not establish held generation: snapshot=%#v err=%v", held, err)
			}
			owner.assertJoined(t, []Admission{admitted.admission})
			writer.mu.Lock()
			records := append([]AsyncRecord(nil), writer.records...)
			writer.mu.Unlock()
			assertPerformanceAsyncCapture(t, records, []Admission{admitted.admission})
			resumed, err := retained.Resume(held.Generation)
			if err != nil || resumed.Generation != held.Generation+1 || resumed.State != devquiescence.Open {
				t.Fatalf("joined capture generation did not resume: snapshot=%#v err=%v", resumed, err)
			}
			testperf.Report(t, "lambda_async_blocked_"+state, "describe_target_ms", []float64{metadataMS})
			if err := service.Close(joinedCtx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
