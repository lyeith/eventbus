//go:build performance && (linux || darwin)

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
)

const performanceReviewSamples = 20

const performanceReviewPython = `
import os
import sys
import time

def handler(event, context):
    if event.get("log_bytes"):
        print("o" * event["log_bytes"], flush=True)
        print("e" * event["log_bytes"], file=sys.stderr, flush=True)
    if event.get("delay_ms"):
        time.sleep(event["delay_ms"] / 1000)
    return {"requestId": context.aws_request_id, "pid": os.getpid(),
            "executable": sys.executable, "version": sys.version, "prefix": sys.prefix}
`

const performanceReviewNode = `
export function handler() {
  return {requestId: process.env.EVENTBUS_LAMBDA_REQUEST_ID, pid: process.pid,
          executable: process.execPath, version: process.version};
}
`

// The command adapter is an actual bounded child with a stdout response. It has
// no Runtime API/readiness handshake and exits before the test harness writes PASS.
func TestPerformanceReviewCommandChild(t *testing.T) {
	if os.Getenv("EVENTBUS_PERFORMANCE_COMMAND_CHILD") != "1" {
		return
	}
	var input map[string]any
	if json.NewDecoder(os.Stdin).Decode(&input) != nil {
		os.Exit(2)
	}
	if err := json.NewEncoder(os.Stdout).Encode(map[string]any{
		"requestId": os.Getenv("EVENTBUS_LAMBDA_REQUEST_ID"),
		"pid":       os.Getpid(),
	}); err != nil {
		os.Exit(3)
	}
	os.Exit(0)
}

type performanceReviewReply struct {
	RequestID  string `json:"requestId"`
	PID        int    `json:"pid"`
	Executable string `json:"executable,omitempty"`
	Version    string `json:"version,omitempty"`
	Prefix     string `json:"prefix,omitempty"`
	Runtime    string `json:"runtime,omitempty"`
}

type performanceReviewSample struct {
	Case           string                 `json:"case"`
	Index          int                    `json:"index"`
	RequestID      string                 `json:"request_id"`
	PID            int                    `json:"pid"`
	InitMS         float64                `json:"init_ms"`
	InvokeMS       float64                `json:"invoke_ms"`
	WallMS         float64                `json:"joined_invoke_wall_ms"`
	SnapshotStatus string                 `json:"snapshot_status,omitempty"`
	Reply          performanceReviewReply `json:"metadata"`
}

func performanceReviewInterpreter(t *testing.T) string {
	t.Helper()
	path := os.Getenv("EVENTBUS_PERFORMANCE_PYTHON")
	if path == "" {
		path = os.Getenv("EVENTBUS_SMOKE_PYTHON")
	}
	if path == "" {
		t.Skip("set EVENTBUS_PERFORMANCE_PYTHON to an existing frozen absolute Python executable")
	}
	if !filepath.IsAbs(path) {
		t.Fatal("configured performance interpreter must be an absolute frozen path")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatalf("configured frozen Python interpreter unavailable: %s %v", path, err)
	}
	return path
}

func performanceReviewPythonFunction(t *testing.T, directory, interpreter string, managedUV bool) Function {
	t.Helper()
	writeFixture(t, directory, "performance.py", performanceReviewPython)
	command, environment := pythonCommand(t)
	if managedUV {
		// Explicit interpreter selection prevents UV's default discovery from
		// turning the comparison into two different Python implementations.
		command = append(append([]string(nil), command[:len(command)-1]...), "--python", interpreter, "python")
	} else {
		command = []string{interpreter}
	}
	return Function{Runtime: "python", Handler: "performance.handler", Command: command, Environment: environment, Timeout: 5 * time.Second}
}

type performanceReviewOwner struct {
	service *Service
	mu      sync.Mutex
	pids    []int
}

func newPerformanceReviewOwner(t *testing.T, directory string, function Function, diagnostics *DevDiagnosticsConfig) *performanceReviewOwner {
	t.Helper()
	service, err := NewService(&Config{
		Functions:      map[string]Function{"performance": function},
		DevAsync:       &DevAsyncConfig{Workers: 1, LogWriter: io.Discard},
		DevDiagnostics: diagnostics,
	}, directory)
	if err != nil {
		t.Fatal(err)
	}
	owner := &performanceReviewOwner{service: service}
	entry := service.functions["performance"]
	t.Logf("PERFORMANCE_CONFIG case=%s runtime=%s command=%q go=%s os=%s arch=%s diagnostics=%t stacks=%t",
		t.Name(), entry.runtime, entry.command, runtime.Version(), runtime.GOOS, runtime.GOARCH,
		diagnostics != nil, diagnostics != nil && diagnostics.PythonStacks != nil)
	cleanup := service.processCleanup
	service.processCleanup = func(command *exec.Cmd) error {
		err := cleanup(command)
		if command.Process != nil {
			owner.mu.Lock()
			owner.pids = append(owner.pids, command.Process.Pid)
			owner.mu.Unlock()
		}
		return err
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := service.Close(ctx); err != nil {
			t.Errorf("performance fixture actual owner closure: %v", err)
		}
	})
	return owner
}

func (owner *performanceReviewOwner) assertJoined(t *testing.T, replies []performanceReviewSample) {
	t.Helper()
	seen := make(map[int]bool)
	for _, sample := range replies {
		if sample.PID <= 0 || seen[sample.PID] || processAlive(sample.PID) {
			t.Fatalf("actual fresh child PID was reused or remains alive: %#v", sample)
		}
		seen[sample.PID] = true
		if sample.Reply.Runtime != "" {
			assertProvidedPhaseJoined(t, providedPhasePoint{PID: sample.PID, Runtime: sample.Reply.Runtime})
		}
	}
	owner.mu.Lock()
	launchers := append([]int(nil), owner.pids...)
	owner.mu.Unlock()
	if len(launchers) != len(replies) {
		t.Fatalf("unexpected actual process launches/fallback: %d, samples=%d", len(launchers), len(replies))
	}
	for _, pid := range launchers {
		if processAlive(pid) {
			t.Fatalf("actual launcher not joined: %d", pid)
		}
		if err := syscall.Kill(-pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("owned launcher process group remains after native join: pid=%d error=%v", pid, err)
		}
	}
	if err := owner.service.Close(context.Background()); err != nil || owner.service.DevEvidence() != nil {
		t.Fatalf("measurement owner uncertain: close=%v evidence=%v", err, owner.service.DevEvidence())
	}
}

func performanceReviewInvoke(owner *performanceReviewOwner, name string, index int, payload []byte) (performanceReviewSample, error) {
	sample := performanceReviewSample{Case: name, Index: index, RequestID: uuid.NewString()}
	entry := owner.service.functions["performance"]
	input := prepareInvocation(entry, "performance", InvokeInput{FunctionName: "performance", Payload: payload}, sample.RequestID)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	started := time.Now()
	result, err := owner.service.invoke(ctx, entry, input)
	sample.WallMS = float64(time.Since(started)) / float64(time.Millisecond)
	if err != nil || result.functionError || result.state != InvocationSucceeded || result.ownershipErr != nil {
		return sample, fmt.Errorf("actual invocation failed/uncertain: error=%v state=%s ownership=%v response=%s", err, result.state, result.ownershipErr, result.payload)
	}
	if len(result.phases) != 1 {
		return sample, fmt.Errorf("normal no-op unexpectedly retried Init: %#v", result.phases)
	}
	phase := result.phases[0]
	command := entry.runtime == "command"
	if phase.InitAttempt != 1 || phase.InvokeState != "succeeded" ||
		(command && (phase.Mode != "command" || phase.InitMS != 0)) ||
		(!command && (phase.Mode != "initial" || phase.InitState != "succeeded")) {
		return sample, fmt.Errorf("unexpected measured phase states: %#v", phase)
	}
	sample.InitMS, sample.InvokeMS = phase.InitMS, phase.InvokeMS
	if err := json.Unmarshal(result.payload, &sample.Reply); err != nil {
		return sample, err
	}
	sample.PID = sample.Reply.PID
	if sample.PID <= 0 || sample.Reply.RequestID != sample.RequestID {
		return sample, fmt.Errorf("actual child identity changed: %#v", sample)
	}
	for _, value := range []float64{sample.WallMS, sample.InitMS, sample.InvokeMS} {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return sample, fmt.Errorf("invalid monotonic timing: %#v", sample)
		}
	}
	if sample.InitMS+sample.InvokeMS > sample.WallMS+1 {
		return sample, fmt.Errorf("native joined phases exceed enclosing call: %#v", sample)
	}
	return sample, nil
}

func logPerformanceReviewSamples(t *testing.T, samples []performanceReviewSample) {
	t.Helper()
	init, invoke, wall := make([]float64, 0, len(samples)), make([]float64, 0, len(samples)), make([]float64, 0, len(samples))
	for _, sample := range samples {
		data, err := json.Marshal(sample)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("PERFORMANCE_SAMPLE %s", data)
		init, invoke, wall = append(init, sample.InitMS), append(invoke, sample.InvokeMS), append(wall, sample.WallMS)
	}
	for name, values := range map[string][]float64{"init_ms": init, "invoke_ms": invoke, "joined_invoke_wall_ms": wall} {
		sort.Float64s(values)
		median := (values[len(values)/2-1] + values[len(values)/2]) / 2
		t.Logf("PERFORMANCE_SUMMARY case=%s metric=%s count=%d first=separately_logged min=%.6f median=%.6f max=%.6f", samples[0].Case, name, len(values), values[0], median, values[len(values)-1])
	}
}

func assertPerformanceReviewPython(t *testing.T, reply performanceReviewReply, interpreter string) {
	t.Helper()
	expected, err := filepath.EvalSymlinks(interpreter)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := filepath.EvalSymlinks(reply.Executable)
	if err != nil {
		t.Fatal(err)
	}
	if actual != expected || reply.Version == "" || reply.Prefix == "" {
		t.Fatalf("UV/direct fixture selected a different frozen interpreter: expected=%s actual=%#v", expected, reply)
	}
}

func TestPerformanceReviewLambdaRuntimes(t *testing.T) {
	interpreter := performanceReviewInterpreter(t)
	var pythonReference *performanceReviewReply
	for _, runtime := range []string{"python_direct", "python_managed_uv", "node", "provided", "command"} {
		t.Run(runtime, func(t *testing.T) {
			directory := t.TempDir()
			var function Function
			switch runtime {
			case "python_direct", "python_managed_uv":
				function = performanceReviewPythonFunction(t, directory, interpreter, runtime == "python_managed_uv")
			case "node":
				if _, err := exec.LookPath("node"); err != nil {
					t.Fatal("opt-in performance lane requires Node")
				}
				writeFixture(t, directory, "performance.mjs", performanceReviewNode)
				function = Function{Runtime: "node", Handler: "performance.mjs.handler", Timeout: 5 * time.Second}
			case "provided":
				function = providedFunction(t, "phase-echo")
			case "command":
				executable, err := os.Executable()
				if err != nil {
					t.Fatal(err)
				}
				function = Function{Runtime: "command", Command: []string{executable, "-test.run=^TestPerformanceReviewCommandChild$"},
					Environment: map[string]string{"EVENTBUS_PERFORMANCE_COMMAND_CHILD": "1"}, Timeout: 5 * time.Second}
			}
			owner := newPerformanceReviewOwner(t, directory, function, nil)
			var samples []performanceReviewSample
			for index := range performanceReviewSamples {
				sample, err := performanceReviewInvoke(owner, runtime, index, []byte("{}"))
				if err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(runtime, "python") {
					assertPerformanceReviewPython(t, sample.Reply, interpreter)
					if pythonReference == nil {
						reference := sample.Reply
						pythonReference = &reference
					} else if sample.Reply.Version != pythonReference.Version || sample.Reply.Prefix != pythonReference.Prefix {
						t.Fatalf("direct/UV interpreter version or environment differs: reference=%#v actual=%#v", pythonReference, sample.Reply)
					}
				}
				samples = append(samples, sample)
			}
			owner.assertJoined(t, samples)
			logPerformanceReviewSamples(t, samples)
		})
	}
}

func TestPerformanceReviewLambdaPrivateCapture(t *testing.T) {
	interpreter := performanceReviewInterpreter(t)
	for _, mode := range []string{"disabled", "terminal", "python_stack"} {
		t.Run(mode, func(t *testing.T) {
			directory := t.TempDir()
			function := performanceReviewPythonFunction(t, directory, interpreter, false)
			path := filepath.Join(directory, "private", "performance.jsonl")
			var diagnostics *DevDiagnosticsConfig
			if mode != "disabled" {
				diagnostics = &DevDiagnosticsConfig{LogPath: path}
			}
			if mode == "python_stack" {
				diagnostics.PythonStacks = &DevPythonStacksConfig{SnapshotAfter: 200 * time.Millisecond}
			}
			owner := newPerformanceReviewOwner(t, directory, function, diagnostics)
			var samples []performanceReviewSample
			// All modes perform the same handler delay and moderate stdout/stderr.
			// The enabled stack case must collect during actual handler work.
			payload := []byte(`{"delay_ms":300,"log_bytes":1024}`)
			for index := range performanceReviewSamples {
				sample, err := performanceReviewInvoke(owner, mode, index, payload)
				if err != nil {
					t.Fatal(err)
				}
				assertPerformanceReviewPython(t, sample.Reply, interpreter)
				samples = append(samples, sample)
			}
			owner.assertJoined(t, samples)
			if diagnostics != nil {
				var records []invocationDiagnosticRecord
				var snapshots []pythonStackRecord
				if mode == "python_stack" {
					snapshots, records = readPythonStackEvidence(t, path)
				} else {
					records = readDiagnosticRecords(t, path)
				}
				if len(records) != len(samples) {
					t.Fatalf("joined private terminal count: %d samples=%d", len(records), len(samples))
				}
				for index, record := range records {
					if record.RequestID != samples[index].RequestID || record.Attempt != 1 || !record.OwnershipConfirmed ||
						record.State != InvocationSucceeded || len(record.ExecutionPhases) != 1 {
						t.Fatalf("private capture changed actual result: %#v", record)
					}
					if mode == "python_stack" {
						if len(snapshots) != len(samples) || snapshots[index].RequestID != record.RequestID || snapshots[index].Attempt != 1 ||
							snapshots[index].Status != "captured" || record.PythonStack == nil || record.PythonStack.Status != "captured" {
							t.Fatalf("enabled snapshot was not actually captured: snapshots=%#v terminal=%#v", snapshots, record)
						}
						foundHandler := false
						for _, function := range pythonStackFrameFunctions(snapshots[index]) {
							if function == "handler" {
								foundHandler = true
							}
						}
						if !foundHandler {
							t.Fatalf("snapshot did not measure actual delayed handler work: %#v", snapshots[index])
						}
						samples[index].SnapshotStatus = snapshots[index].Status
					}
				}
			}
			logPerformanceReviewSamples(t, samples)
		})
	}
}

func TestPerformanceReviewLambdaConcurrencyFour(t *testing.T) {
	interpreter := performanceReviewInterpreter(t)
	directory := t.TempDir()
	owner := newPerformanceReviewOwner(t, directory, performanceReviewPythonFunction(t, directory, interpreter, false), nil)
	jobs := make(chan int)
	samples := make([]performanceReviewSample, performanceReviewSamples)
	failures := make(chan error, performanceReviewSamples)
	var workers sync.WaitGroup
	batchStarted := time.Now()
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				sample, err := performanceReviewInvoke(owner, "python_concurrency_4", index, []byte("{}"))
				samples[index] = sample
				if err != nil {
					failures <- err
				}
			}
		}()
	}
	for index := range performanceReviewSamples {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	batchMS := float64(time.Since(batchStarted)) / float64(time.Millisecond)
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}
	for _, sample := range samples {
		assertPerformanceReviewPython(t, sample.Reply, interpreter)
	}
	owner.assertJoined(t, samples)
	logPerformanceReviewSamples(t, samples)
	t.Logf("PERFORMANCE_BATCH case=python_concurrency_4 count=%d concurrency=4 joined_batch_ms=%.6f", len(samples), batchMS)
}
