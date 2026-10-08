//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/lyeith/eventbus/internal/devcapture"
)

func TestLaunchFactsRetainRetiredFailureAndCumulativeOwnership(t *testing.T) {
	priorProcess := strings.Repeat("p", maxDiagnosticDetail+17)
	priorOwnership := errors.New(strings.Repeat("o", maxDiagnosticDetail+11))
	initial := newRuntimePhase(context.Background(), time.Second, time.Second, "initial", nil)
	defer initial.stop()
	initial.cancel(errInitializationBudget)
	prior := invocationResult{diagnostics: invocationDiagnostics{processError: priorProcess}, ownershipErr: priorOwnership}
	first, completion := initial.complete(1, prior)
	if first.ProcessError != priorProcess || first.OwnershipErr != priorOwnership ||
		first.InitState != "timed_out" || first.ContextErr != completion.contextErr ||
		first.CancellationCause != completion.cancellationCause || !errors.Is(first.ContextErr, context.Canceled) ||
		!errors.Is(first.CancellationCause, errInitializationBudget) {
		t.Fatalf("core lost raw immutable launch facts: %#v", first)
	}
	// The merger's defensive cumulative ownership policy is tested separately
	// from service retry policy, which refuses to launch after dirty ownership.
	fallback := newRuntimePhase(context.Background(), time.Second, time.Second, "fallback", nil)
	defer fallback.stop()
	if _, err := fallback.beginInvoke(); err != nil {
		t.Fatal(err)
	}
	next := invocationResult{state: InvocationSucceeded}
	last, _ := fallback.complete(2, next)
	result := mergeLaunchDiagnostics(prior, next)
	result.phases = []runtimePhaseRecord{first, last}
	if result.diagnostics.processError != "" || !errors.Is(result.ownershipErr, priorOwnership) {
		t.Fatalf("retired error contaminated final process or erased uncertainty: %#v", result)
	}
	path := filepath.Join(t.TempDir(), "private.jsonl")
	sink, err := devcapture.OpenPrivate(path, "launch facts test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	service := &Service{diagnosticCapture: sink}
	at := time.Now()
	if err := service.captureDiagnostics(executableFunction{name: "facts:live", runtime: "command", timeout: time.Second},
		invocation{requestID: "native-request", name: "facts:live", attempt: 1}, result, false,
		at.Add(-time.Millisecond), diagnosticCompletion{at: at}); err != nil {
		t.Fatal(err)
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("terminal records=%d", len(records))
	}
	record := records[0]
	if record.SchemaVersion != "eventbus.lambda.invocation-diagnostic.v1" || record.RequestID != "native-request" ||
		record.Attempt != 1 || record.State != InvocationSucceeded || record.ProcessError != "" ||
		record.OwnershipConfirmed || len(record.OwnershipError) != maxDiagnosticDetail || !record.DetailTruncated ||
		len(record.ExecutionPhases) != 2 {
		t.Fatalf("invocation attribution/bounds lost: %#v", record)
	}
	retired, final := record.ExecutionPhases[0], record.ExecutionPhases[1]
	if retired.ProcessError != strings.Repeat("p", maxDiagnosticDetail) ||
		retired.OwnershipError != strings.Repeat("o", maxDiagnosticDetail) || retired.OwnershipConfirmed || !retired.DetailTruncated ||
		retired.ContextError != "deadline_exceeded" || retired.TerminationCause != "initialization_timeout" ||
		final.ProcessError != "" || !final.OwnershipConfirmed || final.OwnershipError != "" || final.DetailTruncated {
		t.Fatalf("per-launch evidence lost or unbounded: %#v", record.ExecutionPhases)
	}
}

func TestRetiredPhaseTruncationMarksWholeDiagnostic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private.jsonl")
	sink, err := devcapture.OpenPrivate(path, "retired detail test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	service := &Service{diagnosticCapture: sink}
	at := time.Now()
	result := invocationResult{state: InvocationSucceeded, phases: []runtimePhaseRecord{
		{InitAttempt: 1, Mode: "initial", InitState: "timed_out", ProcessError: strings.Repeat("p", maxDiagnosticDetail+1),
			ContextErr: context.Canceled, CancellationCause: errInitializationBudget},
		{InitAttempt: 2, Mode: "fallback", InitState: "succeeded", InvokeState: "succeeded"},
	}}
	if err := service.captureDiagnostics(executableFunction{name: "facts", runtime: "command", timeout: time.Second},
		invocation{requestID: "native-request", name: "facts", attempt: 1}, result, false, at,
		diagnosticCompletion{at: at}); err != nil {
		t.Fatal(err)
	}
	records := readDiagnosticRecords(t, path)
	if len(records) != 1 || !records[0].DetailTruncated || records[0].ProcessError != "" ||
		!records[0].OwnershipConfirmed || records[0].OwnershipError != "" || !records[0].ExecutionPhases[0].DetailTruncated {
		t.Fatalf("retired-only truncation was hidden: %#v", records)
	}
}

func TestDiagnosticDetailRemainsBoundedAfterJSONEncoding(t *testing.T) {
	for _, test := range []struct {
		name      string
		value     string
		truncated bool
	}{
		{"small", "short detail", false},
		{"ascii", strings.Repeat("x", maxDiagnosticDetail+1), true},
		{"rune boundary", strings.Repeat("€", maxDiagnosticDetail/3+1), true},
		{"invalid utf8 expands", strings.Repeat("\xffx", maxDiagnosticDetail/2), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, truncated := boundedDiagnosticDetail(test.value)
			if truncated != test.truncated || len(value) > maxDiagnosticDetail || !utf8.ValidString(value) {
				t.Fatalf("detail bound/encoding: bytes=%d truncated=%v utf8=%v", len(value), truncated, utf8.ValidString(value))
			}
			data, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			var decoded string
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded != value || len(decoded) > maxDiagnosticDetail {
				t.Fatalf("JSON expanded or changed bounded detail: bytes=%d", len(decoded))
			}
		})
	}
}

func TestLaunchCauseProjectionUsesFrozenContextFacts(t *testing.T) {
	parent, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	phase := newRuntimePhase(parent, time.Second, time.Second, "initial", nil)
	defer phase.stop()
	if _, err := phase.beginInvoke(); err != nil {
		t.Fatal(err)
	}
	record, completion := phase.complete(1, invocationResult{})
	cancel(errServiceCancellation)
	projected := diagnosticPhases([]runtimePhaseRecord{record})
	if record.ContextErr != nil || record.CancellationCause != nil || completion.contextErr != nil ||
		completion.cancellationCause != nil || completion.contextError != "" || completion.cause != "" ||
		projected[0].ContextError != "" || projected[0].TerminationCause != "" {
		t.Fatalf("late live cancellation changed frozen launch: raw=%#v completion=%#v projected=%#v", record, completion, projected)
	}
}
