//go:build linux || darwin

package lambda

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestDevAsyncDrainSummaryRealFutureRetryAndRunningAttempt(t *testing.T) {
	for _, retainedAbort := range []bool{false, true} {
		name := "deadline-drain"
		if retainedAbort {
			name = "canceled-final-close"
		}
		t.Run(name, func(t *testing.T) {
			service, directory, _ := shutdownNativeService(t)
			retry := shutdownAdmit(t, service, `{"label":"future","fail":true}`)
			retrying := waitAsyncState(t, service, retry.RequestID, "retrying")
			if retrying.NextAttemptAt.Sub(retrying.UpdatedAt) < 50*time.Second {
				t.Fatalf("native retry interval changed:%+v", retrying)
			}
			active := shutdownAdmit(t, service, `{"label":"active","gate":true}`)
			pid := shutdownStartedPID(t, directory, "active")
			defer shutdownRelease(t, directory, "active")
			var err error
			if retainedAbort {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				err = service.Close(ctx)
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("retained abort-style Close:%v", err)
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer cancel()
				err = service.DrainAsync(ctx)
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("deadline drain:%v", err)
				}
			}
			summary := service.DevAsyncDrainSummary()
			if summary.CapturedAt.IsZero() || summary.Outstanding != 2 || summary.PendingAdmissions != 0 || summary.Shown != 2 || summary.Omitted != 0 {
				t.Fatalf("pre-abort counts:%+v", summary)
			}
			var sawRetry, sawActive bool
			for _, entry := range summary.Invocations {
				switch entry.RequestID {
				case retry.RequestID:
					sawRetry = true
					if entry.FunctionName != "event" || entry.State != "retrying" || entry.Attempts != 1 || !entry.NextAttemptAt.Equal(retrying.NextAttemptAt) || !entry.NextAttemptAt.After(summary.CapturedAt) {
						t.Fatalf("native future retry metadata lost:%+v", entry)
					}
				case active.RequestID:
					sawActive = true
					if entry.State != "running" || entry.Attempts != 1 || !entry.NextAttemptAt.IsZero() {
						t.Fatalf("active identity invented retry:%+v", entry)
					}
				}
			}
			if !sawRetry || !sawActive || processAlive(pid) {
				t.Fatal("summary identity or existing process join changed")
			}
			for _, id := range []string{retry.RequestID, active.RequestID} {
				record := waitAsyncState(t, service, id, "canceled")
				if record.ErrorType != "ServiceShutdown" || !record.NextAttemptAt.IsZero() {
					t.Fatalf("native abort semantics changed:%+v", record)
				}
			}
			if service.DevDiscardedAsyncCount() != 0 {
				t.Fatal("timeout silently selected disposable policy")
			}
			capturedAt := summary.CapturedAt
			wantErr := context.DeadlineExceeded
			if retainedAbort {
				wantErr = context.Canceled
			}
			if closeErr := service.Close(context.Background()); !errors.Is(closeErr, wantErr) {
				t.Fatalf("default sticky shutdown changed:%v", closeErr)
			}
			if service.DevAsyncDrainSummary().CapturedAt != capturedAt {
				t.Fatal("joined Close overwrote pre-abort evidence")
			}
		})
	}
}

func TestDevAsyncDrainSummaryPendingCaptureHasNoPublishedIdentity(t *testing.T) {
	writer := &shutdownCaptureGate{state: "queued", entered: make(chan struct{}), proceed: make(chan struct{})}
	var once sync.Once
	open := func() { once.Do(func() { close(writer.proceed) }) }
	defer open()
	directory := t.TempDir()
	output := filepath.Join(directory, "must-not-run")
	service := newAsyncTestService(t, map[string]Function{"event": asyncCommand(t, output, "", "", "success")}, directory, &DevAsyncConfig{Workers: 1, LogWriter: writer})
	type admitted struct {
		value Admission
		err   error
	}
	admittedDone := make(chan admitted, 1)
	go func() {
		value, err := service.Admit(context.Background(), InvokeInput{FunctionName: "event", Payload: []byte("{}")})
		admittedDone <- admitted{value, err}
	}()
	<-writer.entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	drained := make(chan error, 1)
	go func() { drained <- service.DrainAsync(ctx) }()
	deadline := time.Now().Add(time.Second)
	for service.DevAsyncDrainSummary().CapturedAt.IsZero() {
		if time.Now().After(deadline) {
			t.Fatal("abort snapshot waited on queued capture/transition lock")
		}
		time.Sleep(time.Millisecond)
	}
	summary := service.DevAsyncDrainSummary()
	if summary.Outstanding != 1 || summary.PendingAdmissions != 1 || summary.Shown != 0 || summary.Omitted != 0 || len(summary.Invocations) != 0 {
		t.Fatalf("unpublished capture became native queued identity:%+v", summary)
	}
	open()
	accepted := <-admittedDone
	if accepted.err != nil || accepted.value.RequestID == "" {
		t.Fatalf("existing accepted reservation changed:%+v", accepted)
	}
	if err := <-drained; !errors.Is(err, context.Canceled) {
		t.Fatalf("native canceled drain:%v", err)
	}
	record := waitAsyncState(t, service, accepted.value.RequestID, "canceled")
	if record.ErrorType != "ServiceShutdown" || record.Attempts != 0 {
		t.Fatalf("reserved event launched:%+v", record)
	}
	frozen := service.DevAsyncDrainSummary()
	if frozen.CapturedAt != summary.CapturedAt || frozen.PendingAdmissions != 1 || len(frozen.Invocations) != 0 {
		t.Fatalf("later publication rewrote frozen evidence:%+v", frozen)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatal("abort reservation launched process")
	}
}

func TestDevAsyncDrainSummaryAbsentAfterHealthyDefaultDrain(t *testing.T) {
	service := newAsyncTestService(t, map[string]Function{"event": providedFunction(t, "echo")}, t.TempDir(), nil)
	admission := shutdownAdmit(t, service, "{}")
	if err := service.DrainAsync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if record := waitAsyncState(t, service, admission.RequestID, "succeeded"); record.Attempts != 1 {
		t.Fatalf("default success changed:%+v", record)
	}
	if got := service.DevAsyncDrainSummary(); !got.CapturedAt.IsZero() || got.Outstanding != 0 {
		t.Fatalf("healthy drain falsely reported abort:%+v", got)
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !service.DevAsyncDrainSummary().CapturedAt.IsZero() {
		t.Fatal("healthy Close invented timeout evidence")
	}
}
