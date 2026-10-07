//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDrainAsyncDeadlineJoinsOnlyAsyncProcesses(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node unavailable")
	}
	directory := t.TempDir()
	asyncMarker, syncMarker := filepath.Join(directory, "async-pids"), filepath.Join(directory, "sync-pid")
	writeFixture(t, directory, "hang.mjs", `import {spawn} from 'node:child_process'; import fs from 'node:fs';
export async function handler(event) { const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'inherit'}); fs.writeFileSync(process.env.PIDS,JSON.stringify([process.pid,child.pid])); await new Promise(()=>{}); }`)
	synchronous := providedFunction(t, "wait")
	synchronous.Timeout = time.Minute
	synchronous.Environment["EVENTBUS_LAMBDA_TEST_PID"] = syncMarker
	service := newAsyncTestService(t, map[string]Function{
		"hang": {Runtime: "node", Handler: "hang.mjs.handler", Timeout: time.Minute, Environment: map[string]string{"PIDS": asyncMarker}},
		"wait": synchronous,
		"echo": providedFunction(t, "echo"),
	}, directory, &DevAsyncConfig{Workers: 1, Capacity: 2, RetryDelays: []time.Duration{0, 0}})
	syncCtx, syncCancel := context.WithCancel(context.Background())
	defer syncCancel()
	syncDone := make(chan error, 1)
	go func() {
		_, err := service.Execute(syncCtx, InvokeInput{FunctionName: "wait", Payload: []byte(`{}`)})
		syncDone <- err
	}()
	waitForFile(t, syncMarker)
	first, err := service.Admit(context.Background(), InvokeInput{FunctionName: "hang", Payload: []byte(`{"first":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, asyncMarker)
	second, err := service.Admit(context.Background(), InvokeInput{FunctionName: "hang", Payload: []byte(`{"queued":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := service.DrainAsync(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("aborted async drain: %v", err)
	}
	for index, id := range []string{first.RequestID, second.RequestID} {
		record := waitAsyncState(t, service, id, "canceled")
		if record.Attempts != 1-index {
			t.Fatalf("queued task ran: %#v", record)
		}
	}
	data, err := os.ReadFile(asyncMarker)
	if err != nil {
		t.Fatal(err)
	}
	var asyncPIDs []int
	if json.Unmarshal(data, &asyncPIDs) != nil || len(asyncPIDs) != 2 {
		t.Fatal("invalid process fixture")
	}
	for _, pid := range asyncPIDs {
		if processAlive(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Errorf("async owned process %d remains after DrainAsync", pid)
		}
	}
	data, err = os.ReadFile(syncMarker)
	if err != nil {
		t.Fatal(err)
	}
	syncPID, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || !processAlive(syncPID) {
		t.Fatalf("async drain killed unrelated synchronous process: %s %v", data, err)
	}
	select {
	case err := <-syncDone:
		t.Fatalf("async drain canceled unrelated Execute: %v", err)
	default:
	}
	_, err = service.Admit(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte(`{}`)})
	assertServiceUnavailable(t, err)
	result, err := service.Execute(context.Background(), InvokeInput{FunctionName: "echo", Payload: []byte(`{}`)})
	if err != nil || result.FunctionError {
		t.Fatalf("sync execution after aborted async drain: %#v %v", result, err)
	}
	syncCancel()
	select {
	case err := <-syncDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("synchronous caller cancellation: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("synchronous child did not join")
	}
	if processAlive(syncPID) {
		t.Fatal("synchronous process remains after caller cancellation")
	}
	if err := service.DrainAsync(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("repeated DrainAsync erased aborted work: %v", err)
	}
	if err := service.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("final Close erased aborted async work: %v", err)
	}
}
