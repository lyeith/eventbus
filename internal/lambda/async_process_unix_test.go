//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestEventCloseDeadlineCancelsQueuedAndJoinsDescendants(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node unavailable")
	}
	directory := t.TempDir()
	marker := filepath.Join(directory, "pids")
	writeFixture(t, directory, "hang.mjs", `import {spawn} from 'node:child_process'; import fs from 'node:fs';
export async function handler(event) { const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'inherit'}); fs.writeFileSync(process.env.PIDS,JSON.stringify([process.pid,child.pid])); await new Promise(()=>{}); }`)
	service := newAsyncTestService(t, map[string]Function{"hang": {Runtime: "node", Handler: "hang.mjs.handler", Timeout: time.Minute, Environment: map[string]string{"PIDS": marker}}}, directory, &DevAsyncConfig{Workers: 1, Capacity: 2, RetryDelays: []time.Duration{0, 0}})
	first, err := service.Admit(context.Background(), InvokeInput{FunctionName: "hang", Payload: []byte(`{"first":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitForFile(t, marker)
	second, err := service.Admit(context.Background(), InvokeInput{FunctionName: "hang", Payload: []byte(`{"queued":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := service.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("aborted drain: %v", err)
	}
	for index, id := range []string{first.RequestID, second.RequestID} {
		record := waitAsyncState(t, service, id, "canceled")
		if record.Attempts != 1-index {
			t.Fatalf("queued task ran: %#v", record)
		}
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	if json.Unmarshal(data, &pids) != nil || len(pids) != 2 {
		t.Fatal("invalid process fixture")
	}
	for _, pid := range pids {
		if processAlive(pid) {
			_ = syscall.Kill(pid, syscall.SIGKILL)
			t.Errorf("owned process %d remains after Close", pid)
		}
	}
	if err := service.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("later Close erased aborted work: %v", err)
	}
}
