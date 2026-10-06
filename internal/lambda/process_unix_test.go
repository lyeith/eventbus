//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNodeDescendantsStoppedAfterSuccessAndCancellation(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Skip("Node not installed")
	}
	for _, hang := range []bool{false, true} {
		t.Run(fmt.Sprint(hang), func(t *testing.T) {
			directory := t.TempDir()
			writeFixture(t, directory, "children.mjs", `
import {spawn} from 'node:child_process';
import fs from 'node:fs';
export async function handler(event) {
  const child = spawn(process.execPath, ['-e', 'setInterval(()=>{}, 1000)'], {stdio:'inherit'});
  fs.writeFileSync(process.env.PID_FILE, String(child.pid));
  if (event.hang) await new Promise(()=>{});
  return {child:child.pid};
}
`)
			marker := filepath.Join(directory, "childpid")
			service := newTestService(t, map[string]Function{"child": {Runtime: "node", Handler: "children.mjs.handler", Environment: map[string]string{"PID_FILE": marker}, Timeout: 5 * time.Second}}, directory)
			done := make(chan struct{})
			if hang {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				request := httptest.NewRequest(http.MethodPost, invokePrefix+"child"+invokeSuffix, strings.NewReader(`{"hang":true}`)).WithContext(ctx)
				writer := httptest.NewRecorder()
				go func() { service.ServeHTTP(writer, request); close(done) }()
				waitForFile(t, marker)
				cancel()
			} else {
				response := requestInvoke(service, "child", `{}`, nil)
				var result map[string]any
				if json.Unmarshal(response.Body.Bytes(), &result) != nil || result["child"] == nil {
					t.Fatalf("response: %s", response.Body.String())
				}
				close(done)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("process cleanup did not finish")
			}
			data, err := os.ReadFile(marker)
			if err != nil {
				t.Fatal(err)
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for processAlive(pid) && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if processAlive(pid) {
				_ = syscall.Kill(pid, syscall.SIGKILL)
				t.Fatal("handler descendant remained running")
			}
		})
	}
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	if errors.Is(err, syscall.ESRCH) {
		return false
	}
	// Linux may retain a terminated orphan briefly as a zombie until init
	// reaps it. A zombie has no running process or open result descriptors.
	if state, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); readErr == nil {
		if position := strings.LastIndex(string(state), ") "); position >= 0 && len(state) > position+2 && state[position+2] == 'Z' {
			return false
		}
	}
	return err == nil
}
