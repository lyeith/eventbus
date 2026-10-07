//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestDevPipeOwnershipProcess is a bounded pipe-holder fixture. It has no
// business effects: the test owns its marker and kills the sole holder if an
// assertion fails. Escaped holders deliberately exercise detectable uncertainty;
// this does not add support for arbitrary detached application processes.
func TestDevPipeOwnershipProcess(t *testing.T) {
	mode := os.Getenv("EVENTBUS_PIPE_OWNERSHIP_FIXTURE")
	if mode == "" {
		return
	}
	if mode == "hold" {
		if os.Getenv("PIPE_RESULT_CHANNEL") == "1" {
			result := os.NewFile(3, "held-result-channel")
			if _, err := result.Stat(); err != nil {
				os.Exit(3)
			}
			defer result.Close()
		}
		time.Sleep(8 * time.Second)
		os.Exit(0)
	}
	if mode == "provided-response" {
		// The direct runtime's exit closes stdin. Its owned log pipes remain
		// open here, so the valid native response precedes Wait's pipe deadline.
		_, _ = io.ReadAll(os.Stdin)
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Post(os.Getenv("PIPE_RESPONSE_URL"), "application/json", strings.NewReader(`{"ok":true}`))
		if err != nil {
			os.Exit(4)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusAccepted {
			os.Exit(5)
		}
		time.Sleep(8 * time.Second)
		os.Exit(0)
	}
	if mode != "provided" {
		os.Exit(6)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	base := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + runtimePrefix
	response, err := client.Get(base + "invocation/next")
	if err != nil {
		os.Exit(7)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	requestID := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	reader, writer, err := os.Pipe()
	if err != nil {
		os.Exit(8)
	}
	executable, err := os.Executable()
	if err != nil {
		os.Exit(9)
	}
	holder := exec.Command(executable, "-test.run=^TestDevPipeOwnershipProcess$")
	holder.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	holder.Env = []string{"EVENTBUS_PIPE_OWNERSHIP_FIXTURE=provided-response", "PIPE_RESPONSE_URL=" + base + "invocation/" + requestID + "/response"}
	holder.Stdin, holder.Stdout, holder.Stderr = reader, os.Stdout, os.Stderr
	if holder.Start() != nil {
		os.Exit(10)
	}
	_ = reader.Close()
	if os.WriteFile(os.Getenv("PIPE_HOLDER_MARKER"), []byte(strconv.Itoa(holder.Process.Pid)), 0600) != nil {
		_ = holder.Process.Kill()
		os.Exit(11)
	}
	// Keep the write endpoint until actual process exit; the EOF, rather than a
	// sleep or PID observation, sequences the independent response fixture.
	_ = writer
	os.Exit(0)
}

func pipeHolderFixture(t *testing.T, directory string) (executable, marker string) {
	t.Helper()
	var err error
	executable, err = os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	marker = filepath.Join(directory, "pipe-holder.pid")
	t.Cleanup(func() {
		data, err := os.ReadFile(marker)
		if os.IsNotExist(err) {
			return
		}
		if err != nil {
			t.Errorf("read owned pipe holder marker: %v", err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err != nil || pid <= 0 {
			t.Errorf("invalid owned pipe holder marker: %q", data)
			return
		}
		// This is disposal of the test's one known holder, never barrier proof.
		_ = syscall.Kill(pid, syscall.SIGKILL)
	})
	return executable, marker
}

func TestDevPipeOwnershipValidNodeResultKeepsWaitDelayPrivate(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Fatal("Node is required for trusted pipe ownership proof")
	}
	for _, boundary := range []string{"execute", "event"} {
		t.Run(boundary, func(t *testing.T) {
			directory := t.TempDir()
			executable, marker := pipeHolderFixture(t, directory)
			writeFixture(t, directory, "pipes.mjs", `
import {spawn} from 'node:child_process';
import fs from 'node:fs';
export async function handler() {
  const holder=spawn(process.env.PIPE_HOLDER_EXE,['-test.run=^TestDevPipeOwnershipProcess$'],{
    stdio:['ignore','inherit','inherit'],env:{EVENTBUS_PIPE_OWNERSHIP_FIXTURE:'hold'}});
  holder.unref();
  fs.writeFileSync(process.env.PIPE_HOLDER_MARKER,String(holder.pid));
  return {ok:true};
}`)
			observer := &activityRecorder{}
			function := Function{Runtime: "node", Handler: "pipes.mjs.handler", Timeout: 5 * time.Second,
				Environment: map[string]string{"PIPE_HOLDER_EXE": executable, "PIPE_HOLDER_MARKER": marker}}
			service := newActivityService(t, map[string]Function{"pipes": function}, directory, observer, nil, []time.Duration{0, 0})
			if boundary == "execute" {
				output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "pipes", Payload: []byte(`{}`)})
				if err != nil || output.FunctionError || string(output.Payload) != `{"ok":true}` {
					t.Fatalf("private pipe uncertainty changed valid native result: %#v %v", output, err)
				}
				assertActivityFinished(t, observer, "lambda_invoke", exec.ErrWaitDelay)
			} else {
				admitted, err := service.Admit(context.Background(), InvokeInput{FunctionName: "pipes", Payload: []byte(`{}`)})
				if err != nil {
					t.Fatal(err)
				}
				record := waitAsyncState(t, service, admitted.RequestID, "succeeded")
				if record.Attempts != 1 {
					t.Fatalf("private pipe uncertainty changed native retry policy: %#v", record)
				}
				assertActivityFinished(t, observer, "lambda_async", exec.ErrWaitDelay)
				if err := service.DrainAsync(context.Background()); !errors.Is(err, exec.ErrWaitDelay) {
					t.Fatalf("async pipe uncertainty was lost during drain: %v", err)
				}
			}
			if err := service.Close(context.Background()); !errors.Is(err, exec.ErrWaitDelay) {
				t.Fatalf("strict owner close hid pipe uncertainty: %v", err)
			}
		})
	}
}

func TestDevPipeOwnershipResultChannelTimeoutIsPrivate(t *testing.T) {
	if _, err := exec.LookPath("node"); err != nil {
		t.Fatal("Node is required for trusted pipe ownership proof")
	}
	directory := t.TempDir()
	executable, marker := pipeHolderFixture(t, directory)
	writeFixture(t, directory, "result.mjs", `
import {spawn} from 'node:child_process';
import fs from 'node:fs';
export async function handler() {
  const holder=spawn(process.env.PIPE_HOLDER_EXE,['-test.run=^TestDevPipeOwnershipProcess$'],{
    detached:true,stdio:['ignore','ignore','ignore',3],
    env:{EVENTBUS_PIPE_OWNERSHIP_FIXTURE:'hold',PIPE_RESULT_CHANNEL:'1'}});
  holder.unref();
  fs.writeFileSync(process.env.PIPE_HOLDER_MARKER,String(holder.pid));
  return {ok:true};
}`)
	observer := &activityRecorder{}
	service := newActivityService(t, map[string]Function{"pipes": {Runtime: "node", Handler: "result.mjs.handler", Timeout: 5 * time.Second,
		Environment: map[string]string{"PIPE_HOLDER_EXE": executable, "PIPE_HOLDER_MARKER": marker}}}, directory, observer, nil, nil)
	output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "pipes", Payload: []byte(`{}`)})
	var native struct {
		ErrorType string `json:"errorType"`
	}
	if err != nil || !output.FunctionError || json.Unmarshal(output.Payload, &native) != nil || native.ErrorType != "Runtime.InvalidResponse" {
		t.Fatalf("result-channel deadline changed native failure precedence: %#v %v", output, err)
	}
	assertActivityFinished(t, observer, "lambda_invoke", os.ErrDeadlineExceeded)
	if err := service.Close(context.Background()); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("strict owner close hid result-channel uncertainty: %v", err)
	}
}

func TestDevPipeOwnershipProvidedResponsePrecedesWaitDelay(t *testing.T) {
	directory := t.TempDir()
	executable, marker := pipeHolderFixture(t, directory)
	observer := &activityRecorder{}
	function := Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestDevPipeOwnershipProcess$"}, Timeout: 5 * time.Second,
		Environment: map[string]string{"EVENTBUS_PIPE_OWNERSHIP_FIXTURE": "provided", "PIPE_HOLDER_MARKER": marker}}
	service := newActivityService(t, map[string]Function{"pipes": function}, directory, observer, nil, nil)
	output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "pipes", Payload: []byte(`{}`)})
	if err != nil || output.FunctionError || string(output.Payload) != `{"ok":true}` {
		t.Fatalf("private pipe uncertainty changed native Runtime API response: %#v %v", output, err)
	}
	assertActivityFinished(t, observer, "lambda_invoke", exec.ErrWaitDelay)
	if err := service.Close(context.Background()); !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("strict owner close hid provided pipe uncertainty: %v", err)
	}
}
