//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"testing"
	"time"
)

func TestDevOutputCopyExitErrorKeepsNativeFailurePolicy(t *testing.T) {
	for _, runtime := range []string{"command", "node"} {
		t.Run(runtime, func(t *testing.T) {
			directory := t.TempDir()
			function := Function{Runtime: "command", Command: []string{"/bin/sh", "-c", "sleep 60 & exit 7"}, Timeout: 5 * time.Second}
			if runtime == "node" {
				if _, err := exec.LookPath("node"); err != nil {
					t.Fatal("Node is required for managed native copy ownership")
				}
				writeFixture(t, directory, "copy.mjs", `import {spawn} from 'node:child_process';
export async function handler(){
 const child=spawn(process.execPath,['-e','setInterval(()=>{},1000);setTimeout(()=>process.exit(0),5000)'],{stdio:'inherit'});
 child.unref(); process.exit(7);
}`)
				function = Function{Runtime: "node", Handler: "copy.mjs.handler", Timeout: 5 * time.Second}
			}
			observer := &activityRecorder{}
			service := newActivityService(t, map[string]Function{"copy": function}, directory, observer, nil, nil)
			output, err := service.Execute(t.Context(), InvokeInput{FunctionName: "copy", Payload: []byte(`{}`)})
			var failure struct {
				ErrorType string `json:"errorType"`
			}
			if err != nil || !output.FunctionError || json.Unmarshal(output.Payload, &failure) != nil || failure.ErrorType != "Runtime.ExitError" {
				t.Fatalf("private copy uncertainty changed native exit failure: %#v %v", output, err)
			}
			assertActivityFinished(t, observer, "lambda_invoke", exec.ErrWaitDelay)
			if err := service.Close(context.Background()); !errors.Is(err, exec.ErrWaitDelay) {
				t.Fatalf("strict service close lost masked copy uncertainty: %v", err)
			}
		})
	}
}

func TestDevOutputCopyExitErrorKeepsNativeAsyncRetries(t *testing.T) {
	service := newActivityService(t, map[string]Function{"copy": {Runtime: "command", Command: []string{"/bin/sh", "-c", "sleep 60 & exit 7"}, Timeout: 5 * time.Second}}, t.TempDir(), nil, nil, []time.Duration{0, 0})
	admission, err := service.Admit(t.Context(), InvokeInput{FunctionName: "copy", Payload: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	record := waitAsyncState(t, service, admission.RequestID, "failed")
	if record.Attempts != 3 || record.ErrorType != "Runtime.ExitError" {
		t.Fatalf("private copy uncertainty changed native retries: %#v", record)
	}
	if err := service.DrainAsync(t.Context()); !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("async owner drain lost masked copy evidence: %v", err)
	}
	if err := service.Close(context.Background()); !errors.Is(err, exec.ErrWaitDelay) {
		t.Fatalf("service close lost async copy evidence: %v", err)
	}
}
