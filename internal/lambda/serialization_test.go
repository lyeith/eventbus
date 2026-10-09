package lambda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Serialization can execute application code (dict.items / toJSON). It must
// produce the bytes once before they cross the selected result transport.
func TestManagedResultSerializationRunsOnce(t *testing.T) {
	for _, language := range []string{"python", "node"} {
		for _, warm := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/warm_%t", language, warm), func(t *testing.T) {
				dir := t.TempDir()
				path := filepath.Join(dir, "serialization-calls")
				name, source := "result.mjs", `import fs from 'node:fs';
export function handler(event, context) {
  let calls = 0;
  return {toJSON(key) {
    calls++;
    fs.appendFileSync(event.path, String(calls) + '\n');
    if (calls !== 1) throw new Error('result encoded twice');
    return {calls, key, text:'é', id:context.awsRequestId};
  }};
}
`
				command := []string{"node"}
				var environment map[string]string
				if language == "python" {
					command, environment = pythonCommand(t)
					name, source = "result.py", `import os
def handler(event, context):
    class Result(dict):
        def items(self):
            with open(event['path'], 'a') as f:
                f.write('1\n')
            if getattr(self, 'encoded', False):
                raise ValueError('result encoded twice')
            self.encoded = True
            return super().items()
    return Result(text='é', id=context.aws_request_id, calls=1)
`
				} else if _, err := exec.LookPath("node"); err != nil {
					t.Skip(err)
				}
				writeFixture(t, dir, name, source)
				config := &Config{Functions: map[string]Function{"reply": {Runtime: language, Command: command, Environment: environment, Handler: name + "#handler", Timeout: 5 * time.Second}}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}
				if warm {
					config.DevWarm = &DevWarmConfig{MaxWorkers: 1}
				}
				service, err := NewService(config, dir)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := service.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				for index := 0; index < 2; index++ {
					out, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "reply", Payload: []byte(fmt.Sprintf("{\"path\":%q}", path))}, nil)
					if err != nil || out.State != InvocationSucceeded || out.Output.FunctionError || out.OwnershipErr != nil {
						t.Fatalf("native completion %+v err=%v", out, err)
					}
					var value struct {
						Calls         int
						Key, Text, ID string
					}
					if err := json.Unmarshal(out.Output.Payload, &value); err != nil {
						t.Fatal(err)
					}
					wantKey := ""
					if language == "node" && !warm {
						wantKey = "result" // Preserve the cold private-envelope toJSON key.
					}
					if value.Calls != 1 || value.Text != "é" || value.Key != wantKey || value.ID != out.Metadata.RequestID {
						t.Fatalf("serialized value %+v", value)
					}
				}
				calls, err := os.ReadFile(path)
				if err != nil || string(calls) != "1\n1\n" {
					t.Fatalf("application serialization calls %q err=%v", calls, err)
				}
			})
		}
	}
}

func TestWarmSerializationFailuresRemainNativeFunctionErrors(t *testing.T) {
	for _, language := range []string{"python", "node"} {
		t.Run(language, func(t *testing.T) {
			dir := t.TempDir()
			name, source := "error.mjs", `export async function handler(event) {
  if (event.mode === 'cycle') { const result = {}; result.self=result; return result; }
  if (event.mode === 'throw') return {toJSON(){throw new TypeError('serialization failure');}};
  if (event.mode === 'bigint') return 1n;
  if (event.mode === 'undefined') return undefined;
  if (event.mode === 'toJSON_undefined') return {toJSON(){return undefined;}};
  return {ok:true};
}`
			command := []string{"node"}
			var environment map[string]string
			cases := []struct{ mode, kind string }{{"cycle", "TypeError"}, {"throw", "TypeError"}, {"bigint", "TypeError"}, {"toJSON_undefined", "Runtime.InvalidResponse"}}
			if language == "python" {
				command, environment = pythonCommand(t)
				name, source = "error.py", `def handler(event, context):
    if event['mode'] == 'cycle':
        result = {}
        result['self'] = result
        return result
    if event['mode'] == 'nan':
        return float('nan')
    if event['mode'] == 'unsupported':
        return object()
    return {'ok':True}
`
				cases = []struct{ mode, kind string }{{"cycle", "ValueError"}, {"nan", "ValueError"}, {"unsupported", "TypeError"}}
			} else if _, err := exec.LookPath("node"); err != nil {
				t.Skip(err)
			}
			writeFixture(t, dir, name, source)
			service, err := NewService(&Config{Functions: map[string]Function{"reply": {Runtime: language, Command: command, Environment: environment, Handler: name + "#handler", Timeout: 5 * time.Second}}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := service.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			for _, test := range cases {
				output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "reply", Payload: []byte(fmt.Sprintf("{\"mode\":%q}", test.mode))})
				if err != nil || !output.FunctionError || !strings.Contains(string(output.Payload), fmt.Sprintf("\"errorType\":%q", test.kind)) {
					t.Fatalf("%s: payload=%s error=%v", test.mode, output.Payload, err)
				}
				output, err = service.Execute(context.Background(), InvokeInput{FunctionName: "reply", Payload: []byte(`{"mode":"ok"}`)})
				if err != nil || output.FunctionError || string(output.Payload) != `{"ok":true}` {
					t.Fatalf("healthy worker after %s: %s err=%v", test.mode, output.Payload, err)
				}
			}
			if language == "node" {
				output, err := service.Execute(context.Background(), InvokeInput{FunctionName: "reply", Payload: []byte(`{"mode":"undefined"}`)})
				if err != nil || output.FunctionError || string(output.Payload) != "null" {
					t.Fatalf("undefined result: %s err=%v", output.Payload, err)
				}
			}
		})
	}
}
