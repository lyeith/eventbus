//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestWarmWorkerRetirementJoinsActualDescendants(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node unavailable")
	}
	directory := t.TempDir()
	source := `import {spawn} from 'node:child_process'; export function handler() { const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)'],{stdio:'inherit'}); return {child:child.pid}; }`
	if err := os.WriteFile(filepath.Join(directory, "children.mjs"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(&Config{Functions: map[string]Function{"children": {Runtime: "node", Command: []string{node}, Handler: "children.mjs#handler", Timeout: time.Second}}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	out, err := service.Execute(context.Background(), InvokeInput{FunctionName: "children", Payload: []byte(`{}`)})
	if err != nil || out.FunctionError {
		t.Fatalf("%s %v", out.Payload, err)
	}
	var result struct {
		Child int `json:"child"`
	}
	if err := json.Unmarshal(out.Payload, &result); err != nil {
		t.Fatal(err)
	}
	if !processAlive(result.Child) {
		t.Fatal("fixture did not create retained descendant")
	}
	if err := service.DevBeginWarmDrain(); err != nil {
		t.Fatal(err)
	}
	warmWaitRetired(t, service)
	deadline := time.Now().Add(time.Second)
	for processAlive(result.Child) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if processAlive(result.Child) {
		t.Fatalf("worker descendant %d survived retirement", result.Child)
	}
}
