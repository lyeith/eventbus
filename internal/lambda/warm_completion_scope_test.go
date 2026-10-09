package lambda

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWarmCompletionScopeDescribesActualRetirementAndDiagnosticBoundary(t *testing.T) {
	directory := t.TempDir()
	function := warmTestFunction(t, "python", directory)
	logPath := filepath.Join(directory, "private.jsonl")
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}, DevDiagnostics: &DevDiagnosticsConfig{LogPath: logPath}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	for index, test := range []struct {
		event string
		scope CompletionScope
		mode  string
	}{{`{}`, CompletionInvocation, "initial"}, {`{}`, CompletionInvocation, "warm"}, {`{"fail":true}`, CompletionProcess, "warm"}, {`{}`, CompletionProcess, "initial"}} {
		if index == 3 {
			service.DevBeginWarmDrain()
			warmWaitRetired(t, service)
		}
		outcome, err := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(test.event)}, nil)
		if err != nil || outcome.CompletionScope != test.scope || outcome.OwnershipErr != nil {
			t.Fatalf("outcome%d %+v %v", index, outcome, err)
		}
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	want := []CompletionScope{CompletionInvocation, CompletionInvocation, CompletionProcess, CompletionProcess}
	modes := []string{"initial", "warm", "warm", "initial"}
	for index, scope := range want {
		var record invocationDiagnosticRecord
		if err := decoder.Decode(&record); err != nil {
			t.Fatal(err)
		}
		if record.CompletionScope != scope || !record.OwnershipConfirmed || len(record.ExecutionPhases) != 1 || record.ExecutionPhases[0].CompletionScope != scope || record.ExecutionPhases[0].Mode != modes[index] {
			t.Fatalf("record%d %+v", index, record)
		}
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		t.Fatalf("unexpected extra evidence %v %v", extra, err)
	}
}
