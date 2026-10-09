package lambda

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWarmCanceledAdmissionDoesNotLaunchOrImport(t *testing.T) {
	directory := t.TempDir()
	function := warmTestFunction(t, "python", directory)
	marker := filepath.Join(directory, "must-not-import")
	source := "from pathlib import Path\nimport os\nPath(os.environ['MARKER']).write_text('imported')\ndef handler(event, context): return None\n"
	if err := os.WriteFile(filepath.Join(directory, "handler.py"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	function.Environment["MARKER"] = marker
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "warm", Payload: []byte(`{}`)}, func(InvocationMetadata) error { cancel(); return nil })
	if !errors.Is(err, context.Canceled) || outcome.State != InvocationCanceled || outcome.CompletionScope != CompletionProcess || outcome.OwnershipErr != nil {
		t.Fatalf("canceled admission %+v %v", outcome, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("canceled attempt imported application: %v", err)
	}
	warmWaitRetired(t, service)
}
