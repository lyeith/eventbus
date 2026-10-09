package lambda

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWarmPythonReloadReadsSameTimestampSameSizePrimarySource(t *testing.T) {
	service, _, directory := warmTestService(t, "python", 1)
	warmTestExecute(t, service, `{}`)
	filename := filepath.Join(directory, "handler.py")
	info, err := os.Stat(filename)
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Replace(warmPythonHandler, "actual handler failure", "edited handler failure", 1)
	if len(source) != len(warmPythonHandler) {
		t.Fatal("fixture must preserve bytecode size key")
	}
	if err := os.WriteFile(filename, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filename, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	if err := service.ReloadFunction(context.Background(), "warm"); err != nil {
		t.Fatal(err)
	}
	out, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(`{"fail":true}`)})
	if err != nil || !out.FunctionError || !strings.Contains(string(out.Payload), "edited handler failure") {
		t.Fatalf("reload read stale primary bytecode: %s %v", out.Payload, err)
	}
}

func TestWarmPythonPackageReexportInitializesPrimaryOnce(t *testing.T) {
	directory := t.TempDir()
	function := warmTestFunction(t, "python", directory)
	packageDir := filepath.Join(directory, "application")
	if err := os.Mkdir(packageDir, 0700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"__init__.py": "from .entry import handler\n",
		"helper.py":   "VALUE = 'sibling'\n",
		"entry.py":    "from pathlib import Path\nfrom .helper import VALUE\nimport os\nwith open(os.environ['INIT_MARKER'], 'a') as f: f.write('initialized\\n')\ndef handler(event, context):\n    import application.entry\n    return {'same': application.entry.handler is handler, 'value': VALUE}\n",
	}
	for name, source := range files {
		if err := os.WriteFile(filepath.Join(packageDir, name), []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	marker := filepath.Join(directory, "initialized")
	function.Handler = "application.entry.handler"
	function.Environment["INIT_MARKER"] = marker
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	for index := 0; index < 2; index++ {
		body := warmTestExecute(t, service, `{}`)
		if body["same"] != true || body["value"] != "sibling" {
			t.Fatalf("package metadata/re-export changed %v", body)
		}
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "initialized\n" {
		t.Fatalf("primary initialized more than once %q", data)
	}
}
