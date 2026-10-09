package lambda

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWarmNativeMergedLogsPreserveWriteOrdering(t *testing.T) {
	for _, runtime := range []string{"python", "node"} {
		t.Run(runtime, func(t *testing.T) {
			directory := t.TempDir()
			command := "node"
			filename := "order.mjs"
			source := `import fs from 'node:fs'; export function handler() { fs.writeSync(1,'a'); fs.writeSync(2,'b'); return null; }`
			if runtime == "python" {
				command = "python3"
				filename = "order.py"
				source = "import os\ndef handler(event, context):\n    os.write(1, b'a')\n    os.write(2, b'b')\n    return None\n"
			}
			executable, err := exec.LookPath(command)
			if err != nil {
				t.Skip(err)
			}
			if err := os.WriteFile(filepath.Join(directory, filename), []byte(source), 0600); err != nil {
				t.Fatal(err)
			}
			service, err := NewService(&Config{Functions: map[string]Function{"order": {Runtime: runtime, Command: []string{executable}, Handler: filename + "#handler", Timeout: time.Second}}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, directory)
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close(context.Background())
			for index := 0; index < 3; index++ {
				request := httptest.NewRequest(http.MethodPost, invokePrefix+"order"+invokeSuffix, strings.NewReader(`{}`))
				request.Header.Set("X-Amz-Log-Type", "Tail")
				response := httptest.NewRecorder()
				service.ServeHTTP(response, request)
				logs, err := base64.StdEncoding.DecodeString(response.Header().Get("X-Amz-Log-Result"))
				if err != nil || response.Code != 200 || response.Header().Get("X-Amz-Function-Error") != "" || string(logs) != "ab" {
					t.Fatalf("attempt%d response%d %s logs%q err%v", index, response.Code, response.Body.String(), logs, err)
				}
			}
		})
	}
}
