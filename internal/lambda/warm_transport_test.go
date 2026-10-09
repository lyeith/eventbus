package lambda

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWarmResponseJoinsStalledAdmittedRuntimeBodyAfterDeadline(t *testing.T) {
	directory := t.TempDir()
	function := warmTestFunction(t, "python", directory)
	point, release := filepath.Join(directory, "runtime-point"), filepath.Join(directory, "release-handler")
	source := `import json, os, pathlib, time
def handler(event, context):
    pathlib.Path(os.environ['POINT']).write_text(json.dumps({'address':os.environ['AWS_LAMBDA_RUNTIME_API'],'id':context.aws_request_id}))
    while not pathlib.Path(os.environ['RELEASE']).exists(): time.sleep(0.002)
    return {'ok':True}
`
	if err := os.WriteFile(filepath.Join(directory, "handler.py"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	function.Environment["POINT"], function.Environment["RELEASE"] = point, release
	function.Timeout = 400 * time.Millisecond
	service, err := NewService(&Config{Functions: map[string]Function{"warm": function}, DevWarm: &DevWarmConfig{MaxWorkers: 1}, DevAsync: &DevAsyncConfig{LogWriter: io.Discard}}, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	done := make(chan InvocationOutcome, 1)
	go func() {
		outcome, _ := service.ExecuteObserved(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte(`{}`)}, nil)
		done <- outcome
	}()
	warmWaitFile(t, point)
	data, err := os.ReadFile(point)
	if err != nil {
		t.Fatal(err)
	}
	var selected struct {
		Address string `json:"address"`
		ID      string `json:"id"`
	}
	if err := json.Unmarshal(data, &selected); err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTimeout("tcp", selected.Address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, err = fmt.Fprintf(connection, "POST %sinvocation/%s/response HTTP/1.1\r\nHost: %s\r\nContent-Length: 128\r\nExpect: 100-continue\r\n\r\n", runtimePrefix, selected.ID, selected.Address)
	if err != nil {
		t.Fatal(err)
	}
	// 100 Continue is sent only when the actual admitted handler begins reading
	// this declared body. The parent deliberately retains that TCP transport.
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 100 {
		t.Fatalf("expected admitted body read, got%d", response.StatusCode)
	}
	if _, err := connection.Write([]byte("{")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, []byte("release"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-done:
		if outcome.State != InvocationTimedOut || outcome.CompletionScope != CompletionProcess || !errors.Is(outcome.OwnershipErr, context.DeadlineExceeded) {
			t.Fatalf("stalled transport did not join/fence: %+v", outcome)
		}
	case <-time.After(4 * time.Second):
		connection.Close()
		t.Fatal("response boundary blocked past phase/transport cleanup budgets")
	}
	warmWaitRetired(t, service)
	if err := service.DevEvidence(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retirement fault was erased: %v", err)
	}
	if err := service.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close erased transport uncertainty: %v", err)
	}
}
