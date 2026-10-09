//go:build linux

package lambda

import (
	"context"
	"fmt"
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

	"github.com/stretchr/testify/require"
)

func TestDevOutputCopyCancellationKeepsNativeContextError(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "native.pid")
	function := Function{Runtime: "command", Command: []string{"/bin/sh", "-c", "echo \"$$\" > \"$COPY_PID_FILE\"; exec sleep 60"}, Timeout: 5 * time.Second,
		Environment: map[string]string{"COPY_PID_FILE": marker}}
	service := newActivityService(t, map[string]Function{"copy": function}, directory, nil, nil, nil)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	type result struct {
		outcome InvocationOutcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		outcome, err := service.ExecuteObserved(ctx, InvokeInput{FunctionName: "copy", Payload: []byte(`{}`)}, nil)
		done <- result{outcome, err}
	}()
	var pid int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(marker)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil && pid > 0
	}, 3*time.Second, 5*time.Millisecond)
	// The fixture owns one exact writer, independently of the native group.
	// Reaping that group cannot manufacture EOF while this descriptor is open.
	held, err := os.OpenFile(fmt.Sprintf("/proc/%d/fd/1", pid), os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })
	cancel()
	select {
	case completed := <-done:
		require.ErrorIs(t, completed.err, context.Canceled)
		require.Equal(t, InvocationCanceled, completed.outcome.State)
		require.NotEmpty(t, completed.outcome.Metadata.RequestID)
		require.ErrorIs(t, completed.outcome.OwnershipErr, exec.ErrWaitDelay)
	case <-time.After(3 * time.Second):
		t.Fatal("native cancellation did not join its actual output-copy cutoff")
	}
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
	require.NoError(t, held.Close())
	require.ErrorIs(t, service.Close(context.Background()), exec.ErrWaitDelay)
}

// This helper is the real provided runtime, running in the Lambda-owned group.
// The parent test retains a descriptor and explicitly opens the response gate.
func TestDevOutputCopyProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_OUTPUT_COPY_RUNTIME_FIXTURE") != "1" {
		return
	}
	client := &http.Client{Timeout: 2 * time.Second}
	base := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + runtimePrefix
	next, err := client.Get(base + "invocation/next")
	if err != nil {
		os.Exit(2)
	}
	_, err = io.Copy(io.Discard, next.Body)
	_ = next.Body.Close()
	if err != nil || next.StatusCode != http.StatusOK {
		os.Exit(3)
	}
	requestID := next.Header.Get("Lambda-Runtime-Aws-Request-Id")
	if os.WriteFile(os.Getenv("COPY_PID_FILE"), []byte(strconv.Itoa(os.Getpid())), 0600) != nil {
		os.Exit(4)
	}
	deadline := time.Now().Add(4 * time.Second)
	for {
		if _, err := os.Stat(os.Getenv("COPY_GATE_FILE")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			os.Exit(5)
		}
		time.Sleep(5 * time.Millisecond)
	}
	response, err := client.Post(base+"invocation/"+requestID+"/response", "application/json", strings.NewReader(`{"ok":true}`))
	if err != nil {
		os.Exit(6)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		os.Exit(8)
	}
	os.Exit(7)
}

func TestDevOutputCopyProvidedResponseKeepsNativeSuccess(t *testing.T) {
	directory := t.TempDir()
	marker, gate := filepath.Join(directory, "native.pid"), filepath.Join(directory, "response.gate")
	executable, err := os.Executable()
	require.NoError(t, err)
	function := Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestDevOutputCopyProvidedProcess$"}, Timeout: 5 * time.Second,
		Environment: map[string]string{"EVENTBUS_OUTPUT_COPY_RUNTIME_FIXTURE": "1", "COPY_PID_FILE": marker, "COPY_GATE_FILE": gate}}
	service := newActivityService(t, map[string]Function{"copy": function}, directory, nil, nil, nil)
	type result struct {
		outcome InvocationOutcome
		err     error
	}
	done := make(chan result, 1)
	go func() {
		outcome, err := service.ExecuteObserved(t.Context(), InvokeInput{FunctionName: "copy", Payload: []byte(`{}`)}, nil)
		done <- result{outcome, err}
	}()
	var pid int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(marker)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil && pid > 0
	}, 3*time.Second, 5*time.Millisecond)
	held, err := os.OpenFile(fmt.Sprintf("/proc/%d/fd/1", pid), os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })
	require.NoError(t, os.WriteFile(gate, nil, 0600))
	select {
	case completed := <-done:
		require.NoError(t, completed.err)
		require.Equal(t, InvocationSucceeded, completed.outcome.State)
		require.False(t, completed.outcome.Output.FunctionError)
		require.Equal(t, `{"ok":true}`, string(completed.outcome.Output.Payload))
		require.ErrorIs(t, completed.outcome.OwnershipErr, exec.ErrWaitDelay, "valid native response wins while independent output uncertainty remains private")
	case <-time.After(3 * time.Second):
		t.Fatal("provided response did not join its actual output-copy cutoff")
	}
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH)
	require.NoError(t, held.Close())
	require.ErrorIs(t, service.Close(context.Background()), exec.ErrWaitDelay)
}
