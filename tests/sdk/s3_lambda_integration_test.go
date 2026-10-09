//go:build sdksmoke && integration

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

const localstackS3Image = "localstack/localstack:3.8.1"

type sdkS3WireRequest struct {
	method, path, invocationType string
	status                       int
}
type sdkS3StatusWriter struct {
	http.ResponseWriter
	status int
}

func (writer *sdkS3StatusWriter) WriteHeader(status int) {
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}
func (writer *sdkS3StatusWriter) Write(data []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(data)
}

func TestLocalStackS3RegisteredLambdaSDKIntegration(t *testing.T) {
	if os.Getenv("EVENTBUS_LOCALSTACK_S3_INTEGRATION") != "1" {
		t.Skip("set EVENTBUS_LOCALSTACK_S3_INTEGRATION=1 for an owned LocalStack 3.8.1 container")
	}
	require.Equal(t, "linux", runtime.GOOS, "this fixture uses native Linux container host networking")
	python := sdkPython(t)
	engine, err := exec.LookPath("docker")
	require.NoError(t, err)
	containerCommand := func(ctx context.Context, args ...string) ([]byte, error) {
		command := exec.CommandContext(ctx, engine, args...)
		command.WaitDelay = 2 * time.Second
		return command.CombinedOutput()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
	defer cancel()
	_, err = containerCommand(ctx, "image", "inspect", localstackS3Image)
	require.NoError(t, err, "pre-pull the pinned fixture image; the test never pulls or starts a shared stack")
	directory := t.TempDir()
	output, gate := filepath.Join(directory, "effects.jsonl"), filepath.Join(directory, "shutdown-gate")
	asyncPath := filepath.Join(directory, "lambda.jsonl")
	function := lambda.Function{Runtime: "python", Command: []string{python, "-E", "-s"},
		Handler: fixturePath("python", "s3_lambda_smoke.py") + "#handler", Timeout: 20 * time.Second,
		Environment: map[string]string{"S3_LAMBDA_OUTPUT": output, "S3_LAMBDA_GATE": gate}}
	functions, err := lambda.NewService(&lambda.Config{
		Functions: map[string]lambda.Function{"s3-handler": function, "s3-handler:live": function},
		DevAsync:  &lambda.DevAsyncConfig{Workers: 1, Capacity: 16, RetryDelays: []time.Duration{0, 0}, LogPath: asyncPath},
	}, directory)
	require.NoError(t, err)
	var wireMu sync.Mutex
	var wire []sdkS3WireRequest
	native := server.New(server.Services{Lambda: functions})
	serving := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		tracked := &sdkS3StatusWriter{ResponseWriter: writer}
		native.ServeHTTP(tracked, request)
		wireMu.Lock()
		wire = append(wire, sdkS3WireRequest{method: request.Method, path: request.URL.Path,
			invocationType: request.Header.Get("X-Amz-Invocation-Type"), status: tracked.status})
		wireMu.Unlock()
	}))
	t.Cleanup(func() {
		// A failed assertion must also release and join the owned handler.
		_ = os.WriteFile(gate, []byte("release"), 0600)
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer closeCancel()
		drainErr := functions.DrainAsync(closeCtx)
		serving.Close()
		closeErr := functions.Close(closeCtx)
		if drainErr != nil || closeErr != nil {
			t.Errorf("owned Lambda cleanup: drain=%v close=%v", drainErr, closeErr)
		}
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s3Endpoint := "http://" + listener.Addr().String()
	listenAddress := listener.Addr().String()
	require.NoError(t, listener.Close())
	name := "eventbus-s3-" + uuid.NewString()
	label := "io.eventbus.s3-fixture=" + name
	var containerID string
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer closeCancel()
		if containerID == "" {
			// A timed-out start can still have created the exact named
			// container. Confirm our unique ownership label before cleanup.
			found, inspectErr := containerCommand(closeCtx, "container", "inspect", "--format",
				`{{.Id}} {{index .Config.Labels "io.eventbus.s3-fixture"}}`, name)
			if inspectErr != nil {
				if !strings.Contains(string(found), "No such") {
					t.Errorf("owned start cleanup inspection: %v %s", inspectErr, found)
				}
				return
			}
			parts := strings.Fields(string(found))
			if len(parts) != 2 || parts[1] != name {
				t.Errorf("owned start cleanup label differs: %s", found)
				return
			}
			containerID = parts[0]
		}
		logs, logErr := containerCommand(closeCtx, "logs", containerID)
		t.Logf("owned LocalStack %s logs (error=%v):\n%s", containerID, logErr, logs)
		// The returned immutable ID is the only cleanup target; never prune or
		// stop a container discovered through a broad name/filter.
		stopped, stopErr := containerCommand(closeCtx, "stop", "--time", "10", containerID)
		if stopErr != nil {
			t.Errorf("exact owned container stop: %v %s", stopErr, stopped)
		}
		remaining, inspectErr := containerCommand(closeCtx, "container", "inspect", containerID)
		if inspectErr == nil || !strings.Contains(string(remaining), "No such") {
			t.Errorf("owned --rm container remains: err=%v output=%s", inspectErr, remaining)
		}
	})
	started, err := containerCommand(ctx, "run", "--pull=never", "--rm", "-d", "--network", "host",
		"--name", name, "--label", label,
		"-e", "SERVICES=s3,lambda", "-e", "DISTRIBUTED_MODE=1",
		"-e", "GATEWAY_LISTEN="+listenAddress, "-e", "AWS_ENDPOINT_URL="+s3Endpoint,
		"-e", "AWS_ENDPOINT_URL_LAMBDA="+serving.URL,
		"-e", "AWS_EC2_METADATA_DISABLED=true", "-e", "SKIP_SSL_CERT_DOWNLOAD=1",
		"-e", "DNS_ADDRESS=0", "-e", "PERSISTENCE=0", localstackS3Image)
	containerID = strings.TrimSpace(string(started))
	if err != nil {
		containerID = ""
	}
	require.NoError(t, err, "%s", started)
	require.NotEmpty(t, containerID)
	httpClient := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(35 * time.Second)
	for {
		response, requestErr := httpClient.Get(s3Endpoint + "/_localstack/health")
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				break
			}
		}
		require.True(t, time.Now().Before(deadline), "owned LocalStack did not become ready")
		time.Sleep(100 * time.Millisecond)
	}
	env := append(sdkEnvironment(directory, "", "", ""),
		"S3_LAMBDA_ENDPOINT="+serving.URL, "S3_LAMBDA_S3_ENDPOINT="+s3Endpoint,
		"S3_LAMBDA_OUTPUT="+output, "S3_LAMBDA_GATE="+gate)
	proof, err := runSDKProcess(ctx, python, fixturePath("python", "s3_lambda_smoke.py"), env)
	t.Logf("actual S3 SDK/native registered Lambda proof:\n%s", proof)
	require.NoError(t, err)
	drained := make(chan error, 1)
	go func() { drained <- functions.DrainAsync(ctx) }()
	select {
	case drainErr := <-drained:
		t.Fatalf("drain returned before the held actual S3 handler joined: %v", drainErr)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, os.WriteFile(gate, []byte("release"), 0600))
	require.NoError(t, <-drained, "release must join the actual native event handler")
	require.NoError(t, functions.Close(ctx))
	data, err := os.ReadFile(output)
	require.NoError(t, err)
	var shutdownCompleted bool
	pids := make(map[int]bool)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var effect struct {
			Phase string `json:"phase"`
			Key   string `json:"key"`
			PID   int    `json:"pid"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &effect))
		pids[effect.PID] = true
		if effect.Key == "accepted/shutdown.csv" && effect.Phase == "completed" {
			shutdownCompleted = true
		}
	}
	require.True(t, shutdownCompleted)
	for pid := range pids {
		_, statErr := os.Stat(fmt.Sprintf("/proc/%d", pid))
		require.True(t, errors.Is(statErr, os.ErrNotExist), "actual handler child %d must be reaped: %v", pid, statErr)
	}
	wireMu.Lock()
	defer wireMu.Unlock()
	var getFunction, missingDestination, dryRun, events int
	for _, request := range wire {
		switch {
		case request.method == http.MethodGet && request.status == 200:
			getFunction++
		case request.method == http.MethodGet && strings.HasSuffix(request.path, ":function:missing") && request.status == 404:
			missingDestination++
		case request.invocationType == "DryRun" && request.status == 204:
			dryRun++
		case request.invocationType == "Event" && request.status == 202:
			events++
		}
	}
	require.GreaterOrEqual(t, getFunction, 4, "SDK queries and LocalStack normal validation must reach GetFunction")
	require.GreaterOrEqual(t, missingDestination, 1, "unknown destination must fail through LocalStack normal validation")
	require.Equal(t, 1, dryRun, "LocalStack normal destination validation uses native DryRun")
	require.Equal(t, 5, events, "only original matching S3 uploads/completions enter the runtime; retries remain native-owned")
	t.Log("normal metadata/DryRun validation, actual Put/Copy/multipart, exact filter refusal, native retries, joined shutdown and exact fixture cleanup passed")
}
