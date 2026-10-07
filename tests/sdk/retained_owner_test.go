//go:build sdksmoke

package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

type retainedSDKCapture struct {
	mu      sync.Mutex
	data    bytes.Buffer
	entered chan struct{}
	release chan struct{}
	blocked atomic.Bool
	once    sync.Once
}

func (writer *retainedSDKCapture) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte(`"operation":"Publish"`)) && bytes.Contains(data, []byte("retained-blocked")) && writer.blocked.CompareAndSwap(false, true) {
		close(writer.entered)
		<-writer.release // before writing: the second publisher has no capture yet
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.data.Write(data)
}
func (writer *retainedSDKCapture) unblock() { writer.once.Do(func() { close(writer.release) }) }
func (writer *retainedSDKCapture) length() int {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.data.Len()
}

type retainedSDKRow struct {
	Stage     string `json:"stage"`
	Chain     string `json:"chain"`
	Attempt   int    `json:"attempt"`
	PID       int    `json:"pid"`
	Owner     string `json:"owner"`
	RequestID string `json:"request_id"`
}

type retainedSDKFixture struct {
	owner       string
	root        string
	source      *httptest.Server
	callback    *httptest.Server
	control     *httptest.Server
	coordinator *devquiescence.Coordinator
	functions   *lambda.Service
	capture     *retainedSDKCapture
}

func newRetainedSDKFixture(t *testing.T, python, owner string) *retainedSDKFixture {
	t.Helper()
	directory := t.TempDir()
	root := filepath.Join(directory, "evidence")
	require.NoError(t, os.Mkdir(root, 0700))
	writer := &retainedSDKCapture{entered: make(chan struct{}), release: make(chan struct{})}
	capture := messaging.NewSNSCapture(writer)
	fixture := &retainedSDKFixture{owner: owner, root: root, source: httptest.NewUnstartedServer(nil), callback: httptest.NewUnstartedServer(nil), control: httptest.NewUnstartedServer(nil), coordinator: devquiescence.New(capture.Err), capture: writer}
	callbackURL := "http://" + fixture.callback.Listener.Addr().String()
	controlURL := "http://" + fixture.control.Listener.Addr().String()
	started := make(chan struct{})
	var startedOnce sync.Once
	control := devquiescence.NewHandler(fixture.coordinator)
	fixture.control.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__fixture/nested-started" && r.Method == http.MethodPost {
			var row retainedSDKRow
			if json.NewDecoder(r.Body).Decode(&row) != nil {
				http.Error(w, "invalid owned observation", http.StatusBadRequest)
				return
			}
			if row.Chain == "lost" && row.Attempt == 1 {
				startedOnce.Do(func() { close(started) })
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		control.ServeHTTP(w, r)
	})
	fixture.control.Start()
	function := lambda.Function{Runtime: "python", Command: []string{python, "-E", "-s"}, Handler: fixturePath("python", "retained_owner_smoke.py") + "#root_handler", Timeout: 12 * time.Second,
		Environment: map[string]string{"RETAINED_ROOT": root, "RETAINED_OWNER": owner, "RETAINED_CALLBACK_ENDPOINT": callbackURL, "RETAINED_CONTROL_URL": controlURL}}
	nested := function
	nested.Handler = fixturePath("python", "retained_owner_smoke.py") + "#nested_handler"
	functions, err := lambda.NewService(&lambda.Config{Functions: map[string]lambda.Function{"retained-root:live": function, "retained-nested:live": nested}, DevActivity: fixture.coordinator,
		DevAsync: &lambda.DevAsyncConfig{Workers: 1, Capacity: 4, RetryDelays: []time.Duration{4 * time.Second, 4 * time.Second}, LogPath: filepath.Join(directory, "async.jsonl")}}, directory)
	require.NoError(t, err)
	fixture.functions = functions
	broker := messaging.NewBroker("us-east-1", "000000000000", fixture.source.Listener.Addr().(*net.TCPAddr).Port)
	broker.SetSNSCapture(capture)
	broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions: functions})
	aws := server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions})
	// This fixture's held callback allowlist delegates only pure native cleanup.
	// Producing operations never reach the native router while held.
	cleanup := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		if target == "AmazonSQS.DeleteQueue" {
			aws.ServeHTTP(w, r)
			return
		}
		if target == "" && r.ParseForm() == nil && r.FormValue("Action") == "DeleteTopic" && r.FormValue("Version") == "2010-03-31" {
			aws.ServeHTTP(w, r)
			return
		}
		devquiescence.WriteAdmissionError(w, r, devquiescence.ErrFenced)
	})
	var sourceDropped, nestedDropped atomic.Bool
	sourceWork := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The outer Source envelope is already counted before this transport
		// fixture parses anything. A lost reply does not cancel async admission.
		if r.Header.Get("X-Amz-Target") == "" && r.ParseForm() == nil && r.FormValue("Action") == "Publish" {
			var input struct {
				DropSource bool `json:"drop_source"`
			}
			_ = json.Unmarshal([]byte(r.FormValue("Message")), &input)
			if input.DropSource && sourceDropped.CompareAndSwap(false, true) {
				recorder := httptest.NewRecorder()
				aws.ServeHTTP(recorder, r)
				connection, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = connection.Close()
				}
				return
			}
		}
		aws.ServeHTTP(w, r)
	})
	callbackWork := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if owner == "A" && strings.Contains(r.URL.Path, "/functions/retained-nested:live/invocations") && nestedDropped.CompareAndSwap(false, true) {
			// A controlled transport fault simulates an independently accepted
			// peer: the native router retains its own context and actual child,
			// while the root's SDK connection closes after observed admission.
			// No execution, event, retry or settlement policy is replaced.
			finished, observerDone := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(observerDone)
				select {
				case <-started:
					connection, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						_ = connection.Close()
					}
				case <-finished:
				}
			}()
			aws.ServeHTTP(httptest.NewRecorder(), r.WithContext(context.WithoutCancel(r.Context())))
			close(finished)
			<-observerDone // transport observer is owned by this envelope too
			return
		}
		aws.ServeHTTP(w, r)
	})
	fixture.source.Config.Handler = fixture.coordinator.Wrap(devquiescence.Source, sourceWork, nil)
	fixture.callback.Config.Handler = fixture.coordinator.Wrap(devquiescence.Callback, callbackWork, cleanup)
	fixture.source.Start()
	fixture.callback.Start()
	t.Cleanup(func() {
		writer.unblock()
		for _, gate := range []string{"release-lost-1", "release-lost-2", "release-shutdown-root", "release-shutdown-1"} {
			_ = os.WriteFile(filepath.Join(root, gate), nil, 0600)
		}
		fixture.coordinator.Shutdown()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, barrierErr := fixture.coordinator.Quiesce(ctx)
		drainErr := functions.DrainAsync(ctx)
		fixture.source.Close()
		fixture.callback.Close()
		fixture.control.Close()
		closeErr := functions.Close(ctx)
		captureErr := capture.Close()
		if drainErr != nil || barrierErr != nil || closeErr != nil || captureErr != nil {
			t.Errorf("retained SDK owned cleanup: drain=%v barrier=%v runtime=%v capture=%v", drainErr, barrierErr, closeErr, captureErr)
		}
	})
	return fixture
}

func retainedSDKRows(t *testing.T, fixture *retainedSDKFixture, chain, stage string) []retainedSDKRow {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(fixture.root, "row-*.json"))
	require.NoError(t, err)
	var rows []retainedSDKRow
	for _, path := range files {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		var row retainedSDKRow
		require.NoError(t, json.Unmarshal(data, &row))
		if row.Chain == chain && (stage == "" || row.Stage == stage) {
			rows = append(rows, row)
		}
	}
	return rows
}
func retainedSDKAlive(pid int) bool { return syscall.Kill(pid, 0) == nil }
func runRetainedSDKPhase(t *testing.T, fixture *retainedSDKFixture, python, phase, chain string) {
	t.Helper()
	env := append(sdkEnvironment(t.TempDir(), "", "", ""), "RETAINED_ROOT="+fixture.root, "RETAINED_OWNER="+fixture.owner,
		"RETAINED_SOURCE_ENDPOINT="+fixture.source.URL, "RETAINED_CALLBACK_ENDPOINT="+fixture.callback.URL,
		"RETAINED_CONTROL_URL="+fixture.control.URL, "RETAINED_PHASE="+phase, "RETAINED_CHAIN="+chain)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "retained_owner_smoke.py"), env)
	t.Logf("actual retained-owner SDK origin %s/%s/%s (exited before barrier):\n%s", fixture.owner, phase, chain, output)
	require.NoError(t, err)
}
func retainedSDKControl(t *testing.T, fixture *retainedSDKFixture, operation string, input any, status int) devquiescence.Snapshot {
	t.Helper()
	var body []byte
	var err error
	method := http.MethodGet
	if input != nil {
		body, err = json.Marshal(input)
		require.NoError(t, err)
		method = http.MethodPost
	}
	request, err := http.NewRequest(method, fixture.control.URL+devquiescence.ControlPath+operation, bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 8 * time.Second}
	defer client.CloseIdleConnections()
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, status, response.StatusCode, "%s", data)
	var snapshot devquiescence.Snapshot
	require.NoError(t, json.Unmarshal(data, &snapshot))
	require.Equal(t, "eventbus.retained-owner.v1", snapshot.SchemaVersion)
	require.NotContains(t, string(data), "private input must not appear")
	return snapshot
}
func retainedSDKRelease(t *testing.T, fixture *retainedSDKFixture, gate string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(fixture.root, gate), nil, 0600))
}

func TestRetainedOwnerPythonSDKBarrier(t *testing.T) {
	python := sdkPython(t)
	first, peer := newRetainedSDKFixture(t, python, "A"), newRetainedSDKFixture(t, python, "B")
	// Two received HTTP publications remain live, including one hidden behind
	// topic publication serialization before any capture or child exists.
	runRetainedSDKPhase(t, first, python, "blocked-publications", "")
	select {
	case <-first.capture.entered:
	case <-time.After(time.Second):
		t.Fatal("actual source Publish did not enter the borrowed capture gate")
	}
	require.Eventually(t, func() bool { return first.coordinator.Snapshot().WorkCount >= 2 }, time.Second, time.Millisecond)
	require.Zero(t, first.capture.length(), "second publication has no evidence yet; HTTP envelope ownership must cover it")
	dirty := retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.Equal(t, devquiescence.Draining, dirty.State)
	require.False(t, dirty.FixtureSafe)
	require.GreaterOrEqual(t, dirty.WorkCount, 2)
	first.capture.unblock()
	held := retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 5000}, http.StatusOK)
	require.True(t, held.FixtureSafe)
	runRetainedSDKPhase(t, first, python, "cleanup-blocked", "")
	opened := retainedSDKControl(t, first, "/resume", map[string]uint64{"generation": held.Generation}, http.StatusOK)
	require.Equal(t, devquiescence.Open, opened.State)
	require.Equal(t, held.Generation+1, opened.Generation)

	// The originating SDK process loses its Publish reply and exits. A nested
	// native Invoke independently remains live after the root loses that reply,
	// fails, joins its own child and enters the actual async retry interval.
	runRetainedSDKPhase(t, first, python, "origin", "lost")
	require.Eventually(t, func() bool {
		roots := retainedSDKRows(t, first, "lost", "root-failed")
		nested := retainedSDKRows(t, first, "lost", "nested-started")
		return len(roots) == 1 && len(nested) == 1 && !retainedSDKAlive(roots[0].PID) && retainedSDKAlive(nested[0].PID)
	}, 3*time.Second, 10*time.Millisecond, "lost nested reply must leave an independently accepted actual child")
	dirty = retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.False(t, dirty.FixtureSafe)
	require.Greater(t, dirty.WorkCount, 0)
	retainedSDKRelease(t, first, "release-lost-1")
	require.Eventually(t, func() bool {
		rows := retainedSDKRows(t, first, "lost", "")
		if len(retainedSDKRows(t, first, "lost", "nested-completed")) != 1 {
			return false
		}
		for _, row := range rows {
			if retainedSDKAlive(row.PID) {
				return false
			}
		}
		for _, record := range first.functions.AsyncSnapshot() {
			if record.State == "retrying" && record.Attempts == 1 {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "retry ownership must survive a real interval with no child PID")
	dirty = retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.False(t, dirty.FixtureSafe)
	require.Greater(t, dirty.WorkCount, 0)
	require.Eventually(t, func() bool { return len(retainedSDKRows(t, first, "lost", "nested-started")) == 2 }, 5*time.Second, 10*time.Millisecond)
	retainedSDKRelease(t, first, "release-lost-2")
	held = retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 5000}, http.StatusOK)
	require.Equal(t, devquiescence.Held, held.State)
	require.True(t, held.FixtureSafe)
	require.Zero(t, held.WorkCount)
	for _, row := range retainedSDKRows(t, first, "lost", "") {
		require.False(t, retainedSDKAlive(row.PID), "fixture-safe must follow actual root/nested child join")
	}
	require.Len(t, retainedSDKRows(t, first, "lost", "root-completed"), 1)
	require.Len(t, retainedSDKRows(t, first, "lost", "nested-completed"), 2)
	runRetainedSDKPhase(t, first, python, "held-cleanup", "lost")
	retainedSDKControl(t, first, "/resume", map[string]uint64{"generation": held.Generation - 1}, http.StatusConflict)
	retainedSDKControl(t, first, "/resume", map[string]uint64{"generation": held.Generation}, http.StatusOK)
	runRetainedSDKPhase(t, first, python, "origin", "next")
	held = retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 5000}, http.StatusOK)
	require.True(t, held.FixtureSafe)
	runRetainedSDKPhase(t, first, python, "held-cleanup", "next")
	retainedSDKControl(t, first, "/resume", map[string]uint64{"generation": held.Generation}, http.StatusOK)

	// Shutdown is terminal, but an accepted root may still admit a new async
	// descendant. Join shared ownership before permanently draining Lambda.
	// The other owner retains its native sources throughout this callback chain.
	runRetainedSDKPhase(t, first, python, "origin", "shutdown")
	require.Eventually(t, func() bool { return len(retainedSDKRows(t, first, "shutdown", "root-started")) == 1 }, 3*time.Second, 10*time.Millisecond)
	require.Empty(t, retainedSDKRows(t, first, "shutdown", "nested-started"))
	shutdown := first.coordinator.Shutdown()
	require.Equal(t, devquiescence.Shutdown, shutdown.State)
	require.False(t, shutdown.FixtureSafe)
	retainedSDKControl(t, first, "/resume", map[string]uint64{"generation": shutdown.Generation}, http.StatusConflict)
	dirty = retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.Equal(t, devquiescence.Shutdown, dirty.State)
	require.False(t, dirty.FixtureSafe)
	runRetainedSDKPhase(t, peer, python, "origin", "peer")
	require.Eventually(t, func() bool {
		return peer.coordinator.Snapshot().WorkCount == 0 && len(retainedSDKRows(t, peer, "peer", "root-completed")) == 1
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, devquiescence.Open, peer.coordinator.Snapshot().State)
	retainedSDKRelease(t, first, "release-shutdown-root")
	require.Eventually(t, func() bool {
		roots := retainedSDKRows(t, first, "shutdown", "root-descendant-admitted")
		nested := retainedSDKRows(t, first, "shutdown", "nested-started")
		return len(roots) == 1 && len(nested) == 1 && !retainedSDKAlive(roots[0].PID) && retainedSDKAlive(nested[0].PID)
	}, 3*time.Second, 10*time.Millisecond, "accepted root must admit a new native Event descendant after shutdown fencing")
	dirty = retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 100}, http.StatusRequestTimeout)
	require.Equal(t, devquiescence.Shutdown, dirty.State)
	require.False(t, dirty.FixtureSafe)
	require.Greater(t, dirty.WorkCount, 0)
	retainedSDKRelease(t, first, "release-shutdown-1")
	joined := retainedSDKControl(t, first, "/quiesce", map[string]int{"timeout_ms": 5000}, http.StatusOK)
	require.Equal(t, devquiescence.Shutdown, joined.State)
	require.False(t, joined.FixtureSafe, "shutdown never grants fixture cleanup or resume")
	require.Zero(t, joined.WorkCount)
	for _, row := range retainedSDKRows(t, first, "shutdown", "") {
		require.False(t, retainedSDKAlive(row.PID))
	}
	require.Len(t, retainedSDKRows(t, first, "shutdown", "root-completed"), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	require.NoError(t, first.functions.DrainAsync(ctx))
}
