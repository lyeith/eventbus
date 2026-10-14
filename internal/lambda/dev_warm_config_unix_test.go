//go:build linux || darwin

package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func requireWarmControlError(t *testing.T, err error, code string) {
	t.Helper()
	var control *DevWarmControlError
	if !errors.As(err, &control) || control.Code != code {
		t.Fatalf("expected %s, got %v", code, err)
	}
}

func TestDevWarmUpdateRefusalsLeaveExistingPoolUntouched(t *testing.T) {
	for _, config := range []DevWarmConfig{{}, {0, 1}, {1, 0}, {-1, 1}, {33, 1}, {1, -1}, {1, 1025}} {
		service := &Service{warm: newWarmWorkers(2, 4)}
		_, err := service.DevUpdateWarm(context.Background(), config)
		requireWarmControlError(t, err, "invalid_warm_limits")
		if service.warm.idleLimit != 2 || service.warm.executionLimit != 4 {
			t.Fatalf("invalid input changed limits: %+v", config)
		}
	}
	for _, test := range []struct {
		name      string
		configure func(*Service)
	}{
		{"service-closed", func(service *Service) { service.closed = true }},
		{"pool-closed", func(service *Service) { service.warm.closed = true }},
		{"uncertain-retirement", func(service *Service) { service.warm.closeErr = errors.New("secret-runtime-path") }},
		{"held-lifecycle", func(service *Service) { service.devActivity = &activityRecorder{reject: true} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &Service{warm: newWarmWorkers(2, 4)}
			test.configure(service)
			_, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{1, 1})
			requireWarmControlError(t, err, "warm_unavailable")
			if service.warm.idleLimit != 2 || service.warm.executionLimit != 4 {
				t.Fatal("refused control changed limits")
			}
		})
	}
	// Cancellation that wins against publication also retains the prior limits.
	service := &Service{warm: newWarmWorkers(2, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	observed := &observedAdmissionContext{Context: ctx, firstCheck: make(chan struct{})}
	done := make(chan error, 1)
	service.mu.Lock()
	go func() { _, err := service.DevUpdateWarm(observed, DevWarmConfig{1, 1}); done <- err }()
	<-observed.firstCheck
	cancel()
	service.mu.Unlock()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled publication: %v", err)
	}
	if service.warm.idleLimit != 2 || service.warm.executionLimit != 4 {
		t.Fatal("canceled control changed limits")
	}
}

func TestDevWarmUpdateCanceledWaitRetainsOwnerAndPublishedLimits(t *testing.T) {
	pool := newWarmWorkers(3, 2)
	var idle []*warmWorker
	for index := 0; index < 3; index++ {
		worker, err := pool.acquire(context.Background(), executableFunction{generation: &functionGeneration{}}, true)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.release(worker, true); err != nil {
			t.Fatal(err)
		}
		idle = append(idle, worker)
	}
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	idle[0].release = func(error) { close(entered); <-proceed }
	activity := &warmTestActivity{counts: make(map[string]int)}
	service := &Service{warm: pool, devActivity: activity}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		limits, err := service.DevUpdateWarm(ctx, DevWarmConfig{1, 1})
		if limits != (DevWarmConfig{1, 1}) {
			done <- errors.New("published limits missing")
			return
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("idle retirement did not reach owned join")
	}
	pool.mu.Lock()
	if pool.idleLimit != 1 || pool.executionLimit != 1 || pool.workers[idle[0]].phase != warmRetiringIdle {
		pool.mu.Unlock()
		t.Fatal("budgets/retirement not published")
	}
	pool.mu.Unlock()
	if activity.count("lambda_warm_update") != 1 {
		t.Fatal("wait did not retain update activity")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait: %v", err)
	}
	if activity.count("lambda_warm_update") != 0 {
		t.Fatal("canceled waiter retained its own activity")
	}
	select {
	case <-idle[0].retired:
		t.Fatal("canceled wait prematurely released retirement")
	default:
	}
	// Admission uses the published execution limit even while old idle cleanup
	// remains held. The youngest idle worker is preserved and reused.
	current, err := pool.acquire(context.Background(), idle[2].entry, true)
	if err != nil || current != idle[2] {
		t.Fatalf("preserved idle worker: %p %v", current, err)
	}
	_, err = pool.acquire(context.Background(), current.entry, true)
	warmAssertThrottled(t, err)
	once.Do(func() { close(proceed) })
	for _, worker := range idle[:2] {
		if err := pool.retire(worker); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.release(current, true); err != nil {
		t.Fatal(err)
	}
	warmAssertCounts(t, pool, 0, 1)
	if err := pool.retire(current); err != nil {
		t.Fatal(err)
	}
	activity.mu.Lock()
	defer activity.mu.Unlock()
	for _, err := range activity.errors {
		if err != nil {
			t.Fatalf("ordinary canceled wait dirtied lifecycle: %v", err)
		}
	}
}

type warmTuningFixture struct {
	service     *Service
	started     chan string
	gates       map[string]chan struct{}
	server      *httptest.Server
	releaseOnce map[string]*sync.Once
}

func newWarmTuningFixture(t *testing.T, limits DevWarmConfig, asyncWorkers int) *warmTuningFixture {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node runtime unavailable")
	}
	fixture := &warmTuningFixture{started: make(chan string, 16), gates: make(map[string]chan struct{}), releaseOnce: make(map[string]*sync.Once)}
	for _, name := range []string{"one", "two", "three", "event", "queued"} {
		fixture.gates[name], fixture.releaseOnce[name] = make(chan struct{}), &sync.Once{}
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		name := strings.TrimPrefix(request.URL.Path, "/")
		gate, found := fixture.gates[name]
		if !found {
			http.NotFound(w, request)
			return
		}
		fixture.started <- name
		select {
		case <-gate:
			w.WriteHeader(http.StatusNoContent)
		case <-request.Context().Done():
		}
	}))
	directory := t.TempDir()
	source := "let count=0; export async function handler(event) { count++; if (event.gate) await fetch(process.env.BARRIER+'/'+event.gate); return {pid:process.pid,count,setting:process.env.SETTING}; }"
	if err := os.WriteFile(filepath.Join(directory, "tuning.mjs"), []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
	fixture.service, err = NewService(&Config{
		Functions: map[string]Function{"warm": {Runtime: "node", Command: []string{node}, Handler: "tuning.mjs#handler", Timeout: 10 * time.Second, Environment: map[string]string{"BARRIER": fixture.server.URL, "SETTING": "original"}}},
		DevWarm:   &limits, DevAsync: &DevAsyncConfig{Workers: asyncWorkers, LogWriter: io.Discard},
	}, directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for name := range fixture.gates {
			fixture.release(name)
		}
		if err := fixture.service.Close(context.Background()); err != nil {
			t.Error(err)
		}
		fixture.server.Close()
	})
	return fixture
}

func (fixture *warmTuningFixture) release(name string) {
	fixture.releaseOnce[name].Do(func() { close(fixture.gates[name]) })
}

func (fixture *warmTuningFixture) wait(t *testing.T, name string) {
	t.Helper()
	select {
	case actual := <-fixture.started:
		if actual != name {
			t.Fatalf("started %s, expected %s", actual, name)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("actual handler %s did not start", name)
	}
}

type warmTuningResult struct {
	output InvokeOutput
	err    error
}

func (fixture *warmTuningFixture) execute(name string) <-chan warmTuningResult {
	done := make(chan warmTuningResult, 1)
	go func() {
		body := []byte("{}")
		if name != "" {
			body = []byte("{\"gate\":\"" + name + "\"}")
		}
		output, err := fixture.service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: body})
		done <- warmTuningResult{output, err}
	}()
	return done
}

func warmTuningResultBody(t *testing.T, done <-chan warmTuningResult) map[string]any {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil || result.output.FunctionError {
			t.Fatalf("active work failed: %s %v", result.output.Payload, result.err)
		}
		var body map[string]any
		if err := json.Unmarshal(result.output.Payload, &body); err != nil {
			t.Fatal(err)
		}
		if body["setting"] != "original" {
			t.Fatalf("registration changed: %v", body)
		}
		return body
	case <-time.After(5 * time.Second):
		t.Fatal("actual execution did not complete")
	}
	return nil
}

func TestDevWarmUpdateRaisesAndLowersAroundActiveCalls(t *testing.T) {
	fixture := newWarmTuningFixture(t, DevWarmConfig{3, 2}, 1)
	service := fixture.service
	service.mu.Lock()
	generation := service.functions["warm"].generation
	service.mu.Unlock()
	one := fixture.execute("one")
	fixture.wait(t, "one")
	two := fixture.execute("two")
	fixture.wait(t, "two")
	_, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte("{}")})
	warmAssertThrottled(t, err)
	if limits, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{3, 3}); err != nil || limits != (DevWarmConfig{3, 3}) {
		t.Fatalf("raise: %+v %v", limits, err)
	}
	three := fixture.execute("three")
	fixture.wait(t, "three")
	warmAssertCounts(t, service.warm, 3, 0)
	// Lowering neither waits for nor cancels active handlers.
	if _, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{1, 1}); err != nil {
		t.Fatal(err)
	}
	warmAssertCounts(t, service.warm, 3, 0)
	bodies := make([]map[string]any, 0, 3)
	for _, call := range []struct {
		name string
		done <-chan warmTuningResult
	}{{"one", one}, {"two", two}, {"three", three}} {
		_, err := service.Execute(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte("{}")})
		warmAssertThrottled(t, err)
		fixture.release(call.name)
		bodies = append(bodies, warmTuningResultBody(t, call.done))
	}
	warmAssertCounts(t, service.warm, 0, 1)
	if len(map[any]bool{bodies[0]["pid"]: true, bodies[1]["pid"]: true, bodies[2]["pid"]: true}) != 3 {
		t.Fatalf("active calls shared environment: %v", bodies)
	}
	continued := warmTuningResultBody(t, fixture.execute(""))
	if continued["pid"] != bodies[2]["pid"] || continued["count"] != float64(2) {
		t.Fatalf("preserved imported state: %v", continued)
	}
	service.mu.Lock()
	sameGeneration := service.functions["warm"].generation == generation
	service.mu.Unlock()
	if !sameGeneration {
		t.Fatal("tuning replaced function registration")
	}
	for _, body := range bodies[:2] {
		if processAlive(int(body["pid"].(float64))) {
			t.Fatal("excess environment did not join")
		}
	}
	if err := service.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if processAlive(int(continued["pid"].(float64))) {
		t.Fatal("final retained environment did not join")
	}
}

func TestDevWarmUpdateRetiresOldestIdleThroughActualProcessOwner(t *testing.T) {
	fixture := newWarmTuningFixture(t, DevWarmConfig{3, 3}, 1)
	var calls []<-chan warmTuningResult
	for _, name := range []string{"one", "two", "three"} {
		calls = append(calls, fixture.execute(name))
		fixture.wait(t, name)
	}
	var bodies []map[string]any
	for index, name := range []string{"one", "two", "three"} {
		fixture.release(name)
		bodies = append(bodies, warmTuningResultBody(t, calls[index]))
	}
	warmAssertCounts(t, fixture.service.warm, 0, 3)
	if _, err := fixture.service.DevUpdateWarm(context.Background(), DevWarmConfig{1, 2}); err != nil {
		t.Fatal(err)
	}
	warmAssertCounts(t, fixture.service.warm, 0, 1)
	for _, body := range bodies[:2] {
		if processAlive(int(body["pid"].(float64))) {
			t.Fatal("idle eviction returned before actual child join")
		}
	}
	retained := warmTuningResultBody(t, fixture.execute(""))
	if retained["pid"] != bodies[2]["pid"] || retained["count"] != float64(2) {
		t.Fatalf("wrong idle worker retained: %v", retained)
	}
}

func TestDevWarmUpdateRetainsNativeQueuedEventAndRetryBudget(t *testing.T) {
	fixture := newWarmTuningFixture(t, DevWarmConfig{2, 2}, 1)
	service := fixture.service
	one := fixture.execute("one")
	fixture.wait(t, "one")
	admitted, err := service.Admit(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte("{\"gate\":\"event\"}")})
	if err != nil {
		t.Fatal(err)
	}
	fixture.wait(t, "event")
	queued, err := service.Admit(context.Background(), InvokeInput{FunctionName: "warm", Payload: []byte("{\"gate\":\"queued\"}")})
	if err != nil {
		t.Fatal(err)
	}
	waitAsyncState(t, service, queued.RequestID, "queued")
	if _, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{1, 1}); err != nil {
		t.Fatal(err)
	}
	fixture.release("event")
	waitAsyncState(t, service, admitted.RequestID, "succeeded")
	record := waitAsyncState(t, service, queued.RequestID, "retrying")
	if record.Attempts != 0 || record.ErrorType != "TooManyRequestsException" {
		t.Fatalf("queued event/retry budget changed: %+v", record)
	}
	fixture.release("one")
	warmTuningResultBody(t, one)
	if _, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{2, 2}); err != nil {
		t.Fatal(err)
	}
	fixture.wait(t, "queued")
	fixture.release("queued")
	record = waitAsyncState(t, service, queued.RequestID, "succeeded")
	if record.Attempts != 1 {
		t.Fatalf("throttled event consumed handler attempts: %+v", record)
	}
	if err := service.DrainAsync(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDevWarmUpdateCloseJoinsPublishedIdleRetirement(t *testing.T) {
	fixture := newWarmTuningFixture(t, DevWarmConfig{2, 2}, 1)
	one := fixture.execute("one")
	fixture.wait(t, "one")
	two := fixture.execute("two")
	fixture.wait(t, "two")
	fixture.release("one")
	first := warmTuningResultBody(t, one)
	fixture.release("two")
	second := warmTuningResultBody(t, two)
	service := fixture.service
	pool := service.warm
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	pool.mu.Lock()
	for worker := range pool.workers {
		if worker.command.Process.Pid == int(first["pid"].(float64)) {
			original := worker.release
			worker.release = func(err error) {
				close(entered)
				<-proceed
				if original != nil {
					original(err)
				}
			}
		}
	}
	pool.mu.Unlock()
	updateDone := make(chan error, 1)
	go func() { _, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{1, 2}); updateDone <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("actual idle cleanup did not reach retirement publication")
	}
	if processAlive(int(first["pid"].(float64))) {
		t.Fatal("owned gate preceded actual process join")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- service.Close(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		service.mu.Lock()
		closed := service.closed
		service.mu.Unlock()
		if closed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Close did not fence service")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before update retirement join: %v", err)
	default:
	}
	_, err := service.DevUpdateWarm(context.Background(), DevWarmConfig{2, 4})
	requireWarmControlError(t, err, "warm_unavailable")
	// Close joins admitted controls before global warm shutdown. Their shared
	// retirement owner must stay available until the control wait finishes.
	pool.mu.Lock()
	closed := pool.closed
	pool.mu.Unlock()
	if closed {
		t.Fatal("global pool close preceded admitted tuning join")
	}
	once.Do(func() { close(proceed) })
	if err := <-updateDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if processAlive(int(second["pid"].(float64))) {
		t.Fatal("Close retained surviving warm child")
	}
}
