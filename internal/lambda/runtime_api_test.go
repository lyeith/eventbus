package lambda

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type runtimeWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *runtimeWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestRuntimeCloseCancelsAndJoinsAdmittedDuplicateNext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &runtimeInvocation{ctx: ctx, input: invocation{requestID: "request-id", payload: []byte(`{}`)}, result: make(chan invocationResult, 1)}
	first := httptest.NewRecorder()
	runtime.ServeHTTP(first, httptest.NewRequest(http.MethodGet, runtimePrefix+"invocation/next", nil))
	if first.Code != 200 || first.Body.String() != `{}` {
		t.Fatalf("first invocation: %d %s", first.Code, first.Body.String())
	}
	waiting := &runtimeWaitContext{Context: context.Background(), waiting: make(chan struct{})}
	duplicateDone := make(chan struct{})
	go func() {
		runtime.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, runtimePrefix+"invocation/next", nil).WithContext(waiting))
		close(duplicateDone)
	}()
	select {
	case <-waiting.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("duplicate /next was not admitted into the cancellation wait")
	}
	select {
	case <-duplicateDone:
		t.Fatal("duplicate /next returned before runtime cancellation")
	default:
	}
	serveDone := make(chan struct{})
	close(serveDone)
	if err := runtime.close(&http.Server{}, cancel, serveDone); err != nil {
		t.Fatal(err)
	}
	select {
	case <-duplicateDone:
	case <-time.After(time.Second):
		t.Fatal("runtime close did not join its admitted duplicate /next")
	}
	late := httptest.NewRecorder()
	runtime.ServeHTTP(late, httptest.NewRequest(http.MethodGet, runtimePrefix+"invocation/next", nil))
	if late.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed Runtime API admitted a late handler: %d", late.Code)
	}
}

// The wrapper flushes the actual Runtime API acknowledgement, then holds the
// admitted handler. Closing its listener/socket must not count as joining it.
type heldRuntimeAck struct {
	http.ResponseWriter
	ack  chan struct{}
	gate <-chan struct{}
}

func (writer heldRuntimeAck) Flush() {
	writer.ResponseWriter.(http.Flusher).Flush()
	close(writer.ack)
	<-writer.gate
}

func TestRuntimeCloseJoinsHeldAcknowledgementAndRetainsShutdownUncertainty(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &runtimeInvocation{ctx: ctx, input: invocation{requestID: "request-id", payload: []byte(`{}`)}, result: make(chan invocationResult, 1)}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ack := make(chan struct{})
	gate := make(chan struct{})
	var once sync.Once
	openGate := func() { once.Do(func() { close(gate) }) }
	server := &http.Server{
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.Method == http.MethodPost {
				writer = heldRuntimeAck{ResponseWriter: writer, ack: ack, gate: gate}
			}
			runtime.ServeHTTP(writer, request)
		}),
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	serveDone := make(chan struct{})
	go func() { _ = server.Serve(listener); close(serveDone) }()
	t.Cleanup(func() {
		openGate()
		cancel()
		_ = server.Close()
		<-serveDone
		runtime.requests.Wait()
	})
	client := &http.Client{Timeout: 5 * time.Second}
	base := "http://" + listener.Addr().String()
	next, err := client.Get(base + runtimePrefix + "invocation/next")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, next.Body)
	_ = next.Body.Close()
	if next.StatusCode != 200 {
		t.Fatalf("next: %d", next.StatusCode)
	}
	response, err := client.Post(base+runtimePrefix+"invocation/request-id/response", "application/json", strings.NewReader(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("Runtime API acknowledgement was lost: %d", response.StatusCode)
	}
	<-ack
	bodyClosed := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, response.Body); close(bodyClosed) }()
	joined := make(chan error, 1)
	go func() { joined <- runtime.close(server, cancel, serveDone) }()
	select {
	case <-serveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Runtime API listener did not stop")
	}
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err == nil {
		_ = connection.Close()
		t.Fatal("Runtime API listener remained open")
	}
	// Graceful shutdown expires while Flush is held; forced Close then interrupts
	// the actual response socket, without unblocking the application handler.
	select {
	case <-bodyClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("forced HTTP close did not interrupt the held acknowledgement socket")
	}
	select {
	case err := <-joined:
		t.Fatalf("listener/socket closure was mistaken for handler completion: %v", err)
	default:
	}
	openGate()
	select {
	case err := <-joined:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("graceful shutdown uncertainty was discarded: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime close did not join the released request")
	}
	select {
	case result := <-runtime.result:
		if result.functionError || string(result.payload) != `{"ok":true}` {
			t.Fatalf("native function result changed: %#v", result)
		}
	default:
		t.Fatal("accepted Runtime API response was not delivered after its handler joined")
	}
}
