package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

const runtimePrefix = "/2018-06-01/runtime/"

// Each provided-runtime invocation gets a private loopback listener. A Go
// binary built with aws-lambda-go/lambda.Start uses its unmodified Runtime API
// client; it never needs the EventBus stdin/stdout command adapter.
func runProvided(ctx context.Context, entry executableFunction, input invocation, cleanup func(*exec.Cmd) error) (result invocationResult) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return notStartedFailure("Runtime.InternalError", "Cannot start Lambda Runtime API")
	}
	runtimeCtx, stopRuntime := context.WithCancel(ctx)
	runtime := &runtimeInvocation{ctx: runtimeCtx, input: input, arn: functionARN(entry, input), result: make(chan invocationResult, 1)}
	server := &http.Server{
		Handler:           runtime,
		ReadHeaderTimeout: time.Second,
		MaxHeaderBytes:    64 << 10,
		BaseContext:       func(net.Listener) context.Context { return runtimeCtx },
		ErrorLog:          log.New(io.Discard, "", 0),
	}
	serveDone := make(chan struct{})
	go func() { _ = server.Serve(listener); close(serveDone) }()
	defer func() {
		result.ownershipErr = errors.Join(result.ownershipErr, runtime.close(server, stopRuntime, serveDone))
	}()
	command, err := newCommand(ctx, entry, input, entry.command[1:], listener.Addr().String())
	if err != nil {
		return notStartedFailure("Runtime.InternalError", "Cannot own function process group")
	}
	logs := newInvocationLogs(input.diagnostics, false)
	command.Stdout, command.Stderr = logs.stdoutWriter(), logs.stderrWriter()
	if err := command.Start(); err != nil {
		result := notStartedFailure("Runtime.InvalidEntrypoint", "Cannot start configured function")
		result.logs, result.diagnostics = logs.merged.Bytes(), logs.diagnostics()
		result.diagnostics.processError = err.Error()
		return result
	}
	processDone := make(chan error, 1)
	go func() { processDone <- command.Wait() }()
	finished := false
	var waitErr error
	select {
	case result = <-runtime.result:
	case waitErr = <-processDone:
		finished = true
		// A runtime may exit after posting a reply. Prefer the already admitted
		// response over its process exit, regardless of scheduler ordering.
		select {
		case result = <-runtime.result:
		default:
			result = failure("Runtime.ExitError", "Function process exited without a response")
		}
	case <-ctx.Done():
		result = failure("Sandbox.Timedout", "Function invocation timed out")
	}
	cleanupErr := cleanup(command)
	if !finished {
		waitErr = <-processDone
	}
	if cleanupErr != nil {
		result = failure("Runtime.InternalError", "Cannot stop function process group")
	}
	result.logs, result.diagnostics = logs.merged.Bytes(), logs.diagnostics()
	if waitErr != nil {
		result.diagnostics.processError = waitErr.Error()
	}
	result.ownershipErr = cleanupErr
	if errors.Is(waitErr, exec.ErrWaitDelay) {
		// The native Runtime API response wins over process exit, but a forced
		// output-pipe join must still make the developer ownership lease dirty.
		result.ownershipErr = errors.Join(result.ownershipErr, exec.ErrWaitDelay)
	}
	return result
}

type runtimeInvocation struct {
	ctx       context.Context
	input     invocation
	arn       string
	mu        sync.Mutex
	delivered bool
	completed bool
	closing   bool
	requests  sync.WaitGroup
	result    chan invocationResult
}

// close fences handler admission before Wait, cancels duplicate /next polls,
// closes stalled body/write I/O if graceful shutdown expires, and joins every
// admitted request. Joining Serve alone only proves listener closure.
func (runtime *runtimeInvocation) close(server *http.Server, stopRuntime context.CancelFunc, serveDone <-chan struct{}) error {
	runtime.mu.Lock()
	runtime.closing = true
	runtime.mu.Unlock()
	stopRuntime()
	shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := server.Shutdown(shutdown)
	if err != nil {
		err = errors.Join(err, server.Close())
	}
	<-serveDone
	runtime.requests.Wait()
	return err
}

func (runtime *runtimeInvocation) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	runtime.mu.Lock()
	if runtime.closing {
		runtime.mu.Unlock()
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	runtime.requests.Add(1)
	runtime.mu.Unlock()
	defer runtime.requests.Done()
	if request.URL.Path == runtimePrefix+"invocation/next" {
		if request.Method != http.MethodGet {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		runtime.mu.Lock()
		first := !runtime.delivered && !runtime.completed
		if first {
			runtime.delivered = true
		}
		runtime.mu.Unlock()
		if !first {
			select {
			case <-runtime.ctx.Done():
			case <-request.Context().Done():
			}
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Lambda-Runtime-Aws-Request-Id", runtime.input.requestID)
		writer.Header().Set("Lambda-Runtime-Invocation-Id", runtime.input.requestID)
		writer.Header().Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(runtime.input.deadline.UnixMilli(), 10))
		writer.Header().Set("Lambda-Runtime-Invoked-Function-Arn", runtime.arn)
		if runtime.input.traceID != "" {
			writer.Header().Set("Lambda-Runtime-Trace-Id", runtime.input.traceID)
		}
		if runtime.input.clientContext != "" {
			writer.Header().Set("Lambda-Runtime-Client-Context", runtime.input.clientContext)
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(runtime.input.payload)
		return
	}
	initialization := request.URL.Path == runtimePrefix+"init/error"
	response := request.URL.Path == runtimePrefix+"invocation/"+runtime.input.requestID+"/response"
	functionError := request.URL.Path == runtimePrefix+"invocation/"+runtime.input.requestID+"/error"
	if !initialization && !response && !functionError {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	if request.Method != http.MethodPost {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if id := request.Header.Get("Lambda-Runtime-Invocation-Id"); id != "" && id != runtime.input.requestID {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusBadRequest)
		_, _ = writer.Write([]byte(`{"errorType":"InvalidInvocationId","errorMessage":"Invocation ID does not match"}`))
		return
	}
	runtime.mu.Lock()
	allowed := !runtime.completed && (initialization || runtime.delivered)
	runtime.mu.Unlock()
	if !allowed {
		writer.WriteHeader(http.StatusConflict)
		return
	}
	payload, err := io.ReadAll(io.LimitReader(request.Body, maxPayload+1))
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	result := invocationResult{payload: payload, functionError: initialization || functionError}
	status := http.StatusAccepted
	if len(payload) > maxPayload {
		result = failure("Function.ResponseSizeTooLarge", "Response exceeds the 6291456 byte limit")
		status = http.StatusRequestEntityTooLarge
	} else if !json.Valid(payload) {
		result = failure("Runtime.InvalidResponse", "Runtime API response must be JSON")
	}
	runtime.mu.Lock()
	if runtime.completed {
		runtime.mu.Unlock()
		writer.WriteHeader(http.StatusConflict)
		return
	}
	runtime.completed = true
	runtime.mu.Unlock()
	writer.WriteHeader(status)
	// Flush the Runtime API acknowledgement before the owner kills the cold
	// runtime. Native clients otherwise see a spurious transport failure.
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
	runtime.result <- result
}
