package lambda

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const runtimePrefix = "/2018-06-01/runtime/"

// Each provided-runtime invocation gets a private loopback listener. A Go
// binary built with aws-lambda-go/lambda.Start uses its unmodified Runtime API
// client; it never needs the EventBus stdin/stdout command adapter.
func runProvided(ctx context.Context, entry executableFunction, input invocation) invocationResult {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return failure("Runtime.InternalError", "Cannot start Lambda Runtime API")
	}
	runtimeCtx, stopRuntime := context.WithCancel(ctx)
	defer stopRuntime()
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
		stopRuntime()
		shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
		}
		<-serveDone
	}()
	command := newCommand(ctx, entry, input, entry.command[1:], listener.Addr().String())
	logs := &tailOutput{limit: maxLogs}
	command.Stdout, command.Stderr = logs, logs
	if err := command.Start(); err != nil {
		result := failure("Runtime.InvalidEntrypoint", "Cannot start configured function")
		result.logs = logs.Bytes()
		return result
	}
	processDone := make(chan error, 1)
	go func() { processDone <- command.Wait() }()
	var result invocationResult
	finished := false
	select {
	case result = <-runtime.result:
	case <-processDone:
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
	cleanupErr := stopProcessGroup(command)
	if !finished {
		<-processDone
	}
	if cleanupErr != nil {
		result = failure("Runtime.InternalError", "Cannot stop function process group")
	}
	result.logs = logs.Bytes()
	return result
}

type runtimeInvocation struct {
	ctx       context.Context
	input     invocation
	arn       string
	mu        sync.Mutex
	delivered bool
	completed bool
	result    chan invocationResult
}

func (runtime *runtimeInvocation) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
