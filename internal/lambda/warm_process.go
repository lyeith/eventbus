package lambda

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os/exec"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/localexec"
)

// warmWorker owns its process, server, copy goroutines and activity lease until
// close joins all of them. One pool reservation owns each invocation boundary.
type warmWorker struct {
	freshOnly              bool
	entry                  executableFunction
	command                *exec.Cmd
	cancel                 context.CancelFunc
	runtimeCtx             context.Context
	server                 *http.Server
	serveDone, processDone chan struct{}
	waitErr                error
	stdoutCopy, stderrCopy *localexec.TrackedOutput
	stdout, stderr         *warmLogStream
	requests               chan *runtimeInvocation
	mu                     sync.Mutex
	current                *runtimeInvocation
	closing                bool
	handlers               sync.WaitGroup
	retireOnce             sync.Once
	retired                chan struct{}
	closeErr               error
	cleanup                func(*exec.Cmd) error
	release                func(error)
	token                  string
}

func (worker *warmWorker) start(service *Service, input invocation, invocationContext context.Context) error {
	service.mu.Lock()
	var err error
	worker.release, err = service.beginActivityLocked("lambda_warm_worker", uuid.NewString())
	service.mu.Unlock()
	if err != nil {
		return err
	}
	worker.runtimeCtx, worker.cancel = context.WithCancel(context.Background())
	stopLaunchCancellation := linkWarmCancellation(invocationContext, worker.cancel)
	defer stopLaunchCancellation()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	worker.requests = make(chan *runtimeInvocation, 1)
	worker.server = &http.Server{Handler: worker, ReadHeaderTimeout: time.Second, MaxHeaderBytes: 64 << 10,
		BaseContext: func(net.Listener) context.Context { return worker.runtimeCtx }, ErrorLog: log.New(io.Discard, "", 0)}
	worker.serveDone = make(chan struct{})
	go func() { _ = worker.server.Serve(listener); close(worker.serveDone) }()
	arguments := append([]string(nil), worker.entry.command[1:]...)
	if worker.entry.runtime == "node" {
		arguments = append(arguments, "--input-type=module", "--eval", nodeWrapper, "--", worker.entry.module, worker.entry.exported)
	} else {
		arguments = append(arguments, "-c", pythonWrapper, worker.entry.module, worker.entry.exported)
	}
	command, err := newCommand(worker.runtimeCtx, worker.entry, input, arguments, listener.Addr().String())
	if err != nil {
		return err
	}
	command.Env = append(command.Env, "EVENTBUS_LAMBDA_WARM=1", "EVENTBUS_LAMBDA_WARM_LOG_TOKEN="+worker.token)
	worker.stdoutCopy, worker.stderrCopy = localexec.NewTrackedOutputs(worker.stdout, worker.stderr)
	command.Stdout, command.Stderr = worker.stdoutCopy, worker.stderrCopy
	worker.command, worker.cleanup = command, service.processCleanup
	if err := invocationContext.Err(); err != nil {
		return err
	}
	if err := command.Start(); err != nil {
		return err
	}
	worker.processDone = make(chan struct{})
	go func() { worker.waitErr = command.Wait(); close(worker.processDone) }()
	return nil
}

func (worker *warmWorker) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	worker.mu.Lock()
	if worker.closing {
		worker.mu.Unlock()
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	worker.handlers.Add(1)
	current := worker.current
	worker.mu.Unlock()
	defer worker.handlers.Done()
	if request.URL.Path == runtimePrefix+"invocation/next" && request.Method == http.MethodGet {
		select {
		case current = <-worker.requests:
		case <-request.Context().Done():
			return
		case <-worker.runtimeCtx.Done():
			return
		}
	}
	if current == nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	current.ServeHTTP(writer, request)
}

func (service *Service) runWarm(ctx context.Context, worker *warmWorker, input invocation) invocationResult {
	if ctx.Err() != nil {
		result := notStartedFailure("Sandbox.Timedout", "Function invocation canceled before launch")
		result.ownershipErr = worker.close()
		return result
	}
	logs := newInvocationLogs(input.diagnostics, false)
	if worker.token == "" {
		worker.token = uuid.NewString()
	}
	if worker.stdout == nil {
		worker.stdout, worker.stderr = &warmLogStream{}, &warmLogStream{}
		if !input.diagnostics {
			worker.stderr = worker.stdout
		}
	}
	marker := []byte("\x00eventbus-warm:" + worker.token + ":" + input.requestID + "\x00")
	var stdoutDone, stderrDone <-chan struct{}
	if worker.stdout == worker.stderr {
		stdoutDone = worker.stdout.begin(marker, logs.stdoutWriter(), 2)
		stderrDone = stdoutDone
	} else {
		stdoutDone = worker.stdout.begin(marker, logs.stdoutWriter(), 1)
		stderrDone = worker.stderr.begin(marker, logs.stderrWriter(), 1)
	}
	runtimeContext, stopRuntime := context.WithCancel(ctx)
	defer stopRuntime()
	runtime := &runtimeInvocation{ctx: runtimeContext, input: input, arn: functionARN(worker.entry, input), result: make(chan invocationResult, 1)}
	worker.mu.Lock()
	worker.current = runtime
	worker.mu.Unlock()
	if worker.command == nil {
		if err := worker.start(service, input, ctx); err != nil {
			result := notStartedFailure("Runtime.InvalidEntrypoint", "Cannot start configured function")
			result.diagnostics.processError = err.Error()
			result.ownershipErr = worker.close()
			worker.stdout.finish()
			worker.stderr.finish()
			result.logs, result.diagnostics = logs.merged.Bytes(), logs.diagnostics()
			result.diagnostics.processError = err.Error()
			return result
		}
	}
	stopInvocationCancellation := linkWarmCancellation(ctx, worker.cancel)
	defer stopInvocationCancellation()
	worker.requests <- runtime
	var result invocationResult
	select {
	case result = <-runtime.result:
		// A reply cannot release this worker while earlier stdout/stderr bytes
		// remain in copy goroutines. The managed wrapper flushes then frames an
		// exact private drain boundary on each stream before posting its reply.
		for _, done := range []<-chan struct{}{stdoutDone, stderrDone} {
			select {
			case <-done:
			case <-ctx.Done():
			case <-worker.processDone:
			}
		}
	case <-worker.processDone:
		select {
		case result = <-runtime.result:
		default:
			result = failure("Runtime.ExitError", "Function process exited without a response")
		}
	case <-ctx.Done():
		result = failure("Sandbox.Timedout", "Function invocation timed out")
	}
	unhealthy := result.functionError || ctx.Err() != nil
	select {
	case <-worker.processDone:
		unhealthy = true
	default:
	}
	if unhealthy {
		result.ownershipErr = worker.close()
	}
	runtime.mu.Lock()
	runtime.closing = true
	runtime.mu.Unlock()
	stopRuntime()
	requestsDone := make(chan struct{})
	go func() { runtime.requests.Wait(); close(requestsDone) }()
	select {
	case <-requestsDone:
	case <-ctx.Done():
		result.ownershipErr = errors.Join(result.ownershipErr, worker.close())
		<-requestsDone
	}
	worker.stdout.finish()
	worker.stderr.finish()
	worker.mu.Lock()
	worker.current = nil
	worker.mu.Unlock()
	result.logs, result.diagnostics = logs.merged.Bytes(), logs.diagnostics()
	if worker.processDone != nil {
		select {
		case <-worker.processDone:
			if worker.waitErr != nil {
				result.diagnostics.processError = worker.waitErr.Error()
			}
		default:
		}
	}
	return result
}

func (worker *warmWorker) close() error {
	worker.retireOnce.Do(func() {
		worker.mu.Lock()
		worker.closing = true
		worker.mu.Unlock()
		if worker.cancel != nil {
			worker.cancel()
		}
		if worker.command != nil && worker.processDone != nil {
			worker.closeErr = worker.cleanup(worker.command)
			<-worker.processDone
			worker.closeErr = errors.Join(worker.closeErr, worker.stdoutCopy.Err(), worker.stderrCopy.Err())
			if errors.Is(worker.waitErr, exec.ErrWaitDelay) {
				worker.closeErr = errors.Join(worker.closeErr, exec.ErrWaitDelay)
			}
		}
		if worker.server != nil {
			shutdown, cancel := context.WithTimeout(context.Background(), time.Second)
			err := worker.server.Shutdown(shutdown)
			cancel()
			if err != nil {
				err = errors.Join(err, worker.server.Close())
			}
			worker.closeErr = errors.Join(worker.closeErr, err)
			<-worker.serveDone
			worker.handlers.Wait()
		}
		if worker.release != nil {
			worker.release(worker.closeErr)
		}
		close(worker.retired)
	})
	return worker.closeErr
}

// warmLogStream is a streaming delimiter parser with at most one marker's
// length of held bytes. It never retains unbounded output or splits a native
// invocation tail at an arbitrary goroutine scheduling point.
type warmLogStream struct {
	mu              sync.Mutex
	marker, pending []byte
	output          io.Writer
	done            chan struct{}
	ended           bool
	boundaries      int
}

func (stream *warmLogStream) begin(marker []byte, output io.Writer, boundaries int) <-chan struct{} {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.marker, stream.pending, stream.output = marker, nil, output
	stream.done, stream.ended, stream.boundaries = make(chan struct{}), false, boundaries
	return stream.done
}
func (stream *warmLogStream) Write(data []byte) (int, error) {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.output == nil || stream.ended {
		return len(data), nil
	}
	stream.pending = append(stream.pending, data...)
	for {
		at := bytes.Index(stream.pending, stream.marker)
		if at < 0 {
			break
		}
		_, _ = stream.output.Write(stream.pending[:at])
		stream.pending = stream.pending[at+len(stream.marker):]
		stream.boundaries--
		if stream.boundaries == 0 {
			stream.pending = nil
			stream.ended = true
			close(stream.done)
			return len(data), nil
		}
	}
	if count := len(stream.pending) - len(stream.marker) + 1; count > 0 {
		_, _ = stream.output.Write(stream.pending[:count])
		stream.pending = append(stream.pending[:0], stream.pending[count:]...)
	}

	return len(data), nil
}
func (stream *warmLogStream) finish() {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	if stream.output != nil {
		_, _ = stream.output.Write(stream.pending)
	}
	stream.pending, stream.output = nil, nil
}

// Only the current invocation may cancel a worker. A stopped callback cannot
// subsequently kill an environment reused by another invocation.
func linkWarmCancellation(ctx context.Context, cancel context.CancelFunc) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); cancel() })
	return func() {
		if !stop() {
			<-done
		}
	}
}
