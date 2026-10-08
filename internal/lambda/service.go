package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/lyeith/eventbus/internal/localexec"
)

const (
	invokePrefix = "/2015-03-31/functions/"
	invokeSuffix = "/invocations"
	maxPayload   = 6 << 20
	maxLogs      = 64 << 10
)

type executableFunction struct {
	name, runtime, workDir, module, exported string
	command                                  []string
	environment                              map[string]string
	timeout                                  time.Duration
}

// Service owns all admitted invocations until their process groups and
// Runtime API listeners have stopped. Registry entries are immutable.
type Service struct {
	functions   map[string]executableFunction
	initTimeout time.Duration // on-demand Init limit; private override for focused tests
	devActivity DevActivity
	// Immutable in normal construction; private tests can wrap real cleanup to
	// prove that ownership uncertainty stays separate from native responses.
	processCleanup func(*exec.Cmd) error
	mu             sync.Mutex
	closed         bool
	aborted        bool
	next           uint64
	active         map[uint64]invocationOwner
	inflight       sync.WaitGroup
	done           chan struct{}
	closeErr       error
	// Async transitions take asyncTransitionMu before mu. Capture I/O may
	// hold only the transition mutex; admission fences/cancellation use mu.
	asyncTransitionMu                                  sync.Mutex
	asyncAdmissions                                    sync.WaitGroup
	asyncQueue                                         []*asyncTask
	asyncTasks                                         map[string]*asyncTask
	asyncHistory                                       []AsyncRecord
	asyncOutstanding, asyncCapacity, asyncHistoryLimit int
	asyncRetryDelays                                   [2]time.Duration
	asyncWake                                          chan struct{}
	asyncContext                                       context.Context
	asyncCancel                                        context.CancelCauseFunc
	asyncWorkers                                       sync.WaitGroup
	asyncCapture                                       *devcapture.Sink
	asyncEvidenceErr                                   error
	asyncOwnershipErr                                  error
	diagnosticCapture                                  *devcapture.Sink
	diagnosticPath                                     string
	diagnosticEvidenceErr                              error
	diagnosticMu                                       sync.Mutex
	diagnosticClosed                                   bool
	diagnosticCloseErr                                 error
	invocationEvidenceErr                              error
	pythonStacks                                       *DevPythonStacksConfig
	asyncClosed, asyncAborted                          bool
	asyncAbortErr                                      error
	asyncDrainDone                                     chan struct{}
}
type invocationOwner struct {
	cancel       context.CancelFunc
	asynchronous bool
	metadata     InvocationMetadata
	pythonStacks *pythonStackSession
}

func New(filename, workDir string) (*Service, error) {
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(workDir, filename)
	}
	config, err := LoadConfig(filename)
	if err != nil {
		return nil, err
	}
	return NewService(config, workDir)
}

// NewService resolves handlers and executables at startup, before accepting
// requests. It is also useful to hosts which construct fixtures in memory.
func NewService(config *Config, workDir string) (*Service, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if err := processGroupsSupported(); err != nil {
		return nil, err
	}
	root, err := filepath.Abs(workDir)
	if err != nil {
		return nil, err
	}
	service := &Service{functions: make(map[string]executableFunction), initTimeout: initialInitTimeout, devActivity: config.DevActivity, processCleanup: localexec.Cleanup, active: make(map[uint64]invocationOwner), done: make(chan struct{})}
	for name, function := range config.Functions {
		directory := root
		if function.WorkDir != "" {
			directory = function.WorkDir
			if !filepath.IsAbs(directory) {
				directory = filepath.Join(root, directory)
			}
		}
		info, err := os.Stat(directory)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("Lambda function %q: work directory must exist", name)
		}
		entry := executableFunction{name: name, runtime: function.Runtime, timeout: function.Timeout, workDir: directory, environment: maps.Clone(function.Environment), command: append([]string(nil), function.Command...)}
		if len(entry.command) == 0 {
			if entry.runtime == "python" {
				entry.command = []string{"python3"}
			} else {
				entry.command = []string{"node"}
			}
		}
		executable := entry.command[0]
		if strings.ContainsAny(executable, "/\\") && !filepath.IsAbs(executable) {
			executable = filepath.Join(directory, executable)
		}
		executable, err = exec.LookPath(executable)
		if err != nil {
			return nil, fmt.Errorf("Lambda function %q: executable: %w", name, err)
		}
		entry.command[0], err = filepath.Abs(executable)
		if err != nil {
			return nil, err
		}
		if entry.runtime == "python" || entry.runtime == "node" {
			entry.module, entry.exported, _ = handlerReference(function.Handler, entry.runtime)
			if !filepath.IsAbs(entry.module) {
				entry.module = filepath.Join(directory, entry.module)
			}
			if entry.runtime == "node" {
				if _, err := os.Stat(entry.module); os.IsNotExist(err) {
					var matches []string
					for _, extension := range []string{".js", ".mjs", ".cjs"} {
						candidate := entry.module + extension
						if info, err := os.Stat(candidate); err == nil && info.Mode().IsRegular() {
							matches = append(matches, candidate)
						}
					}
					if len(matches) > 1 {
						return nil, fmt.Errorf("Lambda function %q: handler module is ambiguous; include its file extension", name)
					}
					if len(matches) == 1 {
						entry.module = matches[0]
					}
				}
			}
			info, err := os.Stat(entry.module)
			if err != nil || !info.Mode().IsRegular() {
				return nil, fmt.Errorf("Lambda function %q: handler must be a regular file", name)
			}
			file, err := os.Open(entry.module)
			if err != nil {
				return nil, fmt.Errorf("Lambda function %q: handler cannot be read", name)
			}
			_ = file.Close()
		}
		service.functions[name] = entry
	}
	if err := service.configureDiagnostics(config.DevDiagnostics, config.DevAsync, root); err != nil {
		return nil, err
	}
	if err := service.configureAsync(config.DevAsync, root); err != nil {
		if service.diagnosticCapture != nil {
			err = errors.Join(err, service.diagnosticCapture.Close())
		}
		return nil, err
	}
	return service, nil
}

func requestFunctionName(request *http.Request) string {
	return strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, invokePrefix), invokeSuffix)
}

func (service *Service) Match(request *http.Request) bool {
	return strings.HasPrefix(request.URL.Path, invokePrefix)
}

func (service *Service) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("X-Amzn-RequestId", uuid.NewString())
	writer.Header().Set("Content-Type", "application/json")
	if !service.Match(request) || !strings.HasSuffix(request.URL.Path, invokeSuffix) {
		invokeError(writer, http.StatusNotFound, "ResourceNotFoundException", "Lambda operation not found")
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		invokeError(writer, http.StatusMethodNotAllowed, "InvalidRequestContentException", "Lambda Invoke requires POST")
		return
	}
	requestedFunction := requestFunctionName(request)
	entry, name, err := service.resolveTarget(requestedFunction, request.URL.Query().Get("Qualifier"))
	if err != nil {
		var invokeErr *InvokeError
		if errors.As(err, &invokeErr) {
			invokeError(writer, invokeErr.Status, invokeErr.Code, invokeErr.Message)
		}
		return
	}
	invocationType := request.Header.Get("X-Amz-Invocation-Type")
	if invocationType == "" {
		invocationType = "RequestResponse"
	}
	if invocationType != "RequestResponse" && invocationType != "DryRun" && invocationType != "Event" {
		invokeError(writer, http.StatusBadRequest, "InvalidParameterValueException", "InvocationType must be RequestResponse, Event or DryRun")
		return
	}
	logType := request.Header.Get("X-Amz-Log-Type")
	if logType != "" && logType != "None" && logType != "Tail" {
		invokeError(writer, http.StatusBadRequest, "InvalidParameterValueException", "LogType must be None or Tail")
		return
	}
	payloadLimit := maxPayload
	if invocationType == "Event" {
		payloadLimit = maxEventPayload
	}
	payload, err := io.ReadAll(io.LimitReader(request.Body, int64(payloadLimit+1)))
	if err != nil {
		invokeError(writer, http.StatusBadRequest, "InvalidRequestContentException", "Cannot read invocation payload")
		return
	}
	if len(payload) > payloadLimit {
		invokeError(writer, http.StatusRequestEntityTooLarge, "RequestTooLargeException", fmt.Sprintf("Request exceeds the %d byte limit", payloadLimit))
		return
	}
	clientContext := ""
	if encoded := request.Header.Get("X-Amz-Client-Context"); encoded != "" {
		data, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(encoded) > 3583 || !json.Valid(data) {
			invokeError(writer, http.StatusBadRequest, "InvalidParameterValueException", "ClientContext must contain at most 3583 bytes of base64 encoded JSON")
			return
		}
		clientContext = string(data)
	}
	if invocationType == "DryRun" {
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if !json.Valid(payload) {
		invokeError(writer, http.StatusBadRequest, "InvalidRequestContentException", "Invocation payload must be JSON")
		return
	}
	if invocationType == "Event" {
		requestedName := requestFunctionName(request)
		_, err := service.Admit(request.Context(), InvokeInput{FunctionName: requestedName, Qualifier: request.URL.Query().Get("Qualifier"), Payload: payload, TraceID: request.Header.Get("X-Amzn-Trace-Id")})
		if err != nil {
			var admissionErr *InvokeError
			if errors.As(err, &admissionErr) {
				if admissionErr.Status == http.StatusTooManyRequests {
					writer.Header().Set("Retry-After", "1")
				}
				invokeError(writer, admissionErr.Status, admissionErr.Code, admissionErr.Message)
			} else if request.Context().Err() == nil {
				invokeError(writer, http.StatusInternalServerError, "ServiceException", "Cannot admit invocation")
			}
			return
		}
		writer.WriteHeader(http.StatusAccepted)
		return
	}
	invocation := invocation{payload: payload, requestID: uuid.NewString(), name: name, clientContext: clientContext, traceID: request.Header.Get("X-Amzn-Trace-Id")}
	requestedName := requestFunctionName(request)
	if strings.HasPrefix(requestedName, "arn:") {
		invocation.functionARN = requestedName
		if qualifier := request.URL.Query().Get("Qualifier"); qualifier != "" {
			invocation.functionARN += ":" + qualifier
		}
	} else if account, _, found := strings.Cut(requestedName, ":function:"); found {
		region := entry.environment["AWS_REGION"]
		if region == "" {
			region = "us-east-1"
		}
		invocation.functionARN = "arn:aws:lambda:" + region + ":" + account + ":function:" + name
	}
	result, err := service.invoke(request.Context(), entry, invocation)
	if err != nil {
		var executionErr *InvokeError
		if errors.As(err, &executionErr) {
			invokeError(writer, executionErr.Status, executionErr.Code, executionErr.Message)
		} else if errors.Is(err, errClosed) {
			invokeError(writer, http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
		} else if request.Context().Err() == nil {
			invokeError(writer, http.StatusInternalServerError, "ServiceException", "Cannot execute invocation")
		}
		return
	}
	if request.Context().Err() != nil {
		return
	}
	if result.functionError {
		writer.Header().Set("X-Amz-Function-Error", "Unhandled")
	}
	version := "$LATEST"
	if _, qualifier, found := strings.Cut(name, ":"); found {
		if _, err := strconv.ParseUint(qualifier, 10, 64); err == nil {
			version = qualifier
		}
	}
	writer.Header().Set("X-Amz-Executed-Version", version)
	if logType == "Tail" {
		logs := result.logs
		if len(logs) > 4096 {
			logs = logs[len(logs)-4096:]
		}
		writer.Header().Set("X-Amz-Log-Result", base64.StdEncoding.EncodeToString(logs))
	}
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(result.payload)
}

func resolveName(name, qualifier string) (string, error) {
	if strings.HasPrefix(name, "arn:") {
		parts := strings.SplitN(name, ":", 7)
		if len(parts) != 7 || parts[2] != "lambda" || parts[5] != "function" || parts[3] == "" || parts[4] == "" {
			return "", errors.New("Invalid Lambda function ARN")
		}
		name = parts[6]
	} else if account, suffix, found := strings.Cut(name, ":function:"); found {
		if len(account) != 12 || strings.Trim(account, "0123456789") != "" {
			return "", errors.New("Invalid partial Lambda function ARN")
		}
		name = suffix
	}
	if qualifier != "" {
		if strings.Contains(name, ":") {
			return "", errors.New("Qualifier must not be supplied in both FunctionName and Qualifier")
		}
		name += ":" + qualifier
	}
	if !functionName.MatchString(name) {
		return "", errors.New("Invalid Lambda function name")
	}
	return name, nil
}

func invokeError(writer http.ResponseWriter, status int, kind, message string) {
	writer.Header().Set("X-Amzn-ErrorType", kind)
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]string{"Type": "User", "message": message})
}

var errClosed = errors.New("Lambda service is closed")

type invocation struct {
	payload                                              []byte
	requestID, name, functionARN, clientContext, traceID string
	deadline                                             time.Time
	attempt                                              int
	onAdmission                                          func(InvocationMetadata) error
	diagnostics                                          bool
	pythonStacks                                         *pythonStackSession
	phase                                                *runtimePhase
}

type invocationResult struct {
	payload       []byte
	functionError bool
	logs          []byte
	state         InvocationState
	admitted      bool
	diagnostics   invocationDiagnostics
	phases        []runtimePhaseRecord
	// Private ownership uncertainty cannot be supplied by a handler or projected
	// onto native responses. It only makes a developer lifecycle lease dirty.
	ownershipErr error
}

func failure(kind, message string) invocationResult {
	payload, _ := json.Marshal(map[string]any{"errorType": kind, "errorMessage": message})
	return invocationResult{payload: payload, functionError: true, state: InvocationFailed}
}

func (service *Service) invoke(parent context.Context, entry executableFunction, input invocation) (invocationResult, error) {
	return service.invokeOwned(parent, entry, input, false)
}
func (service *Service) invokeOwned(parent context.Context, entry executableFunction, input invocation, asynchronous bool) (result invocationResult, err error) {
	ctx, stop := context.WithCancelCause(parent)
	defer stop(nil)
	cancel := func() { stop(errServiceCancellation) }
	input.deadline = time.Now().Add(service.initTimeout)
	if entry.runtime == "command" {
		input.deadline = time.Now().Add(entry.timeout)
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(input.deadline) {
		input.deadline = deadline
	}
	service.mu.Lock()
	if service.closed && !asynchronous || service.aborted || asynchronous && service.asyncAborted {
		service.mu.Unlock()
		return invocationResult{}, errClosed
	}
	var release func(error)
	if !asynchronous {
		release, err = service.beginActivityLocked("lambda_invoke", input.requestID)
		if err != nil {
			service.mu.Unlock()
			return invocationResult{}, err
		}
	}
	service.next++
	id := service.next
	started := time.Now()
	if entry.runtime == "python" && service.pythonStacks != nil {
		input.pythonStacks = newPythonStackSession(service, invocationMetadata(entry, input), started, input.deadline, *service.pythonStacks)
	}
	service.active[id] = invocationOwner{cancel: cancel, asynchronous: asynchronous, metadata: invocationMetadata(entry, input), pythonStacks: input.pythonStacks}
	service.inflight.Add(1)
	service.mu.Unlock()
	var completion diagnosticCompletion
	var phase *runtimePhase
	defer func() { phase.stop() }()
	normalCompletion, runnerEntered, diagnosticCompletion := false, false, false
	input.diagnostics = service.diagnosticCapture != nil
	defer func() {
		// Even an observer/runner panic or Goexit must retire this admitted
		// lifetime with dirty evidence. Never infer success from zero values.
		defer func() {
			if !diagnosticCompletion {
				result.ownershipErr = errors.Join(result.ownershipErr, errors.New("Lambda invocation diagnostic completion was interrupted"))
			}
			service.mu.Lock()
			if result.ownershipErr != nil && service.invocationEvidenceErr == nil {
				service.invocationEvidenceErr = result.ownershipErr
			}
			delete(service.active, id)
			service.inflight.Done()
			if release != nil {
				release(result.ownershipErr)
			}
			service.mu.Unlock()
		}()
		result.admitted = true
		if !normalCompletion {
			result.state = InvocationNotStarted
			if runnerEntered {
				result.state = InvocationFailed
			}
			result.ownershipErr = errors.Join(result.ownershipErr, errors.New("Lambda invocation exited without normal joined completion"))
		}
		if result.state == "" {
			result.state = InvocationSucceeded
			if result.functionError || err != nil {
				result.state = InvocationFailed
			}
		}
		if input.pythonStacks != nil {
			result.ownershipErr = errors.Join(result.ownershipErr, input.pythonStacks.finish())
		}
		if completion.at.IsZero() {
			completion = snapshotDiagnosticCompletion(ctx, false)
		}
		// Cause/native projection is frozen at runner completion. Optional
		// evidence may hold this lifetime through append/join, never turn an
		// already completed function into a configured-budget timeout.
		completion.at = time.Now()
		result.ownershipErr = errors.Join(result.ownershipErr, service.captureDiagnostics(entry, input, result, asynchronous, started, completion))
		diagnosticCompletion = true
	}()
	if input.onAdmission != nil {
		if err := input.onAdmission(invocationMetadata(entry, input)); err != nil {
			normalCompletion = true
			return invocationResult{state: InvocationNotStarted, ownershipErr: err}, err
		}
	}
	runnerEntered = true
	mode := "initial"
	if entry.runtime == "command" {
		mode = "command"
	}
	var records []runtimePhaseRecord
	launchInput := input
	for initAttempt := 1; initAttempt <= 2; initAttempt++ {
		var onReady func(time.Time)
		if launchInput.pythonStacks != nil {
			onReady = launchInput.pythonStacks.updateDeadline
		}
		phase = newRuntimePhase(ctx, entry.timeout, service.initTimeout, mode, onReady)
		launchInput.phase, launchInput.deadline = phase, phase.deadline
		if launchInput.pythonStacks != nil {
			launchInput.pythonStacks.updateDeadline(phase.deadline)
		}
		var launched invocationResult
		if entry.runtime == "provided" {
			launched = runProvided(phase.ctx, entry, launchInput, service.processCleanup)
		} else {
			launched = runCommand(phase.ctx, entry, launchInput, service.processCleanup)
		}
		// Actual native cleanup decides this launch before any optional evidence
		// join. Internal Init retry never releases the admitted invocation owner.
		var record runtimePhaseRecord
		record, completion = phase.complete(initAttempt, launched)
		records = append(records, record)
		phase.stop()
		result = mergeLaunchDiagnostics(result, launched)
		if mode != "initial" || completion.cause != "initialization_timeout" || ctx.Err() != nil || result.ownershipErr != nil {
			break
		}
		if launchInput.pythonStacks != nil {
			result.ownershipErr = errors.Join(result.ownershipErr, launchInput.pythonStacks.finish())
			// Preserve one optional snapshot per native execution attempt. The
			// initial process's collector must join before launching its fallback.
			launchInput.pythonStacks = nil
		}
		if ctx.Err() != nil || result.ownershipErr != nil {
			break
		}
		mode = "fallback"
	}
	result.phases = records
	if completion.contextError != "" {
		logs, ownershipErr, diagnostics, phases := result.logs, result.ownershipErr, result.diagnostics, result.phases
		result = failure("Sandbox.Timedout", fmt.Sprintf("Task timed out after %.2f seconds", entry.timeout.Seconds()))
		if completion.cause == "runtime_protocol_error" {
			result = failure("Runtime.InvalidResponse", "Managed runtime readiness protocol failed")
		}
		result.logs, result.ownershipErr, result.diagnostics, result.phases = logs, ownershipErr, diagnostics, phases
		result.state = InvocationCanceled
		if completion.contextError == "deadline_exceeded" {
			result.state = InvocationTimedOut
		}
		if completion.cause == "runtime_protocol_error" {
			result.state = InvocationFailed
		}
	}
	normalCompletion = true
	return result, nil
}

// Close stops admission, cancels synchronous invocations, and drains accepted
// Events. A deadline aborts queued/running Events, records cancellation, and
// joins all process cleanup before returning the deadline error. A later Close
// retains that error: canceled accepted work is never called a healthy drain.
// Joined private ownership/capture uncertainty also remains a strict close error,
// independent of ordinary native handler failures and retry decisions.
func (service *Service) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	first := !service.closed
	if first {
		service.closed = true
		service.asyncClosed = true
		for _, owner := range service.active {
			if !owner.asynchronous {
				owner.cancel()
			}
		}
		service.wakeAsyncLocked()
	}
	service.mu.Unlock()
	if first {
		go func() {
			<-service.asyncDrainDone
			service.inflight.Wait()
			service.asyncCancel(errServiceCancellation)
			captureErr := service.asyncCapture.Close()
			if service.diagnosticCapture != nil {
				// Serialize terminal health with completed closure. No invocation
				// can still append after inflight joined, and Lambda.mu stays free.
				service.diagnosticMu.Lock()
				service.diagnosticCloseErr = service.diagnosticCapture.Close()
				service.diagnosticClosed = true
				captureErr = errors.Join(captureErr, service.diagnosticCloseErr)
				service.diagnosticMu.Unlock()
			}
			service.mu.Lock()
			service.closeErr = errors.Join(service.closeErr, service.asyncAbortErr, service.asyncEvidenceErr, service.asyncOwnershipErr, service.invocationEvidenceErr, captureErr)
			close(service.done)
			service.mu.Unlock()
		}()
	}
	select {
	case <-service.done:
		service.mu.Lock()
		err := service.closeErr
		service.mu.Unlock()
		return err
	case <-ctx.Done():
		service.mu.Lock()
		select {
		case <-service.done:
			err := service.closeErr
			service.mu.Unlock()
			return err
		default:
		}
		if !service.aborted {
			service.aborted = true
			service.closeErr = errors.Join(service.closeErr, ctx.Err())
			service.abortAsyncLocked(ctx.Err())
			for _, owner := range service.active {
				owner.cancel()
			}
		}
		service.mu.Unlock()
		<-service.done
		service.mu.Lock()
		err := service.closeErr
		service.mu.Unlock()
		return err
	}
}
