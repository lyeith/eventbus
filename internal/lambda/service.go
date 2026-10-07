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
	functions                                          map[string]executableFunction
	mu                                                 sync.Mutex
	closed                                             bool
	aborted                                            bool
	next                                               uint64
	active                                             map[uint64]invocationOwner
	inflight                                           sync.WaitGroup
	done                                               chan struct{}
	closeErr                                           error
	asyncQueue                                         []*asyncTask
	asyncTasks                                         map[string]*asyncTask
	asyncHistory                                       []AsyncRecord
	asyncOutstanding, asyncCapacity, asyncHistoryLimit int
	asyncRetryDelays                                   [2]time.Duration
	asyncWake                                          chan struct{}
	asyncContext                                       context.Context
	asyncCancel                                        context.CancelFunc
	asyncWorkers                                       sync.WaitGroup
	asyncCapture                                       *devcapture.Sink
	asyncEvidenceErr                                   error
	asyncClosed, asyncAborted                          bool
	asyncAbortErr                                      error
	asyncDrainDone                                     chan struct{}
}
type invocationOwner struct {
	cancel       context.CancelFunc
	asynchronous bool
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
	service := &Service{functions: make(map[string]executableFunction), active: make(map[uint64]invocationOwner), done: make(chan struct{})}
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
	if err := service.configureAsync(config.DevAsync, root); err != nil {
		return nil, err
	}
	return service, nil
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
	requestedFunction := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, invokePrefix), invokeSuffix)
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
		requestedName := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, invokePrefix), invokeSuffix)
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
	requestedName := strings.TrimSuffix(strings.TrimPrefix(request.URL.Path, invokePrefix), invokeSuffix)
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
	if errors.Is(err, errClosed) {
		invokeError(writer, http.StatusServiceUnavailable, "ServiceException", "Lambda service is closing")
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
}

type invocationResult struct {
	payload       []byte
	functionError bool
	logs          []byte
}

func failure(kind, message string) invocationResult {
	payload, _ := json.Marshal(map[string]any{"errorType": kind, "errorMessage": message})
	return invocationResult{payload: payload, functionError: true}
}

func (service *Service) invoke(parent context.Context, entry executableFunction, input invocation) (invocationResult, error) {
	return service.invokeOwned(parent, entry, input, false)
}
func (service *Service) invokeOwned(parent context.Context, entry executableFunction, input invocation, asynchronous bool) (invocationResult, error) {
	ctx, cancel := context.WithTimeout(parent, entry.timeout)
	defer cancel()
	input.deadline, _ = ctx.Deadline()
	service.mu.Lock()
	if service.closed && !asynchronous || service.aborted || asynchronous && service.asyncAborted {
		service.mu.Unlock()
		return invocationResult{}, errClosed
	}
	service.next++
	id := service.next
	service.active[id] = invocationOwner{cancel: cancel, asynchronous: asynchronous}
	service.inflight.Add(1)
	service.mu.Unlock()
	defer func() {
		service.mu.Lock()
		delete(service.active, id)
		service.mu.Unlock()
		service.inflight.Done()
	}()
	var result invocationResult
	if entry.runtime == "provided" {
		result = runProvided(ctx, entry, input)
	} else {
		result = runCommand(ctx, entry, input)
	}
	if ctx.Err() != nil {
		logs := result.logs
		result = failure("Sandbox.Timedout", fmt.Sprintf("Task timed out after %.2f seconds", entry.timeout.Seconds()))
		result.logs = logs
	}
	return result, nil
}

// Close stops admission, cancels synchronous invocations, and drains accepted
// Events. A deadline aborts queued/running Events, records cancellation, and
// joins all process cleanup before returning the deadline error. A later Close
// retains that error: canceled accepted work is never called a healthy drain.
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
			service.asyncCancel()
			captureErr := service.asyncCapture.Close()
			service.mu.Lock()
			service.closeErr = errors.Join(service.closeErr, service.asyncAbortErr, captureErr)
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
