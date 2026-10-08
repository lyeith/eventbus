package lambda

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
)

//go:embed wrapper.mjs
var nodeWrapper string

//go:embed wrapper.py
var pythonWrapper string

const maxWrapperResult = maxPayload + (8 << 10)

// tailOutput holds bounded diagnostics without copying arbitrary handler logs
// into EventBus logs. A mutex covers concurrent stdout/stderr copy goroutines.
type tailOutput struct {
	mu     sync.Mutex
	buffer []byte
	limit  int
	total  int64
}

func (output *tailOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	size := len(data)
	output.total += int64(size)
	if size >= output.limit {
		output.buffer = append(output.buffer[:0], data[size-output.limit:]...)
	} else {
		if excess := len(output.buffer) + size - output.limit; excess > 0 {
			copy(output.buffer, output.buffer[excess:])
			output.buffer = output.buffer[:len(output.buffer)-excess]
		}
		output.buffer = append(output.buffer, data...)
	}
	return size, nil
}

func (output *tailOutput) Bytes() []byte {
	output.mu.Lock()
	defer output.mu.Unlock()
	return append([]byte(nil), output.buffer...)
}

func (output *tailOutput) snapshot() ([]byte, int64) {
	output.mu.Lock()
	defer output.mu.Unlock()
	return append([]byte(nil), output.buffer...), output.total
}

func (output *tailOutput) byteCount() int64 {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.total
}

// Stream separation is opt-in; the original merged native tail stays intact.
// Command stdout is its response channel and is deliberately never diagnosed.
type invocationLogs struct {
	merged, stdout, stderr *tailOutput
}

func newInvocationLogs(enabled, stdoutIsResponse bool) *invocationLogs {
	logs := &invocationLogs{merged: &tailOutput{limit: maxLogs}}
	if enabled {
		logs.stderr = &tailOutput{limit: maxLogs}
		if !stdoutIsResponse {
			logs.stdout = &tailOutput{limit: maxLogs}
		}
	}
	return logs
}

func (logs *invocationLogs) stdoutWriter() io.Writer {
	if logs.stdout == nil {
		return logs.merged
	}
	return io.MultiWriter(logs.merged, logs.stdout)
}
func (logs *invocationLogs) stderrWriter() io.Writer {
	if logs.stderr == nil {
		return logs.merged
	}
	return io.MultiWriter(logs.merged, logs.stderr)
}
func (logs *invocationLogs) diagnostics() invocationDiagnostics {
	var result invocationDiagnostics
	result.tailBytes = logs.merged.byteCount()
	if logs.stdout != nil {
		result.stdout, result.stdoutBytes = logs.stdout.snapshot()
	}
	if logs.stderr != nil {
		result.stderr, result.stderrBytes = logs.stderr.snapshot()
	}
	return result
}

func notStartedFailure(kind, message string) invocationResult {
	result := failure(kind, message)
	result.state = InvocationNotStarted
	return result
}

func environment(entry executableFunction, input invocation, runtimeAPI string) []string {
	values := map[string]string{
		"PATH":                            os.Getenv("PATH"),
		"LANG":                            "C.UTF-8",
		"AWS_EC2_METADATA_DISABLED":       "true",
		"AWS_SHARED_CREDENTIALS_FILE":     os.DevNull,
		"AWS_CONFIG_FILE":                 os.DevNull,
		"NO_PROXY":                        "*",
		"AWS_REGION":                      "us-east-1",
		"AWS_DEFAULT_REGION":              "us-east-1",
		"AWS_LAMBDA_FUNCTION_MEMORY_SIZE": "128",
		"AWS_LAMBDA_LOG_GROUP_NAME":       "/aws/lambda/" + strings.Split(entry.name, ":")[0],
		"AWS_LAMBDA_LOG_STREAM_NAME":      "local/" + input.requestID,
	}
	for name, value := range entry.environment {
		values[name] = value
	}
	// This channel is solely selected by owned development configuration.
	delete(values, "EVENTBUS_DEV_PYTHON_STACKS")
	delete(values, "EVENTBUS_LAMBDA_PHASE_PROTOCOL")
	if (entry.runtime == "python" || entry.runtime == "node") && input.phase != nil {
		values["EVENTBUS_LAMBDA_PHASE_PROTOCOL"] = "1"
	}
	if input.pythonStacks != nil {
		values["EVENTBUS_DEV_PYTHON_STACKS"] = "1"
	}
	version := "$LATEST"
	name := input.name
	if base, qualifier, found := strings.Cut(name, ":"); found {
		name = base
		if _, err := strconv.ParseUint(qualifier, 10, 64); err == nil {
			version = qualifier
		}
	}
	values["AWS_LAMBDA_FUNCTION_NAME"] = name
	values["AWS_LAMBDA_FUNCTION_VERSION"] = version
	values["LAMBDA_TASK_ROOT"] = entry.workDir
	values["EVENTBUS_LAMBDA_REQUEST_ID"] = input.requestID
	values["EVENTBUS_LAMBDA_DEADLINE_MS"] = fmt.Sprint(input.deadline.UnixMilli())
	values["EVENTBUS_LAMBDA_FUNCTION_ARN"] = functionARN(entry, input)
	values["EVENTBUS_LAMBDA_CLIENT_CONTEXT"] = input.clientContext
	// An inherited or fixture-provided Runtime API may never escape this
	// invocation's owned loopback server, or accidentally invoke production.
	delete(values, "AWS_LAMBDA_RUNTIME_API")
	if runtimeAPI != "" {
		values["AWS_LAMBDA_RUNTIME_API"] = runtimeAPI
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func functionARN(entry executableFunction, input invocation) string {
	if input.functionARN != "" {
		return input.functionARN
	}
	region := entry.environment["AWS_REGION"]
	if region == "" {
		region = "us-east-1"
	}
	account := entry.environment["AWS_ACCOUNT_ID"]
	if account == "" {
		account = "000000000000"
	}
	return "arn:aws:lambda:" + region + ":" + account + ":function:" + input.name
}

func newCommand(ctx context.Context, entry executableFunction, input invocation, args []string, runtimeAPI string) (*exec.Cmd, error) {
	command := exec.CommandContext(ctx, entry.command[0], args...)
	command.Dir = entry.workDir
	command.Env = environment(entry, input, runtimeAPI)
	if err := localexec.Configure(command); err != nil {
		return nil, err
	}
	return command, nil
}

func runCommand(ctx context.Context, entry executableFunction, input invocation, cleanup func(*exec.Cmd) error) invocationResult {
	arguments := append([]string(nil), entry.command[1:]...)
	wrapped := entry.runtime == "node" || entry.runtime == "python"
	if entry.runtime == "node" {
		arguments = append(arguments, "--input-type=module", "--eval", nodeWrapper, "--", entry.module, entry.exported)
	} else if entry.runtime == "python" {
		wrapper := pythonWrapper
		if input.pythonStacks != nil {
			source, _ := json.Marshal(pythonStackCollector)
			wrapper = "def _eventbus_start_python_stacks():\n    pass\ntry:\n    import types as _eventbus_types\n    _eventbus_stack_module = _eventbus_types.ModuleType('_eventbus_python_stacks')\n    exec(compile(" + string(source) + ", '<eventbus-python-stacks>', 'exec'), _eventbus_stack_module.__dict__)\n    _eventbus_start_python_stacks = _eventbus_stack_module._eventbus_start_python_stacks\nexcept BaseException:\n    pass\n" + wrapper
		}
		arguments = append(arguments, "-c", wrapper, entry.module, entry.exported)
	}
	command, err := newCommand(ctx, entry, input, arguments, "")
	if err != nil {
		return notStartedFailure("Runtime.InternalError", "Cannot own function process group")
	}
	command.Stdin = bytes.NewReader(input.payload)
	logs := newInvocationLogs(input.diagnostics, entry.runtime == "command")
	resultOutput := localexec.NewBoundedOutput(maxPayload)
	command.Stderr = logs.stderrWriter()
	command.Stdout = resultOutput
	var reader, writer *os.File
	var readDone chan struct{}
	var reply []byte
	var readErr error
	var readerMu sync.Mutex
	var readerClosed bool
	var readerCloseErr error
	// The caller owns readerMu. Completion publishes any deliberate bounded
	// reader close atomically with readDone, before the owner sets a deadline.
	closeReaderLocked := func() error {
		if !readerClosed {
			readerCloseErr = reader.Close()
			readerClosed = true
		}
		return readerCloseErr
	}
	if wrapped {
		var err error
		reader, writer, err = os.Pipe()
		if err != nil {
			return notStartedFailure("Runtime.InternalError", "Cannot create handler result channel")
		}
		defer func() {
			readerMu.Lock()
			_ = closeReaderLocked()
			readerMu.Unlock()
		}()
		defer writer.Close()
		command.ExtraFiles = []*os.File{writer}
		command.Stdout = logs.stdoutWriter()
		readDone = make(chan struct{})
		go func() {
			reply, readErr = io.ReadAll(io.LimitReader(reader, maxWrapperResult+1))
			readerMu.Lock()
			if len(reply) > maxWrapperResult {
				// A wrapper writing a very large result must not block forever
				// after this reader reaches its limit. Closing wakes fd3's writer.
				_ = closeReaderLocked()
			}
			close(readDone)
			readerMu.Unlock()
		}()
	}
	var stackReader, stackWriter, childStackReader, childStackWriter *os.File
	if input.pythonStacks != nil {
		var pipeErr error
		childStackReader, stackWriter, pipeErr = os.Pipe()
		if pipeErr == nil {
			stackReader, childStackWriter, pipeErr = os.Pipe()
		}
		if pipeErr != nil {
			for _, file := range []*os.File{childStackReader, stackWriter, stackReader, childStackWriter} {
				if file != nil {
					_ = file.Close()
				}
			}
			// Optional collection setup cannot replace a native function result.
			input.pythonStacks.unavailableReason = "channel_failed"
			stackReader, stackWriter, childStackReader, childStackWriter = nil, nil, nil, nil
			command.Args = append(command.Args[:len(command.Args)-3], pythonWrapper, entry.module, entry.exported)
			for index, value := range command.Env {
				if strings.HasPrefix(value, "EVENTBUS_DEV_PYTHON_STACKS=") {
					command.Env = append(command.Env[:index], command.Env[index+1:]...)
					break
				}
			}
		} else {
			command.ExtraFiles = append(command.ExtraFiles, childStackReader, childStackWriter)
		}
	}
	var control *managedPhaseChannel
	if wrapped && input.phase != nil {
		control, err = newManagedPhaseChannel(command)
		if err != nil {
			// The process has not started; all already constructed result/stack
			// channels must be retired before returning a startup failure.
			for _, file := range []*os.File{writer, childStackReader, stackWriter, stackReader, childStackWriter} {
				if file != nil {
					_ = file.Close()
				}
			}
			if readDone != nil {
				<-readDone
			}
			return notStartedFailure("Runtime.InternalError", "Cannot create runtime readiness channel")
		}
		defer control.close()
	}
	startErr := command.Start()
	if control != nil {
		control.childStarted(input.phase, startErr == nil)
	}
	if childStackReader != nil {
		_ = childStackReader.Close()
		_ = childStackWriter.Close()
	}
	if stackReader != nil && startErr == nil {
		input.pythonStacks.attach(stackReader, stackWriter)
	}
	if writer != nil {
		// Only the child retains the writing descriptor after launch.
		_ = writer.Close()
	}
	if startErr != nil {
		if stackReader != nil {
			_ = stackReader.Close()
			_ = stackWriter.Close()
		}
		if readDone != nil {
			<-readDone
		}
		result := notStartedFailure("Runtime.InvalidEntrypoint", "Cannot start configured function")
		result.logs, result.diagnostics = logs.merged.Bytes(), logs.diagnostics()
		result.diagnostics.processError = startErr.Error()
		if reader != nil {
			readerMu.Lock()
			result.ownershipErr = closeReaderLocked()
			readerMu.Unlock()
		}
		return result
	}
	waitErr := command.Wait()
	cleanupErr := cleanup(command)
	ownershipErr := cleanupErr
	if control != nil {
		ownershipErr = errors.Join(ownershipErr, control.join())
	}
	if errors.Is(waitErr, exec.ErrWaitDelay) {
		// A valid result may coexist with forcibly closed diagnostic pipes.
		// Preserve native result policy, but never certify that ownership clean.
		ownershipErr = errors.Join(ownershipErr, exec.ErrWaitDelay)
	}
	if reader != nil {
		// Grandchildren could retain fd3. Group termination happens before
		// joining the result reader; closing it also bounds the failure path.
		readerMu.Lock()
		select {
		case <-readDone:
			// A completed bounded read may already own a deliberate fd close.
		default:
			if err := reader.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				ownershipErr = errors.Join(ownershipErr, err)
			}
		}
		readerMu.Unlock()
		<-readDone
		readerMu.Lock()
		ownershipErr = errors.Join(ownershipErr, closeReaderLocked())
		readerMu.Unlock()
		if readErr != nil {
			ownershipErr = errors.Join(ownershipErr, readErr)
		}
	}
	var result invocationResult
	switch {
	case cleanupErr != nil:
		result = failure("Runtime.InternalError", "Cannot stop function process group")
	case resultOutput.Overflowed() || len(reply) > maxWrapperResult:
		result = failure("Function.ResponseSizeTooLarge", "Response exceeds the 6291456 byte limit")
	case waitErr != nil && !(wrapped && errors.Is(waitErr, exec.ErrWaitDelay)):
		result = failure("Runtime.ExitError", "Function process exited without a valid response")
	case wrapped:
		if readErr != nil && !errors.Is(readErr, os.ErrClosed) {
			result = failure("Runtime.InvalidResponse", "Cannot read handler response")
		} else {
			result = unwrapReply(reply)
		}
	default:
		payload := resultOutput.Bytes()
		if !json.Valid(payload) {
			result = failure("Runtime.InvalidResponse", "Command must return one JSON value on stdout")
		} else {
			result.payload = payload
		}
	}
	result.logs, result.diagnostics = logs.merged.Bytes(), logs.diagnostics()
	if waitErr != nil {
		result.diagnostics.processError = waitErr.Error()
	}
	result.ownershipErr = ownershipErr
	if wrapped && input.phase != nil && !result.functionError && !input.phase.invoked() {
		// A native result without the owned readiness boundary must not bypass
		// invocation accounting, even if a user process writes a forged fd3 reply.
		result = preserveLaunchEvidence(result, failure("Runtime.InvalidResponse", "Managed runtime did not become ready"))
	}
	return result
}

func preserveLaunchEvidence(source, result invocationResult) invocationResult {
	result.logs, result.diagnostics, result.ownershipErr = source.logs, source.diagnostics, source.ownershipErr
	return result
}

// Internal Init fallback is still one native attempt. Keep bounded tails and
// actual byte totals from both launches without retaining either success body.
// Process errors describe the final launch; phase facts retain retired errors.
// Ownership uncertainty remains cumulative and can never be erased by success.
func mergeLaunchDiagnostics(prior, next invocationResult) invocationResult {
	tail := func(first, second []byte) []byte {
		merged := append(append([]byte(nil), first...), second...)
		if len(merged) > maxLogs {
			merged = merged[len(merged)-maxLogs:]
		}
		return merged
	}
	next.logs = tail(prior.logs, next.logs)
	next.diagnostics.stdout = tail(prior.diagnostics.stdout, next.diagnostics.stdout)
	next.diagnostics.stderr = tail(prior.diagnostics.stderr, next.diagnostics.stderr)
	next.diagnostics.stdoutBytes += prior.diagnostics.stdoutBytes
	next.diagnostics.stderrBytes += prior.diagnostics.stderrBytes
	next.diagnostics.tailBytes += prior.diagnostics.tailBytes
	next.ownershipErr = errors.Join(prior.ownershipErr, next.ownershipErr)
	return next
}

func unwrapReply(reply []byte) invocationResult {
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if !json.Valid(reply) || json.Unmarshal(reply, &envelope) != nil || (envelope.Result == nil) == (envelope.Error == nil) {
		return failure("Runtime.InvalidResponse", "Handler did not return a valid response")
	}
	if envelope.Error != nil {
		var value struct {
			Message *string `json:"errorMessage"`
			Type    string  `json:"errorType"`
		}
		if json.Unmarshal(envelope.Error, &value) != nil || value.Message == nil || value.Type == "" {
			return failure("Runtime.InvalidResponse", "Handler error response is invalid")
		}
		return invocationResult{payload: envelope.Error, functionError: true}
	}
	if len(envelope.Result) > maxPayload {
		return failure("Function.ResponseSizeTooLarge", "Response exceeds the 6291456 byte limit")
	}
	return invocationResult{payload: envelope.Result}
}
