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
}

func (output *tailOutput) Write(data []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	size := len(data)
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
		arguments = append(arguments, "-c", pythonWrapper, entry.module, entry.exported)
	}
	command, err := newCommand(ctx, entry, input, arguments, "")
	if err != nil {
		return failure("Runtime.InternalError", "Cannot own function process group")
	}
	command.Stdin = bytes.NewReader(input.payload)
	logs := &tailOutput{limit: maxLogs}
	resultOutput := localexec.NewBoundedOutput(maxPayload)
	command.Stderr = logs
	command.Stdout = resultOutput
	var reader, writer *os.File
	var readDone chan struct{}
	var reply []byte
	var readErr error
	if wrapped {
		var err error
		reader, writer, err = os.Pipe()
		if err != nil {
			return failure("Runtime.InternalError", "Cannot create handler result channel")
		}
		defer reader.Close()
		defer writer.Close()
		command.ExtraFiles = []*os.File{writer}
		command.Stdout = logs
		readDone = make(chan struct{})
		go func() {
			reply, readErr = io.ReadAll(io.LimitReader(reader, maxWrapperResult+1))
			if len(reply) > maxWrapperResult {
				// A wrapper writing a very large result must not block forever
				// after this reader reaches its limit. Closing wakes fd3's writer.
				_ = reader.Close()
			}
			close(readDone)
		}()
	}
	startErr := command.Start()
	if writer != nil {
		// Only the child retains the writing descriptor after launch.
		_ = writer.Close()
	}
	if startErr != nil {
		if readDone != nil {
			<-readDone
		}
		result := failure("Runtime.InvalidEntrypoint", "Cannot start configured function")
		result.logs = logs.Bytes()
		return result
	}
	waitErr := command.Wait()
	cleanupErr := cleanup(command)
	if reader != nil {
		// Grandchildren could retain fd3. Group termination happens before
		// joining the result reader; closing it also bounds the failure path.
		_ = reader.SetReadDeadline(time.Now().Add(time.Second))
		<-readDone
		_ = reader.Close()
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
	result.logs = logs.Bytes()
	result.ownershipErr = cleanupErr
	return result
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
