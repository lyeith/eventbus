package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/rs/zerolog/log"
)

const maxOutputBytes = 10 << 20 // 10MB cap on subprocess stdout/stderr

var safeConsumerInheritedEnv = []string{
	"HOME",
	"LANG",
	"LC_ALL",
	"PATH",
	"SSL_CERT_DIR",
	"SSL_CERT_FILE",
	"TMPDIR",
}

// invokeHandler dispatches to the appropriate handler based on consumer type.
func (cm *ConsumerManager) invokeHandler(ctx context.Context, entry ConsumerEntry, event map[string]interface{}) error {
	_, err := cm.invokeHandlerResult(ctx, entry, event)
	return err
}

func (cm *ConsumerManager) invokeHandlerResult(ctx context.Context, entry ConsumerEntry, event map[string]interface{}) (*handlerBatchResult, error) {
	switch entry.Type {
	case "go":
		return cm.invokeGoHandler(ctx, entry, event)
	default:
		return cm.invokePythonHandler(ctx, entry, event)
	}
}

// invokePythonHandler runs the Python Lambda handler via uv subprocess.
func (cm *ConsumerManager) invokePythonHandler(ctx context.Context, entry ConsumerEntry, event map[string]interface{}) (*handlerBatchResult, error) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	// Split handler into module path and function name
	// e.g. "platform_lib.audit.handlers.sns_writer.handler" -> module="platform_lib.audit.handlers.sns_writer", func="handler"
	parts := strings.Split(entry.Handler, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid handler format: %s (expected module.function)", entry.Handler)
	}
	modulePath := strings.Join(parts[:len(parts)-1], ".")
	funcName := parts[len(parts)-1]

	script := fmt.Sprintf(
		`import json, sys; from %s import %s; result = %s(json.loads(sys.stdin.read()), None); print(json.dumps(result) if result else '{}')`,
		modulePath, funcName, funcName,
	)

	timeout := time.Duration(entry.TimeoutSeconds) * time.Second
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, "uv", "run", "python", "-c", script)
	cmd.Stdin = bytes.NewReader(eventJSON)
	cmd.Dir = cm.workDir

	// Set environment
	cmd.Env = consumerProcessEnv(entry.Env)
	if err := localexec.Configure(cmd); err != nil {
		return nil, fmt.Errorf("handler process ownership: %w", err)
	}

	stdout := localexec.NewBoundedOutput(maxOutputBytes)
	stderr := localexec.NewBoundedOutput(maxOutputBytes)
	cmd.Stdout, cmd.Stderr = stdout, stderr

	logger := log.With().Str("consumer", entry.Name).Str("handler", entry.Handler).Logger()

	err = cmd.Run()
	if cleanupErr := localexec.Cleanup(cmd); cleanupErr != nil {
		return nil, fmt.Errorf("stop handler process group: %w", cleanupErr)
	}

	if stderr.Len() > 0 {
		logger.Debug().Str("stderr", stderr.String()).Msg("Handler stderr")
	}

	if err != nil {
		if execCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("handler timed out after %s", timeout)
		}
		return nil, fmt.Errorf("handler failed: %w\nstderr: %s", err, stderr.String())
	}

	if stdout.Len() > 0 {
		logger.Debug().Str("result", stdout.String()).Msg("Handler result")
	}

	return parseHandlerBatchResult(stdout, stderr)
}

// invokeGoHandler runs a compiled Go binary, passing the SQS event via stdin.
// The binary reads a Lambda SQS event from stdin and writes a JSON batch response
// to stdout. Failed records are identified by the optional batchItemFailures array.
func (cm *ConsumerManager) invokeGoHandler(ctx context.Context, entry ConsumerEntry, event map[string]interface{}) (*handlerBatchResult, error) {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("marshal event: %w", err)
	}

	// Resolve handler path relative to workDir if not absolute
	handlerPath := entry.Handler
	if !filepath.IsAbs(handlerPath) {
		handlerPath = filepath.Join(cm.workDir, handlerPath)
	}

	timeout := time.Duration(entry.TimeoutSeconds) * time.Second
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(execCtx, handlerPath)
	cmd.Stdin = bytes.NewReader(eventJSON)
	cmd.Dir = cm.workDir

	cmd.Env = consumerProcessEnv(entry.Env)
	if err := localexec.Configure(cmd); err != nil {
		return nil, fmt.Errorf("handler process ownership: %w", err)
	}

	stdout := localexec.NewBoundedOutput(maxOutputBytes)
	stderr := localexec.NewBoundedOutput(maxOutputBytes)
	cmd.Stdout, cmd.Stderr = stdout, stderr

	logger := log.With().Str("consumer", entry.Name).Str("handler", handlerPath).Logger()

	err = cmd.Run()
	if cleanupErr := localexec.Cleanup(cmd); cleanupErr != nil {
		return nil, fmt.Errorf("stop handler process group: %w", cleanupErr)
	}

	if stderr.Len() > 0 {
		logger.Debug().Str("stderr", stderr.String()).Msg("Handler stderr")
	}

	if err != nil {
		if execCtx.Err() == context.DeadlineExceeded {
			return nil, fmt.Errorf("handler timed out after %s", timeout)
		}
		return nil, fmt.Errorf("handler failed: %w\nstderr: %s", err, stderr.String())
	}

	if stdout.Len() > 0 {
		logger.Debug().Str("result", stdout.String()).Msg("Handler result")
	}

	return parseHandlerBatchResult(stdout, stderr)
}

func parseHandlerBatchResult(stdout, stderr *localexec.BoundedOutput) (*handlerBatchResult, error) {
	if stdout.Overflowed() || stderr.Overflowed() {
		return nil, fmt.Errorf("handler output exceeded the %d byte limit", maxOutputBytes)
	}
	data := bytes.TrimSpace(stdout.Bytes())
	if len(data) == 0 {
		return nil, fmt.Errorf("handler returned an empty batch response")
	}
	var result handlerBatchResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("parse handler batch response: %w", err)
	}
	return &result, nil
}

func consumerProcessEnv(configured map[string]string) []string {
	environment := make(map[string]string, len(safeConsumerInheritedEnv)+len(configured))
	for _, key := range safeConsumerInheritedEnv {
		if value, ok := os.LookupEnv(key); ok {
			environment[key] = value
		}
	}
	for key, value := range configured {
		environment[key] = value
	}
	keys := make([]string, 0, len(environment))
	for key := range environment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, fmt.Sprintf("%s=%s", key, environment[key]))
	}
	return result
}
