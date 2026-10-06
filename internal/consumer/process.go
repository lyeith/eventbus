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

// limitedWriter wraps a bytes.Buffer and stops writing after limit bytes.
type limitedWriter struct {
	buf   *bytes.Buffer
	limit int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	remaining := w.limit - w.buf.Len()
	if remaining <= 0 {
		return len(p), nil // discard silently
	}
	if len(p) > remaining {
		p = p[:remaining]
	}
	return w.buf.Write(p)
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

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{buf: &stdout, limit: maxOutputBytes}
	cmd.Stderr = &limitedWriter{buf: &stderr, limit: maxOutputBytes}

	logger := log.With().Str("consumer", entry.Name).Str("handler", entry.Handler).Logger()

	err = cmd.Run()

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

	return parseHandlerBatchResult(stdout.Bytes())
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

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{buf: &stdout, limit: maxOutputBytes}
	cmd.Stderr = &limitedWriter{buf: &stderr, limit: maxOutputBytes}

	logger := log.With().Str("consumer", entry.Name).Str("handler", handlerPath).Logger()

	err = cmd.Run()

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

	return parseHandlerBatchResult(stdout.Bytes())
}

func parseHandlerBatchResult(stdout []byte) (*handlerBatchResult, error) {
	data := bytes.TrimSpace(stdout)
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

// FindProjectRoot walks up from the given start directory looking for a pyproject.toml or go.work file.
func FindProjectRoot(start string) string {
	dir, _ := filepath.Abs(start)
	for {
		if _, err := os.Stat(filepath.Join(dir, "pyproject.toml")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return start
		}
		dir = parent
	}
}
