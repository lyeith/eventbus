package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

// validIdentifier matches valid Python identifiers (no code injection)
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// validatePythonHandler checks that the handler string is a valid dotted Python path.
// e.g., "platform_lib.audit.handlers.sns_writer.handler" — each part must be a valid Python identifier.
func validatePythonHandler(handler string) error {
	parts := strings.Split(handler, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid handler format: %q (expected module.function)", handler)
	}
	for _, part := range parts {
		if !validIdentifier.MatchString(part) {
			return fmt.Errorf("invalid handler component: %q (must be valid Python identifier)", part)
		}
	}
	return nil
}

// validateGoHandler checks that the handler is a non-empty path (binary to execute).
func validateGoHandler(handler string) error {
	if handler == "" {
		return fmt.Errorf("go handler path cannot be empty")
	}
	if strings.ContainsAny(handler, ";|&`$") {
		return fmt.Errorf("invalid go handler path: %q (contains shell metacharacters)", handler)
	}
	return nil
}

const maxOutputBytes = 10 << 20 // 10MB cap on subprocess stdout/stderr

const (
	defaultMaxReceiveCount = 5
	baseRetryVisibility    = 5 * time.Second
	maxRetryVisibility     = time.Minute
)

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

type ConsumerConfig struct {
	Consumers []ConsumerEntry `yaml:"consumers"`
}

type ConsumerEntry struct {
	Name            string            `yaml:"name"`
	Type            string            `yaml:"type"` // "python" (default) or "go"
	Queue           string            `yaml:"queue"`
	DeadLetterQueue string            `yaml:"dead_letter_queue"`
	MaxReceiveCount int               `yaml:"max_receive_count"`
	Handler         string            `yaml:"handler"` // Python: dotted module path; Go: path to binary
	BatchSize       int               `yaml:"batch_size"`
	TimeoutSeconds  int               `yaml:"timeout_seconds"`
	Env             map[string]string `yaml:"env"`
}

func LoadConsumerConfig(path string) (*ConsumerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read consumer config: %w", err)
	}

	var cfg ConsumerConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse consumer config: %w", err)
	}

	for i, c := range cfg.Consumers {
		if c.Name == "" {
			return nil, fmt.Errorf("consumer %d: name is required", i)
		}
		if c.Queue == "" {
			return nil, fmt.Errorf("consumer %q: queue is required", c.Name)
		}
		if c.Handler == "" {
			return nil, fmt.Errorf("consumer %q: handler is required", c.Name)
		}
		// Default type to "python" for backward compatibility
		if cfg.Consumers[i].Type == "" {
			cfg.Consumers[i].Type = "python"
		}
		switch cfg.Consumers[i].Type {
		case "python":
			if err := validatePythonHandler(c.Handler); err != nil {
				return nil, fmt.Errorf("consumer %q: %w", c.Name, err)
			}
		case "go":
			if err := validateGoHandler(c.Handler); err != nil {
				return nil, fmt.Errorf("consumer %q: %w", c.Name, err)
			}
		default:
			return nil, fmt.Errorf("consumer %q: unknown type %q (expected \"python\" or \"go\")", c.Name, c.Type)
		}
		if cfg.Consumers[i].BatchSize <= 0 {
			cfg.Consumers[i].BatchSize = 1
		}
		if cfg.Consumers[i].TimeoutSeconds <= 0 {
			cfg.Consumers[i].TimeoutSeconds = 60
		}
		if cfg.Consumers[i].DeadLetterQueue == "" {
			cfg.Consumers[i].DeadLetterQueue = c.Queue + "-dlq"
		}
		if cfg.Consumers[i].DeadLetterQueue == c.Queue {
			return nil, fmt.Errorf("consumer %q: dead-letter queue must differ from source queue", c.Name)
		}
		if cfg.Consumers[i].MaxReceiveCount <= 0 {
			cfg.Consumers[i].MaxReceiveCount = defaultMaxReceiveCount
		}
	}

	return &cfg, nil
}

// ConsumerManager manages background consumer goroutines.
type ConsumerManager struct {
	broker  *Broker
	workDir string // project root for uv run
	wg      sync.WaitGroup
}

func NewConsumerManager(broker *Broker, workDir string) *ConsumerManager {
	return &ConsumerManager{broker: broker, workDir: workDir}
}

// Start launches a goroutine per consumer entry. Returns when ctx is cancelled.
func (cm *ConsumerManager) Start(ctx context.Context, consumers []ConsumerEntry) {
	for _, c := range consumers {
		entry := c
		cm.wg.Add(1)
		go func() {
			defer cm.wg.Done()
			cm.pollLoop(ctx, entry)
		}()
	}
}

// Wait blocks until every selected consumer has observed cancellation.
func (cm *ConsumerManager) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		cm.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (cm *ConsumerManager) pollLoop(ctx context.Context, entry ConsumerEntry) {
	logger := log.With().Str("consumer", entry.Name).Str("queue", entry.Queue).Logger()
	logger.Info().Msg("Consumer started")

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Consumer stopping")
			return
		default:
		}

		queue := cm.broker.GetQueue(entry.Queue)
		if queue == nil {
			logger.Warn().Msg("Queue not found, retrying in 5s")
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		messages := cm.broker.ReceiveMessages(queue, entry.BatchSize, 5*time.Second)
		if len(messages) == 0 {
			continue
		}

		logger.Debug().Int("count", len(messages)).Msg("Received messages")
		visibility := time.Duration(entry.TimeoutSeconds)*time.Second + 30*time.Second
		for _, msg := range messages {
			cm.broker.ExtendMessageVisibility(queue, msg.ReceiptHandle, visibility)
		}

		event := buildLambdaEvent(messages)
		result, err := cm.invokeHandlerResult(ctx, entry, event)

		if err != nil {
			logger.Error().Err(err).Int("count", len(messages)).Msg("Handler failed; retaining every record")
			cm.settleBatch(logger, entry, queue, messages, nil, true)
			continue
		}
		failedIDs, err := validateBatchFailures(result, messages)
		if err != nil {
			logger.Error().Err(err).Int("count", len(messages)).Msg("Handler returned an invalid batch response; retaining every record")
			cm.settleBatch(logger, entry, queue, messages, nil, true)
			continue
		}
		cm.settleBatch(logger, entry, queue, messages, failedIDs, false)
	}
}

type batchItemFailure struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

type handlerBatchResult struct {
	BatchItemFailures []batchItemFailure `json:"batchItemFailures"`
}

func validateBatchFailures(result *handlerBatchResult, messages []*Message) (map[string]struct{}, error) {
	known := make(map[string]struct{}, len(messages))
	for _, msg := range messages {
		known[msg.ID] = struct{}{}
	}
	failed := make(map[string]struct{}, len(result.BatchItemFailures))
	for _, item := range result.BatchItemFailures {
		if item.ItemIdentifier == "" {
			return nil, fmt.Errorf("batch failure has an empty itemIdentifier")
		}
		if _, ok := known[item.ItemIdentifier]; !ok {
			return nil, fmt.Errorf("batch failure refers to unknown message %q", item.ItemIdentifier)
		}
		if _, duplicate := failed[item.ItemIdentifier]; duplicate {
			return nil, fmt.Errorf("batch failure repeats message %q", item.ItemIdentifier)
		}
		failed[item.ItemIdentifier] = struct{}{}
	}
	return failed, nil
}

func (cm *ConsumerManager) settleBatch(
	logger zerolog.Logger,
	entry ConsumerEntry,
	queue *Queue,
	messages []*Message,
	failedIDs map[string]struct{},
	failAll bool,
) {
	dlq := cm.broker.GetQueue(entry.DeadLetterQueue)
	deleted, retried, deadLettered := 0, 0, 0
	for _, msg := range messages {
		_, failed := failedIDs[msg.ID]
		if !failAll && !failed {
			if cm.broker.DeleteMessage(queue, msg.ReceiptHandle) {
				deleted++
			}
			continue
		}
		if msg.ReceiveCount >= entry.MaxReceiveCount && dlq != nil {
			if cm.broker.MoveMessage(queue, dlq, msg.ReceiptHandle) {
				deadLettered++
			}
			continue
		}
		if cm.broker.ExtendMessageVisibility(queue, msg.ReceiptHandle, retryVisibility(msg.ReceiveCount)) {
			retried++
		}
	}
	logger.Debug().
		Int("deleted", deleted).
		Int("retried", retried).
		Int("dead_lettered", deadLettered).
		Msg("Settled consumer batch")
}

// retryVisibility applies a small bounded exponential delay between explicit
// handler failures. The broker's existing visibility requeue loop performs the
// eventual retry; no second scheduler or message copy is introduced.
func retryVisibility(receiveCount int) time.Duration {
	if receiveCount < 1 {
		receiveCount = 1
	}
	delay := baseRetryVisibility
	for attempt := 1; attempt < receiveCount && delay < maxRetryVisibility; attempt++ {
		delay *= 2
	}
	if delay > maxRetryVisibility {
		return maxRetryVisibility
	}
	return delay
}

// buildLambdaEvent creates an SQS Lambda event from messages.
// Format: {"Records": [{"messageId": "...", "body": "...SNS envelope...", "receiptHandle": "..."}]}
func buildLambdaEvent(messages []*Message) map[string]interface{} {
	records := make([]map[string]interface{}, 0, len(messages))
	for _, msg := range messages {
		records = append(records, map[string]interface{}{
			"messageId":     msg.ID,
			"receiptHandle": msg.ReceiptHandle,
			"body":          msg.Body,
		})
	}
	return map[string]interface{}{
		"Records": records,
	}
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
// The binary reads JSON from stdin and writes a JSON result to stdout.
// Same contract as Python handlers: {"Records": [...]} in, {"processed": N, "errors": [...]} out.
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
