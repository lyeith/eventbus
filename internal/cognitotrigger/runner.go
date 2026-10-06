package cognitotrigger

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	maxEventBytes      = 1 << 20
	maxResultBytes     = 1 << 20
	maxDiagnosticBytes = 64 << 10
	processWaitDelay   = time.Second
)

//go:embed wrapper.mjs
var nodeWrapper string

type ErrorKind string

const (
	Timeout         ErrorKind = "timeout"
	HandlerFailure  ErrorKind = "handler_failure"
	InvalidResponse ErrorKind = "invalid_response"
	NotConfigured   ErrorKind = "not_configured"
	Closed          ErrorKind = "closed"
)

// InvocationError identifies execution failures without exposing the event,
// private challenge parameters, handler output or diagnostic text.
type InvocationError struct {
	Kind    ErrorKind
	Trigger string
	Cause   error
}

func (e *InvocationError) Error() string {
	return fmt.Sprintf("Cognito %s trigger: %s", e.Trigger, e.Kind)
}
func (e *InvocationError) Unwrap() error       { return e.Cause }
func (e *InvocationError) FailureKind() string { return string(e.Kind) }

type executableEntry struct {
	module, exported string
	timeout          time.Duration
	env              map[string]string
}

// Runner owns every admitted invocation until its process group is terminated
// and the directly launched child is reaped. Configuration is immutable.
type Runner struct {
	node, workDir string
	pools         map[string]map[string]executableEntry
	mu            sync.Mutex
	closed        bool
	next          uint64
	active        map[uint64]context.CancelFunc
	inflight      sync.WaitGroup
	done          chan struct{}
}

// New resolves the executable and handler files at startup. It never falls back
// to built-in challenge logic or an unconfigured handler.
func New(config *Config, workDir string) (*Runner, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := processGroupsSupported(); err != nil {
		return nil, err
	}
	directory, err := filepath.Abs(workDir)
	if err != nil {
		return nil, fmt.Errorf("Cognito trigger work directory: %w", err)
	}
	info, err := os.Stat(directory)
	if err != nil || !info.IsDir() {
		return nil, errors.New("Cognito trigger work directory must exist")
	}
	node := config.Node
	if node == "" {
		node = "node"
	}
	if strings.ContainsAny(node, "/\\") && !filepath.IsAbs(node) {
		node = filepath.Join(directory, node)
	}
	node, err = exec.LookPath(node)
	if err != nil {
		return nil, fmt.Errorf("Cognito trigger Node executable: %w", err)
	}
	node, err = filepath.Abs(node)
	if err != nil {
		return nil, err
	}
	runner := &Runner{
		node: node, workDir: directory, pools: make(map[string]map[string]executableEntry),
		active: make(map[uint64]context.CancelFunc), done: make(chan struct{}),
	}
	for poolID, pool := range config.Pools {
		entries := make(map[string]executableEntry)
		for name, entry := range pool.entries() {
			module, exported, _ := handlerReference(entry.Handler)
			if !filepath.IsAbs(module) {
				module = filepath.Join(directory, module)
			}
			info, err := os.Stat(module)
			if err != nil || !info.Mode().IsRegular() {
				return nil, fmt.Errorf("Cognito trigger pool %q %s module must be a readable regular file", poolID, name)
			}
			file, err := os.Open(module)
			if err != nil {
				return nil, fmt.Errorf("Cognito trigger pool %q %s module cannot be read", poolID, name)
			}
			file.Close()
			seconds := entry.TimeoutSeconds
			if seconds == 0 {
				seconds = defaultTimeoutSeconds
			}
			entries[name] = executableEntry{module: module, exported: exported, timeout: time.Duration(seconds) * time.Second, env: maps.Clone(entry.Env)}
		}
		runner.pools[poolID] = entries
	}
	return runner, nil
}

func (r *Runner) Supports(poolID string) bool {
	if r == nil {
		return false
	}
	_, configured := r.pools[poolID]
	return configured
}

func (r *Runner) SupportsTrigger(poolID, name string) bool {
	if r == nil {
		return false
	}
	_, configured := r.pools[poolID][name]
	return configured
}

func (r *Runner) Invoke(ctx context.Context, poolID, name string, event map[string]any) (map[string]any, error) {
	if !r.SupportsTrigger(poolID, name) {
		return nil, &InvocationError{Kind: NotConfigured, Trigger: name}
	}
	entry := r.pools[poolID][name]
	execution, cancel := context.WithTimeout(ctx, entry.timeout)
	defer cancel()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, &InvocationError{Kind: Closed, Trigger: name}
	}
	r.next++
	id := r.next
	r.active[id] = cancel
	r.inflight.Add(1)
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.active, id)
		r.mu.Unlock()
		r.inflight.Done()
	}()
	input, err := json.Marshal(event)
	if err != nil || len(input) > maxEventBytes {
		return nil, &InvocationError{Kind: InvalidResponse, Trigger: name, Cause: err}
	}
	command := exec.CommandContext(execution, r.node,
		"--input-type=module", "--eval", nodeWrapper, "--",
		entry.module, entry.exported, fmt.Sprint(entry.timeout.Milliseconds()))
	command.Dir = r.workDir
	command.Env = processEnvironment(entry.env)
	command.Stdin = bytes.NewReader(input)
	stdout := &boundedOutput{limit: maxResultBytes}
	stderr := &boundedOutput{limit: maxDiagnosticBytes}
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = processWaitDelay
	ownProcessGroup(command)
	err = command.Run()
	cleanupErr := stopProcessGroup(command)
	if stdout.overflow || stderr.overflow {
		return nil, &InvocationError{Kind: InvalidResponse, Trigger: name, Cause: errors.New("trigger output limit exceeded")}
	}

	if stderr.Len() > 0 {
		// Diagnostics are app-owned. Never log event JSON or successful results,
		// which contain private challenge parameters.
		log.Debug().Str("trigger", name).Str("stderr", stderr.String()).Msg("Cognito trigger diagnostics")
	}
	if execution.Err() != nil {
		return nil, &InvocationError{Kind: Timeout, Trigger: name, Cause: execution.Err()}
	}
	if cleanupErr != nil {
		return nil, &InvocationError{Kind: HandlerFailure, Trigger: name, Cause: cleanupErr}
	}
	if err != nil {
		kind := HandlerFailure
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 21 {
			kind = InvalidResponse
		}
		return nil, &InvocationError{Kind: kind, Trigger: name, Cause: err}
	}
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, &InvocationError{Kind: InvalidResponse, Trigger: name}
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, &InvocationError{Kind: InvalidResponse, Trigger: name}
	}
	if response, ok := result["response"].(map[string]any); !ok || response == nil {
		return nil, &InvocationError{Kind: InvalidResponse, Trigger: name}
	}
	return result, nil
}

// Close stops admission, cancels active process groups and waits for every
// invocation's cleanup. A caller timeout does not cancel the cleanup owner.
func (r *Runner) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	first := !r.closed
	if first {
		r.closed = true
		for _, cancel := range r.active {
			cancel()
		}
	}
	r.mu.Unlock()
	if first {
		go func() { r.inflight.Wait(); close(r.done) }()
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type boundedOutput struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func (b *boundedOutput) Len() int       { return b.buffer.Len() }
func (b *boundedOutput) String() string { return b.buffer.String() }
func (b *boundedOutput) Bytes() []byte  { return b.buffer.Bytes() }

func (b *boundedOutput) Write(data []byte) (int, error) {
	size := len(data)
	remaining := b.limit - b.Len()
	if len(data) > remaining {
		b.overflow = true
		data = data[:remaining]
	}
	_, _ = b.buffer.Write(data)
	return size, nil
}

var inheritedEnvironment = []string{"PATH", "LANG", "LC_ALL", "SSL_CERT_DIR", "SSL_CERT_FILE", "TMPDIR", "TMP", "TEMP"}

func processEnvironment(declared map[string]string) []string {
	env := map[string]string{
		"AWS_EC2_METADATA_DISABLED":   "true",
		"AWS_SHARED_CREDENTIALS_FILE": os.DevNull,
		"AWS_CONFIG_FILE":             os.DevNull,
		"NO_PROXY":                    "*",
	}
	for _, name := range inheritedEnvironment {
		if value, exists := os.LookupEnv(name); exists {
			env[name] = value
		}
	}
	for name, value := range declared {
		env[name] = value
	}
	keys := make([]string, 0, len(env))
	for name := range env {
		keys = append(keys, name)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, name := range keys {
		result = append(result, name+"="+env[name])
	}
	return result
}
