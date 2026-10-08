// Opt-in private diagnostics. Native responses and redacted AsyncRecord remain
// separate; handler logs may contain credentials and have no default sink.
package lambda

import (
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"time"
	"unicode/utf8"

	"github.com/lyeith/eventbus/internal/devcapture"
)

const maxDiagnosticFailure = 64 << 10
const maxDiagnosticDetail = 8 << 10

type DevDiagnosticsConfig struct {
	LogPath      string                 `yaml:"log_path"`
	PythonStacks *DevPythonStacksConfig `yaml:"python_stacks,omitempty"`
}

func validateDevDiagnostics(config *DevDiagnosticsConfig) error {
	if config != nil && (config.LogPath == "" || config.LogPath == "-") {
		return errors.New("dev_diagnostics.log_path requires a private owned file")
	}
	if config != nil && config.PythonStacks != nil {
		return validatePythonStacks(config.PythonStacks)
	}
	return nil
}

type diagnosticOutput struct {
	Data      string `json:"data,omitempty"`
	Encoding  string `json:"encoding"`
	Bytes     int64  `json:"bytes"`
	Truncated bool   `json:"truncated"`
}

func diagnosticBytes(data []byte, total int64) diagnosticOutput {
	output := diagnosticOutput{Encoding: "utf8", Bytes: total, Truncated: total > int64(len(data))}
	if utf8.Valid(data) {
		output.Data = string(data)
	} else {
		output.Encoding, output.Data = "base64", base64.StdEncoding.EncodeToString(data)
	}
	return output
}

type invocationDiagnosticRecord struct {
	SchemaVersion             string              `json:"schema_version"`
	RequestID                 string              `json:"request_id"`
	FunctionName              string              `json:"function_name"`
	FunctionARN               string              `json:"function_arn"`
	Runtime                   string              `json:"runtime"`
	InvocationType            string              `json:"invocation_type"`
	Attempt                   int                 `json:"attempt"`
	StartedAt                 time.Time           `json:"started_at"`
	CompletedAt               time.Time           `json:"completed_at"`
	State                     InvocationState     `json:"state"`
	FunctionError             bool                `json:"function_error"`
	OwnershipConfirmed        bool                `json:"ownership_confirmed"`
	StdoutIsResponse          bool                `json:"stdout_is_response,omitempty"`
	Stdout                    *diagnosticOutput   `json:"stdout,omitempty"`
	Stderr                    diagnosticOutput    `json:"stderr"`
	Tail                      diagnosticOutput    `json:"tail"`
	FunctionDiagnostic        *diagnosticOutput   `json:"function_diagnostic,omitempty"`
	ProcessError              string              `json:"process_error,omitempty"`
	ContextError              string              `json:"context_error,omitempty"`
	TerminationCause          string              `json:"termination_cause,omitempty"`
	ElapsedMS                 float64             `json:"elapsed_ms"`
	ConfiguredTimeoutMS       int64               `json:"configured_timeout_ms"`
	NativeResponseSynthesized bool                `json:"native_response_synthesized,omitempty"`
	PythonStack               *pythonStackSummary `json:"python_stack,omitempty"`
	OwnershipError            string              `json:"ownership_error,omitempty"`
	DetailTruncated           bool                `json:"detail_truncated,omitempty"`
}

type invocationDiagnostics struct {
	stdout, stderr                      []byte
	stdoutBytes, stderrBytes, tailBytes int64
	processError                        string
}

func resolvedLogPath(path, root string) string {
	if path == "" || path == "-" {
		return path
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	return filepath.Clean(path)
}

func (service *Service) configureDiagnostics(config *DevDiagnosticsConfig, async *DevAsyncConfig, root string) error {
	if config == nil {
		return nil
	}
	path := resolvedLogPath(config.LogPath, root)
	asyncPath := ""
	if async != nil {
		asyncPath = resolvedLogPath(async.LogPath, root)
	}
	checkConflict := func() error {
		conflict, err := devcapture.PathsConflict(path, asyncPath)
		if err != nil {
			return err
		}
		if conflict {
			return errors.New("Lambda private diagnostics and redacted async capture must use separate files")
		}
		return nil
	}
	if err := checkConflict(); err != nil {
		return err
	}
	sink, err := devcapture.OpenPrivate(path, "Lambda private diagnostics")
	if err != nil {
		return err
	}
	// Opening makes missing names under symlink-aliased parents observable to
	// SameFile. Refuse before async capture construction or any invocation.
	if err := checkConflict(); err != nil {
		return errors.Join(err, sink.Close())
	}
	service.diagnosticCapture, service.diagnosticPath = sink, path
	if config.PythonStacks != nil {
		copy := *config.PythonStacks
		if copy.DeadlineLead == 0 {
			copy.DeadlineLead = 200 * time.Millisecond
		}
		service.pythonStacks = &copy
	}
	return nil
}

// DevDiagnosticsPath is immutable local configuration, never a native response
// or public manifest field. App composition uses it for capture alias checks.
func (service *Service) DevDiagnosticsPath() string {
	if service == nil {
		return ""
	}
	return service.diagnosticPath
}

// DevEvidence reports terminal capture uncertainty, never live accepted work.
// Actual invocation lifetimes are counted through the optional Activity port.
func (service *Service) DevEvidence() error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	err := errors.Join(service.asyncEvidenceErr, service.diagnosticEvidenceErr, service.invocationEvidenceErr)
	sink := service.diagnosticCapture
	service.mu.Unlock()
	if sink != nil {
		// Sink.Err intentionally reports any closed sink as unavailable. The
		// service owns closure and distinguishes a completed healthy close from
		// an append/close failure. Serialize this check with that actual close
		// so a concurrent health check cannot observe a publication gap.
		service.diagnosticMu.Lock()
		if service.diagnosticClosed {
			err = errors.Join(err, service.diagnosticCloseErr)
		} else {
			err = errors.Join(err, sink.Err())
		}
		service.diagnosticMu.Unlock()
	}
	return err
}

func boundedDiagnosticDetail(value string) (string, bool) {
	if len(value) <= maxDiagnosticDetail {
		return value, false
	}
	return value[:maxDiagnosticDetail], true
}

func (service *Service) captureDiagnostics(entry executableFunction, input invocation, result invocationResult, asynchronous bool, started time.Time, completion diagnosticCompletion) error {
	if service.diagnosticCapture == nil {
		return nil
	}
	identity := invocationMetadata(entry, input)
	logs := result.logs
	if len(logs) > 4096 {
		logs = logs[len(logs)-4096:]
	}
	record := invocationDiagnosticRecord{
		SchemaVersion: "eventbus.lambda.invocation-diagnostic.v1",
		RequestID:     identity.RequestID, FunctionName: identity.FunctionName, FunctionARN: identity.FunctionARN,
		Runtime: entry.runtime, InvocationType: "RequestResponse", Attempt: identity.Attempt,
		StartedAt: started.UTC(), CompletedAt: completion.at.UTC(), State: result.state,
		FunctionError: result.functionError, OwnershipConfirmed: result.ownershipErr == nil,
		Stderr: diagnosticBytes(result.diagnostics.stderr, result.diagnostics.stderrBytes),
		Tail:   diagnosticBytes(logs, result.diagnostics.tailBytes), ContextError: completion.contextError,
		TerminationCause: completion.cause, ElapsedMS: float64(completion.at.Sub(started)) / float64(time.Millisecond),
		ConfiguredTimeoutMS: entry.timeout.Milliseconds(), NativeResponseSynthesized: completion.synthesized,
	}
	if input.pythonStacks != nil {
		record.PythonStack = input.pythonStacks.summary()
	}
	if asynchronous {
		record.InvocationType = "Event"
	}
	if entry.runtime == "command" {
		record.StdoutIsResponse = true // Successful response payload is never copied.
	} else {
		stdout := diagnosticBytes(result.diagnostics.stdout, result.diagnostics.stdoutBytes)
		record.Stdout = &stdout
	}
	if result.functionError && (!completion.synthesized || completion.cause == "function_timeout") {
		failure := result.payload
		if len(failure) > maxDiagnosticFailure {
			failure = failure[:maxDiagnosticFailure]
		}
		output := diagnosticBytes(failure, int64(len(result.payload)))
		record.FunctionDiagnostic = &output
	}
	var truncated bool
	record.ProcessError, truncated = boundedDiagnosticDetail(result.diagnostics.processError)
	record.DetailTruncated = truncated
	if result.ownershipErr != nil {
		record.OwnershipError, truncated = boundedDiagnosticDetail(result.ownershipErr.Error())
		record.DetailTruncated = record.DetailTruncated || truncated
	}
	return service.appendDiagnostic(record)
}

func (service *Service) appendDiagnostic(record any) error {
	if err := service.diagnosticCapture.Append(record); err != nil {
		service.mu.Lock()
		if service.diagnosticEvidenceErr == nil {
			service.diagnosticEvidenceErr = fmt.Errorf("Lambda private diagnostics unavailable: %w", err)
		}
		evidenceErr := service.diagnosticEvidenceErr
		service.mu.Unlock()
		return evidenceErr
	}
	return nil
}
