package localexec

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
)

// TrackedOutput observes the actual os/exec stream copy independently of the
// command's exit status. Cmd.Wait can suppress ErrWaitDelay behind ExitError or
// cancellation, even when it forcibly closes a retained output pipe. One copy
// goroutine owns this wrapper; inspect Err only after successful Start and Wait.
// A failed Start did not admit a copy and must not be inspected as join evidence.
type TrackedOutput struct {
	writer   io.Writer
	finished bool
	copyErr  error
}

func NewTrackedOutput(writer io.Writer) *TrackedOutput { return &TrackedOutput{writer: writer} }

// NewTrackedOutputs preserves os/exec's shared pipe when its original stdout
// and stderr destinations compare equal. Noncomparable writers remain separate.
func NewTrackedOutputs(stdout, stderr io.Writer) (*TrackedOutput, *TrackedOutput) {
	first := NewTrackedOutput(stdout)
	firstValue, secondValue := reflect.ValueOf(stdout), reflect.ValueOf(stderr)
	if firstValue.IsValid() && secondValue.IsValid() && firstValue.Comparable() && secondValue.Comparable() && firstValue.Type() == secondValue.Type() && stdout == stderr {
		return first, first
	}
	return first, NewTrackedOutput(stderr)
}

func (output *TrackedOutput) Write(data []byte) (int, error) { return output.writer.Write(data) }

// ReadFrom is selected by os/exec's io.Copy from its pipe. The writer-only
// adapter avoids recursion and distinguishes writer errors from forced reads.
func (output *TrackedOutput) ReadFrom(reader io.Reader) (int64, error) {
	writer := &outputCopyWriter{writer: output.writer}
	count, err := io.Copy(writer, reader)
	output.finished = true
	output.copyErr = err
	if writer.err == nil && errors.Is(err, os.ErrClosed) {
		output.copyErr = errors.Join(exec.ErrWaitDelay, err)
	}
	return count, err
}

func (output *TrackedOutput) Err() error {
	if !output.finished {
		return errors.New("subprocess output copy completion was not observed")
	}
	if output.copyErr != nil {
		return fmt.Errorf("subprocess output copy did not finish normally: %w", output.copyErr)
	}
	return nil
}

type outputCopyWriter struct {
	writer io.Writer
	err    error
}

func (writer *outputCopyWriter) Write(data []byte) (int, error) {
	count, err := writer.writer.Write(data)
	writer.err = err
	if count != len(data) && err == nil {
		writer.err = io.ErrShortWrite
	}
	return count, err
}
