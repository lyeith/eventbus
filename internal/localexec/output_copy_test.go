package localexec

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestTrackedOutputCopiesToBoundedDestination(t *testing.T) {
	retained := NewBoundedOutput(4)
	output := NewTrackedOutput(retained)
	count, err := output.ReadFrom(strings.NewReader("long output"))
	if err != nil || count != 11 || retained.String() != "long" || !retained.Overflowed() || output.Err() != nil {
		t.Fatalf("copy changed bounded output: count=%d err=%v retained=%q overflow=%t ownership=%v", count, err, retained.String(), retained.Overflowed(), output.Err())
	}
	empty := NewTrackedOutput(io.Discard)
	_, err = empty.ReadFrom(strings.NewReader(""))
	if err != nil || empty.Err() != nil {
		t.Fatalf("empty EOF is a complete copy: %v %v", err, empty.Err())
	}
}

type outputErrorWriter struct{ err error }

func (writer outputErrorWriter) Write([]byte) (int, error) { return 0, writer.err }

func TestTrackedOutputDoesNotCallWriterFailureWaitDelay(t *testing.T) {
	for _, failure := range []error{os.ErrClosed, io.ErrShortWrite} {
		output := NewTrackedOutput(outputErrorWriter{err: failure})
		_, err := output.ReadFrom(strings.NewReader("output"))
		if !errors.Is(err, failure) || !errors.Is(output.Err(), failure) || errors.Is(output.Err(), exec.ErrWaitDelay) {
			t.Fatalf("writer failure was reclassified as a forced pipe close: copy=%v evidence=%v", err, output.Err())
		}
	}
}

func TestTrackedOutputUnobservedCopyCannotAttestEOF(t *testing.T) {
	output := NewTrackedOutput(io.Discard)
	if output.Err() == nil {
		t.Fatal("a stream whose copying was never observed must not attest completion")
	}
}

type outputSliceWriter []byte

func (outputSliceWriter) Write(data []byte) (int, error) { return len(data), nil }

func TestTrackedOutputsPreserveComparableWriterIdentity(t *testing.T) {
	shared := new(strings.Builder)
	stdout, stderr := NewTrackedOutputs(shared, shared)
	if stdout != stderr {
		t.Fatal("equal original destinations must share one native pipe/copy owner")
	}
	stdout, stderr = NewTrackedOutputs(new(strings.Builder), new(strings.Builder))
	if stdout == stderr {
		t.Fatal("independent destinations must keep separate copy owners")
	}
	noncomparable := outputSliceWriter{}
	stdout, stderr = NewTrackedOutputs(noncomparable, noncomparable)
	if stdout == stderr {
		t.Fatal("noncomparable interfaces must retain os/exec's separate-pipe behavior")
	}
}

type outputInterfaceWriter struct{ payload any }

func (outputInterfaceWriter) Write(data []byte) (int, error) { return len(data), nil }

func TestTrackedOutputsHandleNestedNoncomparableWriterValues(t *testing.T) {
	writer := outputInterfaceWriter{payload: []byte("noncomparable")}
	stdout, stderr := NewTrackedOutputs(writer, writer)
	if stdout == stderr {
		t.Fatal("an interface field with a slice cannot share os/exec's native pipe")
	}
}
