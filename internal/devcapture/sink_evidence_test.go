package devcapture

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSinkErrLeavesHealthyBorrowedWriterUsable(t *testing.T) {
	writer := &borrowedWriter{}
	sink := NewWriter(writer, "evidence")
	require.NoError(t, sink.Err())
	require.Error(t, sink.Append(make(chan int)))
	require.NoError(t, sink.Err(), "encoding rejection is not a storage failure")
	for range 2 {
		require.NoError(t, sink.Append(map[string]bool{"accepted": true}))
		require.NoError(t, sink.Err())
	}
	require.Equal(t, 2, bytes.Count(writer.Bytes(), []byte{'\n'}))
	require.Zero(t, writer.closeCalls)
	require.NoError(t, sink.Close())
	require.EqualError(t, sink.Err(), "evidence capture is closed")
	require.Zero(t, writer.closeCalls)
	_, err := writer.WriteString("borrowed writer remains open")
	require.NoError(t, err)
}

func TestSinkErrRetainsWriteAndSyncFailures(t *testing.T) {
	for _, kind := range []string{"short write", "write", "sync"} {
		t.Run(kind, func(t *testing.T) {
			failure := errors.New("evidence storage unavailable")
			var sink *Sink
			if kind == "sync" {
				sink = NewWriter(&bytes.Buffer{}, "evidence")
				sink.syncFile = func() error { return failure }
			} else {
				writer := &partialWriter{failure: failure}
				if kind == "short write" {
					writer.failure = nil
					failure = io.ErrShortWrite
				}
				sink = NewWriter(writer, "evidence")
			}
			first := sink.Append(map[string]bool{"first": true})
			require.ErrorIs(t, first, failure)
			require.Same(t, first, sink.Err())
			require.Same(t, first, sink.Append(map[string]bool{"later": true}))
			require.Same(t, first, sink.Err())
			require.ErrorIs(t, sink.Close(), failure)
			require.ErrorIs(t, sink.Err(), failure)
		})
	}
}

type evidenceGateWriter struct {
	started, release chan struct{}
	failure          error
}

func (writer evidenceGateWriter) Write([]byte) (int, error) {
	close(writer.started)
	<-writer.release
	return 0, writer.failure
}

func TestSinkErrJoinsInProgressAppendBeforeReportingHealth(t *testing.T) {
	failure := errors.New("admitted evidence write failed")
	writer := evidenceGateWriter{started: make(chan struct{}), release: make(chan struct{}), failure: failure}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(writer.release) }) })
	sink := NewWriter(writer, "evidence")
	appended := make(chan error, 1)
	go func() { appended <- sink.Append(map[string]bool{"pending": true}) }()
	<-writer.started
	observed := make(chan error, 1)
	go func() { observed <- sink.Err() }()
	select {
	case err := <-observed:
		t.Fatalf("evidence health returned before append completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(writer.release) })
	require.ErrorIs(t, <-appended, failure)
	require.ErrorIs(t, <-observed, failure)
}

func TestSinkErrReportsUnavailableAndClosedCapture(t *testing.T) {
	var missing *Sink
	require.EqualError(t, missing.Err(), "capture is not configured")
	require.EqualError(t, NewWriter(nil, "evidence").Err(), "evidence capture writer is not configured")
	failure := errors.New("owned file close failed")
	sink := NewWriter(&bytes.Buffer{}, "evidence")
	sink.closeFile = func() error { return failure }
	require.NoError(t, sink.Err())
	require.ErrorIs(t, sink.Close(), failure)
	require.ErrorIs(t, sink.Err(), failure)
}
