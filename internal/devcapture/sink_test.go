package devcapture

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSinkConcurrentAppendVisibleBeforeCloseAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "capture.jsonl")
	sink, err := Open(path, "test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sink.Close() })
	var callers sync.WaitGroup
	failures := make(chan error, 32)
	for index := range 32 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			failures <- sink.Append(map[string]any{"index": index, "text": "hello\nworld"})
		}()
	}
	callers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	seen := map[int]bool{}
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		var record struct {
			Index int    `json:"index"`
			Text  string `json:"text"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		require.Equal(t, "hello\nworld", record.Text)
		require.False(t, seen[record.Index], "duplicate record")
		seen[record.Index] = true
	}
	require.NoError(t, scanner.Err())
	require.Len(t, seen, 32)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	info, err = os.Stat(filepath.Dir(path))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0700), info.Mode().Perm())
	require.NoError(t, sink.Close())
	require.NoError(t, sink.Close())
	require.EqualError(t, sink.Append(map[string]any{"late": true}), "test capture is closed")

	next, err := Open(path, "test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = next.Close() })
	require.NoError(t, next.Append(map[string]any{"restart": true}))
	require.NoError(t, next.Close())
	final, err := os.ReadFile(path)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(final, content))
	require.Equal(t, 33, bytes.Count(final, []byte{'\n'}))
}

func TestSinkRefusesIncompleteFileWithoutTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.jsonl")
	original := []byte(`{"unfinished":`)
	require.NoError(t, os.WriteFile(path, original, 0600))
	_, err := Open(path, "SES")
	require.EqualError(t, err, "SES log ends with an incomplete record; repair it or select a new log")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, original, after)
}

func TestSinkRejectsMissingAndNonRegularPaths(t *testing.T) {
	_, err := Open("", "SNS")
	require.EqualError(t, err, "SNS log path must not be empty")
	_, err = Open(os.DevNull, "SNS")
	require.EqualError(t, err, "SNS log must be a regular file; use '-' for stdout")
}

type partialWriter struct {
	calls   int
	failure error
}

func (writer *partialWriter) Write(data []byte) (int, error) {
	writer.calls++
	return len(data) / 2, writer.failure
}

func TestSinkPartialWriteIsTerminal(t *testing.T) {
	for _, failure := range []error{nil, errors.New("storage unavailable")} {
		writer := &partialWriter{failure: failure}
		sink := NewWriter(writer, "test")
		first := sink.Append(map[string]any{"first": true})
		if failure == nil {
			require.ErrorIs(t, first, io.ErrShortWrite)
		} else {
			require.ErrorIs(t, first, failure)
		}
		require.ErrorContains(t, first, "append test capture: ")
		require.Same(t, first, sink.Append(map[string]any{"second": true}))
		require.Equal(t, 1, writer.calls)
		require.ErrorIs(t, sink.Close(), first)
	}
}

func TestSinkSyncFailureAndCloseFailureAreRetained(t *testing.T) {
	var writer bytes.Buffer
	sink := NewWriter(&writer, "test")
	syncFailure, closeFailure := errors.New("sync failed"), errors.New("close failed")
	syncCalls, closeCalls := 0, 0
	sink.syncFile = func() error {
		syncCalls++
		return syncFailure
	}
	sink.closeFile = func() error {
		closeCalls++
		return closeFailure
	}
	first := sink.Append(map[string]any{"first": true})
	require.ErrorIs(t, first, syncFailure)
	require.EqualError(t, first, "append test capture: sync failed")
	require.Same(t, first, sink.Append(map[string]any{"second": true}))
	require.Equal(t, 1, syncCalls)
	require.Equal(t, 1, bytes.Count(writer.Bytes(), []byte{'\n'}))

	failures := make(chan error, 32)
	var callers sync.WaitGroup
	for range 32 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			failures <- sink.Close()
		}()
	}
	callers.Wait()
	close(failures)
	retained := sink.Close()
	require.ErrorIs(t, retained, syncFailure)
	require.ErrorIs(t, retained, closeFailure)
	for err := range failures {
		require.Same(t, retained, err)
	}
	require.Equal(t, 1, closeCalls)
}

type borrowedWriter struct {
	bytes.Buffer
	closeCalls int
}

func (writer *borrowedWriter) Close() error {
	writer.closeCalls++
	return nil
}

func TestSinkBorrowsWriterAndEncodeFailureDoesNotPoisonIt(t *testing.T) {
	writer := &borrowedWriter{}
	sink := NewWriter(writer, "test")
	require.Error(t, sink.Append(make(chan int)))
	require.Zero(t, writer.Len())
	require.NoError(t, sink.Append(map[string]any{"record": true}))
	require.NoError(t, sink.Close())
	require.Zero(t, writer.closeCalls)
	_, err := writer.WriteString("still open")
	require.NoError(t, err)
}

func TestSinkOwnsOpenedFile(t *testing.T) {
	sink, err := Open(filepath.Join(t.TempDir(), "owned.jsonl"), "test")
	require.NoError(t, err)
	file := sink.writer.(*os.File)
	require.NoError(t, sink.Close())
	_, err = file.WriteString("closed")
	require.ErrorIs(t, err, os.ErrClosed)
}
