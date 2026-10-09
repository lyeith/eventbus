package devcapture

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The gate pauses before the actual file Sync. A returned successful Append
// therefore cannot be explained by a write that has only reached the page cache.
type captureSyncAttempt struct {
	records int
	release chan struct{}
}

func installCaptureSyncGate(t *testing.T, sink *Sink, failureAt int, failure error) <-chan captureSyncAttempt {
	t.Helper()
	attempts := make(chan captureSyncAttempt, 8)
	releaseAll := make(chan struct{})
	actualSync := sink.syncFile
	file := sink.writer.(*os.File)
	calls := 0
	sink.syncFile = func() error {
		calls++
		data, err := os.ReadFile(file.Name())
		if err != nil {
			return err
		}
		attempt := captureSyncAttempt{bytes.Count(data, []byte{'\n'}), make(chan struct{})}
		attempts <- attempt
		select {
		case <-attempt.release:
		case <-releaseAll:
		}
		if err := actualSync(); err != nil {
			return err
		}
		if calls == failureAt {
			return failure
		}
		return nil
	}
	t.Cleanup(func() { close(releaseAll); _ = sink.Close() })
	return attempts
}

func waitCaptureState(t *testing.T, sink *Sink, matches func(*Sink) bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return matches(sink)
	}, 5*time.Second, time.Millisecond)
}

func nextCaptureSync(t *testing.T, attempts <-chan captureSyncAttempt) captureSyncAttempt {
	t.Helper()
	select {
	case attempt := <-attempts:
		return attempt
	case <-time.After(5 * time.Second):
		t.Fatal("capture never reached its next Sync")
		return captureSyncAttempt{}
	}
}

func assertCaptureWaiting(t *testing.T, result <-chan error, description string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s returned before its durable/join boundary: %v", description, err)
	default:
	}
}

func appendCaptureAsync(sink *Sink, record any) <-chan error {
	result := make(chan error, 1)
	go func() { result <- sink.Append(record) }()
	return result
}

func TestSinkGroupCommitAcknowledgesOnlyCoveredRecordsAndJoinsClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "group.jsonl")
	sink, err := OpenPrivate(path, "group")
	require.NoError(t, err)
	attempts := installCaptureSyncGate(t, sink, 0, nil)
	results := []<-chan error{appendCaptureAsync(sink, map[string]int{"index": 0})}
	first := nextCaptureSync(t, attempts)
	require.Equal(t, 1, first.records)
	assertCaptureWaiting(t, results[0], "first Append")
	for index := 1; index < 4; index++ {
		results = append(results, appendCaptureAsync(sink, map[string]int{"index": index}))
		want := index
		waitCaptureState(t, sink, func(s *Sink) bool { return len(s.pending) == want })
	}
	health := make(chan error, 1)
	go func() { health <- sink.Err() }()
	closeStarted, closeRelease := make(chan struct{}), make(chan struct{})
	var releaseClose sync.Once
	t.Cleanup(func() { releaseClose.Do(func() { close(closeRelease) }) })
	actualClose := sink.closeFile
	sink.closeFile = func() error {
		close(closeStarted)
		<-closeRelease
		return actualClose()
	}
	closed := make(chan error, 1)
	go func() { closed <- sink.Close() }()
	waitCaptureState(t, sink, func(s *Sink) bool { return s.closing })
	secondClose := make(chan error, 1)
	go func() { secondClose <- sink.Close() }()
	require.EqualError(t, sink.Append(map[string]int{"index": 4}), "group capture is closed")
	assertCaptureWaiting(t, health, "Err")
	assertCaptureWaiting(t, closed, "Close")
	assertCaptureWaiting(t, secondClose, "concurrent Close")
	close(first.release)
	require.NoError(t, <-results[0], "first group can return while the next group is undurable")
	second := nextCaptureSync(t, attempts)
	require.Equal(t, 4, second.records, "one Sync must cover all three already-waiting records")
	for _, result := range results[1:] {
		assertCaptureWaiting(t, result, "grouped Append")
	}
	assertCaptureWaiting(t, health, "Err")
	assertCaptureWaiting(t, closed, "Close")
	select {
	case <-closeStarted:
		t.Fatal("owned file closed before pending records were synchronized")
	default:
	}
	close(second.release)
	for _, result := range results[1:] {
		require.NoError(t, <-result)
	}
	<-closeStarted
	assertCaptureWaiting(t, closed, "Close during owned file close")
	assertCaptureWaiting(t, secondClose, "concurrent Close during owned file close")
	releaseClose.Do(func() { close(closeRelease) })
	require.NoError(t, <-closed)
	require.NoError(t, <-secondClose)
	// Err started before Close must join close too, rather than attest an open sink.
	require.EqualError(t, <-health, "group capture is closed")
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{\"index\":0}\n{\"index\":1}\n{\"index\":2}\n{\"index\":3}\n", string(data))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	_, err = sink.writer.(*os.File).WriteString("after close")
	require.ErrorIs(t, err, os.ErrClosed)
	select {
	case extra := <-attempts:
		t.Fatalf("unexpected extra Sync covering %d records", extra.records)
	default:
	}
}

func TestSinkGroupSyncFailureFailsCoveredAndPendingRecords(t *testing.T) {
	sink, err := OpenPrivate(filepath.Join(t.TempDir(), "sync-failed.jsonl"), "group")
	require.NoError(t, err)
	failure := errors.New("group Sync failed")
	attempts := installCaptureSyncGate(t, sink, 2, failure)
	firstResult := appendCaptureAsync(sink, map[string]int{"index": 0})
	first := nextCaptureSync(t, attempts)
	var failed []<-chan error
	for index := 1; index < 4; index++ {
		failed = append(failed, appendCaptureAsync(sink, map[string]int{"index": index}))
		want := index
		waitCaptureState(t, sink, func(s *Sink) bool { return len(s.pending) == want })
	}
	close(first.release)
	require.NoError(t, <-firstResult)
	second := nextCaptureSync(t, attempts)
	require.Equal(t, 4, second.records)
	failed = append(failed, appendCaptureAsync(sink, map[string]int{"index": 4}))
	waitCaptureState(t, sink, func(s *Sink) bool { return len(s.pending) == 1 })
	health := make(chan error, 1)
	go func() { health <- sink.Err() }()
	assertCaptureWaiting(t, health, "Err")
	for _, result := range failed {
		assertCaptureWaiting(t, result, "failed-group Append")
	}
	close(second.release)
	firstFailure := <-failed[0]
	require.ErrorIs(t, firstFailure, failure)
	for _, result := range failed[1:] {
		require.Same(t, firstFailure, <-result)
	}
	require.Same(t, firstFailure, <-health)
	require.Same(t, firstFailure, sink.Append(map[string]int{"index": 5}))
	require.ErrorIs(t, sink.Close(), failure)
	data, err := os.ReadFile(sink.writer.(*os.File).Name())
	require.NoError(t, err)
	require.Equal(t, 4, bytes.Count(data, []byte{'\n'}), "pending record must never be written after failed Sync")
}

type captureFailingFile struct {
	file         *os.File
	writeStarted chan struct{}
	writeRelease chan struct{}
	failure      error
	calls        int
}

func (writer *captureFailingFile) Write(data []byte) (int, error) {
	writer.calls++
	if writer.calls != 3 {
		return writer.file.Write(data)
	}
	close(writer.writeStarted)
	<-writer.writeRelease
	count, err := writer.file.Write(data[:len(data)/2])
	if err != nil {
		return count, err
	}
	return count, writer.failure
}

func TestSinkGroupPartialWriteFailsWholeGroupWithoutAnotherSync(t *testing.T) {
	for _, failure := range []error{nil, errors.New("group write unavailable")} {
		t.Run(fmt.Sprint(failure), func(t *testing.T) {
			sink, err := OpenPrivate(filepath.Join(t.TempDir(), "write-failed.jsonl"), "group")
			require.NoError(t, err)
			attempts := installCaptureSyncGate(t, sink, 0, nil)
			actualFile := sink.writer.(*os.File)
			writer := &captureFailingFile{file: actualFile, writeStarted: make(chan struct{}), writeRelease: make(chan struct{}), failure: failure}
			var releaseWrite sync.Once
			t.Cleanup(func() { releaseWrite.Do(func() { close(writer.writeRelease) }) })
			sink.writer = writer
			firstResult := appendCaptureAsync(sink, map[string]int{"index": 0})
			first := nextCaptureSync(t, attempts)
			var failed []<-chan error
			for index := 1; index < 4; index++ {
				failed = append(failed, appendCaptureAsync(sink, map[string]int{"index": index}))
				want := index
				waitCaptureState(t, sink, func(s *Sink) bool { return len(s.pending) == want })
			}
			close(first.release)
			require.NoError(t, <-firstResult)
			<-writer.writeStarted
			failed = append(failed, appendCaptureAsync(sink, map[string]int{"index": 4}))
			waitCaptureState(t, sink, func(s *Sink) bool { return len(s.pending) == 1 })
			closed := make(chan error, 1)
			go func() { closed <- sink.Close() }()
			waitCaptureState(t, sink, func(s *Sink) bool { return s.closing })
			assertCaptureWaiting(t, closed, "Close during partial write")
			releaseWrite.Do(func() { close(writer.writeRelease) })
			firstFailure := <-failed[0]
			expected := failure
			if expected == nil {
				expected = io.ErrShortWrite
			}
			require.ErrorIs(t, firstFailure, expected)
			for _, result := range failed[1:] {
				require.Same(t, firstFailure, <-result)
			}
			require.Same(t, firstFailure, sink.Err())
			require.ErrorIs(t, <-closed, expected)
			require.Equal(t, 3, writer.calls, "remaining group/pending writes must stop on failure")
			select {
			case extra := <-attempts:
				t.Fatalf("partial group was synchronized: %+v", extra)
			default:
			}
		})
	}
}

func TestSinkGroupAdmissionBoundsAndCloseRejectsBlockedCallers(t *testing.T) {
	for _, test := range []struct {
		name, payload string
		queued        int
	}{
		{"record_count", "small", maxPendingRecords},
		{"byte_count", strings.Repeat("x", 64<<10), (maxPendingBytes / ((64 << 10) + 3))},
		{"single_oversized", strings.Repeat("x", maxPendingBytes+1), 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			sink, err := OpenPrivate(filepath.Join(t.TempDir(), "bounded.jsonl"), "group")
			require.NoError(t, err)
			attempts := installCaptureSyncGate(t, sink, 0, nil)
			firstResult := appendCaptureAsync(sink, "active")
			first := nextCaptureSync(t, attempts)
			var pending []<-chan error
			for index := range test.queued {
				pending = append(pending, appendCaptureAsync(sink, test.payload))
				want := index + 1
				waitCaptureState(t, sink, func(s *Sink) bool { return len(s.pending) == want })
			}
			sink.mu.Lock()
			canQueue, admitted, pendingBytes := sink.canQueueLocked(len(test.payload)+3), sink.admitted, sink.pendingBytes
			sink.mu.Unlock()
			require.False(t, canQueue)
			require.Equal(t, uint64(1+test.queued), admitted)
			if test.queued > 1 {
				require.LessOrEqual(t, pendingBytes, maxPendingBytes)
			}
			blocked := appendCaptureAsync(sink, test.payload)
			closed := make(chan error, 1)
			go func() { closed <- sink.Close() }()
			waitCaptureState(t, sink, func(s *Sink) bool { return s.closing })
			require.EqualError(t, <-blocked, "group capture is closed")
			close(first.release)
			require.NoError(t, <-firstResult)
			second := nextCaptureSync(t, attempts)
			require.Equal(t, 1+test.queued, second.records)
			close(second.release)
			for _, result := range pending {
				require.NoError(t, <-result)
			}
			require.NoError(t, <-closed)
			sink.mu.Lock()
			completed, remaining, pendingBytes := sink.completed, len(sink.pending), sink.pendingBytes
			sink.mu.Unlock()
			require.Equal(t, uint64(1+test.queued), completed)
			require.Zero(t, remaining)
			require.Zero(t, pendingBytes)
		})
	}
}

type captureInterruptedResult struct {
	returned bool
	err      error
	panicVal any
}

func captureInterruptedCall(action func() error) <-chan captureInterruptedResult {
	completed := make(chan captureInterruptedResult, 1)
	go func() {
		result := captureInterruptedResult{}
		defer func() {
			result.panicVal = recover()
			completed <- result
		}()
		result.err = action()
		result.returned = true
	}()
	return completed
}

func interruptCapture(mode string, panicVal any) {
	if mode == "goexit" {
		runtime.Goexit()
	}
	panic(panicVal)
}

type captureInterruptedWriter struct {
	started, release chan struct{}
	mode             string
	panicVal         any
}

func (writer captureInterruptedWriter) Write([]byte) (int, error) {
	close(writer.started)
	<-writer.release
	interruptCapture(writer.mode, writer.panicVal)
	return 0, nil
}

func TestSinkInterruptedWriteOrSyncFailsAdmissionsAndPreservesInterruption(t *testing.T) {
	for _, operation := range []string{"borrowed_write", "owned_sync"} {
		for _, mode := range []string{"panic", "goexit"} {
			t.Run(operation+"/"+mode, func(t *testing.T) {
				started, release := make(chan struct{}), make(chan struct{})
				panicVal := &struct{ secret string }{"private writer interruption detail"}
				var open sync.Once
				t.Cleanup(func() { open.Do(func() { close(release) }) })
				var sink *Sink
				if operation == "borrowed_write" {
					sink = NewWriter(captureInterruptedWriter{started, release, mode, panicVal}, "interrupted")
				} else {
					var err error
					sink, err = OpenPrivate(filepath.Join(t.TempDir(), "interrupted.jsonl"), "interrupted")
					require.NoError(t, err)
					actualSync := sink.syncFile
					sink.syncFile = func() error {
						close(started)
						<-release
						if err := actualSync(); err != nil {
							return err
						}
						interruptCapture(mode, panicVal)
						return nil
					}
				}
				// Release the writer before cleanup attempts to drain the sink.
				t.Cleanup(func() { open.Do(func() { close(release) }); _ = sink.Close() })
				interrupted := captureInterruptedCall(func() error { return sink.Append("active") })
				<-started
				var pending []<-chan error
				for index := range 3 {
					pending = append(pending, appendCaptureAsync(sink, "pending"))
					want := index + 1
					waitCaptureState(t, sink, func(s *Sink) bool { return len(s.pending) == want })
				}
				health := make(chan error, 1)
				go func() { health <- sink.Err() }()
				closed := make(chan error, 1)
				go func() { closed <- sink.Close() }()
				waitCaptureState(t, sink, func(s *Sink) bool { return s.closing })
				assertCaptureWaiting(t, health, "Err during interrupted write/Sync")
				assertCaptureWaiting(t, closed, "Close during interrupted write/Sync")
				open.Do(func() { close(release) })
				outcome := <-interrupted
				require.False(t, outcome.returned, "interruption must propagate, rather than turn into Append return")
				require.Nil(t, outcome.err)
				if mode == "panic" {
					require.Same(t, panicVal, outcome.panicVal, "original panic must survive cleanup")
				} else {
					require.Nil(t, outcome.panicVal, "Goexit must remain Goexit")
				}
				firstFailure := <-pending[0]
				require.ErrorIs(t, firstFailure, errInterruptedWrite)
				require.NotContains(t, firstFailure.Error(), panicVal.secret, "capture error must not expose panic content")
				for _, result := range pending[1:] {
					require.Same(t, firstFailure, <-result)
				}
				require.Same(t, firstFailure, <-health)
				require.ErrorIs(t, <-closed, errInterruptedWrite)
				require.ErrorIs(t, sink.Close(), errInterruptedWrite)
				if operation == "owned_sync" {
					_, err := sink.writer.(*os.File).WriteString("closed")
					require.ErrorIs(t, err, os.ErrClosed)
				}
			})
		}
	}
}

func TestSinkInterruptedOwnedCloseReleasesJoinersAndRetainsFailure(t *testing.T) {
	for _, mode := range []string{"panic", "goexit"} {
		t.Run(mode, func(t *testing.T) {
			sink, err := OpenPrivate(filepath.Join(t.TempDir(), "close-interrupted.jsonl"), "interrupted")
			require.NoError(t, err)
			require.NoError(t, sink.Append("durable"))
			started, release := make(chan struct{}), make(chan struct{})
			var open sync.Once
			t.Cleanup(func() { open.Do(func() { close(release) }); _ = sink.Close() })
			panicVal := &struct{ secret string }{"private close interruption detail"}
			actualClose := sink.closeFile
			sink.closeFile = func() error {
				close(started)
				<-release
				if err := actualClose(); err != nil {
					return err
				}
				interruptCapture(mode, panicVal)
				return nil
			}
			interrupted := captureInterruptedCall(sink.Close)
			<-started
			health := make(chan error, 1)
			go func() { health <- sink.Err() }()
			closed := make(chan error, 1)
			go func() { closed <- sink.Close() }()
			assertCaptureWaiting(t, health, "Err during interrupted Close")
			assertCaptureWaiting(t, closed, "concurrent Close during interrupted Close")
			require.EqualError(t, sink.Append("late"), "interrupted capture is closed")
			open.Do(func() { close(release) })
			outcome := <-interrupted
			require.False(t, outcome.returned)
			require.Nil(t, outcome.err)
			if mode == "panic" {
				require.Same(t, panicVal, outcome.panicVal)
			} else {
				require.Nil(t, outcome.panicVal)
			}
			joinedFailure := <-closed
			require.ErrorIs(t, joinedFailure, errInterruptedClose)
			require.Same(t, joinedFailure, <-health)
			require.Same(t, joinedFailure, sink.Close())
			require.NotContains(t, joinedFailure.Error(), panicVal.secret)
			_, err = sink.writer.(*os.File).WriteString("closed")
			require.ErrorIs(t, err, os.ErrClosed)
		})
	}
}
