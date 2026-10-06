package messaging

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSNSCaptureConcurrentAppendAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "notifications.jsonl")
	capture, err := OpenSNSCapture(path)
	require.NoError(t, err)
	var callers sync.WaitGroup
	failures := make(chan error, 32)
	for i := 0; i < 32; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			failures <- capture.Append(SNSCaptureRecord{Operation: "Publish", PhoneNumber: "+12025550123", Message: "local request"})
		}()
	}
	callers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	require.NoError(t, capture.Close())
	require.NoError(t, capture.Close())
	capture, err = OpenSNSCapture(path)
	require.NoError(t, err)
	require.NoError(t, capture.Append(SNSCaptureRecord{Operation: "CreateSMSSandboxPhoneNumber", Details: map[string]any{"otp": "123456"}}))
	require.NoError(t, capture.Close())
	file, err := os.Open(path)
	require.NoError(t, err)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var record SNSCaptureRecord
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		require.Equal(t, "eventbus.sns.capture.v1", record.SchemaVersion)
		require.False(t, record.CapturedAt.IsZero())
		count++
	}
	require.NoError(t, scanner.Err())
	require.Equal(t, 33, count)
	info, err := file.Stat()
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

type snsFailingWriter struct {
	calls   int
	failure error
}

func (writer *snsFailingWriter) Write(data []byte) (int, error) {
	writer.calls++
	return 1, writer.failure
}

func TestSNSCaptureFailedAppendIsTerminal(t *testing.T) {
	for _, failure := range []error{nil, errors.New("storage unavailable")} {
		writer := &snsFailingWriter{failure: failure}
		capture := &SNSCapture{writer: writer}
		first := capture.Append(SNSCaptureRecord{Operation: "Publish", Message: "body"})
		require.Error(t, first)
		if failure == nil {
			require.ErrorIs(t, first, io.ErrShortWrite)
		} else {
			require.ErrorIs(t, first, failure)
		}
		require.Equal(t, first, capture.Append(SNSCaptureRecord{Operation: "Publish"}))
		require.Equal(t, 1, writer.calls)
		closeErr := capture.Close()
		require.ErrorIs(t, closeErr, first)
		require.Equal(t, closeErr, capture.Close(), "close is idempotent")
	}
}

func TestSNSCaptureRefusesIncompleteFileAndClosedAppend(t *testing.T) {
	path := filepath.Join(t.TempDir(), "partial.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(`{"operation":"Publish"}`), 0600))
	_, err := OpenSNSCapture(path)
	require.ErrorContains(t, err, "incomplete record")
	capture, err := OpenSNSCapture(filepath.Join(t.TempDir(), "complete.jsonl"))
	require.NoError(t, err)
	require.NoError(t, capture.Close())
	require.ErrorContains(t, capture.Append(SNSCaptureRecord{Operation: "Publish"}), "closed")
}

func TestBrokerUsesConfiguredSNSCapture(t *testing.T) {
	writer := &snsFailingWriter{failure: errors.New("disk full")}
	broker := newTestBroker()
	broker.SetSNSCapture(&SNSCapture{writer: writer})
	require.ErrorContains(t, broker.CaptureSNS(SNSCaptureRecord{Operation: "Publish"}), "disk full")
}
