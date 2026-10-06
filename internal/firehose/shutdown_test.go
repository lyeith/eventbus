package firehose

import (
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestFirehoseShutdownJoinsDeliveryAndRetainsFailedBuffer(t *testing.T) {
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() { close(release); sink.Close() }()
	fm := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	stream, err := fm.CreateStream("blocked", "bucket", "", "", 1, 3600)
	require.NoError(t, err)
	_, err = fm.PutRecord(stream, make([]byte, 1<<20))
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("size-triggered delivery did not begin")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, fm.ShutdownContext(ctx), context.DeadlineExceeded)
	select {
	case <-stream.done:
	case <-time.After(time.Second):
		t.Fatal("stream delivery owner not joined")
	}
	stream.mu.Lock()
	remaining := len(stream.buffer)
	stream.mu.Unlock()
	require.Equal(t, 1, remaining, "undelivered data must not vanish from a failed final flush")
	_, err = fm.PutRecord(stream, []byte("late"))
	require.Error(t, err)
	_, err = fm.CreateStream("late", "bucket", "", "", 1, 1)
	require.Error(t, err)
	require.ErrorIs(t, fm.ShutdownContext(context.Background()), context.DeadlineExceeded)
}

func TestFirehoseSuccessfulShutdownWaitsForFinalRecordAndIsIdempotent(t *testing.T) {
	var mu sync.Mutex
	var objects [][]byte
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		mu.Lock()
		objects = append(objects, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()
	fm := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	stream, err := fm.CreateStream("complete", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	payload := []byte("original\n")
	_, err = fm.PutRecord(stream, payload)
	require.NoError(t, err)
	payload[0] = 'X'
	results := make(chan error, 8)
	for range 8 {
		go func() { results <- fm.Shutdown() }()
	}
	for range 8 {
		require.NoError(t, <-results)
	}
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, [][]byte{[]byte("original\n")}, objects)
}

func TestFirehoseDeleteDoesNotAcknowledgeFailedFinalDelivery(t *testing.T) {
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer sink.Close()
	fm := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	stream, err := fm.CreateStream("retain", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	_, err = fm.PutRecord(stream, []byte("unacknowledged"))
	require.NoError(t, err)
	require.Error(t, fm.DeleteStream(context.Background(), stream.Name))
	require.Same(t, stream, fm.GetStream(stream.Name))
	stream.mu.Lock()
	require.Len(t, stream.buffer, 1)
	stream.mu.Unlock()
	require.Error(t, fm.Shutdown())
}
