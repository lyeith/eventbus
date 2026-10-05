package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Two real DeleteDeliveryStream operations can overlap. A canceled second
// request must not remain blocked on the first request's final delivery lock.
func TestFirehoseConcurrentDeleteHonorsWaitingCallerCancellation(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(func() { unblock(); sink.Close() })
	manager := NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	stream, err := manager.CreateStream("overlap", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	t.Cleanup(func() { unblock(); require.NoError(t, manager.Shutdown()) })
	_, err = manager.PutRecord(stream, []byte("retain-until-delivered"))
	require.NoError(t, err)
	firstCtx, cancelFirst := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelFirst()
	first := make(chan error, 1)
	go func() { first <- manager.DeleteStream(firstCtx, stream.Name) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first delete never reached final delivery")
	}
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelSecond()
	second := make(chan error, 1)
	go func() { second <- manager.DeleteStream(secondCtx, stream.Name) }()
	var secondResult error
	select {
	case secondResult = <-second:
	case <-time.After(150 * time.Millisecond):
		t.Error("canceled delete waited behind another request's delivery lock")
		unblock()
		secondResult = <-second
	}
	if !errors.Is(secondResult, context.DeadlineExceeded) {
		t.Errorf("waiting caller cancellation was lost: %v", secondResult)
	}
	unblock()
	require.NoError(t, <-first)
}
