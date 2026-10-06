package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/firehose"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

func TestEventBusFailedHTTPDrainRetainsSQLite(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	handlerResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		handlerResult <- store.DB().PingContext(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, _ := server.Client().Get(server.URL)
		if response != nil {
			_ = response.Body.Close()
		}
	}()
	<-entered
	defer func() {
		close(release)
		server.Close()
		<-clientDone
		require.NoError(t, <-handlerResult)
		require.NoError(t, store.Close())
	}()
	triggerClosed := false
	owned := &eventBusLifecycle{store: store, triggers: contextCloseFunc(func(context.Context) error { triggerClosed = true; return nil })}
	manager := newEventBusListener(server.Config, owned, 40*time.Millisecond)
	require.ErrorIs(t, manager.Shutdown(context.Background()), errHTTPNotDrained)
	require.ErrorIs(t, manager.Shutdown(context.Background()), errHTTPNotDrained, "failed drain must remain terminal")
	require.NoError(t, store.DB().Ping(), "shutdown timeout must not close SQLite underneath a live handler")
	require.False(t, triggerClosed, "failed HTTP drain must retain the trigger runtime")
}

func TestEventBusJoinsBackgroundUsersBeforeClosingSQLite(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	owned := &eventBusLifecycle{store: store, cancel: cancel, requeueDone: broker.StartRequeueLoop(ctx), sessionsDone: cognito.StartChallengeCleanup(ctx, store, time.Millisecond)}
	require.NoError(t, owned.Close(context.Background()))
	select {
	case <-owned.sessionsDone:
	default:
		t.Fatal("session cleanup not joined")
	}
	select {
	case <-owned.requeueDone:
	default:
		t.Fatal("requeue not joined")
	}
	require.Error(t, store.DB().Ping(), "completed close must actually release the native SQLite connection")
	require.NoError(t, owned.Close(context.Background()))
}

func TestEventBusFailedBackgroundJoinWithholdsStoreClose(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	notDone := make(chan struct{})
	owned := &eventBusLifecycle{store: store, sessionsDone: notDone}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, owned.Close(ctx), context.DeadlineExceeded)
	require.NoError(t, store.DB().Ping())
	close(notDone)
	require.ErrorIs(t, owned.Close(context.Background()), context.DeadlineExceeded, "failed close must not turn into a success claim")
}

func TestFirehoseFlushErrorDoesNotPreventIndependentSQLiteRelease(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	fm := firehose.NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
	stream, err := fm.CreateStream("fail", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	_, err = fm.PutRecord(stream, []byte("retained"))
	require.NoError(t, err)
	owned := &eventBusLifecycle{store: store, firehose: fm}
	err = owned.Close(context.Background())
	require.Error(t, err)
	require.Error(t, store.DB().Ping())
	require.True(t, errors.Is(owned.Close(context.Background()), err))
}

// A lifetime port needs only Close; operation state remains service-owned.
type closeFunc func() error

func (f closeFunc) Close() error { return f() }

func TestSESLifecycleClosesCaptureAfterWorkers(t *testing.T) {
	closed := false
	capture := closeFunc(func() error { closed = true; return errors.New("close failed") })
	done := make(chan struct{})
	owned := &eventBusLifecycle{ses: capture, requeueDone: done}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.Error(t, owned.Close(ctx))
	require.False(t, closed, "a live resource user prevents capture close")
	close(done)
	joined := &eventBusLifecycle{ses: capture, requeueDone: done}
	require.ErrorContains(t, joined.Close(t.Context()), "close failed")
	require.True(t, closed)
}

type contextCloseFunc func(context.Context) error

func (f contextCloseFunc) Close(ctx context.Context) error { return f(ctx) }

func TestTriggerLifecycleJoinsBeforeReleasingStoresAndCapture(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	workerDone := make(chan struct{})
	entered, release := make(chan struct{}), make(chan struct{})
	captureClosed := false
	owned := &eventBusLifecycle{
		store: store, requeueDone: workerDone,
		ses: closeFunc(func() error { captureClosed = true; return nil }),
		triggers: contextCloseFunc(func(ctx context.Context) error {
			select {
			case <-workerDone:
			default:
				return errors.New("worker not joined")
			}
			if err := store.DB().PingContext(ctx); err != nil {
				return err
			}
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
	}
	close(workerDone)
	result := make(chan error, 1)
	go func() { result <- owned.Close(t.Context()) }()
	<-entered
	require.NoError(t, store.DB().Ping())
	require.False(t, captureClosed)
	close(release)
	require.NoError(t, <-result)
	require.Error(t, store.DB().Ping())
	require.True(t, captureClosed)
	require.NoError(t, owned.Close(t.Context()))
}

func TestFailedTriggerJoinRetainsStoresAndCapture(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	captureClosed := false
	owned := &eventBusLifecycle{
		store:    store,
		ses:      closeFunc(func() error { captureClosed = true; return nil }),
		triggers: contextCloseFunc(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }),
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, owned.Close(ctx), context.DeadlineExceeded)
	require.NoError(t, store.DB().Ping())
	require.False(t, captureClosed)
	require.ErrorIs(t, owned.Close(t.Context()), context.DeadlineExceeded, "failed lifetime join must remain terminal")
}

func TestSNSCaptureClosesWhenSESCloseFails(t *testing.T) {
	expected := errors.New("SES final append failed")
	snsClosed := false
	owned := &eventBusLifecycle{
		ses: closeFunc(func() error { return expected }),
		sns: closeFunc(func() error { snsClosed = true; return nil }),
	}
	require.ErrorIs(t, owned.Close(context.Background()), expected)
	require.True(t, snsClosed)
}
