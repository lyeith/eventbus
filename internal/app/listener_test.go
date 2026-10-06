package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/firehose"
	"github.com/stretchr/testify/require"
)

func TestEventBusBindFailureReleasesConstructedResources(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	listener := newEventBusListener(&http.Server{Addr: occupied.Addr().String()}, &eventBusLifecycle{store: store}, time.Second)
	require.Error(t, listener.Run(context.Background()))
	require.Error(t, store.DB().Ping(), "failed bind must close the unused SQLite store")
}

func TestEventBusPreCanceledRunDoesNotBind(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	listener := newEventBusListener(&http.Server{Addr: occupied.Addr().String()}, &eventBusLifecycle{store: store}, time.Second)
	require.NoError(t, listener.Run(ctx), "canceled-before-start must not attempt the occupied address")
	require.Error(t, store.DB().Ping())
}

func TestEventBusCancellationDrainsWithFreshBudget(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	ready := make(chan string, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	server := &http.Server{
		Addr: "127.0.0.1:0",
		BaseContext: func(bound net.Listener) context.Context {
			ready <- bound.Addr().String()
			return context.Background()
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
			if err := store.DB().PingContext(r.Context()); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}),
	}
	listener := newEventBusListener(server, &eventBusLifecycle{store: store}, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- listener.Run(ctx) }()
	address := <-ready
	responseDone := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: time.Second}
		response, err := client.Get("http://" + address)
		if err == nil {
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				err = errors.New("live handler lost its SQLite store")
			}
		}
		responseDone <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-runDone:
		t.Fatalf("cancellation skipped drain: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, store.DB().Ping(), "store must remain usable while the HTTP handler drains")
	close(release)
	require.NoError(t, <-responseDone)
	require.NoError(t, <-runDone)
	require.Error(t, store.DB().Ping())
}

func TestEventBusConcurrentShutdownCanceledWaiterCannotStopCleanup(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()
	fm := firehose.NewFirehoseManager("us-east-1", "000000000000", sink.URL, "test", "test")
	stream, err := fm.CreateStream("final", "bucket", "", "", 100, 3600)
	require.NoError(t, err)
	_, err = fm.PutRecord(stream, []byte("owned\n"))
	require.NoError(t, err)
	listener := newEventBusListener(&http.Server{}, &eventBusLifecycle{store: store, firehose: fm}, time.Second)
	ownerResult := make(chan error, 1)
	go func() { ownerResult <- listener.Shutdown(context.Background()) }()
	<-entered
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, listener.Shutdown(canceled), context.Canceled)
	require.NoError(t, store.DB().Ping(), "canceled waiter must not release the owner's resource")
	results := make(chan error, 8)
	for range 8 {
		go func() { results <- listener.Shutdown(context.Background()) }()
	}
	close(release)
	require.NoError(t, <-ownerResult)
	for range 8 {
		require.NoError(t, <-results)
	}
	require.Error(t, store.DB().Ping())
}
