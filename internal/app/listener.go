package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// HTTP draining is the barrier before releasing stores. Background SDK callers
// quiesce first, while this listener remains available.
var errHTTPNotDrained = errors.New("HTTP listener did not drain; resource cleanup withheld")

// eventBusListener owns the AWS listener, optional dev callback listener and
// their existing resource owner.
// Concurrent shutdown callers share one result; a canceled waiter cannot stop it.
type eventBusListener struct {
	server      *http.Server
	devRetained *devRetainedHTTP
	owned       *eventBusLifecycle
	timeout     time.Duration
	mu          sync.Mutex
	begun       bool
	running     bool
	done        chan struct{}
	err         error
}

func newEventBusListener(server *http.Server, owned *eventBusLifecycle, timeout time.Duration) *eventBusListener {
	return &eventBusListener{server: server, owned: owned, timeout: timeout, done: make(chan struct{})}
}

func (listener *eventBusListener) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil shutdown context")
	}
	listener.mu.Lock()
	if listener.begun {
		listener.mu.Unlock()
		select {
		case <-listener.done:
			return listener.err
		default:
		}
		select {
		case <-listener.done:
			return listener.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	listener.begun = true
	listener.mu.Unlock()

	result := listener.close(ctx)
	listener.mu.Lock()
	listener.err = result
	close(listener.done)
	listener.mu.Unlock()
	return result
}

func (listener *eventBusListener) close(ctx context.Context) (resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = fmt.Errorf("eventbus cleanup panicked: %v", recovered)
		}
	}()
	if listener.server == nil || listener.timeout <= 0 {
		return errors.Join(errHTTPNotDrained, errors.New("HTTP server and positive shutdown timeout required"))
	}
	deadline, cancel := context.WithTimeout(ctx, listener.timeout)
	defer cancel()
	retainedErr := listener.joinRetained(deadline)
	quiesceErr := errors.Join(retainedErr, listener.owned.Quiesce(deadline))
	var httpErrs []error
	for _, server := range listener.httpServers() {
		if err := server.Shutdown(deadline); err != nil {
			httpErrs = append(httpErrs, fmt.Errorf("drain HTTP listener: %w", err))
		}
	}
	if len(httpErrs) != 0 {
		return errors.Join(quiesceErr, errHTTPNotDrained, errors.Join(httpErrs...))
	}
	if err := deadline.Err(); err != nil {
		return errors.Join(quiesceErr, fmt.Errorf("cleanup budget expired: %w", err))
	}
	if quiesceErr != nil {
		return quiesceErr
	}
	return errors.Join(listener.owned.Close(deadline), deadline.Err())
}

// Run joins serving and shutdown even when binding fails. Signal/cancellation is
// a trigger; draining gets a fresh bounded context. Signal handling stays owned
// through cleanup so a second SIGTERM cannot terminate active resource users.
func (listener *eventBusListener) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil listener context")
	}
	if listener.server == nil || listener.timeout <= 0 {
		return errors.Join(errHTTPNotDrained, errors.New("HTTP server and positive shutdown timeout required"))
	}
	listener.mu.Lock()
	if listener.running {
		listener.mu.Unlock()
		return errors.New("HTTP listener is already running")
	}
	listener.running = true
	begun := listener.begun
	listener.mu.Unlock()
	if begun {
		return listener.Shutdown(context.WithoutCancel(ctx))
	}
	if ctx.Err() != nil {
		return listener.Shutdown(context.WithoutCancel(ctx))
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	servers := listener.httpServers()
	served := make(chan error, len(servers))
	for _, server := range servers {
		go func() { served <- server.ListenAndServe() }()
	}
	var serveErr error
	finished := 0
	select {
	case serveErr = <-served:
		finished++
	case <-ctx.Done():
	case <-signals:
	case <-listener.done:
	}
	shutdownErr := listener.Shutdown(context.WithoutCancel(ctx))
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	for finished < len(servers) {
		err := <-served
		finished++
		if !errors.Is(err, http.ErrServerClosed) {
			serveErr = errors.Join(serveErr, err)
		}
	}
	return errors.Join(serveErr, shutdownErr)
}
