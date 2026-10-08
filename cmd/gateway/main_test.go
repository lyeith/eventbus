package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestShutdownReportsFailedDrainAndJoinsOwnedConsumers(t *testing.T) {
	entered := make(chan struct{})
	released := make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(released) })}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	failure := make(chan error, 1)
	go func() { failure <- server.Serve(listener) }()
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request never entered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ownedErr := errors.New("owned transport close failed")
	var ownedClosed bool
	err = shutdownGateways(ctx, []*http.Server{server}, func() error { ownedClosed = true; return ownedErr }, failure, 1)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ownedErr) || !ownedClosed {
		t.Fatalf("shutdown hid failure or missed owner: %v, %t", err, ownedClosed)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("server-owned handler remained active")
	}
	select {
	case <-clientDone:
	case <-time.After(time.Second):
		t.Fatal("client did not join")
	}
}

func TestGatewayPrivateListenerIsLoopbackAndOptional(t *testing.T) {
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	continuationPort := reserve.Addr().(*net.TCPAddr).Port
	if err := reserve.Close(); err != nil {
		t.Fatal(err)
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	listeners, err := listenGateway(0, continuationPort, handler, handler)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, owned := range listeners {
			_ = owned.listener.Close()
		}
	}()
	if len(listeners) != 2 || !listeners[1].listener.Addr().(*net.TCPAddr).IP.IsLoopback() {
		t.Fatalf("private ingress must be separate loopback: %v", listeners)
	}
	ordinary, err := listenGateway(0, 0, handler, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary[0].listener.Close()
	if len(ordinary) != 1 {
		t.Fatalf("ordinary mode listeners=%d", len(ordinary))
	}
}

func TestGatewayPrivateBindFailureClosesPublicListener(t *testing.T) {
	reservedPublic, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	publicPort := reservedPublic.Addr().(*net.TCPAddr).Port
	if err := reservedPublic.Close(); err != nil {
		t.Fatal(err)
	}
	busyPrivate, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer busyPrivate.Close()
	privatePort := busyPrivate.Addr().(*net.TCPAddr).Port
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	listeners, err := listenGateway(publicPort, privatePort, handler, handler)
	if err == nil || len(listeners) != 0 {
		t.Fatalf("busy private ingress was accepted: %v", err)
	}
	check, err := net.Listen("tcp", fmt.Sprintf(":%d", publicPort))
	if err != nil {
		t.Fatalf("failed private bind leaked public listener: %v", err)
	}
	check.Close()
	if _, err := listenGateway(0, privatePort, handler, nil); err == nil {
		t.Fatal("configured private ingress requires its retained handler")
	}
}

func TestShutdownJoinsBothGatewayListenersBeforeSharedOwnerClose(t *testing.T) {
	failures := make(chan error, 2)
	servers := make([]*http.Server, 0, 2)
	joined := make([]chan struct{}, 0, 2)
	clients := make([]chan struct{}, 0, 2)
	for range 2 {
		entered := make(chan struct{})
		released := make(chan struct{})
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-r.Context().Done()
			close(released)
		})}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		servers = append(servers, server)
		joined = append(joined, released)
		go func() { failures <- server.Serve(listener) }()
		clientDone := make(chan struct{})
		clients = append(clients, clientDone)
		go func() {
			defer close(clientDone)
			response, err := http.Get("http://" + listener.Addr().String())
			if err == nil {
				response.Body.Close()
			}
		}()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("request did not reach listener")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	closed := false
	err := shutdownGateways(ctx, servers, func() error {
		// The shared retained adapter actually joins handlers after forced inbound
		// close; caller cancellation alone is never a completion receipt.
		for _, done := range joined {
			select {
			case <-done:
			case <-time.After(time.Second):
				return errors.New("request owner did not join")
			}
		}
		closed = true
		return nil
	}, failures, 2)
	if !errors.Is(err, context.DeadlineExceeded) || !closed {
		t.Fatalf("drain failure or shared close missing: %v, %t", err, closed)
	}
	for _, done := range clients {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("HTTP caller did not join")
		}
	}
}

func TestShutdownAfterOneGatewayServeFailureConsumesOnlyRemainingPeer(t *testing.T) {
	listeners, err := listenGateway(0, 0, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil)
	if err != nil {
		t.Fatal(err)
	}
	peer, err := listenGateway(0, 0, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}), nil)
	if err != nil {
		t.Fatal(err)
	}
	listeners = append(listeners, peer[0])
	defer func() {
		for _, owned := range listeners {
			_ = owned.server.Close()
			_ = owned.listener.Close()
		}
	}()
	failures := make(chan error, 2)
	// A failed first Serve has already been consumed by run's select.
	if err := listeners[0].listener.Close(); err != nil {
		t.Fatal(err)
	}
	go func() { failures <- listeners[0].server.Serve(listeners[0].listener) }()
	first := <-failures
	if first == nil || errors.Is(first, http.ErrServerClosed) {
		t.Fatalf("expected unexpected Serve failure: %v", first)
	}
	go func() { failures <- listeners[1].server.Serve(listeners[1].listener) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	closed := false
	done := make(chan error, 1)
	go func() {
		done <- errors.Join(normalServeError(first), shutdownGateways(ctx,
			[]*http.Server{listeners[0].server, listeners[1].server},
			func() error { closed = true; return nil }, failures, 1))
	}()
	select {
	case result := <-done:
		if !errors.Is(result, first) || !closed {
			t.Fatalf("original Serve failure or owner close was lost: %v", result)
		}
	case <-ctx.Done():
		t.Fatal("shutdown waited for an already consumed Serve result")
	}
}
