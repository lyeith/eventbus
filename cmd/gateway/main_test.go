package main

import (
	"context"
	"errors"
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
	err = shutdownGateway(ctx, server, func() error { ownedClosed = true; return ownedErr }, failure)
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
