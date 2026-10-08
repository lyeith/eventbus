// eventbus-gateway serves application-owned API Gateway request fixtures.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lyeith/eventbus/internal/gateway"
	"github.com/rs/zerolog"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	port := flag.Int("port", 0, "override listener port")
	configuration := flag.String("config", "gateway.yaml", "application-owned gateway fixture")
	frontendDir := flag.String("frontend-dir", "", "static frontend SPA directory")
	frontendProxy := flag.String("frontend-proxy", "", "frontend development server URL")
	noAuth := flag.Bool("no-auth", false, "explicitly bypass configured authorizers")
	retainedControl := flag.String("retained-owner-control-url", "", "opt-in exclusively owned loopback retained-owner control URL")
	continuationPort := flag.Int("retained-owner-continuation-port", 0, "opt-in exclusively owned loopback gateway continuation listener")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()
	cfg, err := gateway.LoadConfig(*configuration)
	if err != nil {
		return err
	}
	if *port != 0 {
		cfg.Port = *port
	}
	if *retainedControl != "" {
		cfg.RetainedOwnerControlURL = *retainedControl
	}
	flag.Visit(func(option *flag.Flag) {
		if option.Name == "retained-owner-continuation-port" {
			cfg.RetainedOwnerContinuationPort = *continuationPort
		}
	})
	level := zerolog.InfoLevel
	if *debug {
		level = zerolog.DebugLevel
	}
	logger := zerolog.New(os.Stderr).Level(level).With().Timestamp().Logger()
	application, err := gateway.New(*cfg, gateway.Options{NoAuth: *noAuth, FrontendDir: *frontendDir, FrontendProxy: *frontendProxy, Logger: logger})
	if err != nil {
		return err
	}
	listeners, err := listenGateway(cfg.Port, cfg.RetainedOwnerContinuationPort, application, application.RetainedContinuationHandler())
	if err != nil {
		return errors.Join(err, application.Close())
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	failure := make(chan error, len(listeners))
	servers := make([]*http.Server, 0, len(listeners))
	for _, owned := range listeners {
		servers = append(servers, owned.server)
		go func(owned gatewayListener) { failure <- owned.server.Serve(owned.listener) }(owned)
	}
	logger.Info().Int("port", cfg.Port).Int("retained_owner_continuation_port", cfg.RetainedOwnerContinuationPort).Bool("no_auth", *noAuth).Msg("EventBus gateway ready")
	remaining := len(servers)
	var serveErr error
	select {
	case serveErr = <-failure:
		remaining--
	case <-ctx.Done():
	case <-application.RetainedShutdownSignal():
	}
	drain, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return errors.Join(normalServeError(serveErr), shutdownGateways(drain, servers, application.Close, failure, remaining))
}

type gatewayListener struct {
	server   *http.Server
	listener net.Listener
}

// Bind all ingress before serving. A failed private bind cannot leave a public
// gateway running without its configured continuation peer.
func listenGateway(port, continuationPort int, public, continuation http.Handler) ([]gatewayListener, error) {
	endpoints := []struct {
		address string
		handler http.Handler
	}{{fmt.Sprintf(":%d", port), public}}
	if continuationPort != 0 {
		if continuation == nil {
			return nil, errors.New("retained gateway continuation handler is unavailable")
		}
		endpoints = append(endpoints, struct {
			address string
			handler http.Handler
		}{fmt.Sprintf("127.0.0.1:%d", continuationPort), continuation})
	}
	var listeners []gatewayListener
	for _, endpoint := range endpoints {
		listener, err := net.Listen("tcp", endpoint.address)
		if err != nil {
			for _, owned := range listeners {
				err = errors.Join(err, owned.listener.Close())
			}
			return nil, err
		}
		server := &http.Server{Addr: endpoint.address, Handler: endpoint.handler, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
		listeners = append(listeners, gatewayListener{server: server, listener: listener})
	}
	return listeners, nil
}

func normalServeError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func shutdownGateways(ctx context.Context, servers []*http.Server, closeApplication func() error, failure <-chan error, remaining int) error {
	drained := make(chan error, len(servers))
	for _, server := range servers {
		go func(server *http.Server) {
			err := server.Shutdown(ctx)
			if err != nil {
				err = errors.Join(err, server.Close())
			}
			drained <- err
		}(server)
	}
	var result error
	for range servers {
		result = errors.Join(result, <-drained)
	}
	// Both listeners stop intake and join ordinary requests before closing the
	// shared gateway's outbound connections and reconciling retained leases.
	result = errors.Join(result, closeApplication())
	for range remaining {
		result = errors.Join(result, normalServeError(<-failure))
	}
	return result
}
