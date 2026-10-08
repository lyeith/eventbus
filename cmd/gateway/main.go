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
	options, err := readCLIConfig(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:])
	if err != nil {
		return err
	}
	cfg, err := gateway.LoadConfig(options.configuration)
	if err != nil {
		return err
	}
	if options.port != 0 {
		cfg.Port = options.port
	}
	if options.retainedControl != "" {
		cfg.RetainedOwnerControlURL = options.retainedControl
	}
	if options.continuationPortSet {
		cfg.RetainedOwnerContinuationPort = options.continuationPort
	}
	level := zerolog.InfoLevel
	if options.debug {
		level = zerolog.DebugLevel
	}
	logger := zerolog.New(os.Stderr).Level(level).With().Timestamp().Logger()
	application, err := gateway.New(*cfg, gateway.Options{NoAuth: options.noAuth, FrontendDir: options.frontendDir, FrontendProxy: options.frontendProxy, Logger: logger})
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
	logger.Info().Int("port", cfg.Port).Int("retained_owner_continuation_port", cfg.RetainedOwnerContinuationPort).Bool("no_auth", options.noAuth).Msg("EventBus gateway ready")
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
