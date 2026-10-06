// eventbus-gateway serves application-owned REST API Gateway fixtures.
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
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()
	cfg, err := gateway.LoadConfig(*configuration)
	if err != nil {
		return err
	}
	if *port != 0 {
		cfg.Port = *port
	}
	level := zerolog.InfoLevel
	if *debug {
		level = zerolog.DebugLevel
	}
	logger := zerolog.New(os.Stderr).Level(level).With().Timestamp().Logger()
	application, err := gateway.New(*cfg, gateway.Options{NoAuth: *noAuth, FrontendDir: *frontendDir, FrontendProxy: *frontendProxy, Logger: logger})
	if err != nil {
		return err
	}
	defer application.Close()
	server := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Port), Handler: application, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	failure := make(chan error, 1)
	go func() { failure <- server.Serve(listener) }()
	logger.Info().Int("port", cfg.Port).Bool("no_auth", *noAuth).Msg("EventBus gateway ready")
	select {
	case err := <-failure:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		drain, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(drain); err != nil {
			_ = server.Close()
		}
		// net/http does not drain hijacked WebSocket connections. Gateway owns its
		// outbound connections and closes upgrades after ordinary requests drain.
		if err := application.Close(); err != nil {
			return err
		}
		err := <-failure
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
