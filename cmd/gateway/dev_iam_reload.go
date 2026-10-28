// Explicit local fixture reload. Native request verification and admission stay
// with the gateway; SIGHUP never reloads routes, listeners or integration owners.
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/lyeith/eventbus/internal/gateway"
)

func watchIAMFixtureReload() (<-chan os.Signal, func()) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP)
	return signals, func() { signal.Stop(signals) }
}

func reloadIAMFixture(path string, application *gateway.Gateway) error {
	cfg, err := gateway.LoadConfig(path)
	if err != nil {
		return err
	}
	return application.DevReplaceIAMCredentials(cfg.IAMCredentials)
}
