package main

import (
	"flag"
	"fmt"
)

type cliConfig struct {
	port                int
	configuration       string
	frontendDir         string
	frontendProxy       string
	noAuth              bool
	retainedControl     string
	continuationPort    int
	continuationPortSet bool
	debug               bool
}

// Parse the complete command before fixture loading or runtime construction.
// An owned FlagSet keeps parsing independent of process-global flags.
func readCLIConfig(flags *flag.FlagSet, args []string) (cliConfig, error) {
	var cfg cliConfig
	flags.IntVar(&cfg.port, "port", 0, "override listener port")
	flags.StringVar(&cfg.configuration, "config", "gateway.yaml", "application-owned gateway fixture")
	flags.StringVar(&cfg.frontendDir, "frontend-dir", "", "static frontend SPA directory")
	flags.StringVar(&cfg.frontendProxy, "frontend-proxy", "", "frontend development server URL")
	flags.BoolVar(&cfg.noAuth, "no-auth", false, "explicitly bypass configured authorizers")
	flags.StringVar(&cfg.retainedControl, "retained-owner-control-url", "", "opt-in exclusively owned loopback retained-owner control URL")
	flags.IntVar(&cfg.continuationPort, "retained-owner-continuation-port", 0, "opt-in exclusively owned loopback gateway continuation listener")
	flags.BoolVar(&cfg.debug, "debug", false, "enable debug logging")
	if err := flags.Parse(args); err != nil {
		return cliConfig{}, err
	}
	if flags.NArg() != 0 {
		return cliConfig{}, fmt.Errorf("unexpected positional arguments: %q", flags.Args())
	}
	flags.Visit(func(option *flag.Flag) {
		if option.Name == "retained-owner-continuation-port" {
			cfg.continuationPortSet = true
		}
	})
	return cfg, nil
}
