package main

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func TestGatewayCLIConfigDefaultsAndOverrides(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want cliConfig
	}{
		{"defaults", nil, cliConfig{configuration: "gateway.yaml"}},
		{"overrides", []string{
			"--port", "14180", "--config", "private/gateway.yaml",
			"--frontend-dir", "dist", "--frontend-proxy", "http://127.0.0.1:5173",
			"--no-auth", "--retained-owner-control-url", "http://127.0.0.1:4100/__eventbus/dev/retained-owner",
			"--retained-owner-continuation-port", "14181", "--debug",
		}, cliConfig{port: 14180, configuration: "private/gateway.yaml", frontendDir: "dist", frontendProxy: "http://127.0.0.1:5173", noAuth: true, retainedControl: "http://127.0.0.1:4100/__eventbus/dev/retained-owner", continuationPort: 14181, continuationPortSet: true, debug: true}},
		{"explicit-zero-override", []string{"--retained-owner-continuation-port=0"}, cliConfig{configuration: "gateway.yaml", continuationPortSet: true}},
		{"empty-delimiter", []string{"--port", "14180", "--"}, cliConfig{port: 14180, configuration: "gateway.yaml"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags := flag.NewFlagSet("eventbus-gateway", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			cfg, err := readCLIConfig(flags, test.args)
			if err != nil || cfg != test.want {
				t.Fatalf("CLI options changed: %#v; error=%v; want=%#v", cfg, err, test.want)
			}
		})
	}
}

func TestGatewayCLIConfigRejectsUnexpectedPositionals(t *testing.T) {
	for _, args := range [][]string{
		{"version"}, {"--port", "14180", "version"}, {"--", "version"},
		{"version", "--port", "14180"}, {"--config", "missing.yaml", "version", "--debug"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			flags := flag.NewFlagSet("eventbus-gateway", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			cfg, err := readCLIConfig(flags, args)
			if err == nil || !strings.Contains(err.Error(), "unexpected positional arguments") ||
				!strings.Contains(err.Error(), "version") || cfg != (cliConfig{}) {
				t.Fatalf("unused positional command was accepted: %#v; %v", cfg, err)
			}
		})
	}
}

func TestGatewayCLIConfigPreservesFlagErrorsAndHelp(t *testing.T) {
	for _, args := range [][]string{{"--unknown"}, {"--port", "not-an-int"}, {"--help"}} {
		flags := flag.NewFlagSet("eventbus-gateway", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		_, err := readCLIConfig(flags, args)
		if err == nil {
			t.Fatalf("invalid flag or help was swallowed: %v", args)
		}
		if args[0] == "--help" && !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("help semantics changed: %v", err)
		}
	}
}
