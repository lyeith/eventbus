package app

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCLIConfigDefaultsAndOverrides(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		check func(*testing.T, config)
	}{
		{"defaults", nil, func(t *testing.T, c config) {
			require.Equal(t, 4100, c.port)
			require.Equal(t, "us-east-1", c.region)
			require.Equal(t, "000000000000", c.accountID)
			require.Equal(t, "http://localhost:4100", c.issuerBase)
			require.Empty(t, c.jwksBase)
			require.Empty(t, c.cognitoTriggers)
			require.Empty(t, c.lambdaFunctions)
			require.Empty(t, c.sqsDeliveryLog)
			require.Equal(t, "/tmp/cognito-dev.db", c.cognitoDB)
			require.Equal(t, time.Hour, c.accessTokenTTL)
			require.Equal(t, 24*time.Hour, c.refreshTokenTTL)
			require.Equal(t, "-", c.sesLog)
			require.Equal(t, "-", c.snsLog)
		}},
		{"overrides", []string{"--port", "14100", "--region", "local-1", "--account-id", "123", "--s3-endpoint", "http://127.0.0.1:9001", "--consumers", "app/consumers.yaml", "--work-dir", "app", "--issuer-base", "http://issuer", "--jwks-base", "http://keys", "--cognito-pools", "pools.yaml", "--cognito-triggers", "auth/triggers.yaml", "--lambda-functions", "functions.yaml", "--cognito-db", "identities.db", "--access-token-ttl", "30m", "--refresh-token-ttl", "48h", "--sns-log", "notifications.jsonl", "--sqs-delivery-log", "deliveries.jsonl", "--ses-log", "emails.jsonl", "--ses-config", "ses.yaml", "--debug"}, func(t *testing.T, c config) {
			require.Equal(t, 14100, c.port)
			require.Equal(t, "local-1", c.region)
			require.Equal(t, "123", c.accountID)
			require.Equal(t, "http://127.0.0.1:9001", c.s3Endpoint)
			require.Equal(t, "app/consumers.yaml", c.consumersFile)
			require.Equal(t, "app", c.workDir)
			require.Equal(t, "http://issuer", c.issuerBase)
			require.Equal(t, "http://keys", c.jwksBase)
			require.Equal(t, "pools.yaml", c.cognitoPools)
			require.Equal(t, "auth/triggers.yaml", c.cognitoTriggers)
			require.Equal(t, "functions.yaml", c.lambdaFunctions)
			require.Equal(t, "identities.db", c.cognitoDB)
			require.Equal(t, 30*time.Minute, c.accessTokenTTL)
			require.Equal(t, 48*time.Hour, c.refreshTokenTTL)
			require.Equal(t, "emails.jsonl", c.sesLog)
			require.Equal(t, "notifications.jsonl", c.snsLog)
			require.Equal(t, "deliveries.jsonl", c.sqsDeliveryLog)
			require.Equal(t, "ses.yaml", c.sesConfig)
			require.True(t, c.debug)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags := flag.NewFlagSet("eventbus", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			c, err := readConfig(flags, tc.args)
			require.NoError(t, err)
			tc.check(t, c)
		})
	}
}

func TestCLIConfigDeliveryEvidenceRequiresNativeRuntime(t *testing.T) {
	flags := flag.NewFlagSet("eventbus", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	_, err := readConfig(flags, []string{"--sqs-delivery-log", "deliveries.jsonl"})
	require.ErrorContains(t, err, "sqs-delivery-log requires lambda-functions")
}

func TestCLIConfigRejectsUnexpectedPositionals(t *testing.T) {
	for _, args := range [][]string{
		{"version"},
		{"--port", "14100", "version"},
		{"--", "version"},
		{"version", "--port", "14100"},
		{"--port", "14100", "version", "--debug"},
		{"--cognito-profile", "unknown", "version"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			flags := flag.NewFlagSet("eventbus", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			cfg, err := readConfig(flags, args)
			require.ErrorContains(t, err, "unexpected positional arguments")
			require.ErrorContains(t, err, "version")
			require.Equal(t, config{}, cfg)
		})
	}
	flags := flag.NewFlagSet("eventbus", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	cfg, err := readConfig(flags, []string{"--port", "14100", "--"})
	require.NoError(t, err, "an empty end-of-options delimiter is valid")
	require.Equal(t, 14100, cfg.port)
}

// The subprocess calls the production CLI entry point without compiling a
// second binary. It owns every path and port passed after the test delimiter.
func TestCLIStartupProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_CLI_STARTUP_CHILD") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		fmt.Fprintln(os.Stderr, "CLI startup fixture is missing its argument delimiter")
		os.Exit(2)
	}
	os.Args = append([]string{"eventbus"}, os.Args[separator+1:]...)
	if err := Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func cliStartupPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

func cliStartupArguments(directory string, port int) ([]string, []string) {
	paths := []string{
		filepath.Join(directory, "cognito.db"),
		filepath.Join(directory, "sns.jsonl"),
		filepath.Join(directory, "ses.jsonl"),
		filepath.Join(directory, "notifications.jsonl"),
	}
	return []string{
		"--port", strconv.Itoa(port), "--work-dir", directory,
		"--cognito-db", paths[0], "--sns-log", paths[1],
		"--ses-log", paths[2], "--cognito-log", paths[3],
		"--s3-endpoint", "http://127.0.0.1:1",
	}, paths
}

func TestCLIStartupRejectsPositionalsBeforeRuntimeEffects(t *testing.T) {
	executable, err := os.Executable()
	require.NoError(t, err)
	for _, suffix := range [][]string{{"version"}, {"--", "version"}, {"version", "--debug"}} {
		t.Run(strings.Join(suffix, " "), func(t *testing.T) {
			directory := t.TempDir()
			port := cliStartupPort(t)
			args, paths := cliStartupArguments(directory, port)
			delivery := filepath.Join(directory, "deliveries.jsonl")
			// A malformed command must win over even an unreadable runtime fixture.
			args = append(args, "--lambda-functions", filepath.Join(directory, "missing-functions.yaml"), "--sqs-delivery-log", delivery)
			args = append(args, suffix...)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, executable, append([]string{"-test.run=^TestCLIStartupProcess$", "--"}, args...)...)
			command.Dir = directory
			command.Env = append(os.Environ(), "EVENTBUS_CLI_STARTUP_CHILD=1")
			command.WaitDelay = time.Second
			output, err := command.CombinedOutput()
			require.NoError(t, ctx.Err(), "malformed CLI did not exit promptly: %s", output)
			var exit *exec.ExitError
			require.ErrorAs(t, err, &exit, "malformed CLI unexpectedly succeeded: %s", output)
			require.Contains(t, string(output), "unexpected positional arguments")
			require.NotContains(t, string(output), "failed to configure Lambda")
			for _, path := range append(paths, delivery) {
				_, err := os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist, "malformed CLI created %s", path)
			}
			files, err := os.ReadDir(directory)
			require.NoError(t, err)
			require.Empty(t, files, "malformed CLI created runtime files")
			listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
			require.NoError(t, err, "malformed CLI left its listener active")
			require.NoError(t, listener.Close())
		})
	}
}

func TestCLIStartupValidFlagsStillServeAndCloseOwnedPaths(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("production process cleanup and signals require Linux or Darwin")
	}
	directory := t.TempDir()
	port := cliStartupPort(t)
	args, paths := cliStartupArguments(directory, port)
	args = append(args, "--")
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, append([]string{"-test.run=^TestCLIStartupProcess$", "--"}, args...)...)
	command.Dir = directory
	command.Env = append(os.Environ(), "EVENTBUS_CLI_STARTUP_CHILD=1")
	command.WaitDelay = time.Second
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	require.NoError(t, command.Start())
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = command.Wait(); close(done) }()
	t.Cleanup(func() { _ = command.Process.Kill(); <-done })
	client := &http.Client{Timeout: 100 * time.Millisecond}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for {
		response, err := client.Get(fmt.Sprintf("http://127.0.0.1:%d/health", port))
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(response.Body, 1024))
			_ = response.Body.Close()
			if readErr == nil && response.StatusCode == http.StatusOK && bytes.Contains(data, []byte(`"service":"eventbus"`)) {
				break
			}
		}
		select {
		case <-done:
			t.Fatalf("valid CLI exited before readiness: %v; %s", waitErr, output.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("valid CLI did not reach readiness")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, path := range paths {
		_, err := os.Stat(path)
		require.NoError(t, err, "valid CLI omitted its configured path %s", path)
	}
	require.NoError(t, command.Process.Signal(os.Interrupt))
	select {
	case <-done:
		require.NoError(t, waitErr, "valid CLI failed shutdown: %s", output.String())
	case <-ctx.Done():
		t.Fatal("valid CLI did not join shutdown")
	}
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	require.NoError(t, err, "valid CLI did not release its listener")
	require.NoError(t, listener.Close())
}
