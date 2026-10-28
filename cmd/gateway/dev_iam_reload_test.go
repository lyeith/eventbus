package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/lyeith/eventbus/internal/gateway"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestGatewayCLIExplicitIAMReloadKeepsServingAndRetainsInvalidCandidate(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("Unix fixture reload signal")
	}
	directory := t.TempDir()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusAccepted) }))
	t.Cleanup(backend.Close)
	port := gatewayCLIStartupPort(t)
	cfg := gateway.Config{Port: port, IAMCredentials: map[string]gateway.DevIAMCredential{"LOCAL_SERVICE": {
		SecretAccessKey: "first-secret", PrincipalARN: "arn:aws:iam::222222222222:user/service", AccountID: "222222222222",
	}}, Routes: []gateway.RouteConfig{{Path: "/items", Method: "GET", AuthorizationType: "AWS_IAM", Integration: gateway.IntegrationConfig{Type: "HTTP_PROXY", URI: backend.URL}}}}
	path := filepath.Join(directory, "gateway.yaml")
	write := func(candidate gateway.Config) {
		t.Helper()
		data, err := yaml.Marshal(candidate)
		require.NoError(t, err)
		staged := path + ".next"
		require.NoError(t, os.WriteFile(staged, data, 0600))
		require.NoError(t, os.Rename(staged, path))
	}
	write(cfg)
	executable, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, executable, "-test.run=^TestGatewayCLIStartupProcess$", "--", "--config", path)
	command.Dir = directory
	command.Env = append(os.Environ(), "EVENTBUS_GATEWAY_CLI_STARTUP_CHILD=1")
	command.WaitDelay = time.Second
	output, err := os.OpenFile(filepath.Join(directory, "child.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	command.Stdout, command.Stderr = output, output
	require.NoError(t, command.Start())
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = command.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = command.Process.Kill()
			<-done
			t.Error("reload fixture failed to join")
		}
		_ = output.Close()
		if waitErr != nil {
			t.Errorf("gateway fixture exit: %v", waitErr)
		}
		if t.Failed() {
			data, _ := os.ReadFile(output.Name())
			t.Logf("gateway fixture: %s", data)
		}
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			t.Error(err)
		} else {
			_ = listener.Close()
		}
	})
	client := &http.Client{Timeout: 300 * time.Millisecond}
	defer client.CloseIdleConnections()
	origin := "http://127.0.0.1:" + strconv.Itoa(port)
	signedStatus := func(key, secret string) int {
		request, err := http.NewRequestWithContext(ctx, "GET", origin+"/items", nil)
		require.NoError(t, err)
		digest := sha256.Sum256(nil)
		require.NoError(t, v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: key, SecretAccessKey: secret}, request, hex.EncodeToString(digest[:]), "execute-api", "us-east-1", time.Now()))
		response, err := client.Do(request)
		if err != nil {
			return 0
		}
		defer response.Body.Close()
		_, _ = io.Copy(io.Discard, response.Body)
		return response.StatusCode
	}
	await := func(condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !condition() {
			select {
			case <-done:
				t.Fatalf("gateway exited before expected reload: %v", waitErr)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("gateway did not publish expected IAM generation")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	await(func() bool { return signedStatus("LOCAL_SERVICE", "first-secret") == 202 })
	first := cfg.IAMCredentials["LOCAL_SERVICE"]
	next := first
	next.SecretAccessKey = "second-secret"
	cfg.IAMCredentials = map[string]gateway.DevIAMCredential{"NEXT_SERVICE": next}
	write(cfg)
	require.NoError(t, command.Process.Signal(syscall.SIGHUP))
	await(func() bool {
		return signedStatus("LOCAL_SERVICE", "first-secret") == 403 && signedStatus("NEXT_SERVICE", "second-secret") == 202
	})
	// Empty is a valid generation: revoke the last key without changing routes.
	cfg.IAMCredentials = map[string]gateway.DevIAMCredential{}
	write(cfg)
	require.NoError(t, command.Process.Signal(syscall.SIGHUP))
	await(func() bool { return signedStatus("NEXT_SERVICE", "second-secret") == 403 })
	cfg.IAMCredentials = map[string]gateway.DevIAMCredential{"NEXT_SERVICE": next}
	write(cfg)
	require.NoError(t, command.Process.Signal(syscall.SIGHUP))
	await(func() bool { return signedStatus("NEXT_SERVICE", "second-secret") == 202 })
	invalid := cfg
	invalid.IAMCredentials = map[string]gateway.DevIAMCredential{"NEXT_SERVICE": next}
	invalid.IAMCredentials["NEXT_SERVICE"] = gateway.DevIAMCredential{SecretAccessKey: "must-not-be-logged", PrincipalARN: "invalid", AccountID: "222222222222"}
	write(invalid)
	require.NoError(t, command.Process.Signal(syscall.SIGHUP))
	await(func() bool {
		data, _ := os.ReadFile(output.Name())
		return bytes.Contains(data, []byte("IAM credential fixture reload rejected; active generation retained"))
	})
	require.Equal(t, 202, signedStatus("NEXT_SERVICE", "second-secret"))
	data, err := os.ReadFile(output.Name())
	require.NoError(t, err)
	require.NotContains(t, string(data), "must-not-be-logged")
}
