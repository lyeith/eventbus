//go:build sdksmoke && (linux || darwin)

package sdk

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/app"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLocalDeliveryCLIProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_LOCAL_DELIVERY_CLI_CHILD") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		os.Exit(2)
	}
	os.Args = append([]string{"eventbus"}, os.Args[separator+1:]...)
	if err := app.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

type localWebhookRecord struct {
	Kind            string `json:"kind"`
	MessageID       string `json:"message_id"`
	TopicARN        string `json:"topic_arn"`
	SubscriptionARN string `json:"subscription_arn"`
	Raw             string `json:"raw"`
	ContentType     string `json:"content_type"`
	Status          int    `json:"status"`
	Body            string `json:"body,omitempty"`
}

func localDeliveryWebhook(t *testing.T, port int, output io.Writer) http.Handler {
	t.Helper()
	var mu sync.Mutex
	attempts := map[string]int{}
	client := &http.Client{Timeout: 3 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(client.CloseIdleConnections)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/owned-hook" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		defer r.Body.Close()
		payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		record := localWebhookRecord{
			Kind: r.Header.Get("X-Amz-Sns-Message-Type"), MessageID: r.Header.Get("X-Amz-Sns-Message-Id"),
			TopicARN: r.Header.Get("X-Amz-Sns-Topic-Arn"), SubscriptionARN: r.Header.Get("X-Amz-Sns-Subscription-Arn"),
			Raw: r.Header.Get("X-Amz-Sns-Rawdelivery"), ContentType: r.Header.Get("Content-Type"),
			Status: http.StatusNoContent,
		}
		switch record.Kind {
		case "SubscriptionConfirmation":
			// Synchronously call the supplied confirmation URL before the POST
			// completes, which verifies SNS doesn't hold topic locks over delivery.
			record.Status = confirmLocalDeliverySubscription(r.Context(), client, port, record, payload)
		case "Notification":
			record.Body = string(payload)
			var envelope struct{ Message string }
			if record.Raw != "true" && json.Unmarshal(payload, &envelope) != nil {
				record.Status = http.StatusBadRequest
			}
			mu.Lock()
			attempts[record.MessageID]++
			attempt := attempts[record.MessageID]
			mu.Unlock()
			switch envelope.Message {
			case "retry-the-same-message":
				if attempt == 1 {
					record.Status = http.StatusTooManyRequests
				} else if attempt == 2 {
					record.Status = http.StatusServiceUnavailable
				}
			case "drain-retry-on-exit":
				if attempt == 1 {
					record.Status = http.StatusServiceUnavailable
				}
			case "exhaust-retries":
				record.Status = http.StatusServiceUnavailable
			case "permanent-failure":
				record.Status = http.StatusBadRequest
			}
		case "UnsubscribeConfirmation":
			// Evidence records omit the token-bearing control body.
		default:
			record.Status = http.StatusBadRequest
		}
		mu.Lock()
		err = json.NewEncoder(output).Encode(record)
		mu.Unlock()
		if err != nil {
			t.Errorf("write webhook evidence: %v", err)
			record.Status = http.StatusInternalServerError
		}
		w.WriteHeader(record.Status)
	})
}

func confirmLocalDeliverySubscription(ctx context.Context, client *http.Client, port int, record localWebhookRecord, payload []byte) int {
	var envelope struct {
		Type, MessageID, TopicARN, Token, SubscribeURL string
	}
	if json.Unmarshal(payload, &envelope) != nil || envelope.Type != record.Kind || envelope.MessageID != record.MessageID ||
		envelope.TopicARN != record.TopicARN || envelope.Token == "" {
		return http.StatusBadRequest
	}
	link, err := url.Parse(envelope.SubscribeURL)
	if err != nil || link.Scheme != "http" || link.User != nil || link.Fragment != "" || link.Path != "/" ||
		link.Port() != strconv.Itoa(port) || (link.Hostname() != "localhost" && link.Hostname() != "127.0.0.1") {
		return http.StatusBadRequest
	}
	query := link.Query()
	if query.Get("Action") != "ConfirmSubscription" || query.Get("TopicArn") != record.TopicARN || query.Get("Token") != envelope.Token {
		return http.StatusBadRequest
	}
	link.Host = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, link.String(), nil)
	if err != nil {
		return http.StatusBadRequest
	}
	response, err := client.Do(request)
	if err != nil {
		return http.StatusGatewayTimeout
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if response.StatusCode != http.StatusOK {
		return response.StatusCode
	}
	return http.StatusNoContent
}

func TestLocalDeliveryPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	directory := t.TempDir()
	// The SDK fixture also supplies the application-owned Lambda handler.
	// Copy it under TempDir so any runtime-generated cache stays disposable.
	script := filepath.Join(directory, "local_delivery.py")
	code, err := os.ReadFile(fixturePath("python", "local_delivery.py"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(script, code, 0o600))
	worker := lambda.Function{
		Runtime: "python", Command: []string{python, "-E", "-s"}, Handler: script + "#handler", Timeout: 5 * time.Second,
	}
	functions, err := yaml.Marshal(lambda.Config{
		Functions: map[string]lambda.Function{"local-delivery:live": worker, "local-delivery:other": worker},
		DevAsync: &lambda.DevAsyncConfig{Workers: 1, Capacity: 16, RetryDelays: []time.Duration{0, 0},
			LogPath: filepath.Join(directory, "async.jsonl")},
	})
	require.NoError(t, err)
	functionsPath := filepath.Join(directory, "functions.yaml")
	require.NoError(t, os.WriteFile(functionsPath, functions, 0o600))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	endpoint := "http://" + listener.Addr().String()
	hookPath := filepath.Join(directory, "webhook.jsonl")
	hookLog, err := os.OpenFile(hookPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = hookLog.Close() })
	hook := localDeliveryWebhook(t, port, hookLog)
	webhook := httptest.NewServer(hook)
	t.Cleanup(webhook.Close)
	secureWebhook := httptest.NewTLSServer(hook)
	t.Cleanup(secureWebhook.Close)
	caPath := filepath.Join(directory, "webhook-ca.pem")
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: secureWebhook.Certificate().Raw})
	require.NoError(t, os.WriteFile(caPath, ca, 0o600))
	defer listener.Close()
	executable, err := os.Executable()
	require.NoError(t, err)
	hookEndpoint := webhook.URL + "/owned-hook"
	secureHookEndpoint := secureWebhook.URL + "/owned-hook"
	args := []string{"--port", strconv.Itoa(port), "--work-dir", directory, "--issuer-base", endpoint,
		"--cognito-db", filepath.Join(directory, "cognito.db"), "--ses-log", filepath.Join(directory, "ses.jsonl"),
		"--sns-log", filepath.Join(directory, "sns.jsonl"), "--cognito-log", filepath.Join(directory, "notifications.jsonl"),
		"--lambda-functions", functionsPath, "--s3-endpoint", "http://127.0.0.1:1",
		"--sns-http-endpoints", hookEndpoint + "," + secureHookEndpoint, "--fixture-tls-ca", caPath, "--sns-retry-delay-scale", "0.01"}
	command := exec.Command(executable, append([]string{"-test.run=^TestLocalDeliveryCLIProcess$", "--"}, args...)...)
	if binary := os.Getenv("EVENTBUS_SMOKE_BINARY"); binary != "" {
		require.True(t, filepath.IsAbs(binary), "EVENTBUS_SMOKE_BINARY must select an absolute owned executable")
		info, err := os.Stat(binary)
		require.NoError(t, err)
		require.False(t, info.IsDir())
		command = exec.Command(binary, args...)
		t.Logf("packaged local delivery proof: %s", binary)
	}
	command.Dir = directory
	command.Env = append(sdkEnvironment(directory, endpoint, "", ""), "EVENTBUS_LOCAL_DELIVERY_CLI_CHILD=1")
	logPath := filepath.Join(directory, "cli.log")
	log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	command.Stdout, command.Stderr = log, log
	command.WaitDelay = time.Second
	require.NoError(t, listener.Close())
	require.NoError(t, command.Start())
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = command.Wait(); close(done) }()
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() {
			_ = command.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
				if waitErr != nil {
					t.Errorf("local delivery CLI shutdown failed: %v", waitErr)
				}
			case <-time.After(20 * time.Second):
				_ = command.Process.Kill()
				<-done
				t.Error("local delivery CLI did not join pending activity")
			}
		})
	}
	t.Cleanup(func() {
		stop()
		if t.Failed() {
			data, _ := os.ReadFile(logPath)
			t.Logf("owned CLI output:\n%s", data)
		}
	})
	client := &http.Client{Timeout: time.Second}
	t.Cleanup(client.CloseIdleConnections)
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return false
		default:
		}
		response, err := client.Get(endpoint + "/health")
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 10*time.Second, 10*time.Millisecond, "delivery CLI did not reach readiness")
	environment := append(sdkEnvironment(directory, endpoint, "", ""),
		"LOCAL_DELIVERY_ENDPOINT="+endpoint, "LOCAL_DELIVERY_WEBHOOK="+hookEndpoint,
		"LOCAL_DELIVERY_HTTPS_WEBHOOK="+secureHookEndpoint, "LOCAL_DELIVERY_EVIDENCE="+hookPath)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, script, environment)
	t.Logf("local SNS and Lambda delivery through the real CLI:\n%s", output)
	require.NoError(t, err)
	before := localDrainWebhookAttempts(t, hookPath)
	require.Len(t, before, 1, "test must enter shutdown with a retry pending")
	require.Equal(t, http.StatusServiceUnavailable, before[0].Status)
	stop()
	after := localDrainWebhookAttempts(t, hookPath)
	require.Len(t, after, 2)
	require.Equal(t, before[0].MessageID, after[1].MessageID)
	require.Equal(t, http.StatusNoContent, after[1].Status)
}

func localDrainWebhookAttempts(t *testing.T, path string) []localWebhookRecord {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var attempts []localWebhookRecord
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		var record localWebhookRecord
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if strings.Contains(record.Body, "drain-retry-on-exit") {
			attempts = append(attempts, record)
		}
	}
	return attempts
}
