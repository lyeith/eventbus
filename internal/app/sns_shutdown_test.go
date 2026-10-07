//go:build linux || darwin

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestSNSLifecycleChildProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_SNS_LIFECYCLE_CHILD") != "1" {
		return
	}
	for {
		time.Sleep(time.Second)
	}
}

// This test executable is an actual provided runtime. Its child remains in the
// runtime's process group; only the existing Lambda owner cleans up the group.
func TestSNSLifecycleProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_SNS_LIFECYCLE_RUNTIME") != "1" {
		return
	}
	if err := snsLifecycleProvidedInvocation(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func snsLifecycleProvidedInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	api := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, api+"next", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	requestID := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	var event struct {
		Records []struct {
			Source string `json:"EventSource"`
			SNS    struct {
				MessageID string `json:"MessageId"`
			} `json:"Sns"`
		} `json:"Records"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&event)
	_ = response.Body.Close()
	if decodeErr != nil || response.StatusCode != http.StatusOK || requestID == "" || len(event.Records) != 1 || event.Records[0].Source != "aws:sns" {
		return errors.New("provided runtime did not receive native SNS event")
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	child := exec.Command(executable, "-test.run=^TestSNSLifecycleChildProcess$")
	child.Env = append(os.Environ(), "EVENTBUS_SNS_LIFECYCLE_CHILD=1")
	if err := child.Start(); err != nil {
		return err
	}
	gate, err := http.NewRequestWithContext(ctx, http.MethodGet, os.Getenv("SNS_LIFECYCLE_GATE"), nil)
	if err != nil {
		return err
	}
	gate.Header.Set("X-SNS-Runtime-Pid", strconv.Itoa(os.Getpid()))
	gate.Header.Set("X-SNS-Child-Pid", strconv.Itoa(child.Process.Pid))
	ack, err := client.Do(gate)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusOK {
		return errors.New("SNS lifecycle gate failed")
	}
	_, err = snsLifecycleSDK(os.Getenv("SNS_LIFECYCLE_ENDPOINT"), client).Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(os.Getenv("SNS_LIFECYCLE_SIDE_EFFECT_TOPIC")), Message: aws.String("completed-" + event.Records[0].SNS.MessageID),
	})
	if err != nil {
		return err
	}
	reply, err := http.NewRequestWithContext(ctx, http.MethodPost, api+requestID+"/response", bytes.NewReader([]byte(`{"completed":true}`)))
	if err != nil {
		return err
	}
	reply.Header.Set("Content-Type", "application/json")
	ack, err = client.Do(reply)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("SNS runtime response status %d", ack.StatusCode)
	}
	return nil
}

func snsLifecycleSDK(endpoint string, client *http.Client) *sns.Client {
	return sns.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client},
		func(options *sns.Options) { options.BaseEndpoint = aws.String(endpoint) })
}

func snsLifecycleProcessRunning(pid int) bool {
	if pid <= 0 || syscall.Kill(pid, 0) != nil {
		return false
	}
	// An orphaned zombie has stopped execution; its platform parent owns reaping.
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		if _, tail, found := strings.Cut(string(data), ") "); found && strings.HasPrefix(tail, "Z ") {
			return false
		}
	}
	return true
}

func TestEventBusSNSShutdownJoinsAcceptedRuntimeAndChildBeforeCaptureClose(t *testing.T) {
	directory := t.TempDir()
	serving := httptest.NewUnstartedServer(nil)
	endpoint := "http://" + serving.Listener.Addr().String()
	broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
	capturePath := filepath.Join(directory, "sns.jsonl")
	capture, err := messaging.OpenSNSCapture(capturePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = capture.Close() })
	broker.SetSNSCapture(capture)
	topic, sideEffect := broker.CreateTopic("pending-handler"), broker.CreateTopic("handler-side-effect")
	executable, err := os.Executable()
	require.NoError(t, err)
	functions, err := lambdaservice.NewService(&lambdaservice.Config{
		Functions: map[string]lambdaservice.Function{"sns-shutdown": {
			Runtime: "provided", Command: []string{executable, "-test.run=^TestSNSLifecycleProvidedProcess$"}, Timeout: 15 * time.Second,
			Environment: map[string]string{"EVENTBUS_SNS_LIFECYCLE_RUNTIME": "1", "SNS_LIFECYCLE_ENDPOINT": endpoint,
				"SNS_LIFECYCLE_GATE": endpoint + "/__sns/gate", "SNS_LIFECYCLE_SIDE_EFFECT_TOPIC": sideEffect.ARN},
		}}, DevAsync: &lambdaservice.DevAsyncConfig{Workers: 1, Capacity: 2, RetryDelays: []time.Duration{0, 0}, LogPath: filepath.Join(directory, "lambda.jsonl")},
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := functions.Close(ctx); err != nil {
			t.Logf("SNS Lambda cleanup: %v", err)
		}
	})
	broker.SetLambdaDelivery(snsLambdaInvoker{runtime: functions})
	_, err = broker.Subscribe(topic.ARN, "lambda", "arn:aws:lambda:us-east-1:000000000000:function:sns-shutdown", nil)
	require.NoError(t, err)
	var phase, captureClosed, capturedDuringDrain, joinedBeforeCaptureClose atomic.Bool
	var processIDs [2]int
	owner := &backgroundLambdaOwner{Service: functions, entered: make(chan struct{}), phase: &phase}
	owned := &eventBusLifecycle{functions: owner, sns: closeFunc(func() error {
		terminal := functions.AsyncSnapshot()
		joinedBeforeCaptureClose.Store(len(terminal) == 1 && terminal[0].State == "succeeded" &&
			!snsLifecycleProcessRunning(processIDs[0]) && !snsLifecycleProcessRunning(processIDs[1]))
		captureClosed.Store(true)
		return capture.Close()
	})}
	listener := newEventBusListener(serving.Config, owned, 10*time.Second)
	gated := make(chan [2]int, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	awsHandler := server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions})
	serving.Config.Handler = http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/__sns/gate" {
			runtimePID, _ := strconv.Atoi(request.Header.Get("X-SNS-Runtime-Pid"))
			childPID, _ := strconv.Atoi(request.Header.Get("X-SNS-Child-Pid"))
			gated <- [2]int{runtimePID, childPID}
			select {
			case <-release:
				writer.WriteHeader(http.StatusOK)
			case <-request.Context().Done():
			}
			return
		}
		if err := request.ParseForm(); err == nil && request.FormValue("Action") == "Publish" && strings.HasPrefix(request.FormValue("Message"), "completed-") {
			capturedDuringDrain.Store(phase.Load() && !captureClosed.Load())
		}
		awsHandler.ServeHTTP(writer, request)
	})
	serving.Start()
	t.Cleanup(func() {
		releaseGate()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := listener.Shutdown(ctx); err != nil {
			t.Logf("SNS listener cleanup: %v", err)
		}
		serving.Close()
	})
	published, err := snsLifecycleSDK(endpoint, serving.Client()).Publish(t.Context(), &sns.PublishInput{TopicArn: aws.String(topic.ARN), Message: aws.String("accepted-before-shutdown")})
	require.NoError(t, err)
	select {
	case processIDs = <-gated:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted SNS handler did not enter its owned gate")
	}
	for _, pid := range processIDs {
		require.True(t, snsLifecycleProcessRunning(pid), "runtime and child must be live before shutdown")
	}
	done := make(chan error, 1)
	go func() { done <- listener.Shutdown(context.Background()) }()
	backgroundAwait(t, owner.entered, "SNS Lambda async drain")
	require.False(t, captureClosed.Load(), "pending handler still owns SNS capture")
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before accepted handler completion: %v", err)
	default:
	}
	releaseGate()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("SNS shutdown did not join its runtime")
	}
	require.True(t, capturedDuringDrain.Load(), "actual registered handler must publish through AWS HTTP during drain")
	require.True(t, captureClosed.Load())
	require.True(t, joinedBeforeCaptureClose.Load(), "execution and child cleanup must finish before capture closes")
	for _, pid := range processIDs {
		require.False(t, snsLifecycleProcessRunning(pid), "Lambda must stop runtime and child before resource close")
	}
	terminal := functions.AsyncSnapshot()
	require.Len(t, terminal, 1)
	require.Equal(t, "succeeded", terminal[0].State)
	require.Equal(t, 1, terminal[0].Attempts)
	data, err := os.ReadFile(capturePath)
	require.NoError(t, err)
	decoder := json.NewDecoder(bytes.NewReader(data))
	var admitted, sideEffectCaptured bool
	for {
		var record messaging.SNSCaptureRecord
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if record.Operation == "DeliveryAdmission" && record.MessageID == aws.ToString(published.MessageId) {
			require.Equal(t, terminal[0].RequestID, record.Deliveries[0].InvocationRequestID)
			admitted = true
		}
		if record.Operation == "Publish" && record.TargetARN == sideEffect.ARN && record.Message == "completed-"+aws.ToString(published.MessageId) {
			sideEffectCaptured = true
		}
	}
	require.True(t, admitted)
	require.True(t, sideEffectCaptured, "side-effect capture must precede sink closure")
	require.NoError(t, listener.Shutdown(t.Context()), "joined lifecycle result must remain repeatable")
}
