package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/eventsource"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDevEvidenceProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_DEV_EVIDENCE_PROCESS") != "1" {
		return
	}
	if err := devEvidenceInvocation(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

type devEvidenceInput struct {
	QueueName string `json:"queue_name"`
	Suite     string `json:"suite"`
	Secret    string `json:"secret"`
}

type devEvidenceMarker struct {
	RequestID   string `json:"request_id"`
	FunctionARN string `json:"function_arn"`
	MessageID   string `json:"message_id"`
}

func devEvidenceInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	role, directory := os.Getenv("DEV_EVIDENCE_ROLE"), os.Getenv("DEV_EVIDENCE_DIRECTORY")
	if role == "consumer" {
		if err := os.WriteFile(filepath.Join(directory, "consumer-launched"), nil, 0600); err != nil {
			return err
		}
	}
	runtime := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, runtime+"next", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	payload, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	id := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	if readErr != nil || response.StatusCode != 200 || id == "" {
		return errors.New("invalid native Runtime API event")
	}
	endpoint := os.Getenv("AWS_ENDPOINT_URL_SQS")
	if endpoint == "" {
		return errors.New("fixture requires its owned SQS endpoint")
	}
	native := sqs.NewFromConfig(aws.Config{Region: os.Getenv("AWS_REGION"), Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client}, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	result := []byte(`{"runtime_success":true}`)
	switch role {
	case "producer":
		var input devEvidenceInput
		if err := json.Unmarshal(payload, &input); err != nil {
			return err
		}
		queue, err := native.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(input.QueueName)})
		if err != nil {
			return err
		}
		body, err := json.Marshal(input)
		if err != nil {
			return err
		}
		sent, err := native.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: queue.QueueUrl, MessageBody: aws.String(string(body))})
		if err != nil {
			return err
		}
		result, err = json.Marshal(map[string]string{"message_id": *sent.MessageId})
		if err != nil {
			return err
		}
	case "consumer":
		var event sqsevent.Event
		if err := json.Unmarshal(payload, &event); err != nil || len(event.Records) != 1 || event.Records[0].EventSource != "aws:sqs" {
			return errors.New("consumer expected a native SQS record")
		}
		record := event.Records[0]
		var input devEvidenceInput
		if err := json.Unmarshal([]byte(record.Body), &input); err != nil {
			return err
		}
		business := "SUCCESS"
		if input.Suite == "one" {
			business = "FAILURE"
			_, _ = fmt.Fprintf(os.Stdout, "caught application failure: evidence-business-error (%s)\n", input.Secret)
		}
		if err := sqsManualSettlementSync(filepath.Join(directory, input.Suite+".business"), []byte(business)); err != nil {
			return err
		}
		queue, err := native.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(input.QueueName)})
		if err != nil {
			return err
		}
		if _, err := native.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: queue.QueueUrl, ReceiptHandle: aws.String(record.ReceiptHandle)}); err != nil {
			return err
		}
		marker, err := json.Marshal(devEvidenceMarker{RequestID: id, FunctionARN: response.Header.Get("Lambda-Runtime-Invoked-Function-Arn"), MessageID: record.MessageID})
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(directory, input.Suite+".started"), marker, 0600); err != nil {
			return err
		}
		if input.Suite == "one" {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				if _, err := os.Stat(filepath.Join(directory, "one-release")); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ticker.C:
				}
			}
		}
	default:
		return errors.New("unknown application fixture role")
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, runtime+id+"/response", bytes.NewReader(result))
	if err != nil {
		return err
	}
	ack, err := client.Do(request)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("native runtime acknowledgment returned %d", ack.StatusCode)
	}
	return nil
}

func devEvidenceRows[T any](filename string) ([]T, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var rows []T
	decoder := json.NewDecoder(file)
	for {
		var row T
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				return rows, nil
			}
			return nil, err
		}
		rows = append(rows, row)
	}
}

type devEvidenceDiagnostic struct {
	SchemaVersion      string `json:"schema_version"`
	RequestID          string `json:"request_id"`
	FunctionARN        string `json:"function_arn"`
	State              string `json:"state"`
	Attempt            int    `json:"attempt"`
	OwnershipConfirmed bool   `json:"ownership_confirmed"`
	FunctionError      bool   `json:"function_error"`
	Stdout             *struct {
		Data string `json:"data"`
	} `json:"stdout"`
}

// The HTTP action invokes a real registered producer. Its SDK send, the native
// mapping and the consumer's SDK delete remain service-owned. Private capture
// proves joined ownership even when business state deliberately remains FAILURE.
func TestProductionDevDeliveryEvidenceAndDiagnosticsRetainedResume(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.port, cfg.retainedCallbackPort = retainedTestPort(t), retainedTestPort(t)
	require.NotEqual(t, cfg.port, cfg.retainedCallbackPort)
	cfg.sqsDeliveryLog = filepath.Join(cfg.workDir, "delivery.jsonl")
	cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
	flags := flag.NewFlagSet("production-evidence", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	parsed, err := readConfig(flags, []string{"--port", strconv.Itoa(cfg.port), "--retained-owner-callback-port", strconv.Itoa(cfg.retainedCallbackPort), "--sqs-delivery-log", cfg.sqsDeliveryLog, "--lambda-functions", cfg.lambdaFunctions})
	require.NoError(t, err)
	cfg.port, cfg.retainedCallbackPort, cfg.sqsDeliveryLog, cfg.lambdaFunctions = parsed.port, parsed.retainedCallbackPort, parsed.sqsDeliveryLog, parsed.lambdaFunctions
	source, callback := fmt.Sprintf("http://127.0.0.1:%d", cfg.port), fmt.Sprintf("http://127.0.0.1:%d", cfg.retainedCallbackPort)
	cfg.issuerBase = source
	cfg.cognitoLog, cfg.snsLog = filepath.Join(cfg.workDir, "cognito.jsonl"), filepath.Join(cfg.workDir, "sns.jsonl")
	directory := filepath.Join(cfg.workDir, "business")
	require.NoError(t, os.Mkdir(directory, 0700))
	executable, err := os.Executable()
	require.NoError(t, err)
	functions := map[string]lambdaservice.Function{}
	for name, role := range map[string]string{"evidence-producer": "producer", "evidence-consumer:live": "consumer"} {
		functions[name] = lambdaservice.Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestDevEvidenceProvidedProcess$"}, Timeout: 8 * time.Second, Environment: map[string]string{"EVENTBUS_DEV_EVIDENCE_PROCESS": "1", "DEV_EVIDENCE_ROLE": role, "DEV_EVIDENCE_DIRECTORY": directory, "AWS_ENDPOINT_URL_SQS": callback}}
	}
	recipe, err := yaml.Marshal(lambdaservice.Config{Functions: functions, DevDiagnostics: &lambdaservice.DevDiagnosticsConfig{LogPath: "diagnostics.jsonl"}, DevAsync: &lambdaservice.DevAsyncConfig{LogPath: "async.jsonl"}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(directory, "one-release"), nil, 0600)
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Error("production evidence app failed to join")
		}
	})
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	require.Eventually(t, func() bool {
		response, err := client.Get(source + "/health")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == 200
	}, 5*time.Second, 10*time.Millisecond)
	native := sqs.NewFromConfig(aws.Config{Region: cfg.region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client}, func(options *sqs.Options) { options.BaseEndpoint = aws.String(source) })
	callbackNative := sqs.NewFromConfig(aws.Config{Region: cfg.region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client}, func(options *sqs.Options) { options.BaseEndpoint = aws.String(callback) })
	queue, err := native.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("evidence-owned")})
	require.NoError(t, err)
	queueARN := fmt.Sprintf("arn:aws:sqs:%s:%s:evidence-owned", cfg.region, cfg.accountID)
	consumerARN := fmt.Sprintf("arn:aws:lambda:%s:%s:function:evidence-consumer:live", cfg.region, cfg.accountID)
	mapping := messagingCompositionMapping(t, t.Context(), client, http.MethodPost, source, "", 202, map[string]any{"EventSourceArn": queueARN, "FunctionName": consumerARN, "BatchSize": 1})
	const secret = "fake-application-secret-must-not-enter-delivery-evidence"
	produce := func(suite string) string {
		body, err := json.Marshal(devEvidenceInput{QueueName: "evidence-owned", Suite: suite, Secret: secret})
		require.NoError(t, err)
		result := retainedHTTP(t, client, http.MethodPost, source+"/2015-03-31/functions/evidence-producer/invocations", string(body), "application/json", 200)
		var accepted struct {
			MessageID string `json:"message_id"`
		}
		require.NoError(t, json.Unmarshal(result, &accepted))
		require.NotEmpty(t, accepted.MessageID)
		return accepted.MessageID
	}
	firstID := produce("one")
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(directory, "one.started")); return err == nil }, 5*time.Second, 10*time.Millisecond)
	markerData, err := os.ReadFile(filepath.Join(directory, "one.started"))
	require.NoError(t, err)
	var marker devEvidenceMarker
	require.NoError(t, json.Unmarshal(markerData, &marker))
	require.Equal(t, firstID, marker.MessageID)
	rows, err := devEvidenceRows[eventsource.DeliveryRecord](cfg.sqsDeliveryLog)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "admitted", rows[0].State)
	require.False(t, rows[0].Joined)
	require.Equal(t, marker.RequestID, rows[0].RequestID)
	require.Equal(t, consumerARN, rows[0].InvokedFunctionARN)
	quiesced := make(chan retainedResponse, 1)
	go func() {
		quiesced <- retainedRequest(client, http.MethodPost, source+devquiescence.ControlPath+"/quiesce", `{"timeout_ms":8000}`, "application/json")
	}()
	require.Eventually(t, func() bool { return retainedControl(t, client, source, "", "", 200).State == devquiescence.Draining }, time.Second, 10*time.Millisecond)
	attributes, err := callbackNative.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages, sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	require.NoError(t, err)
	require.Equal(t, "0", attributes.Attributes["ApproximateNumberOfMessages"])
	require.Equal(t, "0", attributes.Attributes["ApproximateNumberOfMessagesNotVisible"], "manual settlement is not child completion")
	rows, err = devEvidenceRows[eventsource.DeliveryRecord](cfg.sqsDeliveryLog)
	require.NoError(t, err)
	require.Len(t, rows, 1, "no supervisory terminal may precede actual handler join")
	select {
	case result := <-quiesced:
		t.Fatalf("barrier preceded post-delete child work: %+v", result)
	default:
	}
	require.NoError(t, os.WriteFile(filepath.Join(directory, "one-release"), nil, 0600))
	held := retainedFullStackBarrier(t, quiesced)
	rows, err = devEvidenceRows[eventsource.DeliveryRecord](cfg.sqsDeliveryLog)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	terminal := rows[1]
	require.Equal(t, eventsource.DeliverySchemaVersion, terminal.SchemaVersion)
	require.Equal(t, rows[0].DeliveryID, terminal.DeliveryID)
	require.Equal(t, mapping.UUID, terminal.MappingUUID)
	require.Equal(t, queueARN, terminal.EventSourceARN)
	require.Equal(t, consumerARN, terminal.FunctionARN)
	require.Equal(t, marker.RequestID, terminal.RequestID)
	require.Equal(t, "succeeded", terminal.State)
	require.True(t, terminal.Joined)
	require.Len(t, terminal.Messages, 1)
	require.Equal(t, firstID, terminal.Messages[0].MessageID)
	require.Equal(t, 1, terminal.Messages[0].ReceiveCount)
	require.Equal(t, eventsource.ReceiptNativeSettled, terminal.Messages[0].Settlement)
	diagnostics, err := devEvidenceRows[devEvidenceDiagnostic](filepath.Join(cfg.workDir, "diagnostics.jsonl"))
	require.NoError(t, err)
	found := false
	for _, diagnostic := range diagnostics {
		if diagnostic.RequestID != marker.RequestID {
			continue
		}
		found = true
		require.Equal(t, "eventbus.lambda.invocation-diagnostic.v1", diagnostic.SchemaVersion)
		require.Equal(t, consumerARN, diagnostic.FunctionARN)
		require.Equal(t, "succeeded", diagnostic.State)
		require.Equal(t, 1, diagnostic.Attempt)
		require.True(t, diagnostic.OwnershipConfirmed)
		require.False(t, diagnostic.FunctionError)
		require.NotNil(t, diagnostic.Stdout)
		require.Contains(t, diagnostic.Stdout.Data, "caught application failure: evidence-business-error")
		require.Contains(t, diagnostic.Stdout.Data, secret)
	}
	require.True(t, found, "private diagnostics must correlate to the actual joined native invocation")
	business, err := os.ReadFile(filepath.Join(directory, "one.business"))
	require.NoError(t, err)
	require.Equal(t, "FAILURE", string(business), "runtime success never claims business success")
	raw, err := os.ReadFile(cfg.sqsDeliveryLog)
	require.NoError(t, err)
	require.NotContains(t, string(raw), secret)
	require.NotContains(t, string(raw), "evidence-business-error")
	retainedControl(t, client, source, "/resume", fmt.Sprintf(`{"generation":%d}`, held.Generation), 200)
	secondID := produce("two")
	second := retainedControl(t, client, source, "/quiesce", `{"timeout_ms":8000}`, 200)
	require.True(t, second.FixtureSafe)
	rows, err = devEvidenceRows[eventsource.DeliveryRecord](cfg.sqsDeliveryLog)
	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, mapping.UUID, rows[3].MappingUUID)
	require.Equal(t, secondID, rows[3].Messages[0].MessageID)
	require.True(t, rows[3].Joined)
	require.Equal(t, "succeeded", rows[3].State)
	business, err = os.ReadFile(filepath.Join(directory, "two.business"))
	require.NoError(t, err)
	require.Equal(t, "SUCCESS", string(business))
	for _, path := range []string{cfg.sqsDeliveryLog, filepath.Join(cfg.workDir, "diagnostics.jsonl")} {
		info, err := os.Stat(path)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	}
}
