package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// This registered application child uses the unchanged Go SQS SDK. The private
// Runtime API supplies its native batch; synced files are its business effects.
func TestSQSManualSettlementProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_SQS_MANUAL_SETTLEMENT_PROCESS") != "1" {
		return
	}
	if err := sqsManualSettlementInvocation(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func sqsManualSettlementInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
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
	if readErr != nil || response.StatusCode != http.StatusOK || id == "" {
		return errors.New("invalid private Runtime API invocation")
	}
	var event sqsevent.Event
	if err := json.Unmarshal(payload, &event); err != nil || len(event.Records) != 5 {
		return errors.New("handler expected one native batch of five")
	}
	parts := strings.Split(event.Records[0].EventSourceARN, ":")
	if len(parts) != 6 {
		return errors.New("invalid native SQS source ARN")
	}
	native := sqs.NewFromConfig(aws.Config{Region: os.Getenv("AWS_REGION"), Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client}, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(os.Getenv("SQS_MANUAL_ENDPOINT"))
	})
	queue, err := native.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(parts[5])})
	if err != nil {
		return err
	}
	directory := os.Getenv("SQS_MANUAL_EFFECTS")
	for index, record := range event.Records {
		if record.EventSource != "aws:sqs" || record.EventSourceARN != event.Records[0].EventSourceARN || record.MessageID == "" || record.ReceiptHandle == "" {
			return errors.New("invalid native SQS record")
		}
		if err := sqsManualSettlementSync(filepath.Join(directory, record.MessageID+".effect"), []byte(record.Body)); err != nil {
			return err
		}
		if index < 3 {
			if _, err := native.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: queue.QueueUrl, ReceiptHandle: aws.String(record.ReceiptHandle)}); err != nil {
				return err
			}
		}
	}
	if err := os.WriteFile(filepath.Join(directory, "batch.json"), payload, 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, "manual-ready"), []byte(id), 0600); err != nil {
		return err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(filepath.Join(directory, "return-success")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, runtime+id+"/response", strings.NewReader(`{"effects_committed":5,"manually_deleted":3}`))
	if err != nil {
		return err
	}
	ack, err := client.Do(request)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Runtime API acknowledgment returned %d", ack.StatusCode)
	}
	return nil
}

func sqsManualSettlementSync(filename string, contents []byte) error {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(contents)
	syncErr, closeErr := file.Sync(), file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return err
	}
	// Commit the newly created directory entry before deleting its SQS receipt.
	directory, err := os.Open(filepath.Dir(filename))
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}

func TestProductionSQSManualSettlementJoinsSuccessfulMixedBatch(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.port = retainedTestPort(t)
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", cfg.port)
	cfg.issuerBase = endpoint
	cfg.cognitoLog, cfg.snsLog = filepath.Join(cfg.workDir, "cognito.jsonl"), filepath.Join(cfg.workDir, "sns.jsonl")
	effects := filepath.Join(cfg.workDir, "effects")
	require.NoError(t, os.Mkdir(effects, 0700))
	executable, err := os.Executable()
	require.NoError(t, err)
	recipe, err := yaml.Marshal(lambdaservice.Config{Functions: map[string]lambdaservice.Function{"manual-settlement:live": {
		Runtime: "provided", Command: []string{executable, "-test.run=^TestSQSManualSettlementProvidedProcess$"}, Timeout: 10 * time.Second,
		Environment: map[string]string{"EVENTBUS_SQS_MANUAL_SETTLEMENT_PROCESS": "1", "SQS_MANUAL_ENDPOINT": endpoint, "SQS_MANUAL_EFFECTS": effects},
	}}})
	require.NoError(t, err)
	cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
	require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	joined := false
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		if joined {
			return
		}
		_ = os.WriteFile(filepath.Join(effects, "return-success"), nil, 0600)
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Error("manual-settlement production application did not join")
		}
	})
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	require.Eventually(t, func() bool {
		response, err := client.Get(endpoint + "/health")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 5*time.Second, 10*time.Millisecond)
	native := sqs.NewFromConfig(aws.Config{Region: cfg.region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client}, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	queue, err := native.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("manual-settlement")})
	require.NoError(t, err)
	var entries []sqstypes.SendMessageBatchRequestEntry
	for index := range 5 {
		entries = append(entries, sqstypes.SendMessageBatchRequestEntry{Id: aws.String(fmt.Sprint(index)), MessageBody: aws.String(fmt.Sprintf("durable-effect-%d", index))})
	}
	sent, err := native.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{QueueUrl: queue.QueueUrl, Entries: entries})
	require.NoError(t, err)
	require.Empty(t, sent.Failed)
	require.Len(t, sent.Successful, 5)
	sourceARN := fmt.Sprintf("arn:aws:sqs:%s:%s:manual-settlement", cfg.region, cfg.accountID)
	functionARN := fmt.Sprintf("arn:aws:lambda:%s:%s:function:manual-settlement:live", cfg.region, cfg.accountID)
	mapping := messagingCompositionMapping(t, t.Context(), client, http.MethodPost, endpoint, "", http.StatusAccepted, map[string]any{"EventSourceArn": sourceARN, "FunctionName": functionARN, "BatchSize": 5})
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(effects, "manual-ready")); return err == nil }, 5*time.Second, 10*time.Millisecond)
	payload, err := os.ReadFile(filepath.Join(effects, "batch.json"))
	require.NoError(t, err)
	var event sqsevent.Event
	require.NoError(t, json.Unmarshal(payload, &event))
	require.Len(t, event.Records, 5)
	for _, entry := range sent.Successful {
		body, err := os.ReadFile(filepath.Join(effects, *entry.MessageId+".effect"))
		require.NoError(t, err)
		require.Equal(t, "durable-effect-"+*entry.Id, string(body))
	}
	for _, record := range event.Records {
		require.Equal(t, sourceARN, record.EventSourceARN)
		require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
	}
	beforeReturn := messagingCompositionMapping(t, t.Context(), client, http.MethodGet, endpoint, mapping.UUID, http.StatusOK, nil)
	require.NotEqual(t, "OK", beforeReturn.LastProcessingResult, "completion cannot precede the actual handler return")
	attributes := []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages, sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible}
	pending, err := native.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: attributes})
	require.NoError(t, err)
	require.Equal(t, "0", pending.Attributes["ApproximateNumberOfMessages"])
	require.Equal(t, "2", pending.Attributes["ApproximateNumberOfMessagesNotVisible"], "SDK deleted three actual receipts; mapping still owns the remaining two")
	require.NoError(t, os.WriteFile(filepath.Join(effects, "return-success"), nil, 0600))
	require.Eventually(t, func() bool {
		current := messagingCompositionMapping(t, t.Context(), client, http.MethodGet, endpoint, mapping.UUID, http.StatusOK, nil)
		return current.LastProcessingResult == "OK"
	}, 5*time.Second, 10*time.Millisecond, "joined completion must recognize native handler settlement and acknowledge the other two")
	settled, err := native.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: attributes})
	require.NoError(t, err)
	require.Equal(t, "0", settled.Attributes["ApproximateNumberOfMessages"])
	require.Equal(t, "0", settled.Attributes["ApproximateNumberOfMessagesNotVisible"])
	deleted := messagingCompositionMapping(t, t.Context(), client, http.MethodDelete, endpoint, mapping.UUID, http.StatusAccepted, nil)
	require.Equal(t, mapping.UUID, deleted.UUID)
	response, err := client.Get(endpoint + "/2015-03-31/event-source-mappings/" + mapping.UUID)
	require.NoError(t, err)
	_ = response.Body.Close()
	require.Equal(t, http.StatusNotFound, response.StatusCode, "native delete must join the original mapping before removing it")
	cancel()
	select {
	case err := <-done:
		joined = true
		require.NoError(t, err)
	case <-time.After(35 * time.Second):
		t.Fatal("native app teardown did not join the mapping and runtime owners")
	}
}
