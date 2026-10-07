package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

func TestProductionSQSBatchComposition(t *testing.T) {
	cfg := runTestConfig(t)
	observations := filepath.Join(cfg.workDir, "observations")
	require.NoError(t, os.Mkdir(observations, 0700))
	executable, err := os.Executable()
	require.NoError(t, err)
	recipe, err := yaml.Marshal(lambdaservice.Config{Functions: map[string]lambdaservice.Function{"batch:live": {
		Runtime: "provided", Command: []string{executable, "-test.run=^TestMessagingCompositionProvidedProcess$"}, Timeout: 2 * time.Second,
		Environment: map[string]string{"EVENTBUS_MESSAGING_COMPOSITION_PROCESS": "1", "MESSAGING_OBSERVATIONS": observations, "MESSAGING_HANDLER": "batch-alias"},
	}}})
	require.NoError(t, err)
	cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
	require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	cfg.port = reservation.Addr().(*net.TCPAddr).Port
	require.NoError(t, reservation.Close())
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", cfg.port)
	cfg.issuerBase = endpoint
	cfg.cognitoLog, cfg.snsLog = filepath.Join(cfg.workDir, "cognito.jsonl"), filepath.Join(cfg.workDir, "sns.jsonl")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Error("batch application failed to join")
		}
	})
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	require.Eventually(t, func() bool {
		response, err := client.Get(endpoint + "/health")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == 200
	}, 5*time.Second, 10*time.Millisecond)
	native := sqs.NewFromConfig(aws.Config{Region: cfg.region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: client, RetryMaxAttempts: 1}, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	created, err := native.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("production-batch")})
	require.NoError(t, err)
	var entries []sqstypes.SendMessageBatchRequestEntry
	for i := range 5 {
		entries = append(entries, sqstypes.SendMessageBatchRequestEntry{Id: aws.String(fmt.Sprint(i)), MessageBody: aws.String(fmt.Sprintf("record-%d", i))})
	}
	sent, err := native.SendMessageBatch(t.Context(), &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: entries})
	require.NoError(t, err)
	require.Len(t, sent.Successful, 5)
	sourceARN := "arn:aws:sqs:" + cfg.region + ":" + cfg.accountID + ":production-batch"
	functionARN := "arn:aws:lambda:" + cfg.region + ":" + cfg.accountID + ":function:batch:live"
	mapping := messagingCompositionMapping(t, t.Context(), client, http.MethodPost, endpoint, "", 202, map[string]any{"EventSourceArn": sourceARN, "FunctionName": functionARN, "BatchSize": 5, "ScalingConfig": map[string]int{"MaximumConcurrency": 2}})
	require.Equal(t, 5, mapping.BatchSize)
	require.NotNil(t, mapping.ScalingConfig)
	require.Equal(t, 2, *mapping.ScalingConfig.MaximumConcurrency)
	var got []messagingCompositionObservation
	require.Eventually(t, func() bool {
		got, err = messagingCompositionObservations(observations)
		return err == nil && len(got) > 0
	}, 5*time.Second, 10*time.Millisecond)
	require.Len(t, got, 1, "one backlog batch must reach the production integration")
	require.Equal(t, "batch-alias", got[0].Handler)
	require.Equal(t, functionARN, got[0].FunctionARN)
	var event sqsevent.Event
	require.NoError(t, json.Unmarshal(got[0].Event, &event))
	require.Len(t, event.Records, 5)
	ids := map[string]bool{}
	for _, record := range event.Records {
		require.Equal(t, sourceARN, record.EventSourceARN)
		require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
		ids[record.MessageID] = true
	}
	for _, entry := range sent.Successful {
		require.True(t, ids[*entry.MessageId])
	}
	require.Eventually(t, func() bool {
		got := messagingCompositionMapping(t, t.Context(), client, http.MethodGet, endpoint, mapping.UUID, 200, nil)
		return got.LastProcessingResult == "OK"
	}, 5*time.Second, 10*time.Millisecond)
	queue, err := native.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages, sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	require.NoError(t, err)
	require.Equal(t, "0", queue.Attributes["ApproximateNumberOfMessages"])
	require.Equal(t, "0", queue.Attributes["ApproximateNumberOfMessagesNotVisible"])
	messagingCompositionMapping(t, t.Context(), client, http.MethodDelete, endpoint, mapping.UUID, 202, nil)
}
