package consumer

import (
	"context"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLambdaEvent(t *testing.T) {
	messages := []*messaging.Message{
		{ID: "msg-1", Body: `{"Message":"hello"}`, ReceiptHandle: "rh-1"},
		{ID: "msg-2", Body: `{"Message":"world"}`, ReceiptHandle: "rh-2"},
	}

	event := buildLambdaEvent(messages)

	records, ok := event["Records"].([]map[string]interface{})
	require.True(t, ok)
	require.Len(t, records, 2)

	assert.Equal(t, "msg-1", records[0]["messageId"])
	assert.Equal(t, `{"Message":"hello"}`, records[0]["body"])
	assert.Equal(t, "rh-1", records[0]["receiptHandle"])

	assert.Equal(t, "msg-2", records[1]["messageId"])
}

func TestBuildLambdaEventSingleMessage(t *testing.T) {
	messages := []*messaging.Message{
		{ID: "msg-1", Body: `{"test":true}`, ReceiptHandle: "rh-1"},
	}

	event := buildLambdaEvent(messages)
	records := event["Records"].([]map[string]interface{})
	assert.Len(t, records, 1)
}

func TestBuildLambdaEventEmpty(t *testing.T) {
	event := buildLambdaEvent(nil)
	records := event["Records"].([]map[string]interface{})
	assert.Empty(t, records)
}

func TestValidateBatchFailuresRejectsUnknownAndDuplicateIDs(t *testing.T) {
	messages := []*messaging.Message{{ID: "msg-1"}, {ID: "msg-2"}}

	_, err := validateBatchFailures(&handlerBatchResult{BatchItemFailures: []batchItemFailure{{ItemIdentifier: "unknown"}}}, messages)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown message")

	_, err = validateBatchFailures(&handlerBatchResult{BatchItemFailures: []batchItemFailure{
		{ItemIdentifier: "msg-1"},
		{ItemIdentifier: "msg-1"},
	}}, messages)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "repeats")
}

func TestSettleBatchDeletesSuccessRetriesFailureThenDeadLetters(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	source := broker.CreateQueue("source", 0, 0)
	dlq := broker.CreateQueue("source-dlq", 0, 0)
	topic := broker.CreateTopic("consumer-records")
	_, err := broker.Subscribe(topic.ARN, "sqs", source.ARN, nil)
	require.NoError(t, err)
	_, err = broker.Publish(topic.ARN, `{"record":1}`, nil)
	require.NoError(t, err)
	_, err = broker.Publish(topic.ARN, `{"record":2}`, nil)
	require.NoError(t, err)

	firstBatch := broker.ReceiveMessages(source, 2, 0)
	require.Len(t, firstBatch, 2)
	failed := map[string]struct{}{firstBatch[1].ID: {}}
	manager := NewConsumerManager(broker, t.TempDir())
	entry := ConsumerEntry{
		DeadLetterQueue: "source-dlq",
		MaxReceiveCount: 2,
	}

	retryStarted := time.Now()
	manager.settleBatch(zerolog.Nop(), entry, source, firstBatch, failed, false)
	waiting, inFlight := broker.QueueDepth(source)
	assert.Zero(t, waiting)
	assert.Equal(t, 1, inFlight)
	retryMessage := firstBatch[1]
	assert.WithinDuration(t, retryStarted.Add(baseRetryVisibility), retryMessage.VisibleAt, time.Second)
	require.True(t, broker.ExtendMessageVisibility(source, retryMessage.ReceiptHandle, -time.Second))
	waiting, inFlight = broker.QueueDepth(dlq)
	assert.Zero(t, waiting)
	assert.Zero(t, inFlight)

	assert.Equal(t, 1, broker.RequeueExpired(source))
	secondBatch := broker.ReceiveMessages(source, 1, 0)
	require.Len(t, secondBatch, 1)
	assert.Equal(t, 2, secondBatch[0].ReceiveCount)
	manager.settleBatch(zerolog.Nop(), entry, source, secondBatch, nil, true)
	waiting, inFlight = broker.QueueDepth(source)
	assert.Zero(t, waiting)
	assert.Zero(t, inFlight)
	deadLetters := broker.ReceiveMessages(dlq, 1, 0)
	require.Len(t, deadLetters, 1)
	assert.Equal(t, secondBatch[0].ID, deadLetters[0].ID)
	assert.Equal(t, secondBatch[0].Body, deadLetters[0].Body)
}

func TestRetryVisibilityIsBoundedExponential(t *testing.T) {
	assert.Equal(t, 5*time.Second, retryVisibility(0))
	assert.Equal(t, 5*time.Second, retryVisibility(1))
	assert.Equal(t, 10*time.Second, retryVisibility(2))
	assert.Equal(t, 20*time.Second, retryVisibility(3))
	assert.Equal(t, time.Minute, retryVisibility(20))
}

func TestConsumerPollLoopStops(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	cm := NewConsumerManager(broker, t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())

	entry := ConsumerEntry{
		Name:           "stop-test",
		Queue:          "nonexistent-queue",
		Handler:        "test.handler",
		BatchSize:      1,
		TimeoutSeconds: 5,
	}

	done := make(chan struct{})
	go func() {
		cm.pollLoop(ctx, entry)
		close(done)
	}()

	// Cancel should stop the poller
	cancel()

	select {
	case <-done:
		// Success - poller stopped
	case <-time.After(10 * time.Second):
		t.Fatal("pollLoop did not stop within timeout")
	}
}
