package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSQSLambdaProjectionWireAndOwnedSnapshot(t *testing.T) {
	message := &Message{ID: "message", Body: "hello", ReceiptHandle: "lease", SentTimestamp: time.UnixMilli(1700000000123), FirstReceivedAt: time.UnixMilli(1700000000456), ReceiveCount: 3, SenderID: "sender", GroupID: "tenant", DeduplicationID: "dedup", SequenceNumber: "42", OriginalSourceARN: "arn:aws:sqs:eu-west-1:123456789012:source", Attributes: map[string]MessageAttribute{"text": {DataType: "String", StringValue: ""}, "binary": {DataType: "Binary.custom", BinaryValue: []byte{0, 1, 255}}}, SystemAttributes: map[string]MessageAttribute{"AWSTraceHeader": {DataType: "String", StringValue: "trace"}}}
	event := BuildSQSLambdaEvent([]*Message{message}, "arn:aws:sqs:eu-west-1:123456789012:queue.fifo")
	binary := message.Attributes["binary"]
	binary.BinaryValue[0] = 99
	message.Attributes["text"] = MessageAttribute{StringValue: "changed"}
	message.SystemAttributes["AWSTraceHeader"] = MessageAttribute{StringValue: "changed"}
	message.Body = "changed"
	payload, err := json.Marshal(event)
	require.NoError(t, err)
	require.JSONEq(t, `{"Records":[{"messageId":"message","receiptHandle":"lease","body":"hello","attributes":{"ApproximateReceiveCount":"3","SenderId":"sender","SentTimestamp":"1700000000123","ApproximateFirstReceiveTimestamp":"1700000000456","MessageGroupId":"tenant","MessageDeduplicationId":"dedup","SequenceNumber":"42","DeadLetterQueueSourceArn":"arn:aws:sqs:eu-west-1:123456789012:source","AWSTraceHeader":"trace"},"messageAttributes":{"text":{"dataType":"String","stringValue":"","stringListValues":[],"binaryListValues":[]},"binary":{"dataType":"Binary.custom","binaryValue":"AAH/","stringListValues":[],"binaryListValues":[]}},"md5OfBody":"5d41402abc4b2a76b9719d911017c592","eventSource":"aws:sqs","eventSourceARN":"arn:aws:sqs:eu-west-1:123456789012:queue.fifo","awsRegion":"eu-west-1"}]}`, string(payload))
	empty, err := json.Marshal(BuildSQSLambdaEvent(nil, ""))
	require.NoError(t, err)
	require.JSONEq(t, `{"Records":[]}`, string(empty))
}

func TestSQSBoundConsumptionCancellationOwnershipAndSettlement(t *testing.T) {
	broker := NewBroker("eu-west-1", "123456789012", 0)
	queue := broker.CreateQueue("bound", 30*time.Second, 4*24*time.Hour)
	other := NewBroker("eu-west-1", "123456789012", 0)
	_, err := other.QueueInfo(queue)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { _, err := broker.ReceiveMessagesContext(ctx, queue, 1, 20*time.Second); result <- err }()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("long poll was not canceled")
	}
	_, err = broker.ReceiveMessagesContext(t.Context(), queue, 11, 0)
	require.Error(t, err)
	_, err = broker.SendQueueMessage(queue, QueueMessageInput{Body: "message"})
	require.NoError(t, err)
	records, err := broker.ReceiveMessagesContext(t.Context(), queue, 1, 0)
	require.NoError(t, err)
	require.Len(t, records, 1)
	old := records[0].ReceiptHandle
	// No periodic prune has run: current settlement must itself reject expiration.
	queue.mu.Lock()
	queue.inFlight[old].VisibleAt = time.Now().Add(-time.Second)
	queue.mu.Unlock()
	require.False(t, broker.DeleteMessage(queue, old))
	records, err = broker.ReceiveMessagesContext(t.Context(), queue, 1, 0)
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.NotEqual(t, old, records[0].ReceiptHandle)
	require.Equal(t, 2, records[0].ReceiveCount)
	require.False(t, broker.DeleteMessage(queue, old))
	require.True(t, broker.DeleteMessage(queue, records[0].ReceiptHandle))
	broker.DeleteQueue("bound")
	replacement := broker.CreateQueue("bound", 30*time.Second, 4*24*time.Hour)
	require.NotSame(t, queue, replacement)
	require.Equal(t, queue.ARN, replacement.ARN)
	_, err = broker.ReceiveMessagesContext(t.Context(), queue, 1, 0)
	require.True(t, errors.Is(err, ErrQueueUnavailable))
	_, err = broker.SendQueueMessage(replacement, QueueMessageInput{Body: "replacement"})
	require.NoError(t, err)
	waiting, flight := broker.QueueDepth(replacement)
	require.Equal(t, 1, waiting)
	require.Zero(t, flight)
}
