package messaging

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func queuePerformanceRegressionFixture(t *testing.T, broker *Broker, name string) *Queue {
	t.Helper()
	attributes := map[string]string{"VisibilityTimeout": "3600"}
	if strings.HasSuffix(name, ".fifo") {
		attributes["FifoQueue"] = "true"
	}
	queue, failure := broker.createSQSQueue(name, attributes, nil)
	require.Nil(t, failure)
	return queue
}

func queuePerformanceRegressionSend(t *testing.T, broker *Broker, queue *Queue, body, group string, delay int) QueueSendResult {
	t.Helper()
	input := QueueMessageInput{Body: body, MessageGroupID: group}
	if group != "" && strings.HasSuffix(queue.Name, ".fifo") {
		input.MessageDeduplicationID = body
	}
	if delay != 0 {
		input.DelaySeconds = &delay
	}
	result, err := broker.SendQueueMessage(queue, input)
	require.NoError(t, err)
	return result
}

func TestFIFOVisibilityReturnsRestoreNumericOrderAndRejectOldReceipts(t *testing.T) {
	broker := NewBroker("us-east-1", "123456789012", 0)
	queue := queuePerformanceRegressionFixture(t, broker, "visibility-order.fifo")
	var sent []QueueSendResult
	for index := 1; index <= 12; index++ {
		sent = append(sent, queuePerformanceRegressionSend(t, broker, queue, fmt.Sprint(index), "one-group", 0))
	}
	first, err := broker.ReceiveMessagesContext(context.Background(), queue, 5, 0)
	require.NoError(t, err)
	require.Len(t, first, 5)
	require.True(t, broker.DeleteMessage(queue, first[0].ReceiptHandle))
	sent = append(sent, queuePerformanceRegressionSend(t, broker, queue, "13", "one-group", 0))
	blocked, err := broker.ReceiveMessagesContext(context.Background(), queue, 10, 0)
	require.NoError(t, err)
	require.Empty(t, blocked, "waiting FIFO tail must remain blocked by the original group's leases")
	for _, message := range first[1:] {
		require.True(t, broker.ExtendMessageVisibility(queue, message.ReceiptHandle, 0))
	}
	require.Equal(t, 4, broker.RequeueExpired(queue))
	require.Zero(t, broker.RequeueExpired(queue), "repeated housekeeping must preserve the restored order")
	var redelivered []*Message
	for len(redelivered) < 12 {
		messages, err := broker.ReceiveMessagesContext(context.Background(), queue, 5, 0)
		require.NoError(t, err)
		require.NotEmpty(t, messages)
		for _, message := range messages {
			index := len(redelivered) + 1
			require.Equal(t, sent[index].MessageID, message.ID)
			require.Equal(t, strconv.Itoa(index+1), message.SequenceNumber)
			if index < 5 {
				require.Equal(t, 2, message.ReceiveCount)
				require.NotEqual(t, first[index].ReceiptHandle, message.ReceiptHandle)
				outcome, err := broker.EvaluateSQSLambdaReceiptContext(context.Background(), queue, first[index].ReceiptHandle, true)
				require.NoError(t, err)
				require.Equal(t, SQSLambdaReceiptStaleOrExpired, outcome)
			} else {
				require.Equal(t, 1, message.ReceiveCount)
			}
			require.True(t, broker.DeleteMessage(queue, message.ReceiptHandle))
			redelivered = append(redelivered, message)
		}
	}
	waiting, flight := broker.QueueDepth(queue)
	require.Zero(t, waiting)
	require.Zero(t, flight)
	settled, err := broker.EvaluateSQSLambdaReceiptContext(context.Background(), queue, first[0].ReceiptHandle, false)
	require.NoError(t, err)
	require.Equal(t, SQSLambdaReceiptNativeSettled, settled)
}

func TestReceiveCompactionPreservesDelayedAndByteRejectedMessages(t *testing.T) {
	for _, mode := range []string{"native", "lambda"} {
		t.Run(mode, func(t *testing.T) {
			broker := NewBroker("us-east-1", "123456789012", 0)
			queue := queuePerformanceRegressionFixture(t, broker, "compaction")
			delayed := queuePerformanceRegressionSend(t, broker, queue, "delayed", "", 60)
			large := queuePerformanceRegressionSend(t, broker, queue, strings.Repeat("x", 4096), "", 0)
			ready := queuePerformanceRegressionSend(t, broker, queue, "ready", "", 0)
			tail := queuePerformanceRegressionSend(t, broker, queue, "tail", "", 0)
			var selectedID, receipt string
			if mode == "native" {
				messages, err := broker.ReceiveMessagesContext(context.Background(), queue, 1, 0)
				require.NoError(t, err)
				require.Len(t, messages, 1)
				selectedID, receipt = messages[0].ID, messages[0].ReceiptHandle
				require.Equal(t, large.MessageID, selectedID)
				messages[0].Body = "consumer snapshot mutation"
			} else {
				event, err := broker.ReceiveSQSLambdaEventContext(context.Background(), queue, 1, 0, 2048)
				require.NoError(t, err)
				require.Len(t, event.Records, 1)
				selectedID, receipt = event.Records[0].MessageID, event.Records[0].ReceiptHandle
				require.Equal(t, ready.MessageID, selectedID)
			}
			queue.mu.Lock()
			var remaining []*Message
			for _, message := range queue.messages {
				remaining = append(remaining, cloneMessage(message))
			}
			var owned *Message
			if message := queue.inFlight[receipt]; message != nil {
				owned = cloneMessage(message)
			}
			queue.mu.Unlock()
			require.NotNil(t, owned, "selected receipt must retain its original native owner")
			require.Len(t, remaining, 3)
			require.Equal(t, delayed.MessageID, remaining[0].ID)
			require.Equal(t, tail.MessageID, remaining[2].ID)
			if mode == "native" {
				require.Equal(t, ready.MessageID, remaining[1].ID)
				require.Equal(t, strings.Repeat("x", 4096), owned.Body, "returned snapshots cannot mutate native ownership")
			} else {
				require.Equal(t, large.MessageID, remaining[1].ID)
			}
			for _, message := range remaining {
				require.Zero(t, message.ReceiveCount)
				require.Empty(t, message.ReceiptHandle)
				require.True(t, message.FirstReceivedAt.IsZero())
			}
			require.True(t, broker.DeleteMessage(queue, receipt))
			messages, err := broker.ReceiveMessagesContext(context.Background(), queue, 10, 0)
			require.NoError(t, err)
			require.Len(t, messages, 2)
			require.Equal(t, remaining[1].ID, messages[0].ID)
			require.Equal(t, tail.MessageID, messages[1].ID)
			for _, message := range messages {
				require.True(t, broker.DeleteMessage(queue, message.ReceiptHandle))
			}
			queue.mu.Lock()
			remaining = nil
			for _, message := range queue.messages {
				remaining = append(remaining, cloneMessage(message))
			}
			queue.mu.Unlock()
			require.Len(t, remaining, 1)
			require.Equal(t, delayed.MessageID, remaining[0].ID)
			require.Empty(t, remaining[0].ReceiptHandle)
			require.Zero(t, remaining[0].ReceiveCount)
		})
	}
}

func TestStandardFairGroupCompactionPreservesDelayedFrontAndTenantOrder(t *testing.T) {
	broker := NewBroker("us-east-1", "123456789012", 0)
	queue := queuePerformanceRegressionFixture(t, broker, "fair-compaction")
	delayed := queuePerformanceRegressionSend(t, broker, queue, "A-delayed", "A", 60)
	var sent []QueueSendResult
	for _, body := range []string{"A-ready", "B-ready", "A-tail", "C-ready"} {
		sent = append(sent, queuePerformanceRegressionSend(t, broker, queue, body, body[:1], 0))
	}
	for _, index := range []int{0, 1, 3, 2} {
		messages, err := broker.ReceiveMessagesContext(context.Background(), queue, 1, 0)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		require.Equal(t, sent[index].MessageID, messages[0].ID)
		require.Equal(t, 1, messages[0].ReceiveCount)
		require.True(t, broker.DeleteMessage(queue, messages[0].ReceiptHandle))
	}
	messages, err := broker.ReceiveMessagesContext(context.Background(), queue, 10, 0)
	require.NoError(t, err)
	require.Empty(t, messages)
	queue.mu.Lock()
	var remaining []*Message
	for _, message := range queue.messages {
		remaining = append(remaining, cloneMessage(message))
	}
	queue.mu.Unlock()
	require.Len(t, remaining, 1)
	require.Equal(t, delayed.MessageID, remaining[0].ID)
	require.Zero(t, remaining[0].ReceiveCount)
}

func TestFIFOTransferAppendsDestinationSequenceAfterExistingWaitingMessages(t *testing.T) {
	broker := NewBroker("us-east-1", "123456789012", 0)
	source := queuePerformanceRegressionFixture(t, broker, "source.fifo")
	destination := queuePerformanceRegressionFixture(t, broker, "destination.fifo")
	moved := queuePerformanceRegressionSend(t, broker, source, "moved", "group", 0)
	retained := queuePerformanceRegressionSend(t, broker, source, "retained", "group", 0)
	var expected []string
	for _, body := range []string{"destination-first", "destination-second"} {
		expected = append(expected, queuePerformanceRegressionSend(t, broker, destination, body, "group", 0).MessageID)
	}
	messages, err := broker.ReceiveMessagesContext(context.Background(), source, 1, 0)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.True(t, broker.MoveMessage(source, destination, messages[0].ReceiptHandle))
	expected = append(expected, moved.MessageID)
	require.Zero(t, broker.RequeueExpired(destination))
	messages, err = broker.ReceiveMessagesContext(context.Background(), destination, 10, 0)
	require.NoError(t, err)
	require.Len(t, messages, 3)
	for index, message := range messages {
		require.Equal(t, expected[index], message.ID)
		require.Equal(t, strconv.Itoa(index+1), message.SequenceNumber)
		require.True(t, broker.DeleteMessage(destination, message.ReceiptHandle))
	}
	messages, err = broker.ReceiveMessagesContext(context.Background(), source, 10, 0)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, retained.MessageID, messages[0].ID)
	require.True(t, broker.DeleteMessage(source, messages[0].ReceiptHandle))
	waiting, flight := broker.QueueDepth(destination)
	require.Zero(t, waiting)
	require.Zero(t, flight)
}
