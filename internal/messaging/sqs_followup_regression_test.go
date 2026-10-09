package messaging

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireNoWaitingPayloadTail(t *testing.T, queue *Queue) {
	t.Helper()
	queue.mu.Lock()
	defer queue.mu.Unlock()
	for index, message := range queue.messages[len(queue.messages):cap(queue.messages)] {
		require.Nil(t, message, "removed payload is rooted in waiting backing slot %d", len(queue.messages)+index)
	}
}

func TestSQSWaitingRemovalReleasesPayloadRoots(t *testing.T) {
	t.Run("retention", func(t *testing.T) {
		broker := newTestBroker()
		queue := receiptQueue(t, broker, "retention-roots")
		for index := range 3 {
			queuePerformanceRegressionSend(t, broker, queue, strings.Repeat(fmt.Sprint(index), 1024), "", 0)
		}
		queue.mu.Lock()
		queue.messages[0].SentTimestamp = time.Now().Add(-queue.RetentionPeriod - time.Second)
		queue.messages[2].SentTimestamp = queue.messages[0].SentTimestamp
		queue.mu.Unlock()
		broker.RequeueExpired(queue)
		requireNoWaitingPayloadTail(t, queue)
		queue.mu.Lock()
		require.Len(t, queue.messages, 1)
		require.Equal(t, strings.Repeat("1", 1024), queue.messages[0].Body)
		queue.messages[0].SentTimestamp = time.Now().Add(-queue.RetentionPeriod - time.Second)
		queue.mu.Unlock()
		broker.RequeueExpired(queue)
		requireNoWaitingPayloadTail(t, queue)
		waiting, flight := broker.QueueDepth(queue)
		require.Zero(t, waiting)
		require.Zero(t, flight)
	})
	t.Run("attempt_restore_then_settle", func(t *testing.T) {
		broker := newTestBroker()
		queue := receiptQueue(t, broker, "attempt-roots.fifo")
		_, err := broker.SendQueueMessage(queue, QueueMessageInput{Body: "original payload", MessageGroupID: "group", MessageDeduplicationID: "original-payload"})
		require.NoError(t, err)
		first, failure := broker.receiveSQS(t.Context(), queue, 1, 0, nil, "same-attempt")
		require.Nil(t, failure)
		require.Len(t, first, 1)
		queue.mu.Lock()
		queue.inFlight[first[0].ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
		queue.mu.Unlock()
		require.Equal(t, 1, broker.RequeueExpired(queue))
		restored, failure := broker.receiveSQS(t.Context(), queue, 1, 0, nil, "same-attempt")
		require.Nil(t, failure)
		require.Len(t, restored, 1)
		require.Equal(t, first[0].ReceiptHandle, restored[0].ReceiptHandle)
		require.Equal(t, 1, restored[0].ReceiveCount)
		require.True(t, broker.DeleteMessage(queue, restored[0].ReceiptHandle))
		requireNoWaitingPayloadTail(t, queue)
		requireSQSLambdaReceiptOutcome(t, broker, queue, restored[0].ReceiptHandle, false, SQSLambdaReceiptNativeSettled)
	})
	t.Run("transfer_final_slot", func(t *testing.T) {
		broker := newTestBroker()
		source := receiptQueue(t, broker, "transfer-roots")
		destination := receiptQueue(t, broker, "transfer-destination")
		sent := queuePerformanceRegressionSend(t, broker, source, "source payload", "", 0)
		state := broker.sqsState()
		state.mu.Lock()
		moved, remaining, reason := broker.sqsMoveOneLocked(&sqsMoveTask{DestinationARN: destination.ARN}, source, time.Now())
		state.mu.Unlock()
		require.True(t, moved)
		require.True(t, remaining)
		require.Empty(t, reason)
		requireNoWaitingPayloadTail(t, source)
		received := broker.ReceiveMessages(destination, 1, 0)
		require.Len(t, received, 1)
		require.Equal(t, "source payload", received[0].Body)
		require.NotEqual(t, sent.MessageID, received[0].ID, "native move-task destination has a new enqueue identity")
		require.True(t, broker.DeleteMessage(destination, received[0].ReceiptHandle))
		requireNoWaitingPayloadTail(t, source)
	})
}

func TestSQSDedupExpiryBoundPreservesEntriesAndBoundary(t *testing.T) {
	now := time.Now()
	queue := &Queue{dedup: map[string]sqsDedupEntry{
		"expired": {ID: "old", Sequence: "1", Expires: now},
		"live":    {ID: "live", Sequence: "2", Expires: now.Add(time.Minute)},
	}}
	pruneSQSDedupLocked(queue, now)
	require.NotContains(t, queue.dedup, "expired", "expiry is inclusive")
	require.Equal(t, "live", queue.dedup["live"].ID)
	require.Equal(t, now.Add(time.Minute), queue.dedupExpiry)
	rememberSQSDedupLocked(queue, "live", "replacement", "3", now)
	require.Equal(t, now.Add(time.Minute), queue.dedupExpiry, "replacement cannot move the conservative lower bound later")
	rememberSQSDedupLocked(queue, "earlier", "earlier", "4", now.Add(-4*time.Minute-30*time.Second))
	require.Equal(t, now.Add(30*time.Second), queue.dedupExpiry, "out-of-order insertion must lower the bound")
	pruneSQSDedupLocked(queue, now.Add(30*time.Second-time.Nanosecond))
	require.Contains(t, queue.dedup, "earlier")
	pruneSQSDedupLocked(queue, now.Add(30*time.Second))
	require.NotContains(t, queue.dedup, "earlier")
	require.Equal(t, "replacement", queue.dedup["live"].ID)
	require.Equal(t, now.Add(5*time.Minute), queue.dedupExpiry)
	pruneSQSDedupLocked(queue, now.Add(5*time.Minute))
	require.Empty(t, queue.dedup)
	require.True(t, queue.dedupExpiry.IsZero())
}

func TestSQSDedupNativeSendAndMoveTaskShareExpiryAndGroupScope(t *testing.T) {
	broker := newTestBroker()
	source := receiptQueue(t, broker, "dedup-source.fifo")
	destination := receiptQueue(t, broker, "dedup-destination.fifo")
	require.Nil(t, NewHandler(broker).setQueueAttributes(destination, map[string]string{"DeduplicationScope": "messageGroup", "FifoThroughputLimit": "perMessageGroupId"}))
	old, err := broker.SendQueueMessage(source, QueueMessageInput{Body: "move body", MessageGroupID: "one", MessageDeduplicationID: "move-body"})
	require.NoError(t, err)
	queuePerformanceRegressionSend(t, broker, destination, "existing", "one", 0)
	destination.mu.Lock()
	expiry := time.Now().Add(-time.Second)
	for key, entry := range destination.dedup {
		entry.Expires = expiry
		destination.dedup[key] = entry
	}
	destination.dedupExpiry = expiry
	destination.mu.Unlock()
	state := broker.sqsState()
	state.mu.Lock()
	moved, _, reason := broker.sqsMoveOneLocked(&sqsMoveTask{DestinationARN: destination.ARN}, source, time.Now())
	state.mu.Unlock()
	require.True(t, moved)
	require.Empty(t, reason)
	destination.mu.Lock()
	require.Len(t, destination.dedup, 1, "move insertion prunes due history through the shared owner")
	movedEntry := destination.dedup["one\x00"+old.MessageID]
	require.NotEmpty(t, movedEntry.ID)
	bound := destination.dedupExpiry
	destination.mu.Unlock()
	duplicate, err := broker.SendQueueMessage(destination, QueueMessageInput{Body: "move body", MessageGroupID: "one", MessageDeduplicationID: old.MessageID})
	require.NoError(t, err)
	require.Equal(t, movedEntry.ID, duplicate.MessageID)
	require.Equal(t, movedEntry.Sequence, duplicate.SequenceNumber)
	otherGroup, err := broker.SendQueueMessage(destination, QueueMessageInput{Body: "move body", MessageGroupID: "two", MessageDeduplicationID: old.MessageID})
	require.NoError(t, err)
	require.NotEqual(t, duplicate.MessageID, otherGroup.MessageID)
	destination.mu.Lock()
	require.Equal(t, bound, destination.dedupExpiry)
	expired := destination.dedup["one\x00"+old.MessageID]
	expired.Expires = expiry
	destination.dedup["one\x00"+old.MessageID] = expired
	destination.dedupExpiry = expiry
	destination.mu.Unlock()
	afterExpiry, err := broker.SendQueueMessage(destination, QueueMessageInput{Body: "move body", MessageGroupID: "one", MessageDeduplicationID: old.MessageID})
	require.NoError(t, err)
	require.NotEqual(t, duplicate.MessageID, afterExpiry.MessageID)
	require.NotEqual(t, duplicate.SequenceNumber, afterExpiry.SequenceNumber)
	duplicateAgain, err := broker.SendQueueMessage(destination, QueueMessageInput{Body: "move body", MessageGroupID: "one", MessageDeduplicationID: old.MessageID})
	require.NoError(t, err)
	require.Equal(t, afterExpiry, duplicateAgain)
}

func TestSQSCurrentDeleteLeavesUnrelatedExpiryToNativeOwner(t *testing.T) {
	for _, deleteMode := range []string{"typed", "http"} {
		for _, expiryMode := range []string{"maintenance", "receive"} {
			t.Run(deleteMode+"_"+expiryMode, func(t *testing.T) {
				broker, activity := custodyBroker(t)
				queue := receiptQueue(t, broker, "delete-owner.fifo")
				dead := receiptQueue(t, broker, "delete-dlq.fifo")
				require.Nil(t, NewHandler(broker).setQueueAttributes(queue, map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + dead.ARN + `","maxReceiveCount":1}`}))
				custodyRegister(t, broker, queue)
				custodyRegister(t, broker, dead)
				for _, body := range []string{"current", "visibility", "retention"} {
					queuePerformanceRegressionSend(t, broker, queue, body, body, 0)
				}
				first := broker.ReceiveMessages(queue, 3, 0)
				require.Len(t, first, 3)
				tail := queuePerformanceRegressionSend(t, broker, queue, "tail", "current", 0)
				queue.mu.Lock()
				queue.inFlight[first[1].ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
				queue.inFlight[first[2].ReceiptHandle].SentTimestamp = time.Now().Add(-queue.RetentionPeriod - time.Second)
				queue.mu.Unlock()
				if deleteMode == "typed" {
					require.True(t, broker.DeleteMessage(queue, first[0].ReceiptHandle))
				} else {
					require.Nil(t, NewHandler(broker).deleteSQSReceipt(queue, first[0].ReceiptHandle))
				}
				require.Equal(t, 3, activity.count(), "current delete releases exactly its own custody")
				queue.mu.Lock()
				require.Len(t, queue.inFlight, 2, "successful delete does not scan unrelated expiry")
				queue.mu.Unlock()
				requireSQSLambdaReceiptOutcome(t, broker, queue, first[0].ReceiptHandle, false, SQSLambdaReceiptNativeSettled)
				if expiryMode == "maintenance" {
					require.Zero(t, broker.RequeueExpired(queue))
				}
				next := broker.ReceiveMessages(queue, 1, 0)
				require.Len(t, next, 1)
				require.Equal(t, tail.MessageID, next[0].ID, "native receive releases the settled FIFO group and advances expiry")
				require.Equal(t, 2, activity.count(), "retention releases custody while redrive transfers it without losing the destination owner")
				requireSQSLambdaReceiptOutcome(t, broker, queue, first[1].ReceiptHandle, false, SQSLambdaReceiptStaleOrExpired)
				requireSQSLambdaReceiptOutcome(t, broker, queue, first[2].ReceiptHandle, false, SQSLambdaReceiptStaleOrExpired)
				dlq := broker.ReceiveMessages(dead, 1, 0)
				require.Len(t, dlq, 1)
				require.Equal(t, "visibility", dlq[0].Body)
				require.Equal(t, queue.ARN, dlq[0].OriginalSourceARN)
				require.True(t, broker.DeleteMessage(queue, next[0].ReceiptHandle))
				require.True(t, broker.DeleteMessage(dead, dlq[0].ReceiptHandle))
				require.Zero(t, activity.count())
			})
		}
	}
}

func TestSQSNativeAndLambdaProjectionPreserveSystemFieldsAndSelection(t *testing.T) {
	broker := newTestBroker()
	queue := receiptQueue(t, broker, "projection.fifo")
	input := QueueMessageInput{Body: "hello", MessageGroupID: "group", MessageDeduplicationID: "dedup", SenderID: "sender", Attributes: map[string]MessageAttribute{
		"text.one": {DataType: "String", StringValue: "<>&"},
		"binary":   {DataType: "Binary.custom", BinaryValue: []byte{0, 255}},
	}, SystemAttributes: map[string]MessageAttribute{"AWSTraceHeader": {DataType: "String", StringValue: "Root=1-67891233-abcdef012345678912345678"}}}
	sent, err := broker.SendQueueMessage(queue, input)
	require.NoError(t, err)
	queue.mu.Lock()
	queue.messages[0].OriginalSourceARN = "arn:aws:sqs:us-east-1:123456789012:original.fifo"
	queue.mu.Unlock()
	response, failure := NewHandler(broker).receiveSQSResponse(t.Context(), queue, sqsRequest{MessageSystemAttributeNames: []string{"All"}, MessageAttributeNames: []string{"text.*"}})
	require.Nil(t, failure)
	messages := response["Messages"].([]map[string]any)
	require.Len(t, messages, 1)
	native := messages[0]
	receipt := native["ReceiptHandle"].(string)
	queue.mu.Lock()
	snapshot := cloneMessage(queue.inFlight[receipt])
	queue.mu.Unlock()
	expected := map[string]string{"SenderId": "sender", "SentTimestamp": strconv.FormatInt(snapshot.SentTimestamp.UnixMilli(), 10), "ApproximateReceiveCount": "1", "ApproximateFirstReceiveTimestamp": strconv.FormatInt(snapshot.FirstReceivedAt.UnixMilli(), 10), "MessageGroupId": "group", "MessageDeduplicationId": "dedup", "SequenceNumber": "1", "DeadLetterQueueSourceArn": "arn:aws:sqs:us-east-1:123456789012:original.fifo", "AWSTraceHeader": "Root=1-67891233-abcdef012345678912345678"}
	require.Equal(t, expected, native["Attributes"])
	require.Equal(t, sent.MessageID, native["MessageId"])
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", native["MD5OfBody"])
	require.Equal(t, map[string]sqsMessageAttribute{"text.one": {DataType: "String", StringValue: "<>&"}}, native["MessageAttributes"])
	lambda := BuildSQSLambdaEvent([]*Message{snapshot}, queue.ARN).Records[0]
	require.Equal(t, expected, lambda.Attributes)
	require.Equal(t, native["MD5OfBody"], lambda.MD5OfBody)
	require.Equal(t, "us-east-1", lambda.AWSRegion)
	require.Len(t, lambda.MessageAttributes, 2, "Lambda includes all custom fields while native HTTP selection stays separate")
	require.Equal(t, []byte{0, 255}, lambda.MessageAttributes["binary"].BinaryValue)
	require.Equal(t, "Binary.custom", lambda.MessageAttributes["binary"].DataType)
	lambda.MessageAttributes["binary"].BinaryValue[0] = 42
	require.Equal(t, []byte{0, 255}, snapshot.Attributes["binary"].BinaryValue)
	require.True(t, broker.DeleteMessage(queue, receipt))
}

func TestSQSProjectedReceiveCountsLeasesWithoutNativeSnapshots(t *testing.T) {
	broker := newTestBroker()
	queue := receiptQueue(t, broker, "projected-count.fifo")
	first := queuePerformanceRegressionSend(t, broker, queue, "accepted", "group", 0)
	tail := queuePerformanceRegressionSend(t, broker, queue, "tail", "group", 0)
	var projected []string
	selected, failure := broker.receiveSQSWithOwnership(context.Background(), queue, 1, 0, nil, "", func(candidate *Message) bool { projected = append(projected, candidate.ID); return true }, sqsReceiveOptions{})
	require.Nil(t, failure)
	require.Equal(t, 1, selected.count)
	require.Empty(t, selected.messages)
	require.Equal(t, []string{first.MessageID}, projected)
	require.Empty(t, broker.ReceiveMessages(queue, 1, 0), "counted projected lease must block its FIFO group's tail")
	queue.mu.Lock()
	receipt := queue.messages[0].ReceiptHandle
	require.Empty(t, receipt)
	for handle := range queue.inFlight {
		receipt = handle
	}
	queue.mu.Unlock()
	require.True(t, broker.DeleteMessage(queue, receipt))
	native := broker.ReceiveMessages(queue, 1, 0)
	require.Len(t, native, 1)
	require.Equal(t, tail.MessageID, native[0].ID)
	require.True(t, broker.DeleteMessage(queue, native[0].ReceiptHandle))
}
