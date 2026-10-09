package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
)

const testSQSLambdaPayloadLimit = 6 << 20

func sendSQSLambdaBatchFixture(t *testing.T, broker *Broker, queue *Queue, body, group string) string {
	t.Helper()
	input := QueueMessageInput{Body: body, MessageGroupID: group}
	if queue.Attributes["FifoQueue"] == "true" {
		input.MessageDeduplicationID = md5Body(body)
	}
	result, err := broker.SendQueueMessage(queue, input)
	require.NoError(t, err)
	return result.MessageID
}

func requireSQSLambdaBatchPayload(t *testing.T, batch sqsevent.Batch, limit int) {
	t.Helper()
	payload, err := json.Marshal(sqsevent.Event{Records: batch.Records})
	require.NoError(t, err)
	require.Equal(t, string(payload), batch.Payload, "admitted JSON must be the complete exact native invocation payload")
	require.LessOrEqual(t, len(batch.Payload), limit)
}

func TestSQSLambdaBatchReceivesFiveWithNativeMetadata(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("lambda-five", time.Minute, 0)
	var ids []string
	for index := range 7 {
		ids = append(ids, sendSQSLambdaBatchFixture(t, broker, queue, fmt.Sprintf("message-%d", index), ""))
	}
	event, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 5, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, event.Records, 5)
	requireSQSLambdaBatchPayload(t, event, testSQSLambdaPayloadLimit)
	for index, record := range event.Records {
		require.Equal(t, ids[index], record.MessageID)
		require.Equal(t, fmt.Sprintf("message-%d", index), record.Body)
		require.Equal(t, "aws:sqs", record.EventSource)
		require.Equal(t, queue.ARN, record.EventSourceARN)
		require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
		require.NotEmpty(t, record.ReceiptHandle)
		require.NotEmpty(t, record.Attributes["ApproximateFirstReceiveTimestamp"])
	}
	waiting, inFlight := broker.QueueDepth(queue)
	require.Equal(t, 2, waiting)
	require.Equal(t, 5, inFlight)
}

func TestSQSLambdaBatchByteLimitDoesNotLeaseOrRedriveExcludedTail(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("lambda-byte-budget", time.Minute, 0)
	dead := broker.CreateQueue("lambda-byte-dlq", time.Minute, 0)
	require.Nil(t, NewHandler(broker).setQueueAttributes(queue, map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + dead.ARN + `","maxReceiveCount":1}`}))
	body := strings.Repeat("x", (1<<20)-64)
	var ids []string
	var visibleAt []time.Time
	for index := range 10 {
		ids = append(ids, sendSQSLambdaBatchFixture(t, broker, queue, body, ""))
		visibleAt = append(visibleAt, queue.messages[index].VisibleAt)
	}
	event, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 10, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, event.Records, 5, "six near-1MiB bodies plus actual metadata exceed the 6MiB invocation limit")
	requireSQSLambdaBatchPayload(t, event, testSQSLambdaPayloadLimit)
	queue.mu.Lock()
	require.Len(t, queue.inFlight, 5)
	require.Len(t, queue.messages, 5)
	require.Len(t, queue.receipts, 5, "byte-rejected candidates must not issue receipts")
	for index, message := range queue.messages {
		require.Equal(t, ids[index+5], message.ID)
		require.Zero(t, message.ReceiveCount)
		require.Empty(t, message.ReceiptHandle)
		require.True(t, message.ReceivedAt.IsZero())
		require.True(t, message.FirstReceivedAt.IsZero())
		require.Equal(t, visibleAt[index+5], message.VisibleAt)
	}
	queue.mu.Unlock()
	broker.RequeueExpired(queue)
	waiting, flight := broker.QueueDepth(dead)
	require.Zero(t, waiting, "unreceived tails must not enter a maxReceiveCount=1 DLQ")
	require.Zero(t, flight)
	for _, record := range event.Records {
		require.True(t, broker.DeleteMessage(queue, record.ReceiptHandle))
	}
	second, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 10, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, second.Records, 5)
	for index, record := range second.Records {
		require.Equal(t, ids[index+5], record.MessageID)
		require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
	}
}

func TestSQSLambdaBatchBudgetCountsEscapingBinaryAttributesAndSeparators(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("lambda-json-budget", time.Minute, 0)
	for range 3 {
		_, err := broker.SendQueueMessage(queue, QueueMessageInput{Body: "line\n<>&", Attributes: map[string]MessageAttribute{
			"binary": {DataType: "Binary", BinaryValue: []byte{0, 1, 255}}, "text": {DataType: "String", StringValue: "\n<>&"},
		}})
		require.NoError(t, err)
	}
	prospective := make([]*Message, 2)
	for index := range 2 {
		prospective[index] = cloneMessage(queue.messages[index])
		prospective[index].ReceiptHandle = strings.Repeat("x", 36) // Native UUIDs have the same encoded length.
		prospective[index].ReceiveCount = 1
		prospective[index].FirstReceivedAt = time.Now()
	}
	encoded, err := json.Marshal(BuildSQSLambdaEvent(prospective, queue.ARN))
	require.NoError(t, err)
	limit := len(encoded)
	event, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 3, 0, limit)
	require.NoError(t, err)
	require.Len(t, event.Records, 2)
	actual, err := json.Marshal(sqsevent.Event{Records: event.Records})
	require.NoError(t, err)
	require.Equal(t, limit, len(actual), "budget must cover the exact Records envelope and comma")
	require.Equal(t, string(actual), event.Payload)
	require.Contains(t, string(actual), `\u003c\u003e\u0026`)
	require.Contains(t, string(actual), `"binaryValue":"AAH/"`)
	require.Zero(t, queue.messages[0].ReceiveCount)
}

func TestSQSLambdaBatchFIFORejectBlocksTailButAllowsOtherGroups(t *testing.T) {
	broker := newTestBroker()
	queue, failure := broker.createSQSQueue("lambda-budget.fifo", map[string]string{"FifoQueue": "true", "VisibilityTimeout": "60"}, nil)
	require.Nil(t, failure)
	first := sendSQSLambdaBatchFixture(t, broker, queue, "A1", "A")
	second := sendSQSLambdaBatchFixture(t, broker, queue, strings.Repeat("x", 4096), "A")
	third := sendSQSLambdaBatchFixture(t, broker, queue, "A3", "A")
	other := sendSQSLambdaBatchFixture(t, broker, queue, "B1", "B")
	event, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 10, 0, 2048)
	require.NoError(t, err)
	require.Len(t, event.Records, 2)
	require.Equal(t, []string{first, other}, []string{event.Records[0].MessageID, event.Records[1].MessageID})
	require.Equal(t, []string{second, third}, []string{queue.messages[0].ID, queue.messages[1].ID})
	for _, message := range queue.messages {
		require.Zero(t, message.ReceiveCount)
		require.Empty(t, message.ReceiptHandle)
	}
	blocked, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 10, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Empty(t, blocked.Records, "another receive cannot enter an already leased FIFO group")
	for _, record := range event.Records {
		require.True(t, broker.DeleteMessage(queue, record.ReceiptHandle))
	}
	resumed, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 10, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, resumed.Records, 2)
	require.Equal(t, second, resumed.Records[0].MessageID)
	require.Equal(t, third, resumed.Records[1].MessageID)
}

func TestSQSLambdaBatchConcurrentFIFOReceivesKeepGroupsAndOrder(t *testing.T) {
	broker := newTestBroker()
	queue, failure := broker.createSQSQueue("lambda-concurrent.fifo", map[string]string{"FifoQueue": "true", "VisibilityTimeout": "60"}, nil)
	require.Nil(t, failure)
	for _, body := range []string{"A1", "A2", "A3", "B1", "B2", "B3"} {
		sendSQSLambdaBatchFixture(t, broker, queue, body, body[:1])
	}
	type received struct {
		event sqsevent.Batch
		err   error
	}
	start := make(chan struct{})
	results := make(chan received, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			event, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 2, 0, testSQSLambdaPayloadLimit)
			results <- received{event, err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	groups := map[string]bool{}
	var leased []sqsevent.Record
	for result := range results {
		require.NoError(t, result.err)
		require.Len(t, result.event.Records, 2)
		group := result.event.Records[0].Attributes["MessageGroupId"]
		require.False(t, groups[group], "concurrent batches cannot enter the same in-flight group")
		groups[group] = true
		require.Equal(t, group+"1", result.event.Records[0].Body)
		require.Equal(t, group+"2", result.event.Records[1].Body)
		leased = append(leased, result.event.Records...)
	}
	require.Len(t, groups, 2)
	blocked, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 2, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Empty(t, blocked.Records)
	for _, record := range leased {
		require.True(t, broker.DeleteMessage(queue, record.ReceiptHandle))
	}
	tail, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 2, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, tail.Records, 2)
	require.Equal(t, "A3", tail.Records[0].Body)
	require.Equal(t, "B3", tail.Records[1].Body)
}

func TestSQSLambdaBatchCannotFitReturnsWithoutLeaseOrLongPoll(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("lambda-too-large", time.Minute, 0)
	sendSQSLambdaBatchFixture(t, broker, queue, strings.Repeat("\n", 1000), "")
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	event, err := broker.ReceiveSQSLambdaBatchContext(ctx, queue, 10, 20*time.Second, 1000)
	require.ErrorIs(t, err, ErrSQSLambdaPayloadTooLarge)
	require.Empty(t, event.Records)
	require.NoError(t, ctx.Err(), "eligible oversized record must not repeatedly long poll")
	require.Zero(t, queue.messages[0].ReceiveCount)
	require.True(t, queue.messages[0].FirstReceivedAt.IsZero())
	require.Empty(t, queue.inFlight)
	require.Empty(t, queue.receipts)
}

func TestSQSLambdaBatchContextOwnershipBoundsAndOrdinaryReceive(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("lambda-owned", time.Minute, 0)
	foreign := newTestBroker()
	_, err := foreign.ReceiveSQSLambdaBatchContext(t.Context(), queue, 1, 0, testSQSLambdaPayloadLimit)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	for _, max := range []int{0, 11} {
		_, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, max, 0, testSQSLambdaPayloadLimit)
		require.Error(t, err)
	}
	for _, wait := range []time.Duration{-time.Second, 21 * time.Second} {
		_, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 1, wait, testSQSLambdaPayloadLimit)
		require.Error(t, err)
	}
	_, err = broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 1, 0, 0)
	require.Error(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	sendSQSLambdaBatchFixture(t, broker, queue, "retained", "")
	_, err = broker.ReceiveSQSLambdaBatchContext(ctx, queue, 1, 0, testSQSLambdaPayloadLimit)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, queue.messages[0].ReceiveCount)
	broker.DeleteQueue(queue.Name)
	replacement := broker.CreateQueue(queue.Name, time.Minute, 0)
	_, err = broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 1, 0, testSQSLambdaPayloadLimit)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	pollCtx, stop := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := broker.ReceiveSQSLambdaBatchContext(pollCtx, replacement, 10, 20*time.Second, testSQSLambdaPayloadLimit)
		result <- err
	}()
	stop()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("owned batch receive did not cancel")
	}
	// Ordinary native SQS receives keep the 10-message contract independently
	// of Lambda's invocation-byte budget.
	for range 10 {
		sendSQSLambdaBatchFixture(t, broker, replacement, strings.Repeat("x", 1<<20), "")
	}
	messages, err := broker.ReceiveMessagesContext(t.Context(), replacement, 10, 0)
	require.NoError(t, err)
	require.Len(t, messages, 10)
}

func TestSQSLambdaByteRejectionPreservesNativeReceiveAttemptReplay(t *testing.T) {
	broker := newTestBroker()
	queue, failure := broker.createSQSQueue("lambda-attempt.fifo", map[string]string{"FifoQueue": "true", "VisibilityTimeout": "60"}, nil)
	require.Nil(t, failure)
	sendSQSLambdaBatchFixture(t, broker, queue, "retry-eligible", "A")
	first, failure := broker.receiveSQS(t.Context(), queue, 1, 0, nil, "native-attempt")
	require.Nil(t, failure)
	require.Len(t, first, 1)
	queue.mu.Lock()
	queue.inFlight[first[0].ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
	queue.mu.Unlock()
	_, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 1, 0, 100)
	require.ErrorIs(t, err, ErrSQSLambdaPayloadTooLarge)
	replayed, failure := broker.receiveSQS(t.Context(), queue, 1, 0, nil, "native-attempt")
	require.Nil(t, failure, "a byte-rejected candidate must not invalidate a cached native receive attempt")
	require.Len(t, replayed, 1)
	require.Equal(t, first[0].ReceiptHandle, replayed[0].ReceiptHandle)
	require.Equal(t, first[0].ReceiveCount, replayed[0].ReceiveCount)
}

func TestSQSNativeFIFOReceiveDoesNotSkipDelayedTailAfterSelectingGroup(t *testing.T) {
	broker := newTestBroker()
	queue, failure := broker.createSQSQueue("lambda-native-order.fifo", map[string]string{"FifoQueue": "true", "VisibilityTimeout": "60"}, nil)
	require.Nil(t, failure)
	for _, body := range []string{"A1", "A2", "A3", "B1"} {
		sendSQSLambdaBatchFixture(t, broker, queue, body, body[:1])
	}
	queue.mu.Lock()
	queue.messages[1].VisibleAt = time.Now().Add(time.Minute)
	queue.mu.Unlock()
	messages := broker.ReceiveMessages(queue, 10, 0)
	require.Len(t, messages, 2)
	require.Equal(t, "A1", messages[0].Body)
	require.Equal(t, "B1", messages[1].Body)
	require.Equal(t, "A2", queue.messages[0].Body)
	require.Equal(t, "A3", queue.messages[1].Body)
	require.Zero(t, queue.messages[1].ReceiveCount)
}
