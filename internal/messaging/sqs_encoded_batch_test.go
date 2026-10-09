package messaging

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
)

func TestSQSLambdaEncodedBatchIsExactAndDetachedFromQueueAndRecords(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("encoded-batch", time.Minute, 0)
	for _, body := range []string{"one\n<>&", "two\u2028\u2029"} {
		_, err := broker.SendQueueMessage(queue, QueueMessageInput{Body: body, Attributes: map[string]MessageAttribute{
			"binary": {DataType: "Binary", BinaryValue: []byte{0, 1, 255}},
			"string": {DataType: "String", StringValue: "line\n<>&"},
		}})
		require.NoError(t, err)
	}
	batch, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 10, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, batch.Records, 2)
	requireSQSLambdaBatchPayload(t, batch, testSQSLambdaPayloadLimit)
	admitted := batch.Payload
	first := batch.Records[0]
	batch.Records[0].Body = "changed detached body"
	first.Attributes["ApproximateReceiveCount"] = "99"
	first.MessageAttributes["binary"].BinaryValue[0] = 42
	stringAttribute := first.MessageAttributes["string"]
	*stringAttribute.StringValue = "changed detached attribute"
	require.Equal(t, admitted, batch.Payload)
	var dispatched sqsevent.Event
	require.NoError(t, json.Unmarshal([]byte(batch.Payload), &dispatched))
	require.Equal(t, "one\n<>&", dispatched.Records[0].Body)
	require.Equal(t, "1", dispatched.Records[0].Attributes["ApproximateReceiveCount"])
	require.Equal(t, []byte{0, 1, 255}, dispatched.Records[0].MessageAttributes["binary"].BinaryValue)
	require.Equal(t, "line\n<>&", *dispatched.Records[0].MessageAttributes["string"].StringValue)
	queue.mu.Lock()
	original := cloneMessage(queue.inFlight[first.ReceiptHandle])
	queue.mu.Unlock()
	require.Equal(t, "one\n<>&", original.Body)
	require.Equal(t, 1, original.ReceiveCount)
	require.Equal(t, []byte{0, 1, 255}, original.Attributes["binary"].BinaryValue)
	require.Equal(t, "line\n<>&", original.Attributes["string"].StringValue)
	for _, record := range dispatched.Records {
		settled, err := broker.AcknowledgeSQSLambdaReceiptContext(t.Context(), queue, record.ReceiptHandle)
		require.NoError(t, err)
		require.True(t, settled)
	}
	waiting, inFlight := broker.QueueDepth(queue)
	require.Zero(t, waiting)
	require.Zero(t, inFlight)
}

func TestSQSLambdaEventOnlyConsumerRemainsSupported(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("event-only-consumer", time.Minute, 0)
	id := sendSQSLambdaBatchFixture(t, broker, queue, "existing event consumer", "")
	event, err := broker.ReceiveSQSLambdaEventContext(t.Context(), queue, 1, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, event.Records, 1)
	require.Equal(t, id, event.Records[0].MessageID)
	require.Equal(t, "existing event consumer", event.Records[0].Body)
	require.True(t, broker.DeleteMessage(queue, event.Records[0].ReceiptHandle))
	empty, err := broker.ReceiveSQSLambdaBatchContext(t.Context(), queue, 1, 0, len(sqsevent.EmptyBatchPayload))
	require.NoError(t, err)
	require.Empty(t, empty.Records)
	require.Equal(t, sqsevent.EmptyBatchPayload, empty.Payload)
}
