package messaging

import (
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
)

func TestDevOwnedSQSLambdaBatchCarriesExactWireThroughSettlement(t *testing.T) {
	broker, activity := custodyBroker(t)
	queue := broker.CreateQueue("retained-encoded", time.Minute, 0)
	custodyRegister(t, broker, queue)
	id := custodySend(t, broker, queue, "retained\n<>&")
	require.Equal(t, 1, activity.count())
	batch, err := broker.ReceiveOwnedSQSLambdaBatchContext(t.Context(), queue, 1, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, batch.Records, 1)
	require.Equal(t, id, batch.Records[0].MessageID)
	requireSQSLambdaBatchPayload(t, batch, testSQSLambdaPayloadLimit)
	admitted := batch.Payload
	batch.Records[0].Body = "changed detached snapshot"
	require.Equal(t, admitted, batch.Payload)
	settled, err := broker.AcknowledgeSQSLambdaReceiptContext(t.Context(), queue, batch.Records[0].ReceiptHandle)
	require.NoError(t, err)
	require.True(t, settled)
	require.Zero(t, activity.count())
	// Empty retained custody still completes without a new native lease or
	// waiting for the full 20-second ordinary receive deadline.
	empty, err := broker.ReceiveOwnedSQSLambdaBatchContext(t.Context(), queue, 1, 20*time.Second, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Empty(t, empty.Records)
	require.Equal(t, sqsevent.EmptyBatchPayload, empty.Payload)
}
