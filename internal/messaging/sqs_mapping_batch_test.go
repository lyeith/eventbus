package messaging

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
)

func TestSQSLambdaMappingWindowCollectsNewArrivalsAndFlushesLeasesOnFence(t *testing.T) {
	for _, fenced := range []bool{false, true} {
		name := "window-fills"
		if fenced {
			name = "intake-fence"
		}
		t.Run(name, func(t *testing.T) {
			broker := newTestBroker()
			queue := broker.CreateQueue("window", time.Minute, 0)
			first := sendSQSLambdaBatchFixture(t, broker, queue, "first", "")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			type result struct {
				batch sqsevent.Batch
				err   error
			}
			received := make(chan result, 1)
			go func() {
				batch, err := broker.ReceiveSQSLambdaMappingBatchContext(ctx, queue, SQSLambdaBatchOptions{
					MaxRecords: 2, Wait: time.Second, Window: 15 * time.Second, InvocationTimeout: time.Second, MaxPayloadBytes: testSQSLambdaPayloadLimit,
				})
				received <- result{batch, err}
			}()
			require.Eventually(t, func() bool {
				waiting, active := broker.QueueDepth(queue)
				return waiting == 0 && active == 1
			}, time.Second, time.Millisecond)
			select {
			case got := <-received:
				t.Fatalf("underfilled batch returned before its window: %v", got.err)
			default:
			}
			want := []string{first}
			if fenced {
				cancel()
			} else {
				want = append(want, sendSQSLambdaBatchFixture(t, broker, queue, "second", ""))
			}
			select {
			case got := <-received:
				require.NoError(t, got.err)
				requireSQSLambdaBatchPayload(t, got.batch, testSQSLambdaPayloadLimit)
				var ids []string
				for _, record := range got.batch.Records {
					ids = append(ids, record.MessageID)
					ack, err := broker.AcknowledgeSQSLambdaReceiptContext(t.Context(), queue, record.ReceiptHandle)
					require.NoError(t, err)
					require.True(t, ack)
				}
				require.Equal(t, want, ids)
			case <-time.After(2 * time.Second):
				t.Fatal("ready leases did not flush")
			}
		})
	}
}

func TestSQSLambdaMappingBatchFiltersBeforeWireBudget(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("filter", time.Minute, 0)
	sendSQSLambdaBatchFixture(t, broker, queue, strings.Repeat("<", 8192), "")
	kept := sendSQSLambdaBatchFixture(t, broker, queue, "kept", "")
	const limit = 2048
	batch, err := broker.ReceiveSQSLambdaMappingBatchContext(t.Context(), queue, SQSLambdaBatchOptions{
		MaxRecords: 1, MaxPayloadBytes: limit, Match: func(record sqsevent.Record) bool { return record.Body == "kept" },
	})
	require.NoError(t, err)
	requireSQSLambdaBatchPayload(t, batch, limit)
	require.Len(t, batch.Records, 1)
	require.Equal(t, kept, batch.Records[0].MessageID)
	require.True(t, broker.DeleteMessage(queue, batch.Records[0].ReceiptHandle))
	waiting, active := broker.QueueDepth(queue)
	require.Zero(t, waiting, "filtered oversized input must leave the queue without consuming the batch budget")
	require.Zero(t, active)
}

func TestSQSLambdaMappingWindowPreservesVisibilityForExecution(t *testing.T) {
	broker := newTestBroker()
	queue := broker.CreateQueue("short-visibility", time.Second, 0)
	sendSQSLambdaBatchFixture(t, broker, queue, "first", "")
	started := time.Now()
	batch, err := broker.ReceiveSQSLambdaMappingBatchContext(t.Context(), queue, SQSLambdaBatchOptions{
		MaxRecords: 2, Wait: time.Second, Window: 15 * time.Second, InvocationTimeout: time.Second, MaxPayloadBytes: testSQSLambdaPayloadLimit,
	})
	require.NoError(t, err)
	require.Len(t, batch.Records, 1)
	require.Less(t, time.Since(started), time.Second, "a short visibility lease must dispatch before it expires")
}
