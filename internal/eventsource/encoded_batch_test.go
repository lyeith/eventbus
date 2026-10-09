package eventsource

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The queue port transfers one admitted string; the mapping must dispatch that
// exact representation, including after a detached map changes. No JSON cache
// participates in queue selection, retry or acknowledgment.
type encodedMappingQueue struct {
	*fakeQueue
	batch Batch
}

func (q *encodedMappingQueue) Receive(ctx context.Context, _ int) (Batch, error) {
	select {
	case <-ctx.Done():
		return Batch{}, ctx.Err()
	default:
	}
	batch := q.batch
	q.batch = Batch{}
	return batch, nil
}

func TestMappingDispatchesAdmittedBatchWireAndSettlesItsOriginalReceipts(t *testing.T) {
	records := leasedBatch("encoded", 2, 1)
	batch, err := fixtureBatch(records)
	require.NoError(t, err)
	admitted := batch.Payload
	records[0].Attributes["detached-edit"] = "must not be re-encoded"
	q := &encodedMappingQueue{fakeQueue: newFakeQueue(), batch: batch}
	invoked := make(chan string, 1)
	s := newTestService(t, q, fakeInvoker{invoke: func(_ context.Context, _ string, payload []byte) error {
		invoked <- string(payload)
		// A transport adapter may use mutable bytes without changing the
		// immutable admitted representation retained by the source.
		payload[0] = 'x'
		return nil
	}})
	_, err = s.Create(t.Context(), batchInput(2, 0))
	require.NoError(t, err)
	select {
	case actual := <-invoked:
		require.Equal(t, admitted, actual)
	case <-time.After(time.Second):
		t.Fatal("encoded batch did not reach invocation")
	}
	for _, record := range records {
		select {
		case receipt := <-q.deleted:
			require.Equal(t, record.ReceiptHandle, receipt)
		case <-time.After(time.Second):
			t.Fatal("original admitted receipt was not acknowledged")
		}
	}
	require.Equal(t, admitted, batch.Payload)
}

func TestMappingRejectsMissingEncodedPayloadWithoutAcknowledgment(t *testing.T) {
	q := &encodedMappingQueue{fakeQueue: newFakeQueue(), batch: Batch{Records: leasedBatch("missing-wire", 1, 1)}}
	invoked := make(chan struct{}, 1)
	s := newTestService(t, q, fakeInvoker{invoke: func(context.Context, string, []byte) error {
		invoked <- struct{}{}
		return nil
	}})
	mapping, err := s.Create(t.Context(), createInput())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		current, _ := s.Get(mapping.UUID)
		return current.State == "Disabled"
	}, time.Second, time.Millisecond)
	require.Empty(t, invoked)
	require.Empty(t, q.deleted)
}
