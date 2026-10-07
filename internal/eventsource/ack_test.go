package eventsource

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Receipt classification stays in the queue owner. This fixture supplies only
// the consumer port's completed acknowledgment result and lifetime.
type ackContractQueue struct {
	*fakeQueue
	acknowledge func(context.Context, string) (bool, error)
}

func (q ackContractQueue) Delete(ctx context.Context, receipt string) (bool, error) {
	return q.acknowledge(ctx, receipt)
}

func TestBatchAcknowledgmentFailureKeepsOriginalReceiptContract(t *testing.T) {
	for _, failure := range []struct {
		name string
		err  error
	}{
		{"unsettled original receipt", nil},
		{"queue acknowledgment error", errors.New("original receipt acknowledgment failed")},
	} {
		t.Run(failure.name, func(t *testing.T) {
			batch := leasedBatch("original", 5, 7)
			attempted := make(chan string, len(batch))
			q := ackContractQueue{fakeQueue: newFakeQueue(), acknowledge: func(_ context.Context, receipt string) (bool, error) {
				attempted <- receipt
				if receipt == batch[2].ReceiptHandle {
					return false, failure.err
				}
				return true, nil
			}}
			q.batches <- batch
			s := newTestService(t, q, fakeInvoker{})
			mapping, err := s.Create(t.Context(), batchInput(5, 0))
			require.NoError(t, err)
			for _, record := range batch {
				select {
				case receipt := <-attempted:
					require.Equal(t, record.ReceiptHandle, receipt, "only original batch receipts reach the queue owner")
				case <-time.After(time.Second):
					t.Fatal("a failed acknowledgment skipped remaining owned batch receipts")
				}
			}
			require.Eventually(t, func() bool {
				got, err := s.Get(mapping.UUID)
				return err == nil && got.LastProcessingResult == "Source acknowledgment failed"
			}, time.Second, time.Millisecond, "successful receipt results must not hide a failed acknowledgment")
		})
	}
}

func TestDeleteAndCloseJoinAcknowledgmentBeforeStoppingLaterReceipts(t *testing.T) {
	for _, closeService := range []bool{false, true} {
		t.Run(map[bool]string{false: "Delete", true: "Close"}[closeService], func(t *testing.T) {
			batch := leasedBatch("pending-ack", 5, 1)
			attempted := make(chan string, len(batch))
			started, canceled, cleanup, joined := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var cleanupOnce sync.Once
			release := func() { cleanupOnce.Do(func() { close(cleanup) }) }
			q := ackContractQueue{fakeQueue: newFakeQueue(), acknowledge: func(ctx context.Context, receipt string) (bool, error) {
				attempted <- receipt
				close(started)
				<-ctx.Done()
				close(canceled)
				<-cleanup
				close(joined)
				return false, ctx.Err()
			}}
			q.batches <- batch
			s, err := New(Options{Region: "us-east-1", AccountID: "000000000000"}, fakeSource{queue: q}, fakeInvoker{})
			require.NoError(t, err)
			t.Cleanup(func() { release(); _ = s.Close(context.Background()) })
			mapping, err := s.Create(t.Context(), batchInput(5, 0))
			require.NoError(t, err)
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("completed invocation did not enter acknowledgment")
			}
			ownerCtx, cancelOwner := context.WithCancel(context.Background())
			defer cancelOwner()
			stopped := make(chan error, 1)
			go func() {
				if closeService {
					stopped <- s.Close(ownerCtx)
				} else {
					_, err := s.Delete(ownerCtx, mapping.UUID)
					stopped <- err
				}
			}()
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("teardown did not cancel actual receipt work")
			}
			cancelOwner()
			require.Equal(t, batch[0].ReceiptHandle, <-attempted)
			select {
			case err := <-stopped:
				t.Fatalf("teardown returned before acknowledgment joined: %v", err)
			default:
			}
			release()
			select {
			case err := <-stopped:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("teardown did not join finished receipt cleanup")
			}
			select {
			case <-joined:
			default:
				t.Fatal("receipt cleanup was not joined")
			}
			require.Empty(t, attempted, "canceled teardown must not acknowledge later batch receipts")
		})
	}
}
