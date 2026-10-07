package eventsource

import (
	"context"
	"errors"
)

// DevQueue is the optional queue-owner custody seam. It preserves native queue
// leases/FIFO/redrive and filters accepted continuations before leasing. Its
// change signal is captured atomically with PendingRetained, so parked workers
// cannot miss callback/cleanup work. Registration is released only after every
// mapping worker joins; disabled mappings never register queue custody.
// This port defines no AWS request field or second admission policy.
type DevQueue interface {
	RegisterRetained() (release func(), err error)
	PendingRetained() (pending bool, changed <-chan struct{}, err error)
	ReceiveRetained(context.Context, int) ([]Record, error)
}

func (s *Service) receive(ctx context.Context, item *entry) ([]Record, func(error), error) {
	if s.dev.Source == nil {
		records, err := item.queue.Receive(ctx, item.mapping.BatchSize)
		return records, nil, err
	}
	for ctx.Err() == nil {
		complete, changed, err := s.dev.Source.BeginSource("sqs_mapping", item.mapping.UUID)
		if err == nil {
			if complete == nil {
				return nil, nil, errors.New("development source admission returned no completion")
			}
			records, err := receiveWithFence(ctx, changed, func(receiveCtx context.Context) ([]Record, error) {
				return item.queue.Receive(receiveCtx, item.mapping.BatchSize)
			})
			return records, complete, err
		}
		pending, queueChanged, queueErr := item.retained.PendingRetained()
		if queueErr != nil {
			return nil, nil, queueErr
		}
		if pending {
			complete, activityErr := s.dev.Activity.BeginActivity("sqs_mapping", item.mapping.UUID)
			if activityErr == nil {
				if complete == nil {
					return nil, nil, errors.New("development continuation returned no completion")
				}
				records, err := item.retained.ReceiveRetained(ctx, item.mapping.BatchSize)
				return records, complete, err
			}
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-changed:
		case <-queueChanged:
		}
	}
	return nil, nil, ctx.Err()
}

// A fence interrupts intake only. Already selected records retain the original
// mapping context through actual invocation and acknowledgment. The watcher is
// joined before returning, so mapping shutdown owns every helper goroutine too.
func receiveWithFence(ctx context.Context, changed <-chan struct{}, receive func(context.Context) ([]Record, error)) ([]Record, error) {
	receiveCtx, cancel := context.WithCancel(ctx)
	done, watched := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watched)
		select {
		case <-changed:
			cancel()
		case <-done:
		case <-ctx.Done():
		}
	}()
	records, err := receive(receiveCtx)
	fenced := receiveCtx.Err() != nil && ctx.Err() == nil
	cancel()
	close(done)
	<-watched
	if len(records) == 0 && fenced && errors.Is(err, context.Canceled) {
		return nil, nil // Intake cancellation is a pause, never a native disable.
	}
	return records, err
}
