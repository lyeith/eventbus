package messaging

import (
	"context"
	"errors"
	"time"
)

// BoundQueueInfo is a locked metadata snapshot; it exposes no mutable queue state.
type BoundQueueInfo struct {
	ARN               string
	VisibilityTimeout time.Duration
}

var ErrQueueUnavailable = errors.New("owned queue is unavailable")

func (b *Broker) QueueInfo(queue *Queue) (BoundQueueInfo, error) {
	if queue == nil || b.GetQueueByARN(queue.ARN) != queue {
		return BoundQueueInfo{}, ErrQueueUnavailable
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if queue.deleted {
		return BoundQueueInfo{}, ErrQueueUnavailable
	}
	return BoundQueueInfo{ARN: queue.ARN, VisibilityTimeout: queue.VisibilityTimeout}, nil
}

// ReceiveMessagesContext uses the same native SQS lease/redrive implementation
// as HTTP. The supplied queue handle must belong to this broker and is never
// resolved again by name/ARN after deletion or recreation.
func (b *Broker) ReceiveMessagesContext(ctx context.Context, queue *Queue, max int, wait time.Duration) ([]*Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := b.QueueInfo(queue); err != nil {
		return nil, err
	}
	if max < 1 || max > 10 || wait < 0 || wait > maxWaitTime {
		return nil, errors.New("invalid SQS receive bounds")
	}
	messages, failure := b.receiveSQS(ctx, queue, max, wait, nil, "")
	if failure != nil {
		return nil, ErrQueueUnavailable
	}
	if len(messages) == 0 && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return messages, nil
}
