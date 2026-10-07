package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
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
	if err := b.validateBoundReceive(ctx, queue, max, wait); err != nil {
		return nil, err
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

func (b *Broker) validateBoundReceive(ctx context.Context, queue *Queue, max int, wait time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := b.QueueInfo(queue); err != nil {
		return err
	}
	if max < 1 || max > 10 || wait < 0 || wait > maxWaitTime {
		return errors.New("invalid SQS receive bounds")
	}
	return nil
}

var ErrSQSLambdaPayloadTooLarge = errors.New("visible SQS record exceeds the Lambda event byte budget")

// ReceiveSQSLambdaEventContext admits only complete native records whose actual
// JSON fits the caller's invocation byte budget. Rejected candidates never gain
// a receipt, receive count, first-receive timestamp or visibility lease. The
// ordinary SQS receive/FIFO/redrive path still owns selection and all state.
func (b *Broker) ReceiveSQSLambdaEventContext(ctx context.Context, queue *Queue, max int, wait time.Duration, maxPayloadBytes int) (sqsevent.Event, error) {
	event := BuildSQSLambdaEvent(nil, "")
	if err := b.validateBoundReceive(ctx, queue, max, wait); err != nil {
		return event, err
	}
	emptyPayload, err := json.Marshal(event)
	if err != nil || maxPayloadBytes < len(emptyPayload) {
		return event, errors.New("invalid SQS Lambda event byte budget")
	}
	used := len(emptyPayload)
	var admissionErr error
	admit := func(candidate *Message) bool {
		if err := ctx.Err(); err != nil {
			admissionErr = err
			return false
		}
		record := BuildSQSLambdaEvent([]*Message{candidate}, queue.ARN).Records[0]
		payload, err := json.Marshal(record)
		if err != nil {
			admissionErr = err
			return false
		}
		bytes := len(payload)
		if len(event.Records) != 0 {
			bytes++ // The actual comma between adjacent JSON records.
		}
		if bytes > maxPayloadBytes-used {
			if len(event.Records) == 0 {
				admissionErr = ErrSQSLambdaPayloadTooLarge
			}
			return false
		}
		if err := ctx.Err(); err != nil {
			admissionErr = err
			return false
		}
		used += bytes
		event.Records = append(event.Records, record)
		return true
	}
	_, failure := b.receiveSQSWithAdmission(ctx, queue, max, wait, nil, "", admit)
	if failure != nil {
		return event, ErrQueueUnavailable
	}
	if len(event.Records) != 0 {
		return event, nil
	}
	if err := ctx.Err(); err != nil {
		return event, err
	}
	return event, admissionErr
}
