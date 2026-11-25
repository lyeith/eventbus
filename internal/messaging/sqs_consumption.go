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
	if max < 1 || max > maxSQSReceiveCount || wait < 0 || wait > maxWaitTime {
		return errors.New("invalid SQS receive bounds")
	}
	return nil
}

const (
	maxSQSReceiveCount       = 10
	maxSQSLambdaBatchRecords = 10_000
	maxSQSLambdaBatchWindow  = 300 * time.Second
	maxFilteredSQSPerReceive = 100
)

var ErrSQSLambdaPayloadTooLarge = errors.New("visible SQS record exceeds the Lambda event byte budget")

// ReceiveSQSLambdaEventContext uses the existing one-poll batch contract.
func (b *Broker) ReceiveSQSLambdaEventContext(ctx context.Context, queue *Queue, max int, wait time.Duration, maxPayloadBytes int) (sqsevent.Event, error) {
	batch, err := b.ReceiveSQSLambdaBatchContext(ctx, queue, max, wait, maxPayloadBytes)
	return sqsevent.Event{Records: batch.Records}, err
}

// ReceiveSQSLambdaBatchContext retains the exact admitted wire records.
func (b *Broker) ReceiveSQSLambdaBatchContext(ctx context.Context, queue *Queue, max int, wait time.Duration, maxPayloadBytes int) (sqsevent.Batch, error) {
	return b.ReceiveSQSLambdaMappingBatchContext(ctx, queue, SQSLambdaBatchOptions{MaxRecords: max, Wait: wait, MaxPayloadBytes: maxPayloadBytes})
}

// SQSLambdaBatchOptions controls one mapping receive. Match is pure and executes
// under the bound queue's lock over detached, prospective native records.
type SQSLambdaBatchOptions struct {
	MaxRecords        int
	Wait              time.Duration
	Window            time.Duration
	InvocationTimeout time.Duration
	MaxPayloadBytes   int
	Match             func(sqsevent.Record) bool
	OwnedOnly         bool
}

// ReceiveSQSLambdaMappingBatchContext keeps all selected receipts and their
// exact admitted bytes across a batching window. A canceled intake returns
// any already leased records so the mapping can finish its accepted work.
func (b *Broker) ReceiveSQSLambdaMappingBatchContext(ctx context.Context, queue *Queue, options SQSLambdaBatchOptions) (sqsevent.Batch, error) {
	batch := sqsevent.Batch{Records: []sqsevent.Record{}, Payload: sqsevent.EmptyBatchPayload}
	if err := ctx.Err(); err != nil {
		return batch, err
	}
	if _, err := b.QueueInfo(queue); err != nil {
		return batch, err
	}
	if options.MaxRecords < 1 || options.MaxRecords > maxSQSLambdaBatchRecords ||
		options.Window < 0 || options.Window > maxSQSLambdaBatchWindow || options.InvocationTimeout < 0 ||
		options.MaxRecords > maxSQSReceiveCount && options.Window == 0 ||
		options.Wait < 0 || options.Wait > maxWaitTime || options.MaxPayloadBytes < len(sqsevent.EmptyBatchPayload) {
		return batch, errors.New("invalid SQS Lambda batch bounds")
	}
	used := len(sqsevent.EmptyBatchPayload)
	var encoded [][]byte
	region := sqsRegionFromARN(queue.ARN)
	var admissionErr error
	var deadline time.Time
	payloadLimited := false
	fillingWindow := false
	admit := func(candidate *Message) sqsReceiveDisposition {
		if ctx.Err() != nil || fillingWindow && !time.Now().Before(deadline) {
			return sqsReceiveSkip
		}
		record := buildSQSLambdaRecord(candidate, queue.ARN, region)
		if options.Match != nil && !options.Match(record) {
			if ctx.Err() != nil || fillingWindow && !time.Now().Before(deadline) {
				return sqsReceiveSkip
			}
			return sqsReceiveDiscard
		}
		payload, err := json.Marshal(record)
		if err != nil {
			admissionErr = err
			return sqsReceiveSkip
		}
		size := len(payload)
		if len(batch.Records) > 0 {
			size++
		} // Actual comma ahead of this record.
		if size > options.MaxPayloadBytes-used {
			payloadLimited = true
			if len(batch.Records) == 0 {
				admissionErr = ErrSQSLambdaPayloadTooLarge
			}
			return sqsReceiveSkip
		}
		if ctx.Err() != nil || fillingWindow && !time.Now().Before(deadline) {
			return sqsReceiveSkip
		}
		if options.Window > 0 {
			if deadline.IsZero() {
				deadline = candidate.ReceivedAt.Add(options.Window)
			}
			// Do not consume a lease's available execution time just waiting
			// for more records. Ready records from this receive may still fit.
			if flushBy := candidate.VisibleAt.Add(-options.InvocationTimeout); flushBy.Before(deadline) {
				deadline = flushBy
			}
		}
		used += size
		batch.Records = append(batch.Records, record)
		encoded = append(encoded, payload)
		return sqsReceiveLease
	}

	receiveOptions := sqsReceiveOptions{ownedOnly: options.OwnedOnly && queue.devCustody != nil}
	wait := options.Wait
	for {
		selected, failure := b.receiveSQSWithOwnership(ctx, queue, options.MaxRecords-len(batch.Records), wait, nil, "", admit, receiveOptions)
		if failure != nil {
			return batch, ErrQueueUnavailable
		}
		if len(batch.Records) == 0 {
			if err := ctx.Err(); err != nil {
				return batch, err
			}
			return batch, admissionErr
		}
		if options.Window == 0 || len(batch.Records) == options.MaxRecords ||
			payloadLimited || ctx.Err() != nil || selected.custodyEmpty {
			break
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		fillingWindow = true
		wait = min(maxWaitTime, remaining)
	}
	return sqsevent.AssembleBatch(batch.Records, encoded), nil
}
