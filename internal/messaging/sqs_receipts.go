package messaging

import (
	"context"
	"time"
)

// SQSLambdaReceiptOutcome is queue-owned evidence about one original delivery.
// It contains no receipt handle or message payload. NativeSettled proves a native
// caller deleted the current receipt; it does not identify that caller.
type SQSLambdaReceiptOutcome string

const (
	SQSLambdaReceiptMappingSettled   SQSLambdaReceiptOutcome = "mapping_settled"
	SQSLambdaReceiptNativeSettled    SQSLambdaReceiptOutcome = "native_settled"
	SQSLambdaReceiptUnacknowledged   SQSLambdaReceiptOutcome = "unacknowledged"
	SQSLambdaReceiptStaleOrExpired   SQSLambdaReceiptOutcome = "stale_or_expired"
	SQSLambdaReceiptUnknown          SQSLambdaReceiptOutcome = "unknown"
	SQSLambdaReceiptQueueUnavailable SQSLambdaReceiptOutcome = "queue_unavailable"
)

func (outcome SQSLambdaReceiptOutcome) Settled() bool {
	return outcome == SQSLambdaReceiptMappingSettled || outcome == SQSLambdaReceiptNativeSettled
}

type sqsReceiptSettlement uint8

const (
	sqsReceiptUnsettled sqsReceiptSettlement = iota
	sqsReceiptNativeSettlement
	sqsReceiptMappingSettlement
)

// sqsReceipt retains native issuance until the existing grace deadline. Only an
// actual current/unexpired deletion records its settlement origin. Issuance
// still permits the AWS stale-delete no-op; expiry, redrive and purge never
// produce settlement proof or extend this history's lifetime.
type sqsReceipt struct {
	Expires    time.Time
	Settlement sqsReceiptSettlement
}

func issueSQSReceiptLocked(q *Queue, message *Message) {
	q.receipts[message.ReceiptHandle] = sqsReceipt{Expires: message.SentTimestamp.Add(q.RetentionPeriod + 12*time.Hour)}
}

func pruneSQSReceiptsLocked(q *Queue, now time.Time) {
	for handle, receipt := range q.receipts {
		if !now.Before(receipt.Expires) {
			delete(q.receipts, handle)
		}
	}
}

// DeleteMessage settles only a current, unexpired receipt. Native HTTP and local
// consumers share the same locked settlement; previously issued stale handles
// may be accepted as HTTP no-ops, but cannot delete a later lease.
func (b *Broker) DeleteMessage(q *Queue, receipt string) bool {
	settled, _ := b.deleteSQSReceipt(q, receipt)
	return settled
}

// deleteSQSReceipt owns native deletion mechanics for typed and HTTP callers.
// Exact current settlement needs no unrelated queue-wide scans. Noncurrent
// receipts retain the existing redrive/expiry transitions before deciding the
// AWS issued-stale no-op. The adapter chooses whether that no-op is success.
func (b *Broker) deleteSQSReceipt(q *Queue, receipt string) (bool, *sqsError) {
	if q == nil {
		return false, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
	}
	q.mu.Lock()
	now := time.Now()
	if q.deleted {
		q.mu.Unlock()
		return false, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
	}
	if settleCurrentSQSReceiptLocked(q, receipt, sqsReceiptNativeSettlement, now) {
		q.mu.Unlock()
		return true, nil
	}
	q.mu.Unlock()

	b.redriveExpiredSQS(q, now)
	q.mu.Lock()
	defer q.mu.Unlock()
	now = time.Now()
	pruneQueueLocked(q, now)
	if q.deleted {
		return false, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
	}
	if settleCurrentSQSReceiptLocked(q, receipt, sqsReceiptNativeSettlement, now) {
		return true, nil
	}
	if issued, known := q.receipts[receipt]; known && now.Before(issued.Expires) {
		return false, nil
	}
	return false, newSQSError("ReceiptHandleIsInvalid", "The receipt handle is invalid")
}

func currentSQSReceiptLocked(q *Queue, receipt string, now time.Time) (*Message, bool) {
	message := q.inFlight[receipt]
	return message, message != nil && now.Before(message.VisibleAt) && (q.RetentionPeriod <= 0 || now.Before(message.SentTimestamp.Add(q.RetentionPeriod)))
}

func settleCurrentSQSReceiptLocked(q *Queue, receipt string, origin sqsReceiptSettlement, now time.Time) bool {
	if q.deleted {
		return false
	}
	message, current := currentSQSReceiptLocked(q, receipt, now)
	if !current {
		return false
	}
	delete(q.inFlight, receipt)
	if issued, known := q.receipts[receipt]; known {
		issued.Settlement = origin
		q.receipts[receipt] = issued
	}
	releaseDevSQSMessageLocked(q, message.ID)
	invalidateReceiveAttemptsLocked(q, receipt)
	notifyQueueLocked(q)
	return true
}

func inspectSQSLambdaReceiptLocked(q *Queue, receipt string, now time.Time) SQSLambdaReceiptOutcome {
	if message, current := currentSQSReceiptLocked(q, receipt, now); message != nil {
		if current {
			return SQSLambdaReceiptUnacknowledged
		}
		return SQSLambdaReceiptStaleOrExpired
	}
	issued, known := q.receipts[receipt]
	if !known || !now.Before(issued.Expires) {
		return SQSLambdaReceiptUnknown
	}
	switch issued.Settlement {
	case sqsReceiptNativeSettlement:
		return SQSLambdaReceiptNativeSettled
	case sqsReceiptMappingSettlement:
		return SQSLambdaReceiptMappingSettled
	default:
		// The issued original is no longer current. Its expiry, supersession,
		// purge or redrive cannot be mistaken for successful deletion.
		return SQSLambdaReceiptStaleOrExpired
	}
}

// EvaluateSQSLambdaReceiptContext inspects an original bound delivery without
// mutation when acknowledge is false. On true, only its exact current receipt
// is settled; native prior settlement stays distinguishable from mapping ACK.
// An inspection after failed/canceled execution can use a fresh bounded context
// only after the invocation has joined. Cancellation/error is never success.
// Neither mode runs housekeeping or changes another receipt's ownership.
func (b *Broker) EvaluateSQSLambdaReceiptContext(ctx context.Context, q *Queue, receipt string, acknowledge bool) (SQSLambdaReceiptOutcome, error) {
	if err := ctx.Err(); err != nil {
		return SQSLambdaReceiptUnknown, err
	}
	if _, err := b.QueueInfo(q); err != nil {
		return SQSLambdaReceiptQueueUnavailable, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return SQSLambdaReceiptUnknown, err
	}
	if q.deleted {
		return SQSLambdaReceiptQueueUnavailable, ErrQueueUnavailable
	}
	now := time.Now()
	outcome := inspectSQSLambdaReceiptLocked(q, receipt, now)
	if acknowledge && outcome == SQSLambdaReceiptUnacknowledged {
		if settleCurrentSQSReceiptLocked(q, receipt, sqsReceiptMappingSettlement, now) {
			return SQSLambdaReceiptMappingSettled, nil
		}
	}
	return outcome, nil
}

// AcknowledgeSQSLambdaReceiptContext preserves the original bool mapping port.
// Native current settlement and proven prior settlement are both success; merely
// issued stale/expired/unknown receipts cannot acknowledge any later lease.
func (b *Broker) AcknowledgeSQSLambdaReceiptContext(ctx context.Context, q *Queue, receipt string) (bool, error) {
	outcome, err := b.EvaluateSQSLambdaReceiptContext(ctx, q, receipt, true)
	return err == nil && outcome.Settled(), err
}
