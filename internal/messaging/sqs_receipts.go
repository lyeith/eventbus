package messaging

import (
	"context"
	"time"
)

// sqsReceipt retains native receipt issuance until the existing grace deadline.
// Issuance permits the AWS stale-delete no-op; Settled proves that this exact
// receipt was actually deleted while it was current and unexpired. Expiry,
// redrive and purge never produce that proof.
type sqsReceipt struct {
	Expires time.Time
	Settled bool
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
	now := time.Now()
	b.redriveExpiredSQS(q, now)
	q.mu.Lock()
	defer q.mu.Unlock()
	pruneQueueLocked(q, time.Now())
	return deleteCurrentSQSReceiptLocked(q, receipt)
}

func deleteCurrentSQSReceiptLocked(q *Queue, receipt string) bool {
	if q.deleted {
		return false
	}
	message, ok := q.inFlight[receipt]
	if !ok {
		return false
	}
	now := time.Now()
	if !now.Before(message.VisibleAt) || q.RetentionPeriod > 0 && !now.Before(message.SentTimestamp.Add(q.RetentionPeriod)) {
		return false
	}
	delete(q.inFlight, receipt)
	if issued, known := q.receipts[receipt]; known {
		issued.Settled = true
		q.receipts[receipt] = issued
	}
	releaseDevSQSMessageLocked(q, message.ID)
	invalidateReceiveAttemptsLocked(q, receipt)
	notifyQueueLocked(q)
	return true
}

// AcknowledgeSQSLambdaReceiptContext settles one completed mapping delivery on
// its original queue instance. An SDK handler may have already deleted that
// same current receipt; explicit native settlement history makes its joined ACK
// idempotent. Merely issued stale/expired/unknown receipts are never success.
// Refusal and cancellation perform no housekeeping or mutation of other leases.
func (b *Broker) AcknowledgeSQSLambdaReceiptContext(ctx context.Context, q *Queue, receipt string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := b.QueueInfo(q); err != nil {
		return false, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if q.deleted {
		return false, ErrQueueUnavailable
	}
	if deleteCurrentSQSReceiptLocked(q, receipt) {
		return true, nil
	}
	issued, known := q.receipts[receipt]
	return known && issued.Settled && time.Now().Before(issued.Expires), nil
}
