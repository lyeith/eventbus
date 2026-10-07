package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func receiptQueue(t *testing.T, b *Broker, name string) *Queue {
	t.Helper()
	attrs := map[string]string{"VisibilityTimeout": "60"}
	if strings.HasSuffix(name, ".fifo") {
		attrs["FifoQueue"] = "true"
	}
	q, failure := b.createSQSQueue(name, attrs, nil)
	require.Nil(t, failure)
	return q
}

func requireSQSLambdaReceiptAck(t *testing.T, b *Broker, q *Queue, receipt string, want bool) {
	t.Helper()
	settled, err := b.AcknowledgeSQSLambdaReceiptContext(t.Context(), q, receipt)
	require.NoError(t, err)
	require.Equal(t, want, settled)
}

func TestSQSLambdaReceiptSDKHandlerDeleteThenJoinedAck(t *testing.T) {
	b, _, _, client := setupTestServer(t)
	q := receiptQueue(t, b, "sdk-manual-receipt")
	sendSQSLambdaBatchFixture(t, b, q, "durable effect completed", "")
	event, err := b.ReceiveSQSLambdaEventContext(t.Context(), q, 1, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	require.Len(t, event.Records, 1)
	receipt := event.Records[0].ReceiptHandle
	_, err = client.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: aws.String(q.URL), ReceiptHandle: aws.String(receipt)})
	require.NoError(t, err)
	require.False(t, b.DeleteMessage(q, receipt), "ordinary local DeleteMessage preserves its strict-current contract")
	requireSQSLambdaReceiptAck(t, b, q, receipt, true)
	requireSQSLambdaReceiptAck(t, b, q, receipt, true)
	waiting, flight := b.QueueDepth(q)
	require.Zero(t, waiting)
	require.Zero(t, flight)
}

func TestSQSLambdaReceiptNativeJSONAndQueryDeleteRecordExactSettlement(t *testing.T) {
	for _, protocol := range []string{"json", "query"} {
		t.Run(protocol, func(t *testing.T) {
			b := newTestBroker()
			q := receiptQueue(t, b, "native-manual-receipt")
			sendSQSLambdaBatchFixture(t, b, q, "native handler delete", "")
			receipt := b.ReceiveMessages(q, 1, 0)[0].ReceiptHandle
			response := httptest.NewRecorder()
			handler := NewHandler(b)
			if protocol == "json" {
				payload, err := json.Marshal(map[string]string{"QueueUrl": q.URL, "ReceiptHandle": receipt})
				require.NoError(t, err)
				handler.ServeAction(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload))), "DeleteMessage")
			} else {
				form := url.Values{"Action": {"DeleteMessage"}, "QueueUrl": {q.URL}, "ReceiptHandle": {receipt}}
				request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				handler.ServeQuery(response, request, "DeleteMessage")
			}
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			requireSQSLambdaReceiptAck(t, b, q, receipt, true)
		})
	}
}

func TestSQSLambdaReceiptMixedBatchFiveAndFIFOOrdering(t *testing.T) {
	for _, fifo := range []bool{false, true} {
		t.Run(fmt.Sprintf("fifo-%v", fifo), func(t *testing.T) {
			b, _, _, client := setupTestServer(t)
			name := "mixed-manual-receipts"
			if fifo {
				name += ".fifo"
			}
			q := receiptQueue(t, b, name)
			for i := range 6 {
				sendSQSLambdaBatchFixture(t, b, q, fmt.Sprintf("effect-%d", i), "same-group")
			}
			event, err := b.ReceiveSQSLambdaEventContext(t.Context(), q, 5, 0, testSQSLambdaPayloadLimit)
			require.NoError(t, err)
			require.Len(t, event.Records, 5)
			_, err = client.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: aws.String(q.URL), ReceiptHandle: aws.String(event.Records[0].ReceiptHandle)})
			require.NoError(t, err)
			batch, err := client.DeleteMessageBatch(t.Context(), &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(q.URL), Entries: []sqstypes.DeleteMessageBatchRequestEntry{
				{Id: aws.String("manual-two"), ReceiptHandle: aws.String(event.Records[2].ReceiptHandle)},
				{Id: aws.String("manual-four"), ReceiptHandle: aws.String(event.Records[4].ReceiptHandle)},
				{Id: aws.String("unknown"), ReceiptHandle: aws.String("never-issued")},
			}})
			require.NoError(t, err)
			require.Len(t, batch.Successful, 2)
			require.Len(t, batch.Failed, 1)
			if fifo {
				require.Empty(t, b.ReceiveMessages(q, 1, 0), "manual subset settlement must not release its still-in-flight FIFO group")
			}
			for _, record := range event.Records {
				requireSQSLambdaReceiptAck(t, b, q, record.ReceiptHandle, true)
			}
			tail := b.ReceiveMessages(q, 1, 0)
			require.Len(t, tail, 1)
			require.Equal(t, "effect-5", tail[0].Body)
			require.Equal(t, 1, tail[0].ReceiveCount)
			requireSQSLambdaReceiptAck(t, b, q, tail[0].ReceiptHandle, true)
			waiting, flight := b.QueueDepth(q)
			require.Zero(t, waiting)
			require.Zero(t, flight)
		})
	}
}

func TestSQSLambdaReceiptIssuedStaleHTTPNoopIsNotSettlementProof(t *testing.T) {
	b, _, _, client := setupTestServer(t)
	q := receiptQueue(t, b, "stale-receipt-proof")
	sendSQSLambdaBatchFixture(t, b, q, "original lease", "")
	first := b.ReceiveMessages(q, 1, 0)[0]
	q.mu.Lock()
	q.inFlight[first.ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
	q.mu.Unlock()
	// AWS permits success for an old handle without actually deleting anything.
	_, err := client.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: aws.String(q.URL), ReceiptHandle: aws.String(first.ReceiptHandle)})
	require.NoError(t, err)
	requireSQSLambdaReceiptAck(t, b, q, first.ReceiptHandle, false)
	second := b.ReceiveMessages(q, 1, 0)[0]
	require.Equal(t, first.ID, second.ID)
	require.NotEqual(t, first.ReceiptHandle, second.ReceiptHandle)
	_, err = client.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: aws.String(q.URL), ReceiptHandle: aws.String(first.ReceiptHandle)})
	require.NoError(t, err)
	requireSQSLambdaReceiptAck(t, b, q, first.ReceiptHandle, false)
	q.mu.Lock()
	current := cloneMessage(q.inFlight[second.ReceiptHandle])
	issued := q.receipts[first.ReceiptHandle]
	q.mu.Unlock()
	require.Equal(t, second, current, "stale ACK must not alter the later lease")
	require.Equal(t, sqsReceiptUnsettled, issued.Settlement)
	requireSQSLambdaReceiptAck(t, b, q, second.ReceiptHandle, true)
	requireSQSLambdaReceiptAck(t, b, q, first.ReceiptHandle, false)
}

func TestSQSLambdaReceiptExpiredAckDoesNotHousekeepOriginalOrLaterLease(t *testing.T) {
	b := newTestBroker()
	q := receiptQueue(t, b, "expired-receipt-proof")
	sendSQSLambdaBatchFixture(t, b, q, "expiry owner", "")
	first := b.ReceiveMessages(q, 1, 0)[0]
	q.mu.Lock()
	q.inFlight[first.ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
	before := cloneMessage(q.inFlight[first.ReceiptHandle])
	q.mu.Unlock()
	requireSQSLambdaReceiptAck(t, b, q, first.ReceiptHandle, false)
	q.mu.Lock()
	after := cloneMessage(q.inFlight[first.ReceiptHandle])
	queued := len(q.messages)
	q.mu.Unlock()
	require.Equal(t, before, after)
	require.Zero(t, queued, "rejected ACK must not run unrelated native requeue work")
	require.Equal(t, 1, b.RequeueExpired(q))
	second := b.ReceiveMessages(q, 1, 0)[0]
	q.mu.Lock()
	q.inFlight[second.ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
	before = cloneMessage(q.inFlight[second.ReceiptHandle])
	q.mu.Unlock()
	requireSQSLambdaReceiptAck(t, b, q, first.ReceiptHandle, false)
	q.mu.Lock()
	after = cloneMessage(q.inFlight[second.ReceiptHandle])
	q.mu.Unlock()
	require.Equal(t, before, after, "even an expired later lease belongs to its native expiry owner")
	requireSQSLambdaReceiptAck(t, b, q, second.ReceiptHandle, false)
}

func TestSQSLambdaReceiptRetentionPurgeAndRedriveNeverInventSettlement(t *testing.T) {
	for _, removal := range []string{"retention", "purge", "redrive"} {
		t.Run(removal, func(t *testing.T) {
			b := newTestBroker()
			q := receiptQueue(t, b, "removed-receipt-proof")
			sendSQSLambdaBatchFixture(t, b, q, "native removal", "")
			receipt := b.ReceiveMessages(q, 1, 0)[0].ReceiptHandle
			switch removal {
			case "retention":
				q.mu.Lock()
				q.inFlight[receipt].SentTimestamp = time.Now().Add(-q.RetentionPeriod - time.Second)
				q.mu.Unlock()
				requireSQSLambdaReceiptAck(t, b, q, receipt, false)
				b.RequeueExpired(q)
			case "purge":
				_, failure := NewHandler(b).executeSQS(t.Context(), "PurgeQueue", sqsRequest{QueueURL: q.URL})
				require.Nil(t, failure)
			case "redrive":
				dead := receiptQueue(t, b, "receipt-proof-dlq")
				require.Nil(t, NewHandler(b).setQueueAttributes(q, map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + dead.ARN + `","maxReceiveCount":1}`}))
				require.True(t, b.ExtendMessageVisibility(q, receipt, 0))
				b.RequeueExpired(q)
				requireSQSLambdaReceiptAck(t, b, dead, receipt, false)
				deadMessage := b.ReceiveMessages(dead, 1, 0)
				require.Len(t, deadMessage, 1)
				requireSQSLambdaReceiptAck(t, b, dead, deadMessage[0].ReceiptHandle, true)
			}
			requireSQSLambdaReceiptAck(t, b, q, receipt, false)
			q.mu.Lock()
			issued := q.receipts[receipt]
			q.mu.Unlock()
			require.Equal(t, sqsReceiptUnsettled, issued.Settlement)
		})
	}
}

func TestSQSLambdaReceiptBoundInstanceRejectsUnknownAndReplacement(t *testing.T) {
	b := newTestBroker()
	q := receiptQueue(t, b, "replaced-receipt-proof")
	sendSQSLambdaBatchFixture(t, b, q, "settled original", "")
	receipt := b.ReceiveMessages(q, 1, 0)[0].ReceiptHandle
	require.Nil(t, NewHandler(b).deleteSQSReceipt(q, receipt))
	requireSQSLambdaReceiptAck(t, b, q, receipt, true)
	for _, unknown := range []string{"", "unknown", receipt + "-different"} {
		requireSQSLambdaReceiptAck(t, b, q, unknown, false)
	}
	foreign := newTestBroker().CreateQueue(q.Name, time.Minute, 0)
	settled, err := b.AcknowledgeSQSLambdaReceiptContext(t.Context(), foreign, receipt)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	require.False(t, settled)
	settled, err = b.AcknowledgeSQSLambdaReceiptContext(t.Context(), nil, receipt)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	require.False(t, settled)
	require.True(t, b.DeleteQueue(q.Name))
	replacement := b.CreateQueue(q.Name, time.Minute, 0)
	sendSQSLambdaBatchFixture(t, b, replacement, "replacement owner", "")
	current := b.ReceiveMessages(replacement, 1, 0)[0]
	settled, err = b.AcknowledgeSQSLambdaReceiptContext(t.Context(), q, receipt)
	require.ErrorIs(t, err, ErrQueueUnavailable, "settled history cannot escape its deleted queue instance")
	require.False(t, settled)
	requireSQSLambdaReceiptAck(t, b, replacement, receipt, false)
	replacement.mu.Lock()
	preserved := cloneMessage(replacement.inFlight[current.ReceiptHandle])
	replacement.mu.Unlock()
	require.Equal(t, current, preserved)
	requireSQSLambdaReceiptAck(t, b, replacement, current.ReceiptHandle, true)
}

func TestSQSLambdaReceiptCancellationDoesNotSettleOrPrune(t *testing.T) {
	b := newTestBroker()
	q := receiptQueue(t, b, "canceled-receipt-proof")
	sendSQSLambdaBatchFixture(t, b, q, "cancel-safe lease", "")
	current := b.ReceiveMessages(q, 1, 0)[0]
	ctx, cancel := context.WithCancel(t.Context())
	q.mu.Lock()
	result := make(chan error, 1)
	go func() {
		settled, err := b.AcknowledgeSQSLambdaReceiptContext(ctx, q, current.ReceiptHandle)
		if settled {
			result <- fmt.Errorf("canceled ACK settled an owned lease")
			return
		}
		result <- err
	}()
	cancel()
	q.mu.Unlock()
	require.ErrorIs(t, <-result, context.Canceled)
	q.mu.Lock()
	preserved := cloneMessage(q.inFlight[current.ReceiptHandle])
	issued := q.receipts[current.ReceiptHandle]
	q.mu.Unlock()
	require.Equal(t, current, preserved)
	require.Equal(t, sqsReceiptUnsettled, issued.Settlement)
	requireSQSLambdaReceiptAck(t, b, q, current.ReceiptHandle, true)
	settled, err := b.AcknowledgeSQSLambdaReceiptContext(ctx, q, current.ReceiptHandle)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, settled, "successful history does not override mapping cancellation")
}

func TestSQSLambdaReceiptSettlementUsesExistingIssuedExpiry(t *testing.T) {
	b := newTestBroker()
	q := receiptQueue(t, b, "bounded-receipt-proof")
	sendSQSLambdaBatchFixture(t, b, q, "native history grace", "")
	current := b.ReceiveMessages(q, 1, 0)[0]
	q.mu.Lock()
	before := q.receipts[current.ReceiptHandle]
	q.mu.Unlock()
	require.Equal(t, current.SentTimestamp.Add(q.RetentionPeriod+12*time.Hour), before.Expires)
	require.Equal(t, sqsReceiptUnsettled, before.Settlement)
	require.Nil(t, NewHandler(b).deleteSQSReceipt(q, current.ReceiptHandle))
	q.mu.Lock()
	after := q.receipts[current.ReceiptHandle]
	pruneSQSReceiptsLocked(q, after.Expires.Add(-time.Nanosecond))
	_, kept := q.receipts[current.ReceiptHandle]
	pruneSQSReceiptsLocked(q, after.Expires)
	_, removed := q.receipts[current.ReceiptHandle]
	q.mu.Unlock()
	require.Equal(t, sqsReceiptNativeSettlement, after.Settlement)
	require.Equal(t, before.Expires, after.Expires, "settlement does not extend receipt retention")
	require.True(t, kept)
	require.False(t, removed)
	requireSQSLambdaReceiptAck(t, b, q, current.ReceiptHandle, false)
}

func TestSQSLambdaReceiptNativeAttemptReplayKeepsSameUnmodifiedLease(t *testing.T) {
	b := newTestBroker()
	q := receiptQueue(t, b, "receipt-attempt.fifo")
	sendSQSLambdaBatchFixture(t, b, q, "attempt same owner", "group")
	first, failure := b.receiveSQS(t.Context(), q, 1, 0, nil, "attempt")
	require.Nil(t, failure)
	require.Len(t, first, 1)
	q.mu.Lock()
	q.inFlight[first[0].ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
	q.mu.Unlock()
	requireSQSLambdaReceiptAck(t, b, q, first[0].ReceiptHandle, false)
	replay, failure := b.receiveSQS(t.Context(), q, 1, 0, nil, "attempt")
	require.Nil(t, failure)
	require.Len(t, replay, 1)
	require.Equal(t, first[0].ReceiptHandle, replay[0].ReceiptHandle)
	require.Equal(t, first[0].ReceiveCount, replay[0].ReceiveCount)
	requireSQSLambdaReceiptAck(t, b, q, replay[0].ReceiptHandle, true)
	_, failure = b.receiveSQS(t.Context(), q, 1, 0, nil, "attempt")
	require.NotNil(t, failure, "actual settlement must invalidate native receive-attempt replay")
	requireSQSLambdaReceiptAck(t, b, q, first[0].ReceiptHandle, true)
}

func TestSQSLambdaReceiptConcurrentManualAndMappingSettlement(t *testing.T) {
	for i := range 16 {
		b := newTestBroker()
		q := receiptQueue(t, b, fmt.Sprintf("concurrent-receipt-%d", i))
		sendSQSLambdaBatchFixture(t, b, q, "single exact settlement", "")
		receipt := b.ReceiveMessages(q, 1, 0)[0].ReceiptHandle
		manual := make(chan *sqsError, 1)
		ack := make(chan error, 1)
		start := make(chan struct{})
		go func() {
			<-start
			manual <- NewHandler(b).deleteSQSReceipt(q, receipt)
		}()
		go func() {
			<-start
			settled, err := b.AcknowledgeSQSLambdaReceiptContext(t.Context(), q, receipt)
			if err == nil && !settled {
				err = fmt.Errorf("the exact current receipt was not acknowledged")
			}
			ack <- err
		}()
		close(start)
		require.Nil(t, <-manual)
		require.NoError(t, <-ack)
		waiting, flight := b.QueueDepth(q)
		require.Zero(t, waiting)
		require.Zero(t, flight)
		requireSQSLambdaReceiptAck(t, b, q, receipt, true)
	}
}

func requireSQSLambdaReceiptOutcome(t *testing.T, b *Broker, q *Queue, receipt string, acknowledge bool, expected SQSLambdaReceiptOutcome) {
	t.Helper()
	outcome, err := b.EvaluateSQSLambdaReceiptContext(t.Context(), q, receipt, acknowledge)
	require.NoError(t, err)
	require.Equal(t, expected, outcome)
}

func TestSQSLambdaReceiptEvidencePreservesNativeAndMappingOrigin(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native-%v", native), func(t *testing.T) {
			b := newTestBroker()
			q := receiptQueue(t, b, "origin-receipt-proof")
			sendSQSLambdaBatchFixture(t, b, q, "one original delivery", "")
			current := b.ReceiveMessages(q, 1, 0)[0]
			requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, false, SQSLambdaReceiptUnacknowledged)
			q.mu.Lock()
			before := q.receipts[current.ReceiptHandle]
			q.mu.Unlock()
			expected := SQSLambdaReceiptMappingSettled
			origin := sqsReceiptMappingSettlement
			if native {
				require.Nil(t, NewHandler(b).deleteSQSReceipt(q, current.ReceiptHandle))
				expected = SQSLambdaReceiptNativeSettled
				origin = sqsReceiptNativeSettlement
			}
			requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, true, expected)
			requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, false, expected)
			requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, true, expected)
			requireSQSLambdaReceiptAck(t, b, q, current.ReceiptHandle, true)
			require.False(t, b.DeleteMessage(q, current.ReceiptHandle), "evidence must preserve strict native local deletion")
			q.mu.Lock()
			after := q.receipts[current.ReceiptHandle]
			q.mu.Unlock()
			require.Equal(t, origin, after.Settlement)
			require.Equal(t, before.Expires, after.Expires, "inspection and replay must not extend native receipt history")
		})
	}
}

func TestSQSLambdaReceiptEvidencePureInspectionLeavesCurrentAndExpiredLeasesIntact(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired-%v", expired), func(t *testing.T) {
			b := newTestBroker()
			q := receiptQueue(t, b, "pure-receipt-proof")
			sendSQSLambdaBatchFixture(t, b, q, "current delivery", "")
			current := b.ReceiveMessages(q, 1, 0)[0]
			q.mu.Lock()
			if expired {
				q.inFlight[current.ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
			}
			before := cloneMessage(q.inFlight[current.ReceiptHandle])
			historyBefore := q.receipts[current.ReceiptHandle]
			wakeBefore := q.notify
			q.mu.Unlock()
			expected := SQSLambdaReceiptUnacknowledged
			if expired {
				expected = SQSLambdaReceiptStaleOrExpired
			}
			requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, false, expected)
			q.mu.Lock()
			after := cloneMessage(q.inFlight[current.ReceiptHandle])
			historyAfter := q.receipts[current.ReceiptHandle]
			wakeAfter := q.notify
			queued := len(q.messages)
			q.mu.Unlock()
			require.Equal(t, before, after)
			require.Equal(t, historyBefore, historyAfter)
			require.Equal(t, wakeBefore, wakeAfter)
			require.Zero(t, queued)
		})
	}
}

func TestSQSLambdaReceiptEvidenceSupersededDiscardedAndUnknown(t *testing.T) {
	for _, disappearance := range []string{"superseded", "purge", "redrive"} {
		t.Run(disappearance, func(t *testing.T) {
			b := newTestBroker()
			q := receiptQueue(t, b, "discarded-receipt-proof")
			sendSQSLambdaBatchFixture(t, b, q, "original delivery", "")
			original := b.ReceiveMessages(q, 1, 0)[0]
			var later *Message
			switch disappearance {
			case "superseded":
				require.True(t, b.ExtendMessageVisibility(q, original.ReceiptHandle, 0))
				later = b.ReceiveMessages(q, 1, 0)[0]
			case "purge":
				b.PurgeQueue(q)
			case "redrive":
				dead := receiptQueue(t, b, "discarded-receipt-dlq")
				require.True(t, b.MoveMessage(q, dead, original.ReceiptHandle))
			}
			requireSQSLambdaReceiptOutcome(t, b, q, original.ReceiptHandle, false, SQSLambdaReceiptStaleOrExpired)
			requireSQSLambdaReceiptOutcome(t, b, q, original.ReceiptHandle, true, SQSLambdaReceiptStaleOrExpired)
			require.Nil(t, NewHandler(b).deleteSQSReceipt(q, original.ReceiptHandle), "native stale HTTP success remains a no-op")
			requireSQSLambdaReceiptOutcome(t, b, q, original.ReceiptHandle, false, SQSLambdaReceiptStaleOrExpired)
			requireSQSLambdaReceiptOutcome(t, b, q, "never-issued", false, SQSLambdaReceiptUnknown)
			requireSQSLambdaReceiptOutcome(t, b, q, "never-issued", true, SQSLambdaReceiptUnknown)
			if later != nil {
				requireSQSLambdaReceiptOutcome(t, b, q, later.ReceiptHandle, false, SQSLambdaReceiptUnacknowledged)
				q.mu.Lock()
				preserved := cloneMessage(q.inFlight[later.ReceiptHandle])
				q.mu.Unlock()
				require.Equal(t, later, preserved, "classification cannot mutate the later delivery")
			}
		})
	}
}

func TestSQSLambdaReceiptEvidenceRejectsCanceledContextButFreshCompletionCanInspect(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprintf("native-%v", native), func(t *testing.T) {
			b := newTestBroker()
			q := receiptQueue(t, b, "canceled-evidence-proof")
			sendSQSLambdaBatchFixture(t, b, q, "finished handler delivery", "")
			current := b.ReceiveMessages(q, 1, 0)[0]
			if native {
				require.Nil(t, NewHandler(b).deleteSQSReceipt(q, current.ReceiptHandle))
			}
			canceled, cancel := context.WithCancel(t.Context())
			cancel()
			for _, acknowledge := range []bool{false, true} {
				outcome, err := b.EvaluateSQSLambdaReceiptContext(canceled, q, current.ReceiptHandle, acknowledge)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, SQSLambdaReceiptUnknown, outcome)
				require.False(t, outcome.Settled())
			}
			completion, stop := context.WithTimeout(t.Context(), time.Second)
			defer stop()
			outcome, err := b.EvaluateSQSLambdaReceiptContext(completion, q, current.ReceiptHandle, false)
			require.NoError(t, err)
			expected := SQSLambdaReceiptUnacknowledged
			if native {
				expected = SQSLambdaReceiptNativeSettled
			}
			require.Equal(t, expected, outcome, "joined failure/timeout inspection must not synthesize ACK")
			if !native {
				q.mu.Lock()
				preserved := cloneMessage(q.inFlight[current.ReceiptHandle])
				q.mu.Unlock()
				require.Equal(t, current, preserved)
			}
		})
	}
}

func TestSQSLambdaReceiptEvidenceRemovedReplacedAndForeignQueues(t *testing.T) {
	b := newTestBroker()
	q := receiptQueue(t, b, "bound-evidence-proof")
	sendSQSLambdaBatchFixture(t, b, q, "original receipt", "")
	original := b.ReceiveMessages(q, 1, 0)[0]
	require.Nil(t, NewHandler(b).deleteSQSReceipt(q, original.ReceiptHandle))
	require.True(t, b.DeleteQueue(q.Name))
	replacement := b.CreateQueue(q.Name, time.Minute, 0)
	sendSQSLambdaBatchFixture(t, b, replacement, "replacement receipt", "")
	current := b.ReceiveMessages(replacement, 1, 0)[0]
	foreign := newTestBroker().CreateQueue(q.Name, time.Minute, 0)
	for _, bound := range []*Queue{q, foreign, nil} {
		for _, acknowledge := range []bool{false, true} {
			outcome, err := b.EvaluateSQSLambdaReceiptContext(t.Context(), bound, original.ReceiptHandle, acknowledge)
			require.ErrorIs(t, err, ErrQueueUnavailable)
			require.Equal(t, SQSLambdaReceiptQueueUnavailable, outcome)
			require.False(t, outcome.Settled())
		}
	}
	requireSQSLambdaReceiptOutcome(t, b, replacement, original.ReceiptHandle, false, SQSLambdaReceiptUnknown)
	requireSQSLambdaReceiptOutcome(t, b, replacement, current.ReceiptHandle, false, SQSLambdaReceiptUnacknowledged)
	replacement.mu.Lock()
	preserved := cloneMessage(replacement.inFlight[current.ReceiptHandle])
	replacement.mu.Unlock()
	require.Equal(t, current, preserved)
}

func TestSQSLambdaReceiptEvidenceExpiredHistoryIsUnknownWithoutPruning(t *testing.T) {
	b := newTestBroker()
	q := receiptQueue(t, b, "history-evidence-proof")
	sendSQSLambdaBatchFixture(t, b, q, "history original", "")
	current := b.ReceiveMessages(q, 1, 0)[0]
	requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, true, SQSLambdaReceiptMappingSettled)
	q.mu.Lock()
	issued := q.receipts[current.ReceiptHandle]
	issued.Expires = time.Now().Add(-time.Second)
	q.receipts[current.ReceiptHandle] = issued
	wakeBefore := q.notify
	q.mu.Unlock()
	requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, false, SQSLambdaReceiptUnknown)
	requireSQSLambdaReceiptOutcome(t, b, q, current.ReceiptHandle, true, SQSLambdaReceiptUnknown)
	requireSQSLambdaReceiptAck(t, b, q, current.ReceiptHandle, false)
	q.mu.Lock()
	after, retained := q.receipts[current.ReceiptHandle]
	wakeAfter := q.notify
	q.mu.Unlock()
	require.True(t, retained, "evidence inspection must not perform the maintenance owner's pruning")
	require.Equal(t, issued, after)
	require.Equal(t, wakeBefore, wakeAfter)
}
