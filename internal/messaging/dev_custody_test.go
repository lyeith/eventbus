package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
)

type custodyActivity struct {
	mu        sync.Mutex
	live      map[int]string
	begins    int
	failAt    int
	minDuring int
}

func (a *custodyActivity) BeginActivity(kind, id string) (func(error), error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.begins++
	if a.begins == a.failAt {
		return nil, errors.New("observer refused ownership")
	}
	if kind != "sqs_message" || id == "" || strings.ContainsAny(id, "/\n ") {
		return nil, errors.New("unsafe activity identity")
	}
	if a.live == nil {
		a.live = make(map[int]string)
	}
	lease := a.begins
	a.live[lease] = id
	var once sync.Once
	return func(err error) {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			delete(a.live, lease)
			if a.minDuring > len(a.live) {
				a.minDuring = len(a.live)
			}
		})
	}, nil
}

func (a *custodyActivity) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.live)
}

func (a *custodyActivity) refuseNext() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failAt = a.begins + 1
}

func custodyBroker(t *testing.T) (*Broker, *custodyActivity) {
	t.Helper()
	b := newTestBroker()
	activity := &custodyActivity{}
	require.NoError(t, b.SetDevActivity(activity))
	return b, activity
}

func custodyRegister(t *testing.T, b *Broker, q *Queue) func() {
	t.Helper()
	release, err := b.RegisterSQSLambdaCustody(q)
	require.NoError(t, err)
	t.Cleanup(release)
	return release
}

func custodySend(t *testing.T, b *Broker, q *Queue, body string) string {
	t.Helper()
	input := QueueMessageInput{Body: body}
	if strings.HasSuffix(q.Name, ".fifo") {
		input.MessageGroupID = "group"
		input.MessageDeduplicationID = md5Body(body)
	}
	result, err := b.SendQueueMessage(q, input)
	require.NoError(t, err)
	return result.MessageID
}

func custodyPending(t *testing.T, b *Broker, q *Queue, expected bool) <-chan struct{} {
	t.Helper()
	pending, changed, err := b.SQSLambdaCustodyState(q)
	require.NoError(t, err)
	require.Equal(t, expected, pending)
	require.NotNil(t, changed)
	return changed
}

func custodyReceive(t *testing.T, b *Broker, q *Queue, max int) sqsevent.Event {
	t.Helper()
	event, err := b.ReceiveOwnedSQSLambdaEventContext(t.Context(), q, max, 0, testSQSLambdaPayloadLimit)
	require.NoError(t, err)
	return event
}

func custodyLocked(q *Queue, assertions func()) {
	q.mu.Lock()
	defer q.mu.Unlock()
	assertions()
}

func TestDevSQSCustodyAdoptsAllNativeStatesAndRefcountsMappings(t *testing.T) {
	b, activity := custodyBroker(t)
	q := b.CreateQueue("existing-custody", time.Hour, 0)
	id := custodySend(t, b, q, "in-flight")
	flight := b.ReceiveMessages(q, 1, 0)
	require.Len(t, flight, 1)
	delay := 600
	_, err := b.SendQueueMessage(q, QueueMessageInput{Body: "delayed", DelaySeconds: &delay})
	require.NoError(t, err)
	custodySend(t, b, q, "waiting")
	require.Zero(t, activity.count(), "unmapped data has no autonomous owner")
	first := custodyRegister(t, b, q)
	require.Equal(t, 3, activity.count())
	second := custodyRegister(t, b, q)
	require.Equal(t, 3, activity.count(), "multiple mappings share message custody")
	first()
	first()
	require.Equal(t, 3, activity.count(), "unregister is idempotent and preserves the other mapping")
	require.True(t, b.DeleteMessage(q, flight[0].ReceiptHandle))
	require.Equal(t, 2, activity.count())
	var before []*Message
	custodyLocked(q, func() {
		require.NotContains(t, q.devCustody.messages, id)
		before = append([]*Message(nil), q.messages...)
	})
	second()
	require.Zero(t, activity.count())
	custodyPending(t, b, q, false)
	custodyLocked(q, func() {
		require.Equal(t, before, q.messages, "last mapping leaves paused native data intact")
	})
	require.Empty(t, custodyReceive(t, b, q, 10).Records)
	third := custodyRegister(t, b, q)
	require.Equal(t, 2, activity.count(), "re-enabled mappings adopt the same retained data")
	third()
}

func TestDevSQSCustodyRegistrationRollbackAndConstructionBoundary(t *testing.T) {
	b, activity := custodyBroker(t)
	q := b.CreateQueue("register-rollback", time.Hour, 0)
	for i := range 3 {
		custodySend(t, b, q, fmt.Sprintf("existing-%d", i))
	}
	activity.failAt = 2
	release, err := b.RegisterSQSLambdaCustody(q)
	require.Error(t, err)
	require.Nil(t, release)
	require.Zero(t, activity.count())
	custodyLocked(q, func() {
		require.Zero(t, q.devCustody.registrations)
		require.Empty(t, q.devCustody.messages)
		require.Len(t, q.messages, 3)
		for _, message := range q.messages {
			require.Zero(t, message.ReceiveCount)
			require.Empty(t, message.ReceiptHandle)
		}
	})
	custodyRegister(t, b, q)
	require.Equal(t, 3, activity.count())
	require.Error(t, b.SetDevActivity(&custodyActivity{}))
	require.True(t, b.DeleteQueue(q.Name))
	require.Zero(t, activity.count())
	require.Error(t, b.SetDevActivity(&custodyActivity{}), "deleted queue handles must not cross observer epochs")

	ordinary := newTestBroker()
	ordinaryQueue := ordinary.CreateQueue("ordinary", time.Hour, 0)
	require.Error(t, ordinary.SetDevActivity(activity), "an observer cannot be added after native construction")
	_, err = ordinary.SendQueueMessage(ordinaryQueue, QueueMessageInput{Body: "ordinary body"})
	require.NoError(t, err)
	ordinaryRelease, err := ordinary.RegisterSQSLambdaCustody(ordinaryQueue)
	require.NoError(t, err)
	ordinaryRelease()
	require.Len(t, custodyReceive(t, ordinary, ordinaryQueue, 1).Records, 1, "nil activity preserves normal mode")
}

func TestDevSQSCustodySendFailureDoesNotMutateFIFOState(t *testing.T) {
	b, activity := custodyBroker(t)
	q, failure := b.createSQSQueue("custody-send.fifo", map[string]string{"FifoQueue": "true"}, nil)
	require.Nil(t, failure)
	custodyRegister(t, b, q)
	activity.refuseNext()
	before := custodyPending(t, b, q, false)
	_, err := b.SendQueueMessage(q, QueueMessageInput{Body: "body", MessageGroupID: "group", MessageDeduplicationID: "dedup"})
	require.Error(t, err)
	require.Equal(t, "ServiceUnavailable", err.(*sqsError).Code)
	custodyLocked(q, func() {
		require.Empty(t, q.messages)
		require.Empty(t, q.inFlight)
		require.Empty(t, q.dedup)
		require.Zero(t, q.sequence)
		require.Equal(t, before, (<-chan struct{})(q.notify), "failed admission must not publish native work")
	})
	input := QueueMessageInput{Body: "body", MessageGroupID: "group", MessageDeduplicationID: "dedup"}
	result, err := b.SendQueueMessage(q, input)
	require.NoError(t, err)
	require.Equal(t, "1", result.SequenceNumber)
	require.Equal(t, 1, activity.count())
	duplicate, err := b.SendQueueMessage(q, input)
	require.NoError(t, err)
	require.Equal(t, result, duplicate)
	require.Equal(t, 1, activity.count(), "native FIFO duplicates do not create another message lifetime")

	for _, jsonProtocol := range []bool{false, true} {
		t.Run(fmt.Sprintf("error-json-%v", jsonProtocol), func(t *testing.T) {
			response := httptest.NewRecorder()
			NewHandler(b).writeSQSError(response, newSQSError("ServiceUnavailable", "ownership unavailable"), jsonProtocol)
			require.Equal(t, http.StatusServiceUnavailable, response.Code)
			require.Contains(t, response.Body.String(), "ServiceUnavailable")
		})
	}
}

func TestDevSQSCustodyDelayVisibilityRetryAndCurrentReceiptSettlement(t *testing.T) {
	b, activity := custodyBroker(t)
	q := b.CreateQueue("retry-custody", time.Hour, 0)
	custodyRegister(t, b, q)
	delay := 600
	result, err := b.SendQueueMessage(q, QueueMessageInput{Body: "retry body", DelaySeconds: &delay})
	require.NoError(t, err)
	custodyPending(t, b, q, true)
	require.Equal(t, 1, activity.count())
	require.Empty(t, custodyReceive(t, b, q, 1).Records)
	q.mu.Lock()
	q.messages[0].VisibleAt = time.Now().Add(-time.Second)
	notifyQueueLocked(q)
	q.mu.Unlock()
	first := custodyReceive(t, b, q, 1).Records[0]
	require.Equal(t, result.MessageID, first.MessageID)
	require.Equal(t, "1", first.Attributes["ApproximateReceiveCount"])
	custodyPending(t, b, q, true)
	require.True(t, b.ExtendMessageVisibility(q, first.ReceiptHandle, 0))
	second := custodyReceive(t, b, q, 1).Records[0]
	require.Equal(t, result.MessageID, second.MessageID)
	require.Equal(t, "2", second.Attributes["ApproximateReceiveCount"])
	require.NotEqual(t, first.ReceiptHandle, second.ReceiptHandle)
	require.False(t, b.DeleteMessage(q, first.ReceiptHandle))
	require.Equal(t, 1, activity.count(), "stale receipt cannot release a later retry")
	changed := custodyPending(t, b, q, true)
	require.True(t, b.DeleteMessage(q, second.ReceiptHandle))
	select {
	case <-changed:
	default:
		t.Fatal("settlement failed to notify parked continuations")
	}
	custodyPending(t, b, q, false)
	require.Zero(t, activity.count())
}

func TestDevSQSCustodyEmptyContinuationWakesOnPeerSettlement(t *testing.T) {
	b, activity := custodyBroker(t)
	q := b.CreateQueue("joined-continuation", time.Hour, 0)
	custodyRegister(t, b, q)
	custodySend(t, b, q, "currently executing")
	record := custodyReceive(t, b, q, 1).Records[0]
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		event, err := b.ReceiveOwnedSQSLambdaEventContext(ctx, q, 1, maxWaitTime, testSQSLambdaPayloadLimit)
		if len(event.Records) != 0 {
			err = errors.New("already executing record was re-leased")
		}
		result <- err
	}()
	require.True(t, b.DeleteMessage(q, record.ReceiptHandle))
	require.NoError(t, <-result, "empty continuation must end without waiting for native long-poll timeout")
	require.Zero(t, activity.count())
}

func TestDevSQSCustodyRemovalCoversNativePurgeDeleteAndRetention(t *testing.T) {
	for _, removal := range []string{"native-purge", "direct-purge", "delete", "retention"} {
		t.Run(removal, func(t *testing.T) {
			b, activity := custodyBroker(t)
			q := b.CreateQueue("removal-custody", time.Hour, 0)
			custodyRegister(t, b, q)
			custodySend(t, b, q, "in-flight")
			require.Len(t, custodyReceive(t, b, q, 1).Records, 1)
			custodySend(t, b, q, "queued")
			require.Equal(t, 2, activity.count())
			changed := custodyPending(t, b, q, true)
			switch removal {
			case "native-purge":
				_, err := NewHandler(b).executeSQS(t.Context(), "PurgeQueue", sqsRequest{QueueURL: q.URL})
				require.Nil(t, err)
			case "direct-purge":
				b.PurgeQueue(q)
			case "delete":
				require.True(t, b.DeleteQueue(q.Name))
			case "retention":
				q.mu.Lock()
				pruneQueueLocked(q, time.Now().Add(q.RetentionPeriod+time.Second))
				q.mu.Unlock()
			}
			require.Zero(t, activity.count())
			select {
			case <-changed:
			default:
				t.Fatal("native removal did not wake custody observers")
			}
			custodyLocked(q, func() {
				require.Empty(t, q.messages)
				require.Empty(t, q.inFlight)
			})
		})
	}
}

func TestDevSQSCustodyRedriveTransfersBeforeSourceRelease(t *testing.T) {
	for _, mappedDLQ := range []bool{false, true} {
		t.Run(fmt.Sprintf("mapped-DLQ-%v", mappedDLQ), func(t *testing.T) {
			b, activity := custodyBroker(t)
			source := b.CreateQueue("source-custody", time.Hour, 0)
			dead := b.CreateQueue("dead-custody", time.Hour, 0)
			custodyRegister(t, b, source)
			if mappedDLQ {
				custodyRegister(t, b, dead)
			}
			require.Nil(t, NewHandler(b).setQueueAttributes(source, map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + dead.ARN + `","maxReceiveCount":1}`}))
			id := custodySend(t, b, source, "failed whole batch")
			record := custodyReceive(t, b, source, 1).Records[0]
			activity.mu.Lock()
			activity.minDuring = 1
			activity.mu.Unlock()
			require.True(t, b.ExtendMessageVisibility(source, record.ReceiptHandle, 0))
			b.RequeueExpired(source)
			custodyPending(t, b, source, false)
			custodyLocked(dead, func() {
				require.Len(t, dead.messages, 1)
				require.Equal(t, id, dead.messages[0].ID)
				require.Equal(t, source.ARN, dead.messages[0].OriginalSourceARN)
			})
			if mappedDLQ {
				require.Equal(t, 1, activity.count())
				activity.mu.Lock()
				minimum := activity.minDuring
				activity.mu.Unlock()
				require.Equal(t, 1, minimum, "redrive must never expose an empty ownership gap")
				deadRecord := custodyReceive(t, b, dead, 1).Records[0]
				require.True(t, b.DeleteMessage(dead, deadRecord.ReceiptHandle))
			} else {
				require.Empty(t, custodyReceive(t, b, dead, 1).Records, "unmapped dead-letter data remains paused")
			}
			require.Zero(t, activity.count())
		})
	}
}

func TestDevSQSCustodyFailedTransferPreservesNativeReceiptAndDestination(t *testing.T) {
	b, activity := custodyBroker(t)
	source := b.CreateQueue("transfer-source", time.Hour, 0)
	dead := b.CreateQueue("transfer-dead", time.Hour, 0)
	custodyRegister(t, b, source)
	custodyRegister(t, b, dead)
	custodySend(t, b, source, "failed destination admission")
	record := custodyReceive(t, b, source, 1).Records[0]
	activity.refuseNext()
	require.False(t, b.MoveMessage(source, dead, record.ReceiptHandle))
	require.Equal(t, 1, activity.count())
	custodyLocked(source, func() {
		require.Contains(t, source.inFlight, record.ReceiptHandle)
		require.Equal(t, record.ReceiptHandle, source.inFlight[record.ReceiptHandle].ReceiptHandle)
	})
	custodyLocked(dead, func() {
		require.Empty(t, dead.messages)
		require.Zero(t, dead.sequence)
	})
	require.True(t, b.MoveMessage(source, dead, record.ReceiptHandle))
	require.Equal(t, 1, activity.count())
	b.PurgeQueue(dead)
	require.Zero(t, activity.count())
}

func TestDevSQSCustodyMessageMoveTaskTransfersNewNativeIdentity(t *testing.T) {
	b, activity := custodyBroker(t)
	source := b.CreateQueue("move-task-source", time.Hour, 0)
	destination := b.CreateQueue("move-task-destination", time.Hour, 0)
	custodyRegister(t, b, source)
	custodyRegister(t, b, destination)
	oldID := custodySend(t, b, source, "redrive task body")
	task := &sqsMoveTask{SourceQueue: source, SourceARN: source.ARN, DestinationARN: destination.ARN}
	state := b.sqsState()
	state.mu.Lock()
	moved, remaining, reason := b.sqsMoveOneLocked(task, source, time.Now())
	state.mu.Unlock()
	require.True(t, moved)
	require.True(t, remaining)
	require.Empty(t, reason)
	require.Equal(t, 1, activity.count())
	custodyPending(t, b, source, false)
	record := custodyReceive(t, b, destination, 1).Records[0]
	require.NotEqual(t, oldID, record.MessageID, "native message move tasks assign a new identity")
	require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
	require.True(t, b.DeleteMessage(destination, record.ReceiptHandle))
	require.Zero(t, activity.count())
}

func TestDevSQSCustodyQueueInstanceIsolationAndCleanupDescendants(t *testing.T) {
	b, activity := custodyBroker(t)
	owned := b.CreateQueue("exact-queue", time.Hour, 0)
	sentinel := b.CreateQueue("sentinel-queue", time.Hour, 0)
	sentinelID := custodySend(t, b, sentinel, "unrelated fixture")
	oldRelease := custodyRegister(t, b, owned)
	custodySend(t, b, owned, "old fixture")
	require.Equal(t, 1, activity.count())
	require.True(t, b.DeleteQueue(owned.Name))
	replacement := b.CreateQueue(owned.Name, time.Hour, 0)
	custodyRegister(t, b, replacement)
	changed := custodyPending(t, b, replacement, false)
	id := custodySend(t, b, replacement, "cleanup-produced descendant")
	select {
	case <-changed:
	default:
		t.Fatal("cleanup enqueue failed to wake parked native mapping")
	}
	oldRelease()
	require.Equal(t, 1, activity.count(), "old unregister must never touch replacement queue ownership")
	_, _, err := b.SQSLambdaCustodyState(owned)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	_, err = b.RegisterSQSLambdaCustody(owned)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	other := newTestBroker().CreateQueue(replacement.Name, time.Hour, 0)
	_, err = b.RegisterSQSLambdaCustody(other)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	event, err := b.ReceiveOwnedSQSLambdaEventContext(t.Context(), owned, 1, 0, testSQSLambdaPayloadLimit)
	require.ErrorIs(t, err, ErrQueueUnavailable)
	require.Empty(t, event.Records)
	record := custodyReceive(t, b, replacement, 1).Records[0]
	require.Equal(t, id, record.MessageID)
	require.True(t, b.DeleteMessage(replacement, record.ReceiptHandle))
	require.Zero(t, activity.count())
	require.Empty(t, custodyReceive(t, b, sentinel, 1).Records)
	custodyLocked(sentinel, func() {
		require.Len(t, sentinel.messages, 1)
		require.Equal(t, sentinelID, sentinel.messages[0].ID)
		require.Zero(t, sentinel.messages[0].ReceiveCount)
		require.Empty(t, sentinel.messages[0].ReceiptHandle)
	})
}

func TestDevSQSCustodyUsesCanonicalByteAdmissionAndFIFOBlocking(t *testing.T) {
	b, activity := custodyBroker(t)
	q, failure := b.createSQSQueue("byte-custody.fifo", map[string]string{"FifoQueue": "true"}, nil)
	require.Nil(t, failure)
	custodyRegister(t, b, q)
	firstID := custodySend(t, b, q, strings.Repeat("x", 2048))
	secondID := custodySend(t, b, q, "same-group tail")
	event, err := b.ReceiveOwnedSQSLambdaEventContext(t.Context(), q, 10, 0, 1024)
	require.ErrorIs(t, err, ErrSQSLambdaPayloadTooLarge)
	require.Empty(t, event.Records)
	require.Equal(t, 2, activity.count())
	custodyLocked(q, func() {
		require.Empty(t, q.inFlight)
		require.Empty(t, q.receipts)
		for _, message := range q.messages {
			require.Zero(t, message.ReceiveCount)
			require.Empty(t, message.ReceiptHandle)
		}
		// A deliberately unowned group head proves the native FIFO selector applies
		// custody before committing any lease; it cannot skip to an owned group tail.
		releaseDevSQSMessageLocked(q, firstID)
	})
	require.Empty(t, custodyReceive(t, b, q, 10).Records)
	custodyLocked(q, func() {
		require.Empty(t, q.inFlight)
		require.Empty(t, q.receipts)
		require.Equal(t, secondID, q.messages[1].ID)
	})
}

func TestDevSQSCustodyContinuationRetentionTimer(t *testing.T) {
	b, activity := custodyBroker(t)
	q := b.CreateQueue("retention-clock-custody", time.Hour, 100*time.Millisecond)
	custodyRegister(t, b, q)
	custodySend(t, b, q, "invisible until retention")
	require.Len(t, custodyReceive(t, b, q, 1).Records, 1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	event, err := b.ReceiveOwnedSQSLambdaEventContext(ctx, q, 1, maxWaitTime, testSQSLambdaPayloadLimit)
	require.NoError(t, err, "custody expiry must not depend on the five-second maintenance ticker")
	require.Empty(t, event.Records)
	require.Zero(t, activity.count())
	custodyPending(t, b, q, false)
}

func TestDevSQSCustodyWireAdmissionFailureIsNativeErrorEnvelope(t *testing.T) {
	b, activity := custodyBroker(t)
	q := b.CreateQueue("wire-custody", time.Hour, 0)
	custodyRegister(t, b, q)
	for _, protocol := range []string{"json", "query"} {
		t.Run(protocol, func(t *testing.T) {
			activity.refuseNext()
			response := httptest.NewRecorder()
			handler := NewHandler(b)
			if protocol == "json" {
				payload, err := json.Marshal(map[string]string{"QueueUrl": q.URL, "MessageBody": "not accepted"})
				require.NoError(t, err)
				handler.ServeAction(response, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload))), "SendMessage")
			} else {
				request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=SendMessage&QueueUrl="+q.URL+"&MessageBody=not+accepted"))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				handler.ServeQuery(response, request, "SendMessage")
			}
			require.Equal(t, http.StatusServiceUnavailable, response.Code, response.Body.String())
			require.Contains(t, response.Body.String(), "ServiceUnavailable")
			require.Zero(t, activity.count())
			custodyLocked(q, func() {
				require.Empty(t, q.messages)
			})
		})
	}
}

func TestDevSQSCustodyBatchKeepsOnlyAcceptedMessages(t *testing.T) {
	b, activity := custodyBroker(t)
	q := b.CreateQueue("batch-custody", time.Hour, 0)
	custodyRegister(t, b, q)
	activity.failAt = 2
	result, err := NewHandler(b).executeSQSBatch(q, "SendMessageBatch", []sqsBatchEntry{
		{ID: "one", MessageBody: "accepted-one"},
		{ID: "two", MessageBody: "refused-two"},
		{ID: "three", MessageBody: "accepted-three"},
	})
	require.Nil(t, err)
	require.Len(t, result["Successful"], 2)
	failed := result["Failed"].([]map[string]any)
	require.Len(t, failed, 1)
	require.Equal(t, "two", failed[0]["Id"])
	require.Equal(t, "ServiceUnavailable", failed[0]["Code"])
	require.Equal(t, false, failed[0]["SenderFault"])
	require.Equal(t, 2, activity.count())
	event := custodyReceive(t, b, q, 10)
	require.Len(t, event.Records, 2)
	require.Equal(t, "accepted-one", event.Records[0].Body)
	require.Equal(t, "accepted-three", event.Records[1].Body)
	for _, record := range event.Records {
		require.True(t, b.DeleteMessage(q, record.ReceiptHandle))
	}
	require.Zero(t, activity.count())
}

func TestDevSQSCustodyRealBarrierHoldsRetryAndCleanupDescendants(t *testing.T) {
	owner := devquiescence.New()
	b := newTestBroker()
	require.NoError(t, b.SetDevActivity(owner))
	q := b.CreateQueue("barrier-custody", time.Hour, 0)
	custodyRegister(t, b, q)
	custodySend(t, b, q, "accepted batch")
	first := custodyReceive(t, b, q, 1).Records[0]
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	blocked, err := owner.Quiesce(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, blocked.FixtureSafe)
	require.Equal(t, 1, blocked.WorkCount, "invisible failed batch is still accepted future work")
	_, _, err = owner.BeginSource("sqs_mapping", "unrelated-root")
	require.ErrorIs(t, err, devquiescence.ErrFenced)
	require.True(t, b.ExtendMessageVisibility(q, first.ReceiptHandle, 0))
	second := custodyReceive(t, b, q, 1).Records[0]
	require.Equal(t, "2", second.Attributes["ApproximateReceiveCount"])
	require.False(t, owner.Snapshot().FixtureSafe)
	require.True(t, b.DeleteMessage(q, second.ReceiptHandle))
	held, err := owner.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)

	cleanup, err := owner.BeginCleanup(held.Generation, "fixture_cleanup", "exact-fixture")
	require.NoError(t, err)
	custodySend(t, b, q, "cleanup descendant")
	cleanup(nil)
	require.Equal(t, 1, owner.Snapshot().WorkCount)
	require.False(t, owner.Snapshot().FixtureSafe, "cleanup return cannot hide a queued descendant")
	record := custodyReceive(t, b, q, 1).Records[0]
	require.Equal(t, "cleanup descendant", record.Body)
	require.True(t, b.DeleteMessage(q, record.ReceiptHandle))
	held, err = owner.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	resumed, err := owner.Resume(held.Generation)
	require.NoError(t, err)
	require.Equal(t, held.Generation+1, resumed.Generation)
	custodySend(t, b, q, "next suite same owner")
	require.Equal(t, 1, owner.Snapshot().WorkCount)
	b.PurgeQueue(q)
}
