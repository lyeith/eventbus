package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type retryableSNSFixtureError struct{}

func (retryableSNSFixtureError) Error() string   { return "fixture admission throttled" }
func (retryableSNSFixtureError) Retryable() bool { return true }

type snsRetryLambda struct {
	invoke func(context.Context, []byte) (string, error)
}

func (snsRetryLambda) ValidateLambdaTarget(context.Context, string) error { return nil }
func (f snsRetryLambda) AdmitSNSLambda(ctx context.Context, _ string, payload []byte) (string, error) {
	return f.invoke(ctx, payload)
}

func TestSNSRetryKeepsOriginalPublicationAndHonorsTerminalDLQ(t *testing.T) {
	for _, succeeds := range []bool{true, false} {
		t.Run(map[bool]string{true: "recover", false: "exhaust"}[succeeds], func(t *testing.T) {
			b := newTestBroker()
			require.NoError(t, b.SetSNSRetryOptions(SNSRetryOptions{MaxAttempts: 3}))
			var capture bytes.Buffer
			b.SetSNSCapture(NewSNSCapture(&capture))
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				require.NoError(t, b.CloseSNSDeliveries(ctx))
			})
			var payloads [][]byte
			b.SetLambdaDelivery(snsRetryLambda{invoke: func(_ context.Context, payload []byte) (string, error) {
				payloads = append(payloads, bytes.Clone(payload))
				if succeeds && len(payloads) == 3 {
					return "native-attempt-3", nil
				}
				return "", retryableSNSFixtureError{}
			}})
			topic := b.CreateTopic("retry-native-admission")
			dlq := b.CreateQueue("retry-native-dlq", 0, 0)
			_, err := b.subscribeSNS(topic.ARN, "lambda", snsTestLambdaARN,
				map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":"` + dlq.ARN + `"}`}, "")
			require.NoError(t, err)
			published, err := b.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "preserve payload", RequestID: "publication"})
			require.NoError(t, err)
			drainCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			require.NoError(t, b.DrainSNSDeliveries(drainCtx))
			require.Len(t, payloads, 3)
			for _, p := range payloads {
				require.Equal(t, payloads[0], p)
			}
			require.Contains(t, string(payloads[0]), published.MessageID)
			records := snsLambdaCaptureRecords(t, &capture)
			require.Len(t, records, 2)
			require.Equal(t, published.MessageID, records[1].MessageID)
			messages := b.ReceiveMessages(dlq, 10, 0)
			if succeeds {
				require.Equal(t, "DeliveryAdmission", records[1].Operation)
				require.Equal(t, "native-attempt-3", records[1].Deliveries[0].InvocationRequestID)
				require.Empty(t, messages)
			} else {
				require.Equal(t, "DeliveryFailure", records[1].Operation)
				require.Equal(t, "dead_lettered", records[1].Deliveries[0].Status)
				require.Len(t, messages, 1)
				var envelope snsEnvelope
				require.NoError(t, json.Unmarshal([]byte(messages[0].Body), &envelope))
				require.Equal(t, published.MessageID, envelope.MessageID)
				require.Equal(t, "preserve payload", envelope.Message)
			}
		})
	}
}

type snsRetryCustody struct {
	allowSQS atomic.Bool
	active   atomic.Int64
}

func (f *snsRetryCustody) BeginActivity(kind, _ string) (func(error), error) {
	if kind == "sqs_message" && !f.allowSQS.Load() {
		return nil, errors.New("fixture temporarily refuses SQS custody")
	}
	f.active.Add(1)
	return func(error) { f.active.Add(-1) }, nil
}

func TestSNSRetryFIFOHoldsNextDeliveryBehindRetryAndKeepsCustody(t *testing.T) {
	b := newTestBroker()
	custody := &snsRetryCustody{}
	require.NoError(t, b.SetDevActivity(custody))
	require.NoError(t, b.SetSNSRetryOptions(SNSRetryOptions{Activity: custody, RetryDelayScale: 0.001}))
	t.Cleanup(func() {
		b.DevDiscardPendingSNSDeliveries()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, b.CloseSNSDeliveries(ctx))
	})
	topic := b.CreateTopic("retry-fifo.fifo")
	queue := b.CreateQueue("retry-fifo.fifo", 0, 0)
	queue.mu.Lock()
	queue.Attributes["FifoQueue"] = "true"
	queue.mu.Unlock()
	release, err := b.RegisterSQSLambdaCustody(queue)
	require.NoError(t, err)
	defer release()
	_, err = b.Subscribe(topic.ARN, "sqs", queue.ARN, nil)
	require.NoError(t, err)
	first, err := b.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "first", MessageGroupID: "group", MessageDeduplicationID: "id1"})
	require.NoError(t, err)
	second, err := b.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "second", MessageGroupID: "group", MessageDeduplicationID: "id2"})
	require.NoError(t, err)
	require.False(t, b.SNSDeliveriesIdle())
	require.EqualValues(t, 2, custody.active.Load(), "retain both accepted SNS deliveries while SQS refuses admission")
	require.Empty(t, b.ReceiveMessages(queue, 10, 0))
	custody.allowSQS.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, b.DrainSNSDeliveries(ctx))
	require.True(t, b.SNSDeliveriesIdle())
	messages := b.ReceiveMessages(queue, 10, 0)
	require.Len(t, messages, 2)
	for i, want := range []SNSPublishResult{first, second} {
		var envelope snsEnvelope
		require.NoError(t, json.Unmarshal([]byte(messages[i].Body), &envelope))
		require.Equal(t, want.MessageID, envelope.MessageID)
		b.DeleteMessage(queue, messages[i].ReceiptHandle)
	}
	require.Zero(t, custody.active.Load())
}

func TestSNSRetryCloseDeadlineJoinsActiveAttempt(t *testing.T) {
	b := newTestBroker()
	started := make(chan struct{})
	joined := make(chan struct{})
	var attempts atomic.Int64
	b.SetLambdaDelivery(snsRetryLambda{invoke: func(ctx context.Context, _ []byte) (string, error) {
		if attempts.Add(1) == 1 {
			return "", retryableSNSFixtureError{}
		}
		close(started)
		<-ctx.Done()
		close(joined)
		return "", ctx.Err()
	}})
	topic := b.CreateTopic("retry-join")
	_, err := b.Subscribe(topic.ARN, "lambda", snsTestLambdaARN, nil)
	require.NoError(t, err)
	_, err = b.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "pending"})
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("pending retry did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, b.CloseSNSDeliveries(ctx), context.Canceled)
	select {
	case <-joined:
	default:
		t.Fatal("close returned before joining active attempt")
	}
	require.True(t, b.SNSDeliveriesIdle())
	_, err = b.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "rejected"})
	require.Error(t, err)
	require.EqualValues(t, 2, attempts.Load())
}

func TestSNSRetryCaptureFailureCannotRepeatAcceptedAdmission(t *testing.T) {
	b := newTestBroker()
	writer := &snsAcceptedThenFailWriter{}
	b.SetSNSCapture(NewSNSCapture(writer))
	var calls atomic.Int64
	b.SetLambdaDelivery(snsRetryLambda{invoke: func(context.Context, []byte) (string, error) {
		if calls.Add(1) == 1 {
			return "", retryableSNSFixtureError{}
		}
		return "accepted-after-retry", nil
	}})
	topic := b.CreateTopic("retry-accepted-once")
	_, err := b.Subscribe(topic.ARN, "lambda", snsTestLambdaARN, nil)
	require.NoError(t, err)
	result, err := b.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "accepted"})
	require.NoError(t, err)
	require.NotEmpty(t, result.MessageID)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.ErrorContains(t, b.CloseSNSDeliveries(ctx), "outcome disk failure")
	require.True(t, b.SNSDeliveriesIdle())
	require.EqualValues(t, 2, calls.Load())
	require.Equal(t, 2, writer.calls)
}
