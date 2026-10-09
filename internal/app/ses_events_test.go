package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/stretchr/testify/require"
)

func TestSESEventPublisherUsesNativeSNSFanout(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 4100)
	// A real durable sink is part of SNS acceptance even for queue delivery.
	path := t.TempDir() + "/sns.jsonl"
	capture, err := messaging.OpenSNSCapture(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, capture.Close()) })
	broker.SetSNSCapture(capture)
	topic := broker.CreateTopic("email-events")
	queue := broker.CreateQueue("email-events", 0, 0)
	_, err = broker.Subscribe(topic.ARN, "sqs", queue.ARN, nil)
	require.NoError(t, err)
	publisher := sesSNSPublisher{broker}
	require.NoError(t, publisher.ValidateTopic(topic.ARN))
	require.Error(t, publisher.ValidateTopic(topic.ARN+"-missing"))
	fifo := broker.CreateTopic("email-events.fifo")
	require.Error(t, publisher.ValidateTopic(fifo.ARN))

	message := `{"eventType":"Send","mail":{"messageId":"actual-ses-id"}}`
	require.NoError(t, publisher.PublishEvent(t.Context(), topic.ARN, message, "actual-ses-request"))
	messages := broker.ReceiveMessages(queue, 1, 0)
	require.Len(t, messages, 1)
	var envelope struct{ Type, TopicArn, Message string }
	require.NoError(t, json.Unmarshal([]byte(messages[0].Body), &envelope))
	require.Equal(t, "Notification", envelope.Type)
	require.Equal(t, topic.ARN, envelope.TopicArn)
	require.Equal(t, message, envelope.Message)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, publisher.PublishEvent(ctx, topic.ARN, message, "canceled"), context.Canceled)
	require.Empty(t, broker.ReceiveMessages(queue, 1, 0))
}

type devSESOutcomeProbe struct{ calls int }

func (probe *devSESOutcomeProbe) ServeDevOutcome(w http.ResponseWriter, r *http.Request) {
	probe.calls++
	w.WriteHeader(http.StatusAccepted)
}

func TestDevSESOutcomeRouteIsExactAndSeparate(t *testing.T) {
	outcomes := &devSESOutcomeProbe{}
	native := 0
	router := withDevSESOutcomes(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		native++
		w.WriteHeader(http.StatusNoContent)
	}), outcomes)
	for _, path := range []string{ses.DevOutcomePath, "/v2/email/outbound-emails", ses.DevOutcomePath + "/extra"} {
		request := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		router.ServeHTTP(httptest.NewRecorder(), request)
	}
	require.Equal(t, 1, outcomes.calls)
	require.Equal(t, 2, native)
}
