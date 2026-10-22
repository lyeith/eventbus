package ses

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func devTrackingBulkInput() map[string]any {
	return map[string]any{
		"Source": "sender@example.test", "ConfigurationSetName": "tracking", "Template": "welcome",
		"DefaultTemplateData": `{"name":"bulk"}`,
		"Destinations": []any{
			map[string]any{"Destination": map[string]any{"ToAddresses": []string{"older@example.test"}}},
			map[string]any{"Destination": map[string]any{"ToAddresses": []string{"newest@example.test"}}},
		},
	}
}

func TestSESDevTrackingBulkPreparesSourcesThenReleasesEvictedRenderedBodies(t *testing.T) {
	var captured bytes.Buffer
	handler, publisher := newDevTrackingTestHandler(t, NewSESCapture(&captured))
	manager := handler.ses
	// Send events still need old metadata after all insertions and evictions.
	configureEvent(t, manager, "UpdateConfigurationSetEventDestination", "opens", true, "send", "open")
	input := devTrackingBulkInput()
	result, apiErr := manager.sendV1("SendBulkTemplatedEmail", input)
	require.Nil(t, apiErr)
	original, err := json.Marshal(map[string]any{"request": input, "emails": result.Emails, "response": result.Output})
	require.NoError(t, err)
	prepared, apiErr := manager.prepareEvents("v1", input, result.Emails)
	require.Nil(t, apiErr)
	require.Len(t, prepared, 2)
	for _, message := range prepared {
		require.NotNil(t, message.devTracking, "pre-capture work only records an owned source descriptor")
		require.Nil(t, message.accepted.devTracking, "no rendered HTML or minted token before capture commits")
		require.True(t, message.accepted.retainedAt.IsZero())
	}
	require.Empty(t, manager.devTracking.tokens)
	require.Empty(t, manager.accepted)
	require.Empty(t, captured.Bytes())
	require.Empty(t, publisher.snapshot())

	// Use the native owner's exact byte accounting without a 64 MiB test
	// allocation: two individually admissible accounted entries exceed the cap.
	for _, message := range prepared {
		message.accepted.size = acceptedMessageByteLimit/2 + 1
	}
	require.NoError(t, manager.capture.append(map[string]any{"schema_version": 1,
		"request": input, "emails": result.Emails, "outcome": map[string]any{"response": result.Output}}))
	durable := bytes.Clone(captured.Bytes())
	manager.acceptEvents(context.Background(), "bulk-owned", prepared)
	old, newest := prepared[0].accepted, prepared[1].accepted
	require.NotContains(t, manager.accepted, old.id)
	require.Nil(t, old.devTracking, "outstanding Send/prepared metadata cannot keep evicted HTML alive")
	require.NotNil(t, newest.devTracking)
	require.Len(t, manager.devTracking.tokens, 1)
	require.Equal(t, newest.id, manager.devTracking.tokens[newest.devTracking.token])
	require.LessOrEqual(t, manager.acceptedBytes, acceptedMessageByteLimit)
	require.Equal(t, durable, captured.Bytes(), "post-capture tracking cannot rewrite durable native send evidence")
	after, err := json.Marshal(map[string]any{"request": input, "emails": result.Emails, "response": result.Output})
	require.NoError(t, err)
	require.Equal(t, original, after, "local rendering cannot mutate input, derived captures or native response")
	require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, DevTrackingMessagesPrefix+old.id).Code)
	discovery := readDevTracking(t, handler, newest.id)
	require.Equal(t, http.StatusOK, devTrackingRequest(handler, http.MethodGet, discovery.ViewURL).Code)
	events := publisher.snapshot()
	require.Len(t, events, 2, "eviction retires local body ownership, not captured native Send events")
	require.Equal(t, old.id, sesObject(events[0].payload["mail"])["messageId"])
	require.Equal(t, newest.id, sesObject(events[1].payload["mail"])["messageId"])

	// Expiry uses the same retirement path even while a caller still owns the
	// prepared metadata and an HTTP reader has retained its immutable local view.
	heldView := newest.devTracking
	manager.mu.Lock()
	newest.retainedAt = time.Now().Add(-acceptedMessageTTL)
	manager.mu.Unlock()
	require.Equal(t, http.StatusNotFound, devTrackingRequest(handler, http.MethodGet, discovery.TrackingURL).Code)
	require.Nil(t, newest.devTracking)
	require.Empty(t, manager.devTracking.tokens)
	require.Empty(t, manager.accepted)
	require.Zero(t, manager.acceptedBytes)
	require.Contains(t, heldView.html, discovery.TrackingURL, "an already admitted reader keeps its own immutable view")
}

func TestSESDevTrackingConcurrentBulkAndSingleRetentionOrder(t *testing.T) {
	handler, _ := newDevTrackingTestHandler(t, NewSESCapture(io.Discard))
	manager := handler.ses
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Go(func() {
			for index := 0; index < 20; index++ {
				action, input := "SendBulkTemplatedEmail", devTrackingBulkInput()
				if index%2 == 0 {
					action = "SendEmail"
					input = map[string]any{"Source": "sender@example.test", "ConfigurationSetName": "tracking",
						"Destination": map[string]any{"ToAddresses": []string{"single@example.test"}},
						"Message": map[string]any{"Subject": map[string]any{"Data": "single"},
							"Body": map[string]any{"Html": map[string]any{"Data": "<body>" + strings.Repeat("single ", 100) + "</body>"}}}}
				}
				result, apiErr := manager.sendV1(action, input)
				if apiErr != nil {
					t.Errorf("native send: %v", apiErr)
					return
				}
				prepared, apiErr := manager.prepareEvents("v1", input, result.Emails)
				if apiErr != nil {
					t.Errorf("native event preparation: %v", apiErr)
					return
				}
				manager.acceptEvents(context.Background(), "concurrent-owned", prepared)
			}
		})
	}
	workers.Wait()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	require.Len(t, manager.accepted, 120)
	require.Equal(t, len(manager.accepted), manager.acceptedOrder.Len())
	require.Equal(t, len(manager.accepted), len(manager.devTracking.tokens))
	total := 0
	var previous time.Time
	for item := manager.acceptedOrder.Front(); item != nil; item = item.Next() {
		accepted := item.Value.(*acceptedMessage)
		require.False(t, accepted.retainedAt.Before(previous), "expiry owner requires retention-time list order")
		previous = accepted.retainedAt
		require.Same(t, accepted, manager.accepted[accepted.id])
		require.NotNil(t, accepted.devTracking)
		require.Equal(t, accepted.id, manager.devTracking.tokens[accepted.devTracking.token])
		total += accepted.size
	}
	require.Equal(t, total, manager.acceptedBytes)
	require.LessOrEqual(t, total, acceptedMessageByteLimit)
}
