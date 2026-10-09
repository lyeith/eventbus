package messaging

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSNSFIFOExpiryKeepsNativeGroupScopeAndDuplicateCapture(t *testing.T) {
	for _, scope := range []string{"Topic", "MessageGroup"} {
		t.Run(scope, func(t *testing.T) {
			broker := newTestBroker()
			topic, err := broker.CreateTopicWithAttributes("prepared-expiry.fifo", map[string]string{
				"FifoTopic": "true", "FifoThroughputScope": scope,
			}, nil, "")
			require.NoError(t, err)
			var capture bytes.Buffer
			broker.SetSNSCapture(NewSNSCapture(&capture))
			input := SNSPublishInput{TopicARN: topic.ARN, Message: "body", MessageGroupID: "east", MessageDeduplicationID: "same"}
			first, err := broker.PublishSNS(input)
			require.NoError(t, err)
			topic.publishMu.Lock()
			originalBound := topic.dedupExpiry
			topic.publishMu.Unlock()
			require.WithinDuration(t, time.Now().Add(5*time.Minute), originalBound, time.Second)

			input.MessageGroupID = "west"
			second, err := broker.PublishSNS(input)
			require.NoError(t, err)
			sequence := uint64(1)
			if scope == "MessageGroup" {
				require.NotEqual(t, first.MessageID, second.MessageID)
				require.Equal(t, "2", second.SequenceNumber)
				sequence++
			} else {
				require.Equal(t, first, second)
			}
			duplicate, err := broker.PublishSNS(input)
			require.NoError(t, err)
			require.Equal(t, second, duplicate)
			topic.publishMu.Lock()
			require.Equal(t, originalBound, topic.dedupExpiry, "duplicates cannot extend the five-minute window")
			expired := time.Now().Add(-time.Second)
			for key, entry := range topic.dedup {
				entry.Expires = expired
				topic.dedup[key] = entry
			}
			topic.dedupExpiry = time.Time{}
			topic.publishMu.Unlock()

			afterExpiry, err := broker.PublishSNS(input)
			require.NoError(t, err)
			require.NotEqual(t, second.MessageID, afterExpiry.MessageID)
			require.Equal(t, fmt.Sprint(sequence+1), afterExpiry.SequenceNumber)
			topic.publishMu.Lock()
			require.Len(t, topic.dedup, 1)
			require.True(t, topic.dedupExpiry.After(time.Now()))
			topic.publishMu.Unlock()
			records := snsLambdaCaptureRecords(t, &capture)
			require.Len(t, records, 4)
			require.Equal(t, true, records[2].Details["deduplicated"])
			require.Equal(t, second.MessageID, records[2].MessageID)
			require.Equal(t, afterExpiry.MessageID, records[3].MessageID)
			require.Nil(t, records[3].Details)
		})
	}
}

func TestMessagingDedupExpiryConservativeBoundAndInclusiveExpiry(t *testing.T) {
	now := time.Now()
	entries := map[string]snsDeduplication{
		"expired": {Expires: now},
		"live":    {Expires: now.Add(time.Minute)},
	}
	var bound time.Time
	expires := func(entry snsDeduplication) time.Time { return entry.Expires }
	pruneDedupEntries(entries, &bound, now, expires)
	require.NotContains(t, entries, "expired")
	require.Equal(t, now.Add(time.Minute), bound)
	entries["live"] = snsDeduplication{Expires: now.Add(5 * time.Minute)}
	lowerDedupExpiry(&bound, entries["live"].Expires)
	require.Equal(t, now.Add(time.Minute), bound, "replacement can leave an early bound")
	entries["earlier"] = snsDeduplication{Expires: now.Add(30 * time.Second)}
	lowerDedupExpiry(&bound, entries["earlier"].Expires)
	require.Equal(t, now.Add(30*time.Second), bound)
	pruneDedupEntries(entries, &bound, now.Add(30*time.Second-time.Nanosecond), expires)
	require.Contains(t, entries, "earlier")
	pruneDedupEntries(entries, &bound, now.Add(30*time.Second), expires)
	require.NotContains(t, entries, "earlier")
	require.Equal(t, now.Add(5*time.Minute), bound)
	pruneDedupEntries(entries, &bound, now.Add(5*time.Minute), expires)
	require.Empty(t, entries)
	require.True(t, bound.IsZero())
}
