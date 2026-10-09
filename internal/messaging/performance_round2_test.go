//go:build performance && (linux || darwin)

package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/stretchr/testify/require"
)

// This measures actual SNS validation, per-subscription filtering, envelope
// projection and SQS admission. Capture is disabled to isolate CPU work; no
// external transport, runtime or application handler is substituted. Every
// accepted native message is received, verified and strictly settled outside
// timing, so queue depth does not grow between samples.
func TestPerformanceRound2SNSFiltering(t *testing.T) {
	for _, subscribers := range []int{10, 100} {
		for _, payloadBytes := range []int{1024, 64 << 10} {
			for _, mode := range []string{"no_filter", "body_all", "body_half", "attributes_all"} {
				t.Run(fmt.Sprintf("%s_subscribers_%d_body_%d", mode, subscribers, payloadBytes), func(t *testing.T) {
					broker := NewBroker("us-east-1", "123456789012", 0)
					topic := broker.CreateTopic("round2-filtering")
					require.NotNil(t, topic)
					t.Cleanup(func() { broker.DeleteTopic(topic.ARN) })
					queues := make([]*Queue, subscribers)
					for index := range queues {
						queue := broker.CreateQueue(fmt.Sprintf("round2-filtering-%d", index), time.Hour, 0)
						queues[index] = queue
						t.Cleanup(func() { broker.DeleteQueue(queue.Name) })
						attributes := map[string]string{}
						switch mode {
						case "body_all", "body_half":
							tenant := "east"
							if mode == "body_half" && index%2 != 0 {
								tenant = "west"
							}
							attributes["FilterPolicy"] = fmt.Sprintf("{\"tenant\":[%q],\"amount\":[{\"numeric\":[\">\",1]}]}", tenant)
							attributes["FilterPolicyScope"] = "MessageBody"
						case "attributes_all":
							attributes["FilterPolicy"] = `{"tags":["live"],"amount":[{"numeric":[">",1]}]}`
							attributes["FilterPolicyScope"] = "MessageAttributes"
						}
						_, err := broker.subscribeSNS(topic.ARN, "sqs", queue.ARN, attributes, "")
						require.NoError(t, err)
					}
					encoded, err := json.Marshal(map[string]any{"tenant": "east", "amount": 1.5, "payload": strings.Repeat("x", payloadBytes)})
					require.NoError(t, err)
					body := string(encoded)
					input := SNSPublishInput{TopicARN: topic.ARN, Message: body, Attributes: map[string]MessageAttribute{
						"tags":   {DataType: "String.Array", StringValue: `["live","east"]`},
						"amount": {DataType: "Number", StringValue: "1.5"},
					}}
					var wall, allocBytes, allocCount []float64
					for sample := range performanceMessagingSamples {
						input.RequestID = fmt.Sprintf("filtering-%d", sample)
						var before, after runtime.MemStats
						runtime.ReadMemStats(&before)
						started := time.Now()
						result, err := broker.PublishSNS(input)
						wall = append(wall, performanceMessagingMS(started))
						runtime.ReadMemStats(&after)
						allocBytes = append(allocBytes, float64(after.TotalAlloc-before.TotalAlloc))
						allocCount = append(allocCount, float64(after.Mallocs-before.Mallocs))
						require.NoError(t, err)
						require.NotEmpty(t, result.MessageID)
						for index, queue := range queues {
							waiting, flight := broker.QueueDepth(queue)
							expected := 1
							if mode == "body_half" && index%2 != 0 {
								expected = 0
							}
							require.Equal(t, expected, waiting)
							require.Zero(t, flight)
							if expected == 0 {
								continue
							}
							messages, err := broker.ReceiveMessagesContext(context.Background(), queue, 1, 0)
							require.NoError(t, err)
							require.Len(t, messages, 1)
							var envelope snsEnvelope
							require.NoError(t, json.Unmarshal([]byte(messages[0].Body), &envelope))
							require.Equal(t, result.MessageID, envelope.MessageID)
							require.Equal(t, topic.ARN, envelope.TopicArn)
							require.Equal(t, body, envelope.Message)
							require.Equal(t, "1.5", envelope.MessageAttributes["amount"].Value)
							require.Equal(t, input.Attributes["tags"].StringValue, envelope.MessageAttributes["tags"].Value)
							require.True(t, broker.DeleteMessage(queue, messages[0].ReceiptHandle))
							waiting, flight = broker.QueueDepth(queue)
							require.Zero(t, waiting)
							require.Zero(t, flight)
						}
					}
					testperf.Report(t, t.Name(), "publish_ms", wall)
					testperf.Report(t, t.Name(), "allocated_bytes", allocBytes)
					testperf.Report(t, t.Name(), "allocations", allocCount)
				})
			}
		}
	}
}

// FIFO setup uses native publications without subscribers, so its five-minute
// dedup history grows independently of queue/capture cost. Expiry only changes
// native-issued test entry timestamps under publishMu; it never invents IDs.
func TestPerformanceRound2SNSFIFODedup(t *testing.T) {
	for _, depth := range []int{0, 1000, 10000} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			broker := NewBroker("us-east-1", "123456789012", 0)
			topic := broker.CreateTopic("round2-dedup.fifo")
			require.NotNil(t, topic)
			t.Cleanup(func() { broker.DeleteTopic(topic.ARN) })
			input := SNSPublishInput{TopicARN: topic.ARN, Message: "native-dedup", MessageGroupID: "east"}
			setup := time.Now()
			for index := range depth {
				input.MessageDeduplicationID = fmt.Sprintf("dedup-%d", index)
				result, err := broker.PublishSNS(input)
				require.NoError(t, err)
				require.NotEmpty(t, result.MessageID)
				if index%100 == 0 {
					performanceMessagingBudget(t, setup, 30*time.Second, "native SNS dedup setup")
				}
			}
			testperf.Report(t, t.Name(), "setup_ms", []float64{performanceMessagingMS(setup)})
			var unique, duplicate []float64
			for sample := range performanceMessagingSamples {
				input.MessageDeduplicationID = fmt.Sprintf("dedup-%d", depth+sample)
				started := time.Now()
				result, err := broker.PublishSNS(input)
				unique = append(unique, performanceMessagingMS(started))
				require.NoError(t, err)
				require.Equal(t, fmt.Sprint(depth+sample+1), result.SequenceNumber)
				started = time.Now()
				replayed, err := broker.PublishSNS(input)
				duplicate = append(duplicate, performanceMessagingMS(started))
				require.NoError(t, err)
				require.Equal(t, result, replayed)
			}
			topic.publishMu.Lock()
			historyCount := len(topic.dedup)
			expired := time.Now().Add(-time.Second)
			for key, entry := range topic.dedup {
				entry.Expires = expired
				topic.dedup[key] = entry
			}
			topic.dedupExpiry = time.Time{} // Test mutation invalidates the conservative bound.
			topic.publishMu.Unlock()
			require.Equal(t, depth+performanceMessagingSamples, historyCount)
			input.MessageDeduplicationID = "after-expiry"
			started := time.Now()
			result, err := broker.PublishSNS(input)
			expiryMS := performanceMessagingMS(started)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprint(depth+performanceMessagingSamples+1), result.SequenceNumber)
			topic.publishMu.Lock()
			remaining := len(topic.dedup)
			topic.publishMu.Unlock()
			require.Equal(t, 1, remaining)
			testperf.Report(t, t.Name(), "unique_publish_ms", unique)
			testperf.Report(t, t.Name(), "duplicate_publish_ms", duplicate)
			testperf.Report(t, t.Name(), "due_expiry_publish_ms", []float64{expiryMS})
		})
	}
}
