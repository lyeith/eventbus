//go:build performance && (linux || darwin)

package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/stretchr/testify/require"
)

// The fixture uses real sends and leases. Controlled expiry changes only test
// timestamps under the queue lock; it avoids waiting for five-minute dedup TTLs.
func TestPerformanceSQSFollowupSendExpiryDelete(t *testing.T) {
	for _, depth := range []int{1000, 10000} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			broker := NewBroker("us-east-1", "123456789012", 0)
			queue := queuePerformanceRegressionFixture(t, broker, "followup.fifo")
			t.Cleanup(func() { broker.DeleteQueue(queue.Name) })
			setup := time.Now()
			for index := range depth {
				performanceSendQueue(t, broker, queue, index, 100)
				if index%100 == 0 {
					performanceMessagingBudget(t, setup, 20*time.Second, "FIFO send setup")
				}
			}
			testperf.Report(t, fmt.Sprintf("fifo_depth_%d", depth), "setup_ms", []float64{performanceMessagingMS(setup)})
			var sendMS, duplicateMS, strictMS, nativeBatchMS, pruneMS []float64
			next := depth
			for range performanceMessagingSamples {
				input := performanceQueueInput(next, 100)
				next++
				started := time.Now()
				sent, err := broker.SendQueueMessage(queue, input)
				sendMS = append(sendMS, performanceMessagingMS(started))
				require.NoError(t, err)
				started = time.Now()
				duplicate, err := broker.SendQueueMessage(queue, input)
				duplicateMS = append(duplicateMS, performanceMessagingMS(started))
				require.NoError(t, err)
				require.Equal(t, sent, duplicate)
				for _, mode := range []string{"strict", "native_batch"} {
					messages, err := broker.ReceiveMessagesContext(context.Background(), queue, 10, 0)
					require.NoError(t, err)
					require.Len(t, messages, 10)
					entries := make([]sqsBatchEntry, len(messages))
					for index, message := range messages {
						entries[index] = sqsBatchEntry{ID: fmt.Sprint(index), ReceiptHandle: message.ReceiptHandle}
					}
					results := make([]bool, len(messages))
					var response map[string]any
					var failure *sqsError
					started = time.Now()
					if mode == "strict" {
						for index, message := range messages {
							results[index] = broker.DeleteMessage(queue, message.ReceiptHandle)
						}
						strictMS = append(strictMS, performanceMessagingMS(started))
					} else {
						response, failure = NewHandler(broker).executeSQSBatch(queue, "DeleteMessageBatch", entries)
						nativeBatchMS = append(nativeBatchMS, performanceMessagingMS(started))
					}
					require.Nil(t, failure)
					if mode == "strict" {
						for _, result := range results {
							require.True(t, result)
						}
					} else {
						require.Len(t, response["Successful"], 10)
						require.Empty(t, response["Failed"])
					}
					for _, message := range messages {
						outcome, err := broker.EvaluateSQSLambdaReceiptContext(context.Background(), queue, message.ReceiptHandle, false)
						require.NoError(t, err)
						require.Equal(t, SQSLambdaReceiptNativeSettled, outcome)
					}
					replenish := 10
					if mode == "native_batch" {
						replenish--
					} // Unique send above supplies this last slot.
					for range replenish {
						performanceSendQueue(t, broker, queue, next, 100)
						next++
					}
				}
				started = time.Now()
				requeued := broker.RequeueExpired(queue)
				pruneMS = append(pruneMS, performanceMessagingMS(started))
				require.Zero(t, requeued)
				performanceMessagingBudget(t, setup, 30*time.Second, "FIFO sends and native deletions")
			}
			name := fmt.Sprintf("fifo_depth_%d", depth)
			testperf.Report(t, name, "unique_send_ms", sendMS)
			testperf.Report(t, name, "duplicate_send_ms", duplicateMS)
			testperf.Report(t, name, "strict_delete_10_ms", strictMS)
			testperf.Report(t, name, "native_delete_batch_10_ms", nativeBatchMS)
			testperf.Report(t, name, "unexpired_prune_ms", pruneMS)
			// Copy native-issued identifier history into a fresh initialized queue,
			// then expire its test timestamps. No payload/lease is synthesized.
			// A fresh queue also exercises conservative unknown expiry bounds.
			expiryQueue := queuePerformanceRegressionFixture(t, broker, "followup-expiry.fifo")
			t.Cleanup(func() { broker.DeleteQueue(expiryQueue.Name) })
			expired := time.Now().Add(-time.Second)
			queue.mu.Lock()
			history := make(map[string]sqsDedupEntry, len(queue.dedup))
			for key, entry := range queue.dedup {
				entry.Expires = expired
				history[key] = entry
			}
			queue.mu.Unlock()
			expiryQueue.mu.Lock()
			expiryQueue.dedup = history
			expiryQueue.mu.Unlock()
			started := time.Now()
			_, err := broker.SendQueueMessage(expiryQueue, performanceQueueInput(next, 100))
			dedupExpiryMS := performanceMessagingMS(started)
			require.NoError(t, err)
			expiryQueue.mu.Lock()
			remainingDedup := len(expiryQueue.dedup)
			expiryQueue.mu.Unlock()
			require.Equal(t, 1, remainingDedup)
			testperf.Report(t, name, "dedup_expiry_send_ms", []float64{dedupExpiryMS})
			queue.mu.Lock()
			for _, message := range queue.messages {
				message.SentTimestamp = time.Now().Add(-queue.RetentionPeriod - time.Second)
			}
			queue.mu.Unlock()
			started = time.Now()
			requeued := broker.RequeueExpired(queue)
			retentionMS := performanceMessagingMS(started)
			require.Zero(t, requeued)
			waiting, flight := broker.QueueDepth(queue)
			require.Zero(t, waiting)
			require.Zero(t, flight)
			testperf.Report(t, name, "retention_expiry_ms", []float64{retentionMS})
		})
	}
}

func TestPerformanceSQSFollowupBinaryProjection(t *testing.T) {
	for _, mode := range []string{"native_projection", "lambda_admission"} {
		t.Run(mode, func(t *testing.T) {
			broker := NewBroker("us-east-1", "123456789012", 0)
			queue := queuePerformanceRegressionFixture(t, broker, "followup-binary")
			t.Cleanup(func() { broker.DeleteQueue(queue.Name) })
			binary := bytes.Repeat([]byte{0, 1, 255}, 64*1024/3)
			var wallMS, allocationBytes, allocationCount []float64
			for sample := range performanceMessagingSamples {
				ids := make([]string, 10)
				for index := range 10 {
					input := performanceQueueInput(sample*10+index, 0)
					input.Attributes["binary"] = MessageAttribute{DataType: "Binary", BinaryValue: binary}
					sent, err := broker.SendQueueMessage(queue, input)
					require.NoError(t, err)
					ids[index] = sent.MessageID
				}
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				var event sqsevent.Event
				var payload []byte
				var err, marshalErr error
				if mode == "native_projection" {
					var messages []*Message
					messages, err = broker.ReceiveMessagesContext(context.Background(), queue, 10, 0)
					if err == nil {
						event = BuildSQSLambdaEvent(messages, queue.ARN)
					}
					payload, marshalErr = json.Marshal(event)
				} else {
					var batch sqsevent.Batch
					batch, err = broker.ReceiveSQSLambdaBatchContext(context.Background(), queue, 10, 0, 6<<20)
					event = sqsevent.Event{Records: batch.Records}
					// Match the real mapping boundary: lease/admission plus
					// mutable invocation bytes, without encoding records again.
					payload = []byte(batch.Payload)
				}
				wallMS = append(wallMS, performanceMessagingMS(started))
				runtime.ReadMemStats(&after)
				allocationBytes = append(allocationBytes, float64(after.TotalAlloc-before.TotalAlloc))
				allocationCount = append(allocationCount, float64(after.Mallocs-before.Mallocs))
				require.NoError(t, err)
				require.NoError(t, marshalErr)
				require.LessOrEqual(t, len(payload), 6<<20)
				expected, expectedErr := json.Marshal(event)
				require.NoError(t, expectedErr)
				require.Equal(t, expected, payload, "dispatch must use the exact admitted native wire")
				require.Len(t, event.Records, 10)
				for index, record := range event.Records {
					require.Equal(t, ids[index], record.MessageID)
					require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
					require.Equal(t, binary, record.MessageAttributes["binary"].BinaryValue)
					record.MessageAttributes["binary"].BinaryValue[0] = 42
					queue.mu.Lock()
					original := queue.inFlight[record.ReceiptHandle].Attributes["binary"].BinaryValue[0]
					queue.mu.Unlock()
					require.Zero(t, original, "wire binary snapshots must not alias native queue state")
					require.True(t, broker.DeleteMessage(queue, record.ReceiptHandle))
				}
			}
			testperf.Report(t, mode+"_binary_64k_batch_10", "receive_projection_marshal_ms", wallMS)
			testperf.Report(t, mode+"_binary_64k_batch_10", "bytes_per_batch", allocationBytes)
			testperf.Report(t, mode+"_binary_64k_batch_10", "allocs_per_batch", allocationCount)
		})
	}
}
