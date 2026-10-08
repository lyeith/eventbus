//go:build performance && (linux || darwin)

package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/lyeith/eventbus/internal/testperf"
	"github.com/stretchr/testify/require"
)

// These controlled observations use actual native sends, receives, receipts and
// SNS destinations. Setup, assertions and replenishment are outside receive or
// publish timings. There is no background maintenance or external delivery.
const performanceMessagingSamples = 20

func performanceMessagingMS(started time.Time) float64 {
	return float64(time.Since(started)) / float64(time.Millisecond)
}

func performanceMessagingBudget(t *testing.T, started time.Time, budget time.Duration, stage string) {
	t.Helper()
	if elapsed := time.Since(started); elapsed > budget {
		t.Fatalf("bounded performance fixture exceeded %s during %s: %s", budget, stage, elapsed)
	}
}

type performanceQueueMessage struct {
	input  QueueMessageInput
	result QueueSendResult
}

func performanceQueueInput(index, groups int) QueueMessageInput {
	attributes := make(map[string]MessageAttribute, 10)
	for attribute := range 9 {
		attributes[fmt.Sprintf("attribute_%02d", attribute)] = MessageAttribute{DataType: "String", StringValue: strings.Repeat("<>&\n", 16)}
	}
	attributes["binary"] = MessageAttribute{DataType: "Binary", BinaryValue: bytes.Repeat([]byte{0, 1, 255}, 32)}
	input := QueueMessageInput{Body: fmt.Sprintf("%08d:%s", index, strings.Repeat("x", 1015)), Attributes: attributes}
	if groups != 0 {
		input.MessageGroupID = fmt.Sprintf("group-%03d", index%groups)
		input.MessageDeduplicationID = fmt.Sprintf("dedup-%08d", index)
	}
	return input
}

func performanceSendQueue(t *testing.T, broker *Broker, queue *Queue, index, groups int) performanceQueueMessage {
	t.Helper()
	input := performanceQueueInput(index, groups)
	result, err := broker.SendQueueMessage(queue, input)
	require.NoError(t, err)
	require.NotEmpty(t, result.MessageID)
	return performanceQueueMessage{input: input, result: result}
}

// Native core receive omits HTTP serialization. Its projection+marshal metric
// includes the shared projection; Lambda's marshal starts from already-built
// records after byte admission. These metrics are deliberately distinct.
func TestPerformanceReviewSQSDepth(t *testing.T) {
	for _, depth := range []int{100, 1000, 10000} {
		for _, groups := range []int{0, 100} {
			kind := "standard"
			if groups != 0 {
				kind = "fifo_100_groups"
			}
			caseName := fmt.Sprintf("sqs_%s_depth_%d_attrs_10_body_1024", kind, depth)
			t.Run(caseName, func(t *testing.T) {
				broker := NewBroker("us-east-1", "123456789012", 0)
				name := "performance-queue"
				attributes := map[string]string{"VisibilityTimeout": "3600"}
				if groups != 0 {
					name += ".fifo"
					attributes["FifoQueue"] = "true"
				}
				queue, failure := broker.createSQSQueue(name, attributes, nil)
				require.Nil(t, failure)
				t.Cleanup(func() { broker.DeleteQueue(queue.Name) })
				setup := time.Now()
				model := make([]performanceQueueMessage, 0, depth)
				for index := range depth {
					model = append(model, performanceSendQueue(t, broker, queue, index, groups))
					if index%100 == 0 {
						performanceMessagingBudget(t, setup, 20*time.Second, "native queue setup (including FIFO dedup pruning)")
					}
				}
				t.Logf("PERFORMANCE_MESSAGING_SETUP case=%s messages=%d setup_ms=%.6f", caseName, depth, performanceMessagingMS(setup))
				performanceMessagingBudget(t, setup, 20*time.Second, "native queue setup")
				if groups != 0 {
					duplicate, err := broker.SendQueueMessage(queue, model[depth-1].input)
					require.NoError(t, err)
					require.Equal(t, model[depth-1].result, duplicate, "native FIFO dedup cannot append another message")
				}
				waiting, flight := broker.QueueDepth(queue)
				require.Equal(t, depth, waiting)
				require.Zero(t, flight)
				metrics := map[string][]float64{}
				seenReceipts := map[string]bool{}
				nextIndex := depth
				measurement := setup // Bound setup and measured samples together.
				for sample := range performanceMessagingSamples {
					started := time.Now()
					requeued := broker.RequeueExpired(queue)
					metrics["maintenance_ms"] = append(metrics["maintenance_ms"], performanceMessagingMS(started))
					require.Zero(t, requeued)
					for _, mode := range []string{"native", "lambda"} {
						started = time.Now()
						var messages []*Message
						var event sqsevent.Event
						var err error
						if mode == "native" {
							messages, err = broker.ReceiveMessagesContext(context.Background(), queue, 10, 0)
						} else {
							event, err = broker.ReceiveSQSLambdaEventContext(context.Background(), queue, 10, 0, 6<<20)
						}
						metrics[mode+"_receive_ms"] = append(metrics[mode+"_receive_ms"], performanceMessagingMS(started))
						require.NoError(t, err)
						started = time.Now()
						marshalMetric := mode + "_marshal_ms"
						if mode == "native" {
							event = BuildSQSLambdaEvent(messages, queue.ARN)
							marshalMetric = "native_projection_marshal_ms"
						}
						payload, err := json.Marshal(event)
						metrics[marshalMetric] = append(metrics[marshalMetric], performanceMessagingMS(started))
						require.NoError(t, err)
						require.LessOrEqual(t, len(payload), 6<<20)
						require.Len(t, event.Records, 10)
						waiting, flight = broker.QueueDepth(queue)
						require.Equal(t, depth-10, waiting)
						require.Equal(t, 10, flight)
						for index, record := range event.Records {
							expected := model[index]
							require.Equal(t, expected.result.MessageID, record.MessageID)
							require.Equal(t, expected.input.Body, record.Body)
							require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
							require.Equal(t, queue.ARN, record.EventSourceARN)
							require.Equal(t, expected.input.MessageGroupID, record.Attributes["MessageGroupId"])
							require.Equal(t, expected.result.SequenceNumber, record.Attributes["SequenceNumber"])
							require.Equal(t, expected.result.MD5OfMessageBody, record.MD5OfBody)
							require.NotEmpty(t, record.ReceiptHandle)
							require.False(t, seenReceipts[record.ReceiptHandle], "each actual lease must have a fresh receipt")
							seenReceipts[record.ReceiptHandle] = true
							require.Len(t, record.MessageAttributes, 10)
							for name, attribute := range expected.input.Attributes {
								actual := record.MessageAttributes[name]
								require.Equal(t, attribute.DataType, actual.DataType)
								if attribute.DataType == "Binary" {
									require.Equal(t, attribute.BinaryValue, actual.BinaryValue)
								} else {
									require.NotNil(t, actual.StringValue)
									require.Equal(t, attribute.StringValue, *actual.StringValue)
								}
							}
						}
						settled := make([]bool, len(event.Records))
						outcomes := make([]SQSLambdaReceiptOutcome, len(event.Records))
						errors := make([]error, len(event.Records))
						started = time.Now()
						for index, record := range event.Records {
							if mode == "native" {
								settled[index] = broker.DeleteMessage(queue, record.ReceiptHandle)
							} else {
								outcomes[index], errors[index] = broker.EvaluateSQSLambdaReceiptContext(context.Background(), queue, record.ReceiptHandle, true)
							}
						}
						metrics[mode+"_settle_batch_ms"] = append(metrics[mode+"_settle_batch_ms"], performanceMessagingMS(started))
						for index := range event.Records {
							if mode == "native" {
								require.True(t, settled[index])
							} else {
								require.NoError(t, errors[index])
								require.Equal(t, SQSLambdaReceiptMappingSettled, outcomes[index])
							}
						}
						for _, record := range event.Records {
							outcome, err := broker.EvaluateSQSLambdaReceiptContext(context.Background(), queue, record.ReceiptHandle, false)
							require.NoError(t, err)
							want := SQSLambdaReceiptNativeSettled
							if mode == "lambda" {
								want = SQSLambdaReceiptMappingSettled
							}
							require.Equal(t, want, outcome)
						}
						model = model[10:]
						for range 10 {
							model = append(model, performanceSendQueue(t, broker, queue, nextIndex, groups))
							nextIndex++
						}
						waiting, flight = broker.QueueDepth(queue)
						require.Equal(t, depth, waiting, "every measured receive begins at the declared queue depth")
						require.Zero(t, flight)
					}
					performanceMessagingBudget(t, measurement, 30*time.Second, fmt.Sprintf("queue measurement sample %d", sample))
				}
				for _, metric := range []string{"maintenance_ms", "native_receive_ms", "lambda_receive_ms", "native_projection_marshal_ms", "lambda_marshal_ms", "native_settle_batch_ms", "lambda_settle_batch_ms"} {
					testperf.Report(t, caseName, metric, metrics[metric])
				}
				t.Logf("PERFORMANCE_MESSAGING_STATE case=%s waiting=%d inflight=%d distinct_receipts=%d receipt_history_growth=%d", caseName, waiting, flight, len(seenReceipts), performanceMessagingSamples*20)
			})
		}
	}
}

type performanceSNSFixture struct {
	broker       *Broker
	capture      *SNSCapture
	capturePath  string
	topics       []*Topic
	destinations map[string][]*Queue
}

type performanceSNSPublication struct {
	topicARN, requestID, message string
	result                       SNSPublishResult
	elapsedMS                    float64
	err                          error
}

func newPerformanceSNSFixture(t *testing.T, topics, subscribers int, durable bool) *performanceSNSFixture {
	t.Helper()
	fixture := &performanceSNSFixture{broker: NewBroker("us-east-1", "123456789012", 0), destinations: map[string][]*Queue{}}
	if durable {
		fixture.capturePath = filepath.Join(t.TempDir(), "sns", "capture.jsonl")
		capture, err := OpenSNSCapture(fixture.capturePath)
		require.NoError(t, err)
		fixture.capture = capture
		fixture.broker.SetSNSCapture(capture)
		t.Cleanup(func() { require.NoError(t, capture.Close()) })
	}
	for index := range topics {
		topic := fixture.broker.CreateTopic(fmt.Sprintf("performance-topic-%d", index))
		require.NotNil(t, topic)
		fixture.topics = append(fixture.topics, topic)
		for subscriber := range subscribers {
			queue := fixture.broker.CreateQueue(fmt.Sprintf("destination-%d-%d", index, subscriber), time.Hour, 0)
			_, err := fixture.broker.Subscribe(topic.ARN, "sqs", queue.ARN, nil)
			require.NoError(t, err)
			fixture.destinations[topic.ARN] = append(fixture.destinations[topic.ARN], queue)
			t.Cleanup(func() { fixture.broker.DeleteQueue(queue.Name) })
		}
	}
	return fixture
}

func (fixture *performanceSNSFixture) publish(index, topic int) performanceSNSPublication {
	publication := performanceSNSPublication{topicARN: fixture.topics[topic].ARN, requestID: fmt.Sprintf("publication-%d", index), message: fmt.Sprintf("%08d:%s", index, strings.Repeat("x", 1015))}
	input := SNSPublishInput{TopicARN: publication.topicARN, RequestID: publication.requestID, Message: publication.message, Attributes: map[string]MessageAttribute{"control": {DataType: "String", StringValue: "local-only"}}}
	started := time.Now()
	publication.result, publication.err = fixture.broker.PublishSNS(input)
	publication.elapsedMS = performanceMessagingMS(started)
	return publication
}

func (fixture *performanceSNSFixture) verify(t *testing.T, publications []performanceSNSPublication) {
	t.Helper()
	byTopic := map[string]map[string]performanceSNSPublication{}
	all := map[string]performanceSNSPublication{}
	for _, publication := range publications {
		require.NoError(t, publication.err)
		require.NotEmpty(t, publication.result.MessageID)
		require.NotContains(t, all, publication.result.MessageID)
		all[publication.result.MessageID] = publication
		if byTopic[publication.topicARN] == nil {
			byTopic[publication.topicARN] = map[string]performanceSNSPublication{}
		}
		byTopic[publication.topicARN][publication.result.MessageID] = publication
	}
	for topicARN, destinations := range fixture.destinations {
		expected := byTopic[topicARN]
		for _, queue := range destinations {
			waiting, flight := fixture.broker.QueueDepth(queue)
			require.Equal(t, len(expected), waiting)
			require.Zero(t, flight)
			seen := map[string]bool{}
			for len(seen) < len(expected) {
				messages, err := fixture.broker.ReceiveMessagesContext(context.Background(), queue, 10, 0)
				require.NoError(t, err)
				require.NotEmpty(t, messages)
				for _, message := range messages {
					var envelope snsEnvelope
					require.NoError(t, json.Unmarshal([]byte(message.Body), &envelope))
					publication, exists := expected[envelope.MessageID]
					require.True(t, exists)
					require.False(t, seen[envelope.MessageID], "each accepted publication delivers exactly once to each destination")
					seen[envelope.MessageID] = true
					require.Equal(t, "Notification", envelope.Type)
					require.Equal(t, topicARN, envelope.TopicArn)
					require.Equal(t, publication.message, envelope.Message)
					require.Equal(t, "local-only", envelope.MessageAttributes["control"].Value)
					require.Equal(t, 1, message.ReceiveCount)
					require.NotEmpty(t, message.ReceiptHandle)
					require.True(t, fixture.broker.DeleteMessage(queue, message.ReceiptHandle))
				}
			}
			waiting, flight = fixture.broker.QueueDepth(queue)
			require.Zero(t, waiting)
			require.Zero(t, flight)
		}
	}
	if fixture.capture == nil {
		return
	}
	require.NoError(t, fixture.capture.Err())
	require.NoError(t, fixture.capture.Close())
	info, err := os.Stat(fixture.capturePath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	file, err := os.Open(fixture.capturePath)
	require.NoError(t, err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	seen := map[string]bool{}
	for range publications {
		var record SNSCaptureRecord
		require.NoError(t, decoder.Decode(&record))
		publication, exists := all[record.MessageID]
		require.True(t, exists)
		require.False(t, seen[record.MessageID])
		seen[record.MessageID] = true
		require.Equal(t, "eventbus.sns.capture.v1", record.SchemaVersion)
		require.Equal(t, "Publish", record.Operation)
		require.Equal(t, publication.requestID, record.RequestID)
		require.Equal(t, publication.topicARN, record.TargetARN)
		require.Equal(t, publication.message, record.Message)
		require.Len(t, record.Deliveries, len(fixture.destinations[publication.topicARN]))
		endpoints := map[string]bool{}
		for _, queue := range fixture.destinations[publication.topicARN] {
			endpoints[queue.ARN] = true
		}
		for _, delivery := range record.Deliveries {
			require.Equal(t, "sqs", delivery.Protocol)
			require.Equal(t, "scheduled", delivery.Status, "capture is admission intent, not destination completion")
			require.Equal(t, publication.result.MessageID, delivery.MessageID)
			require.True(t, endpoints[delivery.Endpoint])
			delete(endpoints, delivery.Endpoint)
		}
		require.Empty(t, endpoints)
	}
	var extra SNSCaptureRecord
	require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
}

func TestPerformanceReviewSNSFanout(t *testing.T) {
	for _, subscribers := range []int{1, 10, 100} {
		for _, durable := range []bool{false, true} {
			mode := "disabled"
			if durable {
				mode = "durable"
			}
			caseName := fmt.Sprintf("sns_subscribers_%d_capture_%s", subscribers, mode)
			t.Run(caseName, func(t *testing.T) {
				fixture := newPerformanceSNSFixture(t, 1, subscribers, durable)
				var publications []performanceSNSPublication
				var samples []float64
				started := time.Now()
				for index := range performanceMessagingSamples {
					publication := fixture.publish(index, 0)
					require.NoError(t, publication.err)
					publications = append(publications, publication)
					samples = append(samples, publication.elapsedMS)
					performanceMessagingBudget(t, started, 30*time.Second, "sequential SNS publishes")
				}
				testperf.Report(t, caseName, "publish_wall_ms", samples)
				fixture.verify(t, publications)
			})
		}
	}
}

func TestPerformanceReviewSNSContention(t *testing.T) {
	const workers, perWorker = 4, 10
	for _, sharedTopic := range []bool{true, false} {
		for _, durable := range []bool{false, true} {
			topics, layout, mode := workers, "separate_topics", "disabled"
			if sharedTopic {
				topics, layout = 1, "same_topic"
			}
			if durable {
				mode = "durable"
			}
			caseName := fmt.Sprintf("sns_%s_workers_4_subscribers_10_capture_%s", layout, mode)
			t.Run(caseName, func(t *testing.T) {
				fixture := newPerformanceSNSFixture(t, topics, 10, durable)
				publications := make([]performanceSNSPublication, workers*perWorker)
				start := make(chan struct{})
				var joined sync.WaitGroup
				for worker := range workers {
					joined.Add(1)
					go func() {
						defer joined.Done()
						<-start
						topic := worker
						if sharedTopic {
							topic = 0
						}
						for local := range perWorker {
							index := worker*perWorker + local
							publications[index] = fixture.publish(index, topic)
						}
					}()
				}
				started := time.Now()
				close(start)
				joined.Wait()
				batchMS := performanceMessagingMS(started)
				performanceMessagingBudget(t, started, 30*time.Second, "joined SNS publisher workers")
				var samples []float64
				for _, publication := range publications {
					require.NoError(t, publication.err)
					samples = append(samples, publication.elapsedMS)
				}
				testperf.Report(t, caseName, "publish_wall_ms", samples)
				testperf.Report(t, caseName, "joined_batch_wall_ms", []float64{batchMS})
				fixture.verify(t, publications)
			})
		}
	}
}
