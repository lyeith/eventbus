package messaging

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestBroker() *Broker {
	return NewBroker("us-east-1", "000000000000", 4100)
}

func TestCreateTopic(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("test-topic")

	assert.NotNil(t, topic)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:test-topic", topic.ARN)
	assert.Equal(t, "test-topic", topic.Name)
}

func TestCreateTopicIdempotent(t *testing.T) {
	b := newTestBroker()
	t1 := b.CreateTopic("my-topic")
	t2 := b.CreateTopic("my-topic")

	assert.Same(t, t1, t2, "creating the same topic twice should return the same pointer")
	assert.Equal(t, t1.ARN, t2.ARN)
}

func TestListTopics(t *testing.T) {
	b := newTestBroker()
	b.CreateTopic("topic-a")
	b.CreateTopic("topic-b")
	b.CreateTopic("topic-c")

	topics := b.ListTopics()
	assert.Len(t, topics, 3)

	names := make(map[string]bool)
	for _, topic := range topics {
		names[topic.Name] = true
	}
	assert.True(t, names["topic-a"])
	assert.True(t, names["topic-b"])
	assert.True(t, names["topic-c"])
}

func TestDeleteTopic(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("doomed-topic")

	ok := b.DeleteTopic(topic.ARN)
	assert.True(t, ok)
	assert.Nil(t, b.GetTopic(topic.ARN))

	t.Run("delete nonexistent", func(t *testing.T) {
		ok := b.DeleteTopic("arn:aws:sns:us-east-1:000000000000:no-such-topic")
		assert.False(t, ok)
	})
}

func TestSubscribe(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("events")
	queueARN := "arn:aws:sqs:us-east-1:000000000000:my-queue"

	sub, err := b.Subscribe(topic.ARN, "sqs", queueARN, nil)

	assert.NoError(t, err)
	assert.NotNil(t, sub)
	assert.Equal(t, topic.ARN, sub.TopicARN)
	assert.Equal(t, "sqs", sub.Protocol)
	assert.Equal(t, queueARN, sub.Endpoint)
	assert.Nil(t, sub.FilterPolicy)
	assert.NotEmpty(t, sub.ARN, "subscription ARN should be set")
}

// Provisioning subscribes every queue again on each reseed. A second
// subscription would deliver each message to the queue once more per reseed,
// so a consumer's backlog would grow with the EventBus's age.
func TestSubscribeAgainReturnsTheSubscriptionAndDeliversOnce(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("notifications")
	q := b.CreateQueue("worker-queue", 0, 0)
	policy := func(values ...string) *FilterPolicy {
		return &FilterPolicy{Attributes: map[string][]string{"event_type": values}}
	}

	first, err := b.Subscribe(topic.ARN, "sqs", q.ARN, policy("import.created", "data-hub.changed"))
	require.NoError(t, err)
	again, err := b.Subscribe(topic.ARN, "sqs", q.ARN, policy("data-hub.changed", "import.created"))
	require.NoError(t, err)

	assert.Equal(t, first.ARN, again.ARN)
	subs, err := b.ListSubscriptionsByTopic(topic.ARN)
	require.NoError(t, err)
	assert.Len(t, subs, 1)

	_, err = b.Publish(topic.ARN, `{"event":"data-hub.changed"}`, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "data-hub.changed"},
	})
	require.NoError(t, err)
	waiting, inFlight := b.QueueDepth(q)
	assert.Equal(t, 1, waiting)
	assert.Equal(t, 0, inFlight)
}

// SNS refuses to subscribe an endpoint again under a different filter policy
// rather than silently keeping either one.
func TestSubscribeAgainWithAnotherFilterPolicyIsRefused(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("notifications")
	q := b.CreateQueue("worker-queue", 0, 0)

	_, err := b.Subscribe(topic.ARN, "sqs", q.ARN, &FilterPolicy{
		Attributes: map[string][]string{"event_type": {"import.created"}},
	})
	require.NoError(t, err)
	_, err = b.Subscribe(topic.ARN, "sqs", q.ARN, nil)

	assert.ErrorIs(t, err, errSubscriptionAttributesDiffer)
	subs, err := b.ListSubscriptionsByTopic(topic.ARN)
	require.NoError(t, err)
	assert.Len(t, subs, 1)
}

func TestSubscribeToNonexistentTopic(t *testing.T) {
	b := newTestBroker()

	sub, err := b.Subscribe("arn:aws:sns:us-east-1:000000000000:ghost", "sqs", "some-endpoint", nil)

	assert.Error(t, err)
	assert.Nil(t, sub)
	assert.Contains(t, err.Error(), "topic not found")
}

func TestCreateQueue(t *testing.T) {
	b := newTestBroker()
	q := b.CreateQueue("test-queue", 0, 0)

	assert.NotNil(t, q)
	assert.Equal(t, "test-queue", q.Name)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:test-queue", q.ARN)
	assert.Contains(t, q.URL, "test-queue")
	assert.Equal(t, defaultVisibilityTimeout, q.VisibilityTimeout)
	assert.Equal(t, defaultRetentionPeriod, q.RetentionPeriod)
	assert.NotNil(t, q.messages)
	assert.NotNil(t, q.inFlight)
}

func TestCreateQueueIdempotent(t *testing.T) {
	b := newTestBroker()
	q1 := b.CreateQueue("same-queue", 0, 0)
	q2 := b.CreateQueue("same-queue", 0, 0)

	assert.Same(t, q1, q2, "creating the same queue twice should return the same pointer")
}

func TestCreateQueueCustomAttributes(t *testing.T) {
	b := newTestBroker()
	q := b.CreateQueue("custom-queue", 60*time.Second, 7*24*time.Hour)

	assert.Equal(t, 60*time.Second, q.VisibilityTimeout)
	assert.Equal(t, 7*24*time.Hour, q.RetentionPeriod)
}

func TestDeleteQueue(t *testing.T) {
	b := newTestBroker()
	b.CreateQueue("temp-queue", 0, 0)

	ok := b.DeleteQueue("temp-queue")
	assert.True(t, ok)
	assert.Nil(t, b.GetQueue("temp-queue"))

	t.Run("delete nonexistent", func(t *testing.T) {
		ok := b.DeleteQueue("no-such-queue")
		assert.False(t, ok)
	})
}

func TestPublishFanout(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("notifications")
	q := b.CreateQueue("worker-queue", 0, 0)

	// Subscribe queue to topic
	_, err := b.Subscribe(topic.ARN, "sqs", q.ARN, nil)
	assert.NoError(t, err)

	// Publish a message
	msgID, err := b.Publish(topic.ARN, `{"event":"test"}`, map[string]MessageAttribute{
		"event_type": {DataType: "String", StringValue: "test.created"},
	})

	assert.NoError(t, err)
	assert.NotEmpty(t, msgID)

	// Verify message landed in the queue
	q.mu.Lock()
	defer q.mu.Unlock()
	assert.Len(t, q.messages, 1)
	// Message body is an SNS envelope; the inner message is JSON-escaped
	assert.Contains(t, q.messages[0].Body, `{\"event\":\"test\"}`)
}

func TestPublishNoSubscribers(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("lonely-topic")

	msgID, err := b.Publish(topic.ARN, "hello", nil)

	assert.NoError(t, err)
	assert.NotEmpty(t, msgID)
}

func TestPublishNonexistentTopic(t *testing.T) {
	b := newTestBroker()

	msgID, err := b.Publish("arn:aws:sns:us-east-1:000000000000:ghost", "msg", nil)

	assert.Error(t, err)
	assert.Empty(t, msgID)
	assert.Contains(t, err.Error(), "topic not found")
}

func TestEnqueueSignalsCond(t *testing.T) {
	b := newTestBroker()
	q := b.CreateQueue("signal-queue", 0, 0)

	signaled := make(chan struct{})

	// Start a goroutine that waits on the cond
	go func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		for len(q.messages) == 0 {
			q.cond.Wait()
		}
		close(signaled)
	}()

	// Give the goroutine time to start waiting
	time.Sleep(10 * time.Millisecond)

	b.enqueueMessage(q, `{"test":"signal"}`)

	select {
	case <-signaled:
		// cond was signaled and goroutine woke up
	case <-time.After(1 * time.Second):
		t.Fatal("enqueueMessage did not signal the cond within timeout")
	}

	q.mu.Lock()
	assert.Len(t, q.messages, 1)
	q.mu.Unlock()
}

func TestPublishFanoutMultipleQueues(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("multi-fan")
	q1 := b.CreateQueue("fan-q1", 0, 0)
	q2 := b.CreateQueue("fan-q2", 0, 0)

	_, _ = b.Subscribe(topic.ARN, "sqs", q1.ARN, nil)
	_, _ = b.Subscribe(topic.ARN, "sqs", q2.ARN, nil)

	_, err := b.Publish(topic.ARN, "broadcast", nil)
	assert.NoError(t, err)

	q1.mu.Lock()
	assert.Len(t, q1.messages, 1)
	q1.mu.Unlock()

	q2.mu.Lock()
	assert.Len(t, q2.messages, 1)
	q2.mu.Unlock()
}

func TestDeleteMessage(t *testing.T) {
	b := newTestBroker()
	q := b.CreateQueue("delete-msg-q", 0, 0)
	b.enqueueMessage(q, `{"delete":"test"}`)

	// Receive moves message to in-flight
	msgs := b.ReceiveMessages(q, 1, 0)
	assert.Len(t, msgs, 1)
	receiptHandle := msgs[0].ReceiptHandle

	q.mu.Lock()
	assert.Len(t, q.inFlight, 1, "message should be in-flight after receive")
	q.mu.Unlock()

	// Delete removes from in-flight
	ok := b.DeleteMessage(q, receiptHandle)
	assert.True(t, ok)

	q.mu.Lock()
	assert.Empty(t, q.inFlight, "in-flight should be empty after delete")
	assert.Empty(t, q.messages, "messages should be empty after delete")
	q.mu.Unlock()
}

func TestPurgeQueue(t *testing.T) {
	b := newTestBroker()
	q := b.CreateQueue("purge-q", 0, 0)

	// Enqueue some messages
	for i := 0; i < 5; i++ {
		b.enqueueMessage(q, `{"msg":"purge"}`)
	}

	// Receive one to move it to in-flight
	msgs := b.ReceiveMessages(q, 1, 0)
	assert.Len(t, msgs, 1)

	q.mu.Lock()
	assert.Len(t, q.messages, 4, "4 messages should remain in queue")
	assert.Len(t, q.inFlight, 1, "1 message should be in-flight")
	q.mu.Unlock()

	// Purge clears both messages and inFlight
	b.PurgeQueue(q)

	q.mu.Lock()
	assert.Empty(t, q.messages, "messages should be empty after purge")
	assert.Empty(t, q.inFlight, "in-flight should be empty after purge")
	q.mu.Unlock()
}

func TestRequeueExpired(t *testing.T) {
	b := newTestBroker()
	q := b.CreateQueue("requeue-q", 50*time.Millisecond, 0)
	b.enqueueMessage(q, `{"requeue":"test"}`)

	// Receive moves to in-flight with short visibility timeout
	msgs := b.ReceiveMessages(q, 1, 0)
	assert.Len(t, msgs, 1)

	q.mu.Lock()
	assert.Len(t, q.inFlight, 1)
	assert.Empty(t, q.messages)
	q.mu.Unlock()

	// Wait for visibility timeout to expire
	time.Sleep(100 * time.Millisecond)

	// RequeueExpired should move the message back
	count := b.RequeueExpired(q)
	assert.Equal(t, 1, count)

	q.mu.Lock()
	assert.Len(t, q.messages, 1, "message should be back in queue")
	assert.Empty(t, q.inFlight, "in-flight should be empty after requeue")
	q.mu.Unlock()

	// Should be receivable again
	msgs2 := b.ReceiveMessages(q, 1, 0)
	assert.Len(t, msgs2, 1)
}

func TestReceiveMessagesCap(t *testing.T) {
	b := newTestBroker()
	q := b.CreateQueue("cap-q", 0, 0)

	// Enqueue 15 messages
	for i := 0; i < 15; i++ {
		b.enqueueMessage(q, `{"msg":"cap"}`)
	}

	// Request 15 but should be capped at 10
	msgs := b.ReceiveMessages(q, 15, 0)
	assert.Len(t, msgs, 10, "maxMessages should be capped at 10")

	// Remaining 5 should still be available
	msgs2 := b.ReceiveMessages(q, 10, 0)
	assert.Len(t, msgs2, 5)
}

func TestConcurrentPublish(t *testing.T) {
	b := newTestBroker()
	topic := b.CreateTopic("concurrent")
	q := b.CreateQueue("concurrent-q", 0, 0)
	_, _ = b.Subscribe(topic.ARN, "sqs", q.ARN, nil)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.Publish(topic.ARN, "msg", nil)
		}()
	}
	wg.Wait()

	q.mu.Lock()
	assert.Len(t, q.messages, 50)
	q.mu.Unlock()
}
