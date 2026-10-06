package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

const maxWaitTime = 20 * time.Second

const (
	defaultRegion            = "us-east-1"
	defaultAccountID         = "000000000000"
	defaultVisibilityTimeout = 30 * time.Second
	defaultRetentionPeriod   = 4 * 24 * time.Hour // 4 days
)

type Broker struct {
	topics    map[string]*Topic // ARN → Topic
	queues    map[string]*Queue // name → Queue
	arnIndex  map[string]*Queue // ARN → Queue
	mu        sync.RWMutex
	region    string
	accountID string
	port      int
}

type Topic struct {
	ARN           string
	Name          string
	Subscriptions []*Subscription
	mu            sync.RWMutex
}

type Subscription struct {
	ARN          string
	TopicARN     string
	Protocol     string        // "sqs"
	Endpoint     string        // queue ARN
	FilterPolicy *FilterPolicy // nil = no filter
}

type Queue struct {
	Name              string
	ARN               string
	URL               string
	messages          []*Message
	inFlight          map[string]*Message // receiptHandle → Message
	VisibilityTimeout time.Duration
	RetentionPeriod   time.Duration
	mu                sync.Mutex
	cond              *sync.Cond
}

type Message struct {
	ID            string
	Body          string
	ReceiptHandle string
	SentTimestamp time.Time
	VisibleAt     time.Time
	ReceiveCount  int
}

func NewBroker(region, accountID string, port int) *Broker {
	if region == "" {
		region = defaultRegion
	}
	if accountID == "" {
		accountID = defaultAccountID
	}
	return &Broker{
		topics:    make(map[string]*Topic),
		queues:    make(map[string]*Queue),
		arnIndex:  make(map[string]*Queue),
		region:    region,
		accountID: accountID,
		port:      port,
	}
}

// --- Topic operations ---

func (b *Broker) CreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", b.region, b.accountID, name)

	if existing, ok := b.topics[arn]; ok {
		return existing
	}

	topic := &Topic{
		ARN:  arn,
		Name: name,
	}
	b.topics[arn] = topic
	return topic
}

func (b *Broker) GetTopic(arn string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[arn]
}

func (b *Broker) ListTopics() []*Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	return topics
}

func (b *Broker) DeleteTopic(arn string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.topics[arn]; !ok {
		return false
	}
	delete(b.topics, arn)
	return true
}

// --- Subscription operations ---

// errSubscriptionAttributesDiffer is SNS's refusal to subscribe an endpoint
// again under different attributes.
var errSubscriptionAttributesDiffer = errors.New("subscription already exists with different attributes")

// Subscribe is idempotent, as SNS is: subscribing an endpoint that is already
// subscribed returns its subscription. Provisioning runs on every reseed
// against a long-lived EventBus, and a second subscription would deliver every
// message to the endpoint once more.
func (b *Broker) Subscribe(topicARN, protocol, endpoint string, filterPolicy *FilterPolicy) (*Subscription, error) {
	topic := b.GetTopic(topicARN)
	if topic == nil {
		return nil, fmt.Errorf("topic not found: %s", topicARN)
	}

	topic.mu.Lock()
	defer topic.mu.Unlock()

	for _, existing := range topic.Subscriptions {
		if existing.Protocol != protocol || existing.Endpoint != endpoint {
			continue
		}
		if !existing.FilterPolicy.Equal(filterPolicy) {
			return nil, errSubscriptionAttributesDiffer
		}
		return existing, nil
	}

	sub := &Subscription{
		ARN:          fmt.Sprintf("%s:%s", topicARN, uuid.New().String()),
		TopicARN:     topicARN,
		Protocol:     protocol,
		Endpoint:     endpoint,
		FilterPolicy: filterPolicy,
	}
	topic.Subscriptions = append(topic.Subscriptions, sub)
	return sub, nil
}

func (b *Broker) ListSubscriptionsByTopic(topicARN string) ([]*Subscription, error) {
	topic := b.GetTopic(topicARN)
	if topic == nil {
		return nil, fmt.Errorf("topic not found: %s", topicARN)
	}

	topic.mu.RLock()
	defer topic.mu.RUnlock()

	subs := make([]*Subscription, len(topic.Subscriptions))
	copy(subs, topic.Subscriptions)
	return subs, nil
}

// --- Queue operations ---

func (b *Broker) CreateQueue(name string, visibilityTimeout, retentionPeriod time.Duration) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.queues[name]; ok {
		return existing
	}

	if visibilityTimeout == 0 {
		visibilityTimeout = defaultVisibilityTimeout
	}
	if retentionPeriod == 0 {
		retentionPeriod = defaultRetentionPeriod
	}

	q := &Queue{
		Name:              name,
		ARN:               fmt.Sprintf("arn:aws:sqs:%s:%s:%s", b.region, b.accountID, name),
		URL:               fmt.Sprintf("http://localhost:%d/queue/%s", b.port, name),
		messages:          make([]*Message, 0),
		inFlight:          make(map[string]*Message),
		VisibilityTimeout: visibilityTimeout,
		RetentionPeriod:   retentionPeriod,
	}
	q.cond = sync.NewCond(&q.mu)
	b.queues[name] = q
	b.arnIndex[q.ARN] = q
	return q
}

func (b *Broker) GetQueue(name string) *Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.queues[name]
}

func (b *Broker) ListQueues() []*Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	queues := make([]*Queue, 0, len(b.queues))
	for _, q := range b.queues {
		queues = append(queues, q)
	}
	return queues
}

func (b *Broker) GetQueueByARN(arn string) *Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.arnIndex[arn]
}

func (b *Broker) DeleteQueue(name string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		return false
	}
	delete(b.arnIndex, q.ARN)
	delete(b.queues, name)
	return true
}

// --- Publish with fanout ---

type MessageAttribute struct {
	DataType    string
	StringValue string
}

func (b *Broker) Publish(topicARN, message string, attrs map[string]MessageAttribute) (string, error) {
	topic := b.GetTopic(topicARN)
	if topic == nil {
		return "", fmt.Errorf("topic not found: %s", topicARN)
	}

	messageID := uuid.New().String()

	topic.mu.RLock()
	subs := make([]*Subscription, len(topic.Subscriptions))
	copy(subs, topic.Subscriptions)
	topic.mu.RUnlock()

	// Build SNS envelope
	envelope, err := buildSNSEnvelope(messageID, topicARN, message, attrs)
	if err != nil {
		return "", fmt.Errorf("failed to build SNS envelope: %w", err)
	}

	for _, sub := range subs {
		if sub.Protocol != "sqs" {
			continue
		}

		if sub.FilterPolicy != nil && !sub.FilterPolicy.Matches(attrs) {
			continue
		}

		queue := b.GetQueueByARN(sub.Endpoint)
		if queue == nil {
			continue
		}

		b.enqueueMessage(queue, envelope)
	}

	return messageID, nil
}

func (b *Broker) enqueueMessage(q *Queue, body string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg := &Message{
		ID:            uuid.New().String(),
		Body:          body,
		SentTimestamp: time.Now(),
	}
	q.messages = append(q.messages, msg)
	q.cond.Signal()
}

// ReceiveMessages returns up to maxMessages from the queue.
// If no messages are available and waitTime > 0, it long-polls.
func (b *Broker) ReceiveMessages(q *Queue, maxMessages int, waitTime time.Duration) []*Message {
	if maxMessages <= 0 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		maxMessages = 10
	}
	if waitTime > maxWaitTime {
		waitTime = maxWaitTime
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Try to get visible messages
	messages := b.collectVisible(q, maxMessages)
	if len(messages) > 0 || waitTime <= 0 {
		return messages
	}

	// Long-poll: wait for messages or timeout
	done := make(chan struct{})
	timer := time.AfterFunc(waitTime, func() {
		q.mu.Lock()
		q.cond.Signal()
		q.mu.Unlock()
		close(done)
	})
	defer timer.Stop()

	for {
		q.cond.Wait()
		messages = b.collectVisible(q, maxMessages)
		if len(messages) > 0 {
			return messages
		}
		select {
		case <-done:
			return nil
		default:
		}
	}
}

// collectVisible extracts up to n visible messages, moving them to inFlight.
// Caller must hold q.mu.
func (b *Broker) collectVisible(q *Queue, n int) []*Message {
	now := time.Now()
	var result []*Message
	var remaining []*Message

	for _, msg := range q.messages {
		if len(result) >= n {
			remaining = append(remaining, msg)
			continue
		}
		// Assign receipt handle and move to in-flight
		msg.ReceiptHandle = uuid.New().String()
		msg.VisibleAt = now.Add(q.VisibilityTimeout)
		msg.ReceiveCount++
		q.inFlight[msg.ReceiptHandle] = msg
		result = append(result, msg)
	}

	q.messages = remaining
	return result
}

// ExtendMessageVisibility keeps a long-running handler from being raced by
// the visibility requeue loop while it still owns the receipt.
func (b *Broker) ExtendMessageVisibility(q *Queue, receiptHandle string, timeout time.Duration) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	msg, ok := q.inFlight[receiptHandle]
	if !ok {
		return false
	}
	msg.VisibleAt = time.Now().Add(timeout)
	return true
}

// MoveMessage transfers one in-flight message to a dead-letter queue. Queue
// locks are acquired in stable name order so independently configured
// consumers cannot deadlock each other.
func (b *Broker) MoveMessage(source, destination *Queue, receiptHandle string) bool {
	if source == nil || destination == nil || source == destination {
		return false
	}

	first, second := source, destination
	if second.Name < first.Name {
		first, second = second, first
	}
	first.mu.Lock()
	second.mu.Lock()
	defer second.mu.Unlock()
	defer first.mu.Unlock()

	msg, ok := source.inFlight[receiptHandle]
	if !ok {
		return false
	}
	delete(source.inFlight, receiptHandle)
	msg.ReceiptHandle = ""
	msg.VisibleAt = time.Time{}
	destination.messages = append(destination.messages, msg)
	destination.cond.Signal()
	return true
}

func (b *Broker) DeleteMessage(q *Queue, receiptHandle string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.inFlight[receiptHandle]; ok {
		delete(q.inFlight, receiptHandle)
		return true
	}
	return false
}

// QueueDepth counts the messages waiting to be received and those received
// but not yet deleted or returned to the queue.
func (b *Broker) QueueDepth(q *Queue) (waiting, inFlight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages), len(q.inFlight)
}

func (b *Broker) PurgeQueue(q *Queue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.messages = q.messages[:0]
	q.inFlight = make(map[string]*Message)
}

// RequeueExpired moves expired in-flight messages back to the queue.
func (b *Broker) RequeueExpired(q *Queue) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	count := 0
	for handle, msg := range q.inFlight {
		if now.After(msg.VisibleAt) {
			msg.ReceiptHandle = ""
			q.messages = append(q.messages, msg)
			delete(q.inFlight, handle)
			count++
		}
	}
	if count > 0 {
		q.cond.Signal()
	}
	return count
}

// --- SNS envelope ---

type snsEnvelope struct {
	Type              string                     `json:"Type"`
	MessageID         string                     `json:"MessageId"`
	TopicArn          string                     `json:"TopicArn"`
	Message           string                     `json:"Message"`
	Timestamp         string                     `json:"Timestamp"`
	MessageAttributes map[string]snsEnvelopeAttr `json:"MessageAttributes,omitempty"`
}

type snsEnvelopeAttr struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

func buildSNSEnvelope(messageID, topicARN, message string, attrs map[string]MessageAttribute) (string, error) {
	env := snsEnvelope{
		Type:      "Notification",
		MessageID: messageID,
		TopicArn:  topicARN,
		Message:   message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	if len(attrs) > 0 {
		env.MessageAttributes = make(map[string]snsEnvelopeAttr, len(attrs))
		for k, v := range attrs {
			env.MessageAttributes[k] = snsEnvelopeAttr{
				Type:  v.DataType,
				Value: v.StringValue,
			}
		}
	}

	data, err := json.Marshal(env)
	if err != nil {
		return "", fmt.Errorf("failed to marshal SNS envelope: %w", err)
	}
	return string(data), nil
}

// StartRequeueLoop runs a background goroutine that periodically requeues
// expired in-flight messages across all queues. Cancel the context to stop.
func (b *Broker) StartRequeueLoop(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.mu.RLock()
				queues := make([]*Queue, 0, len(b.queues))
				for _, q := range b.queues {
					queues = append(queues, q)
				}
				b.mu.RUnlock()

				for _, q := range queues {
					b.RequeueExpired(q)
				}
			}
		}
	}()
	return done
}
