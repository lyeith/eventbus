package messaging

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Queue owns all mutable SQS delivery state. The public identifiers never
// change; delivery snapshots returned to consumers are copies.
type Queue struct {
	Name, ARN, URL                          string
	messages                                []*Message
	inFlight                                map[string]*Message
	VisibilityTimeout, RetentionPeriod      time.Duration
	Attributes, Tags                        map[string]string
	CreatedTimestamp, LastModifiedTimestamp time.Time
	LastPurgeTimestamp                      time.Time
	OriginalSourceARN                       string
	deleted                                 bool
	receipts                                map[string]sqsReceipt
	dedup                                   map[string]sqsDedupEntry
	dedupExpiry                             time.Time
	attempts                                map[string]sqsReceiveAttempt
	sequence                                uint64
	lastFairGroup                           string
	notify                                  chan struct{}
	mu                                      sync.Mutex
	cond                                    *sync.Cond
	devCustody                              *devQueueCustody
}
type Message struct {
	ID, Body, ReceiptHandle                                               string
	SentTimestamp, VisibleAt, ReceivedAt, FirstReceivedAt                 time.Time
	ReceiveCount                                                          int
	Attributes, SystemAttributes                                          map[string]MessageAttribute
	GroupID, DeduplicationID, SequenceNumber, SenderID, OriginalSourceARN string
}
type sqsDedupEntry struct {
	ID, Sequence string
	Expires      time.Time
}
type sqsReceiveAttempt struct {
	Handles []string
	IDs     []string
	Expires time.Time
	Invalid bool
}
type sqsBrokerState struct {
	mu            sync.Mutex
	DeletedQueues map[string]time.Time
	MoveTasks     map[string]*sqsMoveTask
}

func (b *Broker) sqsStateLocked() *sqsBrokerState {
	if b.sqs == nil {
		b.sqs = &sqsBrokerState{DeletedQueues: make(map[string]time.Time), MoveTasks: make(map[string]*sqsMoveTask)}
	}
	return b.sqs
}

func (b *Broker) sqsState() *sqsBrokerState {
	b.mu.Lock()
	state := b.sqsStateLocked()
	b.mu.Unlock()
	return state
}

// QueueMessageInput is the SNS/consumer-independent enqueue contract.
type QueueMessageInput struct {
	Body                                             string
	Attributes, SystemAttributes                     map[string]MessageAttribute
	DelaySeconds                                     *int
	MessageGroupID, MessageDeduplicationID, SenderID string
}
type QueueSendResult struct{ MessageID, MD5OfMessageBody, MD5OfMessageAttributes, MD5OfMessageSystemAttributes, SequenceNumber string }

func defaultQueueAttributes() map[string]string {
	return map[string]string{"VisibilityTimeout": "30", "MessageRetentionPeriod": "345600", "DelaySeconds": "0", "MaximumMessageSize": "1048576", "ReceiveMessageWaitTimeSeconds": "0", "SqsManagedSseEnabled": "true"}
}
func newQueue(b *Broker, name string, attrs, tags map[string]string) *Queue {
	now := time.Now()
	q := &Queue{Name: name, ARN: fmt.Sprintf("arn:aws:sqs:%s:%s:%s", b.region, b.accountID, name), URL: fmt.Sprintf("http://localhost:%d/queue/%s", b.port, name), messages: make([]*Message, 0), inFlight: make(map[string]*Message), Attributes: attrs, Tags: tags, CreatedTimestamp: now, LastModifiedTimestamp: now, receipts: make(map[string]sqsReceipt), dedup: make(map[string]sqsDedupEntry), attempts: make(map[string]sqsReceiveAttempt), notify: make(chan struct{})}
	q.cond = sync.NewCond(&q.mu)
	q.devCustody = newDevQueueCustodyLocked(b)
	applyQueueDurationsLocked(q)
	return q
}
func applyQueueDurationsLocked(q *Queue) {
	visibility, _ := strconv.Atoi(q.Attributes["VisibilityTimeout"])
	retention, _ := strconv.Atoi(q.Attributes["MessageRetentionPeriod"])
	q.VisibilityTimeout = time.Duration(visibility) * time.Second
	q.RetentionPeriod = time.Duration(retention) * time.Second
}
func notifyQueueLocked(q *Queue)            { close(q.notify); q.notify = make(chan struct{}); q.cond.Broadcast() }
func (q *Queue) nextSequenceLocked() string { q.sequence++; return strconv.FormatUint(q.sequence, 10) }
func (q *Queue) fifoLocked() bool           { return q.Attributes["FifoQueue"] == "true" }
func cloneMessageAttributes(attributes map[string]MessageAttribute) map[string]MessageAttribute {
	if attributes == nil {
		return nil
	}
	copy := make(map[string]MessageAttribute, len(attributes))
	for name, value := range attributes {
		value.BinaryValue = append([]byte(nil), value.BinaryValue...)
		copy[name] = value
	}
	return copy
}
func cloneMessage(msg *Message) *Message {
	copy := *msg
	copy.Attributes = cloneMessageAttributes(msg.Attributes)
	copy.SystemAttributes = cloneMessageAttributes(msg.SystemAttributes)
	return &copy
}

// CreateQueue preserves the harness's direct broker constructor. API creation
// uses createSQSQueue so explicitly supplied zero attributes remain zero.
func (b *Broker) CreateQueue(name string, visibility, retention time.Duration) *Queue {
	b.mu.Lock()
	defer b.mu.Unlock()
	if existing := b.queues[name]; existing != nil {
		return existing
	}
	attrs := defaultQueueAttributes()
	if visibility != 0 {
		attrs["VisibilityTimeout"] = strconv.Itoa(int(visibility.Seconds()))
	}
	if retention != 0 {
		attrs["MessageRetentionPeriod"] = strconv.Itoa(int(retention.Seconds()))
	}
	q := newQueue(b, name, attrs, make(map[string]string))
	if visibility != 0 {
		q.VisibilityTimeout = visibility
	}
	if retention != 0 {
		q.RetentionPeriod = retention
	}
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
	sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })
	return queues
}
func (b *Broker) GetQueueByARN(arn string) *Queue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.arnIndex[arn]
}
func (b *Broker) DeleteQueue(name string) bool {
	b.mu.Lock()
	q := b.queues[name]
	if q == nil {
		b.mu.Unlock()
		return false
	}
	delete(b.arnIndex, q.ARN)
	delete(b.queues, name)
	b.sqsStateLocked().DeletedQueues[name] = time.Now()
	b.mu.Unlock()
	q.mu.Lock()
	q.deleted = true
	releaseAllDevSQSCustodyLocked(q)
	q.messages = nil
	q.inFlight = make(map[string]*Message)
	notifyQueueLocked(q)
	q.mu.Unlock()
	return true
}
func (b *Broker) enqueueMessage(q *Queue, body string) {
	_, _ = b.SendQueueMessage(q, QueueMessageInput{Body: body})
}

func (b *Broker) SendQueueMessage(q *Queue, input QueueMessageInput) (QueueSendResult, error) {
	if q == nil {
		return QueueSendResult{}, newSQSError("QueueDoesNotExist", "Queue does not exist")
	}
	if err := validateMessageInput(input); err != nil {
		return QueueSendResult{}, err
	}
	input.Attributes = cloneMessageAttributes(input.Attributes)
	for name, attribute := range input.Attributes {
		if strings.SplitN(attribute.DataType, ".", 2)[0] == "Number" {
			attribute.StringValue, _ = normalizeSQSNumber(attribute.StringValue)
			input.Attributes[name] = attribute
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.deleted {
		return QueueSendResult{}, newSQSError("QueueDoesNotExist", "Queue does not exist")
	}
	maximum, _ := strconv.Atoi(q.Attributes["MaximumMessageSize"])
	if messageSize(input.Body, input.Attributes) > maximum {
		return QueueSendResult{}, newSQSError("InvalidParameterValue", "Message exceeds MaximumMessageSize")
	}
	delay, _ := strconv.Atoi(q.Attributes["DelaySeconds"])
	if input.DelaySeconds != nil {
		delay = *input.DelaySeconds
	}
	if q.fifoLocked() {
		if input.DelaySeconds != nil {
			return QueueSendResult{}, newSQSError("InvalidParameterValue", "DelaySeconds is not supported per message on FIFO queues")
		}
		if input.MessageGroupID == "" {
			return QueueSendResult{}, newSQSError("MissingParameter", "MessageGroupId is required for FIFO queues")
		}
		if input.MessageDeduplicationID == "" {
			if q.Attributes["ContentBasedDeduplication"] != "true" {
				return QueueSendResult{}, newSQSError("InvalidParameterValue", "MessageDeduplicationId is required when content-based deduplication is disabled")
			}
			sum := sha256.Sum256([]byte(input.Body))
			input.MessageDeduplicationID = hex.EncodeToString(sum[:])
		}
	} else if input.MessageDeduplicationID != "" {
		return QueueSendResult{}, newSQSError("InvalidParameterValue", "MessageDeduplicationId is only supported on FIFO queues")
	}
	result := QueueSendResult{MD5OfMessageBody: md5Body(input.Body), MD5OfMessageAttributes: md5Attributes(input.Attributes), MD5OfMessageSystemAttributes: md5Attributes(input.SystemAttributes)}
	now := time.Now()
	dedupKey := sqsDedupKeyLocked(q, input.MessageGroupID, input.MessageDeduplicationID)
	if q.fifoLocked() {
		if existing, ok := q.dedup[dedupKey]; ok && now.Before(existing.Expires) {
			result.MessageID = existing.ID
			result.SequenceNumber = existing.Sequence
			return result, nil
		}
	}
	result.MessageID = uuid.NewString()
	if err := admitDevSQSMessageLocked(q, result.MessageID); err != nil {
		return QueueSendResult{}, err
	}
	if q.fifoLocked() {
		result.SequenceNumber = q.nextSequenceLocked()
		rememberSQSDedupLocked(q, dedupKey, result.MessageID, result.SequenceNumber, now)
	}
	sender := input.SenderID
	if sender == "" {
		sender = b.accountID
	}
	message := &Message{ID: result.MessageID, Body: input.Body, SentTimestamp: now, VisibleAt: now.Add(time.Duration(delay) * time.Second), Attributes: cloneMessageAttributes(input.Attributes), SystemAttributes: cloneMessageAttributes(input.SystemAttributes), GroupID: input.MessageGroupID, DeduplicationID: input.MessageDeduplicationID, SequenceNumber: result.SequenceNumber, SenderID: sender}
	q.messages = append(q.messages, message)
	notifyQueueLocked(q)
	return result, nil
}

func sqsDedupKeyLocked(q *Queue, group, id string) string {
	if q.Attributes["DeduplicationScope"] == "messageGroup" {
		return group + "\x00" + id
	}
	return id
}

func pruneSQSDedupLocked(q *Queue, now time.Time) {
	pruneDedupEntries(q.dedup, &q.dedupExpiry, now, func(entry sqsDedupEntry) time.Time { return entry.Expires })
}

func rememberSQSDedupLocked(q *Queue, key, id, sequence string, now time.Time) {
	pruneSQSDedupLocked(q, now)
	if q.dedup == nil {
		q.dedup = make(map[string]sqsDedupEntry)
	}
	expiry := now.Add(5 * time.Minute)
	q.dedup[key] = sqsDedupEntry{ID: id, Sequence: sequence, Expires: expiry}
	lowerDedupExpiry(&q.dedupExpiry, expiry)
}

// pruneQueueLocked owns retention and visibility transitions; a receive never
// depends on the background maintenance ticker for a message to become visible.
func pruneQueueLocked(q *Queue, now time.Time) int {
	changed := false
	waiting := q.messages[:0]
	for _, message := range q.messages {
		if q.RetentionPeriod <= 0 || now.Before(message.SentTimestamp.Add(q.RetentionPeriod)) {
			waiting = append(waiting, message)
		} else {
			releaseDevSQSMessageLocked(q, message.ID)
			changed = true
		}
	}
	clear(q.messages[len(waiting):])
	q.messages = waiting
	count := 0
	for handle, message := range q.inFlight {
		if q.RetentionPeriod > 0 && !now.Before(message.SentTimestamp.Add(q.RetentionPeriod)) {
			delete(q.inFlight, handle)
			releaseDevSQSMessageLocked(q, message.ID)
			changed = true
			continue
		}
		if !now.Before(message.VisibleAt) {
			delete(q.inFlight, handle)
			message.ReceiptHandle = ""
			q.messages = append(q.messages, message)
			count++
		}
	}
	// New sends and transfers append destination-owned increasing sequences;
	// pruning/receiving only removes entries. Visibility returns are the only
	// insertion which can place an older FIFO sequence behind a waiting tail.
	if count > 0 && q.fifoLocked() {
		sort.SliceStable(q.messages, func(i, j int) bool {
			left, _ := strconv.ParseUint(q.messages[i].SequenceNumber, 10, 64)
			right, _ := strconv.ParseUint(q.messages[j].SequenceNumber, 10, 64)
			return left < right
		})
	}
	pruneSQSReceiptsLocked(q, now)
	for attempt, entry := range q.attempts {
		if !now.Before(entry.Expires) {
			delete(q.attempts, attempt)
		}
	}
	if count > 0 || changed {
		notifyQueueLocked(q)
	}
	return count
}
func (b *Broker) RequeueExpired(q *Queue) int {
	now := time.Now()
	b.redriveExpiredSQS(q, now)
	q.mu.Lock()
	defer q.mu.Unlock()
	return pruneQueueLocked(q, now)
}

type sqsReceiveOptions struct {
	ownedOnly       bool
	nativeSnapshots bool
}

type sqsReceiveResult struct {
	messages []*Message // Detached native snapshots, only when requested.
	count    int        // Actual leases, independent of their wire projection.
}

// admission observes a prospective native receive snapshot before its receipt,
// count or visibility is committed. Lambda batch byte admission uses this same
// FIFO/fairness owner; ordinary SQS receives pass nil and keep their wire flow.
func collectVisibleForReceiveLocked(q *Queue, max int, visibility time.Duration, now time.Time, admission func(*Message) bool, snapshots bool) sqsReceiveResult {
	fifo := q.fifoLocked()
	blocked := make(map[string]bool)
	if fifo {
		for _, message := range q.inFlight {
			blocked[message.GroupID] = true
		}
	}
	var result sqsReceiveResult
	// Queue.mu exclusively owns this slice. In-place stable compaction only
	// writes behind the current read index; returned delivery values are copies.
	candidates := q.messages
	remaining := candidates[:0]
	// Standard MessageGroupId provides tenant fairness without FIFO locking.
	if !fifo && q.lastFairGroup != "" {
		sort.SliceStable(candidates, func(i, j int) bool {
			return candidates[i].GroupID != q.lastFairGroup && candidates[j].GroupID == q.lastFairGroup
		})
	}
	for index, message := range candidates {
		if result.count >= max {
			remaining = append(remaining, candidates[index:]...)
			break
		}
		if now.Before(message.VisibleAt) || (fifo && blocked[message.GroupID]) {
			remaining = append(remaining, message)
			if fifo && now.Before(message.VisibleAt) {
				blocked[message.GroupID] = true
			}
			continue
		}
		prepared := *message
		prepared.ReceiptHandle = uuid.NewString()
		prepared.VisibleAt = now.Add(visibility)
		prepared.ReceivedAt = now
		if prepared.FirstReceivedAt.IsZero() {
			prepared.FirstReceivedAt = now
		}
		prepared.ReceiveCount++
		if admission != nil && !admission(&prepared) {
			remaining = append(remaining, message)
			if fifo {
				// A rejected candidate must block its group's tail even when an
				// earlier member was admitted into this same batch.
				blocked[message.GroupID] = true
			}
			continue
		}
		invalidateReceiveAttemptsForMessageLocked(q, message.ID)
		*message = prepared
		q.inFlight[message.ReceiptHandle] = message
		issueSQSReceiptLocked(q, message)
		q.lastFairGroup = message.GroupID
		result.count++
		if snapshots {
			result.messages = append(result.messages, cloneMessage(message))
		}
	}
	// The reused backing array must not retain selected/settled messages beyond
	// the waiting slice. In-flight ownership lives in q.inFlight independently.
	clear(candidates[len(remaining):])
	q.messages = remaining
	return result
}
func (b *Broker) ReceiveMessages(q *Queue, max int, wait time.Duration) []*Message {
	if max <= 0 {
		max = 1
	}
	if max > 10 {
		max = 10
	}
	if wait > maxWaitTime {
		wait = maxWaitTime
	}
	messages, _ := b.receiveSQS(context.Background(), q, max, wait, nil, "")
	return messages
}
func (b *Broker) receiveSQS(ctx context.Context, q *Queue, max int, wait time.Duration, visibility *time.Duration, attempt string) ([]*Message, *sqsError) {
	return b.receiveSQSWithAdmission(ctx, q, max, wait, visibility, attempt, nil)
}

func (b *Broker) receiveSQSWithAdmission(ctx context.Context, q *Queue, max int, wait time.Duration, visibility *time.Duration, attempt string, admission func(*Message) bool) ([]*Message, *sqsError) {
	result, failure := b.receiveSQSWithOwnership(ctx, q, max, wait, visibility, attempt, admission, sqsReceiveOptions{nativeSnapshots: true})
	return result.messages, failure
}

func (b *Broker) receiveSQSWithOwnership(ctx context.Context, q *Queue, max int, wait time.Duration, visibility *time.Duration, attempt string, admission func(*Message) bool, options sqsReceiveOptions) (sqsReceiveResult, *sqsError) {
	deadline := time.Now().Add(wait)
	for {
		now := time.Now()
		if options.ownedOnly {
			q.mu.Lock()
			empty := !hasDevSQSCustodyLocked(q)
			deleted := q.deleted
			q.mu.Unlock()
			if deleted {
				return sqsReceiveResult{}, newSQSError("QueueDoesNotExist", "Queue does not exist")
			}
			if empty {
				return sqsReceiveResult{}, nil
			}
		} else {
			b.advanceSQSMessageMoveTasks(now)
		}
		b.redriveExpiredSQS(q, now)
		q.mu.Lock()
		if q.deleted {
			q.mu.Unlock()
			return sqsReceiveResult{}, newSQSError("QueueDoesNotExist", "Queue does not exist")
		}
		pruneQueueLocked(q, now)
		if options.ownedOnly && !hasDevSQSCustodyLocked(q) {
			q.mu.Unlock()
			return sqsReceiveResult{}, nil
		}
		timeout := q.VisibilityTimeout
		if visibility != nil {
			timeout = *visibility
		}
		if attempt != "" {
			if cached, ok := q.attempts[attempt]; ok {
				if cached.Invalid {
					q.mu.Unlock()
					return sqsReceiveResult{}, newSQSError("InvalidParameterValue", "ReceiveRequestAttemptId has been modified")
				}
				selected := make([]*Message, 0, len(cached.Handles))
				for i, handle := range cached.Handles {
					message := q.inFlight[handle]
					if message == nil && i < len(cached.IDs) {
						for _, waiting := range q.messages {
							if waiting.ID == cached.IDs[i] && waiting.ReceiptHandle == "" {
								message = waiting
								break
							}
						}
					}
					if message == nil {
						q.mu.Unlock()
						return sqsReceiveResult{}, newSQSError("InvalidParameterValue", "ReceiveRequestAttemptId has been modified")
					}
					selected = append(selected, message)
				}
				restored := make(map[*Message]bool, len(selected))
				result := make([]*Message, 0, len(selected))
				for i, message := range selected {
					handle := cached.Handles[i]
					message.ReceiptHandle = handle
					message.VisibleAt = now.Add(timeout)
					q.inFlight[handle] = message
					restored[message] = true
					result = append(result, cloneMessage(message))
				}
				remaining := q.messages[:0]
				for _, message := range q.messages {
					if !restored[message] {
						remaining = append(remaining, message)
					}
				}
				clear(q.messages[len(remaining):])
				q.messages = remaining

				q.mu.Unlock()
				return sqsReceiveResult{messages: result, count: len(selected)}, nil
			}
		}
		candidateSeen := false
		choose := admission
		if admission != nil || options.ownedOnly {
			choose = func(candidate *Message) bool {
				if options.ownedOnly && !ownsDevSQSMessageLocked(q, candidate.ID) {
					return false
				}
				candidateSeen = true
				return admission == nil || admission(candidate)
			}
		}
		result := collectVisibleForReceiveLocked(q, max, timeout, now, choose, options.nativeSnapshots || attempt != "")
		if result.count > 0 {
			if attempt != "" {
				handles := make([]string, len(result.messages))
				ids := make([]string, len(result.messages))
				for i, message := range result.messages {
					handles[i] = message.ReceiptHandle
					ids[i] = message.ID
				}
				q.attempts[attempt] = sqsReceiveAttempt{Handles: handles, IDs: ids, Expires: now.Add(5 * time.Minute)}
			}
			q.mu.Unlock()
			return result, nil
		}
		// A consumer byte budget that rejects every eligible candidate is a
		// completed bounded receive, not an empty queue to repeatedly long poll.
		if candidateSeen || wait <= 0 || !now.Before(deadline) {
			q.mu.Unlock()
			return sqsReceiveResult{}, nil
		}
		wake := q.notify
		until := deadline.Sub(now)
		retentionWake := func(message *Message) {
			if options.ownedOnly && q.RetentionPeriod > 0 {
				remaining := message.SentTimestamp.Add(q.RetentionPeriod).Sub(now)
				if remaining > 0 && remaining < until {
					until = remaining
				}
			}
		}
		for _, message := range q.messages {
			retentionWake(message)
			if message.VisibleAt.After(now) && message.VisibleAt.Sub(now) < until {
				until = message.VisibleAt.Sub(now)
			}
		}
		for _, message := range q.inFlight {
			retentionWake(message)
			if message.VisibleAt.After(now) && message.VisibleAt.Sub(now) < until {
				until = message.VisibleAt.Sub(now)
			}
		}
		q.mu.Unlock()
		timer := time.NewTimer(until)
		select {
		case <-ctx.Done():
			timer.Stop()
			return sqsReceiveResult{}, nil
		case <-wake:
			timer.Stop()
		case <-timer.C:
		}
	}
}
func (b *Broker) ExtendMessageVisibility(q *Queue, receipt string, timeout time.Duration) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	message := q.inFlight[receipt]
	if message == nil {
		return false
	}
	message.VisibleAt = time.Now().Add(timeout)
	invalidateReceiveAttemptsLocked(q, receipt)
	notifyQueueLocked(q)
	return true
}
func invalidateReceiveAttemptsForMessageLocked(q *Queue, id string) {
	for attempt, entry := range q.attempts {
		for _, cachedID := range entry.IDs {
			if cachedID == id {
				entry.Invalid = true
				q.attempts[attempt] = entry
				break
			}
		}
	}
}
func invalidateReceiveAttemptsLocked(q *Queue, receipt string) {
	for attempt, entry := range q.attempts {
		for _, handle := range entry.Handles {
			if handle == receipt {
				entry.Invalid = true
				q.attempts[attempt] = entry
				break
			}
		}
	}
}

func (b *Broker) QueueDepth(q *Queue) (waiting, inFlight int) {
	now := time.Now()
	b.redriveExpiredSQS(q, now)
	q.mu.Lock()
	defer q.mu.Unlock()
	pruneQueueLocked(q, now)
	for _, message := range q.messages {
		if !now.Before(message.VisibleAt) {
			waiting++
		}
	}
	return waiting, len(q.inFlight)
}
func (b *Broker) PurgeQueue(q *Queue) {
	q.mu.Lock()
	defer q.mu.Unlock()
	releaseAllDevSQSCustodyLocked(q)
	q.messages = nil
	q.inFlight = make(map[string]*Message)
	q.attempts = make(map[string]sqsReceiveAttempt)
	q.LastPurgeTimestamp = time.Now()
	notifyQueueLocked(q)
}
func (b *Broker) MoveMessage(source, destination *Queue, receipt string) bool {
	return b.moveSQSMessage(source, destination, receipt, nil, 0)
}
func (b *Broker) moveSQSMessage(source, destination *Queue, receipt string, deadline *time.Time, maxCount int) bool {
	if source == nil || destination == nil || source == destination {
		return false
	}
	first, second := source, destination
	if first.Name > second.Name {
		first, second = second, first
	}
	first.mu.Lock()
	second.mu.Lock()
	defer second.mu.Unlock()
	defer first.mu.Unlock()
	message := source.inFlight[receipt]
	if message == nil || source.deleted || destination.deleted {
		return false
	}
	if deadline != nil && (deadline.Before(message.VisibleAt) || message.ReceiveCount < maxCount || source.RetentionPeriod > 0 && !deadline.Before(message.SentTimestamp.Add(source.RetentionPeriod))) {
		return false
	}
	if err := admitDevSQSMessageLocked(destination, message.ID); err != nil {
		return false
	}
	delete(source.inFlight, receipt)
	invalidateReceiveAttemptsLocked(source, receipt)
	message.ReceiptHandle = ""
	message.VisibleAt = time.Now()
	message.OriginalSourceARN = source.ARN
	if destination.fifoLocked() {
		message.SequenceNumber = destination.nextSequenceLocked()
		message.DeduplicationID = message.ID
		message.SentTimestamp = time.Now()
	}
	destination.messages = append(destination.messages, message)
	releaseDevSQSMessageLocked(source, message.ID)
	notifyQueueLocked(source)
	notifyQueueLocked(destination)
	return true
}
func (b *Broker) redriveExpiredSQS(source *Queue, now time.Time) {
	source.mu.Lock()
	raw := source.Attributes["RedrivePolicy"]
	source.mu.Unlock()
	if raw == "" {
		return
	}
	policy, err := parseRedrivePolicy(raw)
	if err != nil {
		return
	}
	destination := b.GetQueueByARN(policy.DeadLetterTargetARN)
	if destination == nil || destination == source {
		return
	}
	source.mu.Lock()
	var handles []string
	for handle, message := range source.inFlight {
		if !now.Before(message.VisibleAt) && message.ReceiveCount >= policy.MaxReceiveCount && (source.RetentionPeriod <= 0 || now.Before(message.SentTimestamp.Add(source.RetentionPeriod))) {
			handles = append(handles, handle)
		}
	}
	source.mu.Unlock()
	for _, handle := range handles {
		b.moveSQSMessage(source, destination, handle, &now, policy.MaxReceiveCount)
	}
}
func queueNameFromURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	parts := strings.Split(strings.TrimRight(parsed.Path, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}
