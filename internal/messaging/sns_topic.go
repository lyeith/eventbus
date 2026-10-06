package messaging

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/mail"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Topic struct {
	ARN, Name            string
	Subscriptions        []*Subscription
	Attributes, Tags     map[string]string
	DataProtectionPolicy string
	mu                   sync.RWMutex
	// No Broker mutex or queue code acquires these ownership locks. State is
	// snapshotted under mu, then released before capture or queue lookups.
	controlMu            sync.Mutex
	publishMu            sync.Mutex
	sequence             uint64
	dedup                map[string]snsDeduplication
	archive              []snsArchivedPublication
	archiveBytes         int
	deletedSubscriptions map[string]*Subscription
}

type Subscription struct {
	ARN, TopicARN, Protocol, Endpoint string
	FilterPolicy                      *FilterPolicy
	Attributes                        map[string]string
	Pending                           bool
	Token                             string
	TokenExpires                      time.Time
	Paused                            bool
}

type snsDeduplication struct {
	Result  SNSPublishResult
	Expires time.Time
}

func (b *Broker) CreateTopic(name string) *Topic {
	attrs := map[string]string{}
	if strings.HasSuffix(name, ".fifo") {
		attrs["FifoTopic"] = "true"
	}
	topic, _ := b.CreateTopicWithAttributes(name, attrs, nil, "")
	return topic
}

func (b *Broker) CreateTopicWithAttributes(name string, attributes, tags map[string]string, protection string) (*Topic, error) {
	if len(name) > 256 || !snsTopicName.MatchString(name) {
		return nil, snsInvalid("Invalid topic name")
	}
	fifo := strings.HasSuffix(name, ".fifo")
	if fifo != (attributes["FifoTopic"] == "true") {
		return nil, snsInvalid("FIFO topic names must end in .fifo and set FifoTopic=true")
	}
	defaults := b.topicDefaults(name)
	for key, value := range attributes {
		if err := validateSNSTopicAttribute(key, value, fifo, true); err != nil {
			return nil, err
		}
		defaults[key] = value
	}
	if err := validateSNSTags(tags); err != nil {
		return nil, err
	}
	if protection != "" {
		if fifo {
			return nil, snsInvalid("DataProtectionPolicy is supported only on standard topics")
		}
		if err := validateSNSProtectionPolicy(protection); err != nil {
			return nil, err
		}
	}
	arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", b.region, b.accountID, name)
	b.mu.Lock()
	defer b.mu.Unlock()
	if topic := b.topics[arn]; topic != nil {
		return topic, nil
	}
	topic := &Topic{ARN: arn, Name: name, Attributes: defaults, Tags: maps.Clone(tags), DataProtectionPolicy: protection, dedup: make(map[string]snsDeduplication)}
	if topic.Tags == nil {
		topic.Tags = make(map[string]string)
	}
	b.topics[arn] = topic
	return topic, nil
}

func (b *Broker) topicDefaults(name string) map[string]string {
	arn := fmt.Sprintf("arn:aws:sns:%s:%s:%s", b.region, b.accountID, name)
	policy, _ := json.Marshal(map[string]any{"Version": "2008-10-17", "Id": "__default_policy_ID", "Statement": []any{map[string]any{"Sid": "__default_statement_ID", "Effect": "Allow", "Principal": map[string]any{"AWS": "*"}, "Action": []string{"SNS:GetTopicAttributes", "SNS:SetTopicAttributes", "SNS:AddPermission", "SNS:RemovePermission", "SNS:DeleteTopic", "SNS:Subscribe", "SNS:ListSubscriptionsByTopic", "SNS:Publish"}, "Resource": arn, "Condition": map[string]any{"StringEquals": map[string]string{"AWS:SourceOwner": b.accountID}}}}})
	attrs := map[string]string{"DisplayName": "", "Policy": string(policy), "TracingConfig": "PassThrough", "SignatureVersion": "1", "MaximumMessageSize": "262144", "FifoTopic": "false"}
	if strings.HasSuffix(name, ".fifo") {
		attrs["FifoTopic"] = "true"
		attrs["ContentBasedDeduplication"] = "false"
		attrs["FifoThroughputScope"] = "Topic"
	}
	return attrs
}

func validateSNSTopicAttribute(name, value string, fifo, creating bool) error {
	switch name {
	case "FifoTopic":
		if !creating {
			return snsInvalid("FifoTopic cannot be changed")
		}
		if value != "true" && value != "false" {
			return snsInvalid("FifoTopic must be true or false")
		}
	case "DisplayName":
		if len(value) > 100 {
			return snsInvalid("DisplayName exceeds 100 bytes")
		}
	case "Policy":
		var object map[string]any
		if json.Unmarshal([]byte(value), &object) != nil || object == nil {
			return snsInvalid("Policy must be a JSON object")
		}
		if _, ok := object["Statement"]; !ok {
			return snsInvalid("Policy must contain Statement")
		}
	case "DeliveryPolicy":
		var object map[string]any
		if json.Unmarshal([]byte(value), &object) != nil || object == nil {
			return snsInvalid("DeliveryPolicy must be a JSON object")
		}
	case "TracingConfig":
		if fifo || value != "PassThrough" && value != "Active" {
			return snsInvalid("TracingConfig must be PassThrough or Active on a standard topic")
		}
	case "SignatureVersion":
		if value != "1" && value != "2" {
			return snsInvalid("SignatureVersion must be 1 or 2")
		}
	case "MaximumMessageSize":
		size, err := strconv.Atoi(value)
		if err != nil || size < 1024 || size > 1048576 {
			return snsInvalid("MaximumMessageSize must be 1024..1048576")
		}
	case "ContentBasedDeduplication":
		if !fifo || value != "true" && value != "false" {
			return snsInvalid("ContentBasedDeduplication requires FIFO and a boolean value")
		}
	case "FifoThroughputScope":
		if !fifo || value != "Topic" && value != "MessageGroup" {
			return snsInvalid("FifoThroughputScope must be Topic or MessageGroup for FIFO")
		}
	case "ArchivePolicy":
		if !fifo {
			return snsInvalid("ArchivePolicy requires a FIFO topic")
		}
		if _, err := snsArchiveRetention(value); err != nil {
			return err
		}
	case "KmsMasterKeyId":
		if value == "" {
			return snsInvalid("KmsMasterKeyId must not be empty")
		}
	default:
		prefixes := []string{"HTTP", "Firehose", "Lambda", "Application", "SQS"}
		allowed := false
		for _, prefix := range prefixes {
			if name == prefix+"SuccessFeedbackRoleArn" || name == prefix+"FailureFeedbackRoleArn" {
				allowed = true
				if value != "" && !strings.HasPrefix(value, "arn:") {
					return snsInvalid("Feedback role must be an ARN")
				}
			}
			if name == prefix+"SuccessFeedbackSampleRate" {
				allowed = true
				rate, err := strconv.Atoi(value)
				if err != nil || rate < 0 || rate > 100 {
					return snsInvalid("Feedback sample rate must be 0..100")
				}
			}
		}
		if !allowed {
			return snsInvalid("Invalid topic attribute: " + name)
		}
	}
	return nil
}

func validateSNSProtectionPolicy(value string) error {
	var policy map[string]any
	if len(value) > 30720 || json.Unmarshal([]byte(value), &policy) != nil || policy == nil {
		return snsInvalid("DataProtectionPolicy must be a JSON object of at most 30720 bytes")
	}
	if _, ok := policy["Statement"]; !ok {
		return snsInvalid("DataProtectionPolicy requires Statement")
	}
	return nil
}

func validateSNSTags(tags map[string]string) error {
	if len(tags) > 50 {
		return &snsError{400, "TagLimitExceeded", "Cannot add more than 50 tags to a topic"}
	}
	for key, value := range tags {
		if key == "" || len([]rune(key)) > 128 || len([]rune(value)) > 256 || strings.HasPrefix(strings.ToLower(key), "aws:") {
			return snsInvalid("Invalid tag key or value")
		}
	}
	return nil
}

func (b *Broker) GetTopic(arn string) *Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.topics[arn]
}
func (b *Broker) ListTopics() []*Topic {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, topic := range b.topics {
		topics = append(topics, topic)
	}
	b.mu.RUnlock()
	sort.Slice(topics, func(i, j int) bool { return topics[i].ARN < topics[j].ARN })
	return topics
}
func (b *Broker) DeleteTopic(arn string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.topics[arn] == nil {
		return false
	}
	delete(b.topics, arn)
	return true
}

func (b *Broker) topicAttributes(topic *Topic) map[string]string {
	topic.publishMu.Lock()
	defer topic.publishMu.Unlock()
	topic.mu.RLock()
	defer topic.mu.RUnlock()
	attributes := maps.Clone(topic.Attributes)
	retention, _ := snsArchiveRetention(attributes["ArchivePolicy"])
	snsPruneArchive(topic, retention, time.Now())
	if len(topic.archive) > 0 {
		attributes["BeginningArchiveTime"] = topic.archive[0].PublishedAt.UTC().Format(time.RFC3339Nano)
	}
	confirmed, pending := 0, 0
	for _, subscription := range topic.Subscriptions {
		if subscription.Pending {
			pending++
		} else {
			confirmed++
		}
	}
	attributes["TopicArn"] = topic.ARN
	attributes["Owner"] = b.accountID
	attributes["SubscriptionsConfirmed"] = strconv.Itoa(confirmed)
	attributes["SubscriptionsPending"] = strconv.Itoa(pending)
	attributes["SubscriptionsDeleted"] = "0"
	if _, exists := attributes["EffectiveDeliveryPolicy"]; !exists {
		if policy := attributes["DeliveryPolicy"]; policy != "" {
			attributes["EffectiveDeliveryPolicy"] = policy
		} else {
			attributes["EffectiveDeliveryPolicy"] = `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":20,"maxDelayTarget":20,"numRetries":3,"numMaxDelayRetries":0,"numNoDelayRetries":0,"numMinDelayRetries":0,"backoffFunction":"linear"},"disableSubscriptionOverrides":false}}`
		}
	}
	return attributes
}

var errSubscriptionAttributesDiffer = errors.New("subscription already exists with different attributes")

func (b *Broker) Subscribe(topicARN, protocol, endpoint string, filter *FilterPolicy) (*Subscription, error) {
	attributes := map[string]string{}
	if filter != nil {
		attributes["FilterPolicy"] = filter.JSON()
	}
	return b.subscribeSNS(topicARN, protocol, endpoint, attributes, "")
}

func (b *Broker) validateSNSSubscriptionEndpoint(protocol, endpoint string) error {
	switch protocol {
	case "sqs", "lambda", "firehose":
		parts := strings.SplitN(endpoint, ":", 6)
		expected := protocol
		if protocol == "firehose" {
			expected = "firehose"
		}
		if len(parts) != 6 || parts[0] != "arn" || parts[2] != expected || parts[3] == "" || parts[4] == "" || parts[5] == "" {
			return snsInvalid("Invalid " + protocol + " endpoint ARN")
		}
	case "application":
		return b.validateSNSApplicationEndpoint(endpoint)
	case "sms":
		if !smsE164Pattern.MatchString(endpoint) {
			return snsInvalid("SMS endpoint must be an E.164 phone number")
		}
	case "http", "https":
		parsed, err := url.Parse(endpoint)
		if err != nil || parsed.Scheme != protocol || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
			return snsInvalid("Invalid HTTP/S endpoint")
		}
	case "email", "email-json":
		parsed, err := mail.ParseAddress(endpoint)
		if err != nil || parsed.Address != endpoint {
			return snsInvalid("Invalid email endpoint")
		}
	default:
		return snsInvalid("Invalid subscription protocol")
	}
	return nil
}

// SNS owns redrive policy validation. This checks immutable resource identity,
// without acquiring Broker/queue locks while a caller holds topic state locks.
func (b *Broker) validateSNSRedrivePolicy(topicARN, raw string) error {
	if raw == "" {
		return nil
	}
	var policy map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &policy) != nil || len(policy) != 1 {
		return snsInvalid("RedrivePolicy must contain only deadLetterTargetArn")
	}
	var arn string
	if json.Unmarshal(policy["deadLetterTargetArn"], &arn) != nil {
		return snsInvalid("RedrivePolicy requires a deadLetterTargetArn")
	}
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "sqs" || parts[3] != b.region || parts[4] != b.accountID {
		return snsInvalid("SNS dead-letter queue must be in the subscription's account and region")
	}
	fifo := strings.HasSuffix(parts[5], ".fifo")
	name := strings.TrimSuffix(parts[5], ".fifo")
	if name == "" || len(parts[5]) > 80 || strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" {
		return snsInvalid("Invalid dead-letter queue ARN")
	}
	if fifo != strings.HasSuffix(topicARN, ".fifo") {
		return snsInvalid("SNS dead-letter queue type must match the topic's FIFO type")
	}
	return nil
}

func validateSNSSubscriptionAttributes(attributes map[string]string, protocol string) (*FilterPolicy, error) {
	scope := attributes["FilterPolicyScope"]
	if scope == "" {
		scope = "MessageAttributes"
	}
	if scope != "MessageAttributes" && scope != "MessageBody" {
		return nil, snsInvalid("FilterPolicyScope must be MessageAttributes or MessageBody")
	}
	filter, err := ParseFilterPolicy(attributes["FilterPolicy"])
	if err != nil {
		return nil, snsInvalid("Invalid FilterPolicy: " + err.Error())
	}
	if filter != nil {
		if err := filter.ValidateScope(scope); err != nil {
			return nil, snsInvalid("Invalid FilterPolicy: " + err.Error())
		}
	}
	for name, value := range attributes {
		switch name {
		case "FilterPolicy", "FilterPolicyScope":
		case "RawMessageDelivery":
			if value != "true" && value != "false" {
				return nil, snsInvalid("RawMessageDelivery must be true or false")
			}
			if value == "true" && protocol != "sqs" && protocol != "http" && protocol != "https" && protocol != "firehose" {
				return nil, snsInvalid("RawMessageDelivery is unsupported for this protocol")
			}
		case "DeliveryPolicy":
			var policy map[string]any
			if protocol != "http" && protocol != "https" || json.Unmarshal([]byte(value), &policy) != nil || policy == nil {
				return nil, snsInvalid("DeliveryPolicy requires HTTP/S and valid JSON")
			}
		case "RedrivePolicy":
			// Broker validates the policy and its topic/resource relationship.
		case "SubscriptionRoleArn":
			if protocol != "firehose" || !strings.Contains(value, ":iam:") {
				return nil, snsInvalid("SubscriptionRoleArn requires a Firehose subscription and IAM role ARN")
			}
		case "ReplayPolicy":
			if _, _, err := snsReplayPolicy(value); err != nil {
				return nil, err
			}
		default:
			return nil, snsInvalid("Invalid subscription attribute: " + name)
		}
	}
	if protocol == "firehose" && attributes["SubscriptionRoleArn"] == "" {
		return nil, snsInvalid("Firehose subscriptions require SubscriptionRoleArn")
	}
	return filter, nil
}

func (b *Broker) subscribeSNS(topicARN, protocol, endpoint string, attributes map[string]string, requestID string) (*Subscription, error) {
	if !snsTopicARNValid(topicARN) {
		return nil, snsInvalid("Invalid TopicArn")
	}
	topic := b.GetTopic(topicARN)
	if topic == nil {
		return nil, snsNotFound("Topic does not exist")
	}
	if err := b.validateSNSSubscriptionEndpoint(protocol, endpoint); err != nil {
		return nil, err
	}
	filter, err := validateSNSSubscriptionAttributes(attributes, protocol)
	if err != nil {
		return nil, err
	}
	if err := b.validateSNSRedrivePolicy(topicARN, attributes["RedrivePolicy"]); err != nil {
		return nil, err
	}
	normalized := maps.Clone(attributes)
	if normalized == nil {
		normalized = make(map[string]string)
	}
	if normalized["RawMessageDelivery"] == "" {
		normalized["RawMessageDelivery"] = "false"
	}
	if normalized["FilterPolicyScope"] == "" {
		normalized["FilterPolicyScope"] = "MessageAttributes"
	}
	topic.controlMu.Lock()
	defer topic.controlMu.Unlock()
	topic.mu.RLock()
	for _, existing := range topic.Subscriptions {
		if existing.Protocol == protocol && existing.Endpoint == endpoint {
			equal := existing.FilterPolicy.Equal(filter)
			for name, value := range normalized {
				if name != "FilterPolicy" && existing.Attributes[name] != value {
					equal = false
				}
			}
			snapshot := cloneSNSSubscription(existing)
			topic.mu.RUnlock()
			if !equal {
				return nil, errSubscriptionAttributesDiffer
			}
			return snapshot, nil
		}
	}
	maxSize, _ := strconv.Atoi(topic.Attributes["MaximumMessageSize"])
	fifo := topic.Attributes["FifoTopic"] == "true"
	count := len(topic.Subscriptions)
	topic.mu.RUnlock()
	if fifo && protocol != "sqs" {
		return nil, snsInvalid("FIFO topics support only SQS subscriptions")
	}
	if maxSize > 262144 && (protocol != "sqs" && protocol != "lambda" && protocol != "firehose" || count >= 100) {
		return nil, snsInvalid("Topics larger than 256 KiB require at most 100 SQS, Lambda or Firehose subscriptions")
	}
	if normalized["ReplayPolicy"] != "" && !fifo {
		return nil, snsInvalid("ReplayPolicy requires a FIFO topic")
	}
	pending := protocol == "http" || protocol == "https" || protocol == "email" || protocol == "email-json"
	subscription := &Subscription{ARN: topicARN + ":" + uuid.NewString(), TopicARN: topicARN, Protocol: protocol, Endpoint: endpoint, Attributes: normalized, FilterPolicy: filter, Pending: pending}
	if pending {
		subscription.Token = uuid.NewString()
		subscription.TokenExpires = time.Now().Add(48 * time.Hour)
		message, _ := json.Marshal(map[string]string{"Type": "SubscriptionConfirmation", "Token": subscription.Token, "TopicArn": topicARN, "Message": "Confirm the subscription using this token", "SubscribeURL": fmt.Sprintf("http://localhost:%d/?Action=ConfirmSubscription&TopicArn=%s&Token=%s", b.port, url.QueryEscape(topicARN), subscription.Token)})
		if err := b.CaptureSNS(SNSCaptureRecord{Operation: "Subscribe", RequestID: requestID, TargetARN: topicARN, Message: string(message), Deliveries: []SNSCaptureDelivery{{Protocol: protocol, Endpoint: endpoint, Status: "captured"}}, Details: map[string]any{"token": subscription.Token, "subscription_arn": subscription.ARN}}); err != nil {
			return nil, snsInternal("Unable to capture subscription confirmation")
		}
	}
	if policy := normalized["ReplayPolicy"]; policy != "" {
		topic.publishMu.Lock()
		defer topic.publishMu.Unlock()
		if err := b.snsReplay(topic, subscription, policy, "Subscribe", requestID); err != nil {
			return nil, err
		}
		_, end, _ := snsReplayPolicy(policy)
		subscription.Paused = !end.IsZero()
		subscription.Attributes["ReplayStatus"] = "Completed"
	}
	topic.mu.Lock()
	topic.Subscriptions = append(topic.Subscriptions, subscription)
	topic.mu.Unlock()
	return cloneSNSSubscription(subscription), nil
}

func cloneSNSSubscription(subscription *Subscription) *Subscription {
	copy := *subscription
	copy.Attributes = maps.Clone(subscription.Attributes)
	return &copy
}

func (b *Broker) ListSubscriptionsByTopic(topicARN string) ([]*Subscription, error) {
	topic := b.GetTopic(topicARN)
	if topic == nil {
		return nil, snsNotFound("Topic does not exist")
	}
	topic.mu.RLock()
	defer topic.mu.RUnlock()
	subscriptions := make([]*Subscription, len(topic.Subscriptions))
	for index, subscription := range topic.Subscriptions {
		subscriptions[index] = cloneSNSSubscription(subscription)
	}
	sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].ARN < subscriptions[j].ARN })
	return subscriptions, nil
}

func (b *Broker) findSNSSubscription(arn string) (*Topic, *Subscription) {
	position := strings.LastIndex(arn, ":")
	if position < 0 {
		return nil, nil
	}
	topic := b.GetTopic(arn[:position])
	if topic == nil {
		return nil, nil
	}
	topic.mu.RLock()
	defer topic.mu.RUnlock()
	for _, subscription := range topic.Subscriptions {
		if subscription.ARN == arn {
			return topic, cloneSNSSubscription(subscription)
		}
	}
	return nil, nil
}

func (b *Broker) Publish(topicARN, message string, attributes map[string]MessageAttribute) (string, error) {
	result, err := b.PublishSNS(SNSPublishInput{TopicARN: topicARN, Message: message, Attributes: attributes})
	return result.MessageID, err
}

func (b *Broker) PublishSNS(input SNSPublishInput) (SNSPublishResult, error) {
	targets := 0
	if input.TopicARN != "" {
		targets++
	}
	if input.TargetARN != "" {
		targets++
	}
	if input.PhoneNumber != "" {
		targets++
	}
	if targets != 1 {
		return SNSPublishResult{}, snsInvalid("Exactly one of TopicArn, TargetArn or PhoneNumber is required")
	}
	if input.Operation == "" {
		input.Operation = "Publish"
	}
	if input.TargetARN != "" && snsTopicARNValid(input.TargetARN) {
		input.TopicARN, input.TargetARN = input.TargetARN, ""
	}
	maxSize := 262144
	var topic *Topic
	if input.TopicARN != "" {
		if !snsTopicARNValid(input.TopicARN) {
			return SNSPublishResult{}, snsInvalid("Invalid TopicArn")
		}
		topic = b.GetTopic(input.TopicARN)
		if topic == nil {
			return SNSPublishResult{}, snsNotFound("Topic does not exist")
		}
		topic.mu.RLock()
		maxSize, _ = strconv.Atoi(topic.Attributes["MaximumMessageSize"])
		topic.mu.RUnlock()
	}
	if err := validateSNSPublish(input, maxSize); err != nil {
		return SNSPublishResult{}, err
	}
	if input.PhoneNumber != "" {
		if input.MessageGroupID != "" || input.MessageDeduplicationID != "" {
			return SNSPublishResult{}, snsInvalid("FIFO parameters require a topic")
		}
		return b.publishSMS(input)
	}
	if input.TargetARN != "" {
		if input.MessageGroupID != "" || input.MessageDeduplicationID != "" {
			return SNSPublishResult{}, snsInvalid("FIFO parameters require a topic")
		}
		return b.publishMobile(input)
	}
	topic.publishMu.Lock()
	defer topic.publishMu.Unlock()
	topic.mu.RLock()
	attributes := maps.Clone(topic.Attributes)
	subscriptions := make([]*Subscription, len(topic.Subscriptions))
	for index, subscription := range topic.Subscriptions {
		subscriptions[index] = cloneSNSSubscription(subscription)
	}
	topic.mu.RUnlock()
	fifo := attributes["FifoTopic"] == "true"
	currentMax, _ := strconv.Atoi(attributes["MaximumMessageSize"])
	if err := validateSNSPublish(input, currentMax); err != nil {
		return SNSPublishResult{}, err
	}
	if fifo && input.MessageGroupID == "" {
		return SNSPublishResult{}, snsInvalid("FIFO publications require MessageGroupId")
	}
	if !fifo && input.MessageDeduplicationID != "" {
		return SNSPublishResult{}, snsInvalid("MessageDeduplicationId requires a FIFO topic")
	}
	dedupID := input.MessageDeduplicationID
	if fifo && dedupID == "" {
		if attributes["ContentBasedDeduplication"] != "true" {
			return SNSPublishResult{}, snsInvalid("FIFO publications require MessageDeduplicationId or ContentBasedDeduplication")
		}
		hash := sha256.Sum256([]byte(input.Message))
		dedupID = hex.EncodeToString(hash[:])
	}
	key := dedupID
	if fifo && attributes["FifoThroughputScope"] == "MessageGroup" {
		key = input.MessageGroupID + "\x00" + dedupID
	}
	now := time.Now()
	retention, _ := snsArchiveRetention(attributes["ArchivePolicy"])
	for key, value := range topic.dedup {
		if !now.Before(value.Expires) {
			delete(topic.dedup, key)
		}
	}
	if fifo {
		if previous, exists := topic.dedup[key]; exists {
			if err := b.CaptureSNS(snsCapturePublication(input, previous.Result, []SNSCaptureDelivery{}, map[string]any{"deduplicated": true})); err != nil {
				return SNSPublishResult{}, snsInternal("Unable to capture publication")
			}
			return previous.Result, nil
		}
	}
	result := SNSPublishResult{MessageID: uuid.NewString()}
	if err := snsPrepareArchive(topic, input, retention, now); err != nil {
		return SNSPublishResult{}, err
	}
	if fifo {
		result.SequenceNumber = strconv.FormatUint(topic.sequence+1, 10)
	}
	plans, deliveries := b.snsPlanDeliveries(input, result, subscriptions, now, false)
	if err := b.CaptureSNS(snsCapturePublication(input, result, deliveries, nil)); err != nil {
		return SNSPublishResult{}, snsInternal("Unable to capture publication")
	}
	if fifo {
		topic.sequence++
		topic.dedup[key] = snsDeduplication{result, now.Add(5 * time.Minute)}
	}
	if retention > 0 {
		snsCommitArchive(topic, input, result, now)
	}
	if err := b.snsSendDeliveries(input, result, plans, dedupID, false); err != nil {
		// The publication is already accepted. Reporting a retryable refusal
		// here would duplicate deliveries on a standard topic. Capture's
		// terminal failure rejects the next request before any acceptance.
		log.Printf("SNS outcome capture failed after acceptance operation=%s request_id=%s message_id=%s", input.Operation, input.RequestID, result.MessageID)
	}
	return result, nil
}

func snsCapturePublication(input SNSPublishInput, result SNSPublishResult, deliveries []SNSCaptureDelivery, details map[string]any) SNSCaptureRecord {
	return SNSCaptureRecord{Operation: input.Operation, RequestID: input.RequestID, MessageID: result.MessageID, TargetARN: input.TopicARN, PhoneNumber: input.PhoneNumber, Subject: input.Subject, Message: input.Message, MessageStructure: input.MessageStructure, MessageAttributes: input.Attributes, MessageGroupID: input.MessageGroupID, MessageDeduplicationID: input.MessageDeduplicationID, SequenceNumber: result.SequenceNumber, Deliveries: deliveries, Details: details}
}

type snsEnvelope struct {
	Type              string                     `json:"Type"`
	MessageID         string                     `json:"MessageId"`
	TopicArn          string                     `json:"TopicArn"`
	Subject           string                     `json:"Subject,omitempty"`
	Message           string                     `json:"Message"`
	Timestamp         string                     `json:"Timestamp"`
	SequenceNumber    string                     `json:"SequenceNumber,omitempty"`
	Replayed          bool                       `json:"Replayed,omitempty"`
	MessageAttributes map[string]snsEnvelopeAttr `json:"MessageAttributes,omitempty"`
}
type snsEnvelopeAttr struct {
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

func buildSNSEnvelope(messageID, topicARN, message string, attrs map[string]MessageAttribute) (string, error) {
	return buildSNSEnvelopeWithSubject(messageID, topicARN, message, "", attrs)
}
func buildSNSEnvelopeWithSubject(messageID, topicARN, message, subject string, attrs map[string]MessageAttribute) (string, error) {
	return buildSNSNotification(SNSPublishInput{TopicARN: topicARN, Subject: subject}, SNSPublishResult{MessageID: messageID}, message, attrs, time.Now(), false)
}
func buildSNSNotification(input SNSPublishInput, result SNSPublishResult, message string, attrs map[string]MessageAttribute, publishedAt time.Time, replayed bool) (string, error) {
	envelope := snsEnvelope{Type: "Notification", MessageID: result.MessageID, TopicArn: input.TopicARN, Subject: input.Subject, Message: message, Timestamp: publishedAt.UTC().Format(time.RFC3339Nano), SequenceNumber: result.SequenceNumber, Replayed: replayed}
	if len(attrs) > 0 {
		envelope.MessageAttributes = make(map[string]snsEnvelopeAttr, len(attrs))
		for name, attribute := range attrs {
			value := attribute.StringValue
			if strings.SplitN(attribute.DataType, ".", 2)[0] == "Binary" {
				value = base64.StdEncoding.EncodeToString(attribute.BinaryValue)
			}
			envelope.MessageAttributes[name] = snsEnvelopeAttr{attribute.DataType, value}
		}
	}
	data, err := json.Marshal(envelope)
	return string(data), err
}
