package messaging

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type sqsError struct{ Code, Message string }

func (e *sqsError) Error() string                { return e.Code + ": " + e.Message }
func newSQSError(code, message string) *sqsError { return &sqsError{Code: code, Message: message} }

type sqsMessageAttribute struct {
	DataType         string   `json:"DataType"`
	StringValue      string   `json:"StringValue,omitempty"`
	BinaryValue      []byte   `json:"BinaryValue,omitempty"`
	StringListValues []string `json:"StringListValues,omitempty"`
	BinaryListValues [][]byte `json:"BinaryListValues,omitempty"`
}
type sqsBatchEntry struct {
	ID                                         string `json:"Id"`
	MessageBody                                string `json:"MessageBody"`
	ReceiptHandle                              string `json:"ReceiptHandle"`
	DelaySeconds                               *int   `json:"DelaySeconds"`
	VisibilityTimeout                          *int   `json:"VisibilityTimeout"`
	MessageAttributes, MessageSystemAttributes map[string]sqsMessageAttribute
	MessageGroupID                             string `json:"MessageGroupId"`
	MessageDeduplicationID                     string `json:"MessageDeduplicationId"`
}
type sqsRequest struct {
	QueueURL                                                              string `json:"QueueUrl"`
	QueueName                                                             string
	QueueOwnerAWSAccountID                                                string `json:"QueueOwnerAWSAccountId"`
	QueueNamePrefix                                                       string
	Attributes                                                            map[string]string
	AttributeNames, MessageSystemAttributeNames, MessageAttributeNames    []string
	Tags                                                                  map[string]string `json:"Tags"`
	TagKeys                                                               []string
	Label                                                                 string
	AWSAccountIDs                                                         []string `json:"AWSAccountIds"`
	Actions                                                               []string
	SourceARN                                                             string `json:"SourceArn"`
	DestinationARN                                                        string `json:"DestinationArn"`
	TaskHandle                                                            string
	MaxResults                                                            *int
	NextToken                                                             string
	MaxNumberOfMessagesPerSecond                                          *int
	MessageBody                                                           string
	ReceiptHandle                                                         string
	DelaySeconds, VisibilityTimeout, MaxNumberOfMessages, WaitTimeSeconds *int
	MessageAttributes, MessageSystemAttributes                            map[string]sqsMessageAttribute
	MessageGroupID                                                        string `json:"MessageGroupId"`
	MessageDeduplicationID                                                string `json:"MessageDeduplicationId"`
	ReceiveRequestAttemptID                                               string `json:"ReceiveRequestAttemptId"`
	Entries                                                               []sqsBatchEntry
}

func (s *Handler) queueForURL(raw string) (*Queue, *sqsError) {
	if raw == "" {
		return nil, newSQSError("MissingParameter", "QueueUrl is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" {
		return nil, newSQSError("InvalidAddress", "QueueUrl is invalid")
	}
	parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(parsed.Path, "/"), "/"), "/")
	if len(parts) != 2 || parts[1] == "" || (parts[0] != "queue" && parts[0] != s.broker.accountID) {
		return nil, newSQSError("InvalidAddress", "QueueUrl must identify a queue in this account")
	}
	if !queueNamePattern.MatchString(parts[1]) || len(parts[1]) > 80 {
		return nil, newSQSError("InvalidAddress", "QueueUrl is invalid")
	}

	q := s.broker.GetQueue(queueNameFromURL(raw))
	if q == nil {
		return nil, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
	}
	q.mu.Lock()
	deleted := q.deleted
	q.mu.Unlock()
	if deleted {
		return nil, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
	}
	return q, nil
}
func cloneStringMap(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for name, value := range values {
		result[name] = value
	}
	return result
}
func (b *Broker) createSQSQueue(name string, attrs, tags map[string]string) (*Queue, *sqsError) {
	base := name
	if strings.HasSuffix(name, ".fifo") {
		base = strings.TrimSuffix(name, ".fifo")
	}
	if len(name) < 1 || len(name) > 80 || !queueNamePattern.MatchString(base) {
		return nil, newSQSError("InvalidParameterValue", "QueueName must be 1 to 80 characters using letters, numbers, hyphens, and underscores")
	}
	if err := validateQueueAttributes(attrs, true); err != nil {
		return nil, err
	}
	if err := validateSQSQueueTags(tags); err != nil {
		return nil, err
	}
	merged := defaultQueueAttributes()
	for key, value := range attrs {
		merged[key] = value
	}
	if (merged["FifoQueue"] == "true") != strings.HasSuffix(name, ".fifo") {
		return nil, newSQSError("InvalidParameterValue", "FifoQueue must match the .fifo queue suffix")
	}
	if merged["FifoQueue"] == "true" {
		if _, ok := merged["ContentBasedDeduplication"]; !ok {
			merged["ContentBasedDeduplication"] = "false"
		}
		if merged["DeduplicationScope"] == "" {
			merged["DeduplicationScope"] = "queue"
		}
		if merged["FifoThroughputLimit"] == "" {
			merged["FifoThroughputLimit"] = "perQueue"
		}
	}
	if merged["KmsMasterKeyId"] != "" {
		if _, explicit := attrs["SqsManagedSseEnabled"]; !explicit {
			delete(merged, "SqsManagedSseEnabled")
		}
		if merged["KmsDataKeyReusePeriodSeconds"] == "" {
			merged["KmsDataKeyReusePeriodSeconds"] = "300"
		}
	}
	if err := validateQueueCombination(merged); err != nil {
		return nil, err
	}
	if err := b.validateQueueRedrive(name, merged); err != nil {
		return nil, err
	}
	for {
		b.mu.Lock()
		existing := b.queues[name]
		if existing == nil {
			if deleted, ok := b.sqsStateLocked().DeletedQueues[name]; ok && time.Since(deleted) < 60*time.Second {
				b.mu.Unlock()
				return nil, newSQSError("QueueDeletedRecently", "Wait 60 seconds after deleting a queue before recreating it")
			}
			q := newQueue(b, name, merged, cloneStringMap(tags))
			b.queues[name] = q
			b.arnIndex[q.ARN] = q
			b.mu.Unlock()
			return q, nil
		}
		b.mu.Unlock()
		existing.mu.Lock()
		if existing.deleted {
			existing.mu.Unlock()
			continue
		}
		for key, value := range attrs {
			if existing.Attributes[key] != value {
				existing.mu.Unlock()
				return nil, newSQSError("QueueNameExists", "A queue with this name already exists with different attributes")
			}
		}
		existing.mu.Unlock()
		return existing, nil
	}
}
func (b *Broker) validateQueueRedrive(name string, attrs map[string]string) *sqsError {
	raw := attrs["RedrivePolicy"]
	if raw == "" {
		return nil
	}
	policy, err := parseRedrivePolicy(raw)
	if err != nil {
		return err
	}
	destination := b.GetQueueByARN(policy.DeadLetterTargetARN)
	if destination == nil || destination.Name == name {
		return newSQSError("InvalidParameterValue", "RedrivePolicy must reference an existing, different queue")
	}
	destination.mu.Lock()
	defer destination.mu.Unlock()
	if destination.deleted || destination.fifoLocked() != (attrs["FifoQueue"] == "true") {
		return newSQSError("InvalidParameterValue", "The dead-letter queue must have the same queue type")
	}
	if raw := destination.Attributes["RedriveAllowPolicy"]; raw != "" {
		var allowed struct {
			Permission string   `json:"redrivePermission"`
			Sources    []string `json:"sourceQueueArns"`
		}
		_ = json.Unmarshal([]byte(raw), &allowed)
		if allowed.Permission == "denyAll" {
			return newSQSError("InvalidParameterValue", "Dead-letter queue does not allow this source")
		}
		if allowed.Permission == "byQueue" {
			arn := fmt.Sprintf("arn:aws:sqs:%s:%s:%s", b.region, b.accountID, name)
			found := false
			for _, source := range allowed.Sources {
				found = found || source == arn
			}
			if !found {
				return newSQSError("InvalidParameterValue", "Dead-letter queue does not allow this source")
			}
		}
	}
	return nil
}
func (s *Handler) setQueueAttributes(q *Queue, attrs map[string]string) *sqsError {
	if len(attrs) == 0 {
		return newSQSError("MissingParameter", "Attributes is required")
	}
	if err := validateQueueAttributes(attrs, false); err != nil {
		return err
	}
	for {
		q.mu.Lock()
		snapshot := cloneStringMap(q.Attributes)
		q.mu.Unlock()
		merged := cloneStringMap(snapshot)
		for name, value := range attrs {
			if value == "" && (name == "RedrivePolicy" || name == "RedriveAllowPolicy" || name == "Policy" || name == "KmsMasterKeyId") {
				delete(merged, name)
			} else {
				merged[name] = value
			}
		}
		if attrs["KmsMasterKeyId"] != "" {
			if _, explicit := attrs["SqsManagedSseEnabled"]; !explicit {
				delete(merged, "SqsManagedSseEnabled")
			}
		}
		if err := validateQueueCombination(merged); err != nil {
			return err
		}
		if err := s.broker.validateQueueRedrive(q.Name, merged); err != nil {
			return err
		}
		q.mu.Lock()
		if q.deleted {
			q.mu.Unlock()
			return newSQSError("QueueDoesNotExist", "The specified queue does not exist")
		}
		if !equalSQSAttributes(snapshot, q.Attributes) {
			q.mu.Unlock()
			continue
		}
		q.Attributes = merged
		if _, changed := attrs["DelaySeconds"]; changed && q.fifoLocked() {
			delay, _ := strconv.Atoi(merged["DelaySeconds"])
			for _, message := range q.messages {
				if message.FirstReceivedAt.IsZero() {
					message.VisibleAt = message.SentTimestamp.Add(time.Duration(delay) * time.Second)
				}
			}
		}
		q.LastModifiedTimestamp = time.Now()
		applyQueueDurationsLocked(q)
		notifyQueueLocked(q)
		q.mu.Unlock()
		return nil
	}
}
func equalSQSAttributes(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for name, value := range left {
		if other, ok := right[name]; !ok || other != value {
			return false
		}
	}
	return true
}

var sqsReadableAttributes = map[string]bool{"Policy": true, "VisibilityTimeout": true, "MaximumMessageSize": true, "MessageRetentionPeriod": true, "ApproximateNumberOfMessages": true, "ApproximateNumberOfMessagesNotVisible": true, "CreatedTimestamp": true, "LastModifiedTimestamp": true, "QueueArn": true, "ApproximateNumberOfMessagesDelayed": true, "DelaySeconds": true, "ReceiveMessageWaitTimeSeconds": true, "RedrivePolicy": true, "FifoQueue": true, "ContentBasedDeduplication": true, "KmsMasterKeyId": true, "KmsDataKeyReusePeriodSeconds": true, "DeduplicationScope": true, "FifoThroughputLimit": true, "RedriveAllowPolicy": true, "SqsManagedSseEnabled": true}

func (s *Handler) queueAttributes(q *Queue) map[string]string {
	now := time.Now()
	s.broker.redriveExpiredSQS(q, now)
	q.mu.Lock()
	defer q.mu.Unlock()
	pruneQueueLocked(q, now)
	values := cloneStringMap(q.Attributes)
	visible, delayed := 0, 0
	for _, msg := range q.messages {
		if now.Before(msg.VisibleAt) {
			delayed++
		} else {
			visible++
		}
	}
	values["QueueArn"] = q.ARN
	values["CreatedTimestamp"] = strconv.FormatInt(q.CreatedTimestamp.Unix(), 10)
	values["LastModifiedTimestamp"] = strconv.FormatInt(q.LastModifiedTimestamp.Unix(), 10)
	values["ApproximateNumberOfMessages"] = strconv.Itoa(visible)
	values["ApproximateNumberOfMessagesNotVisible"] = strconv.Itoa(len(q.inFlight))
	values["ApproximateNumberOfMessagesDelayed"] = strconv.Itoa(delayed)
	return values
}
func attributeMap(values map[string]sqsMessageAttribute) (map[string]MessageAttribute, *sqsError) {
	result := make(map[string]MessageAttribute, len(values))
	for name, value := range values {
		if len(value.StringListValues) > 0 || len(value.BinaryListValues) > 0 {
			return nil, newSQSError("InvalidParameterValue", "List message attribute values are reserved and unsupported by SQS")
		}
		result[name] = MessageAttribute{DataType: value.DataType, StringValue: value.StringValue, BinaryValue: append([]byte(nil), value.BinaryValue...)}
	}
	return result, nil
}
func (input sqsRequest) queueMessageInput() (QueueMessageInput, *sqsError) {
	attrs, err := attributeMap(input.MessageAttributes)
	if err != nil {
		return QueueMessageInput{}, err
	}
	system, err := attributeMap(input.MessageSystemAttributes)
	if err != nil {
		return QueueMessageInput{}, err
	}
	return QueueMessageInput{Body: input.MessageBody, Attributes: attrs, SystemAttributes: system, DelaySeconds: input.DelaySeconds, MessageGroupID: input.MessageGroupID, MessageDeduplicationID: input.MessageDeduplicationID}, nil
}
func sendResultMap(result QueueSendResult) map[string]any {
	values := map[string]any{"MessageId": result.MessageID, "MD5OfMessageBody": result.MD5OfMessageBody}
	if result.MD5OfMessageAttributes != "" {
		values["MD5OfMessageAttributes"] = result.MD5OfMessageAttributes
	}
	if result.MD5OfMessageSystemAttributes != "" {
		values["MD5OfMessageSystemAttributes"] = result.MD5OfMessageSystemAttributes
	}
	if result.SequenceNumber != "" {
		values["SequenceNumber"] = result.SequenceNumber
	}
	return values
}
func (s *Handler) executeSQS(ctx context.Context, action string, input sqsRequest) (map[string]any, *sqsError) {
	s.broker.advanceSQSMessageMoveTasks(time.Now())
	switch action {
	case "CreateQueue":
		if input.QueueName == "" {
			return nil, newSQSError("MissingParameter", "QueueName is required")
		}
		q, err := s.broker.createSQSQueue(input.QueueName, input.Attributes, input.Tags)
		if err != nil {
			return nil, err
		}
		return map[string]any{"QueueUrl": q.URL}, nil
	case "GetQueueUrl":
		if input.QueueName == "" {
			return nil, newSQSError("MissingParameter", "QueueName is required")
		}
		q := s.broker.GetQueue(input.QueueName)
		if q == nil || (input.QueueOwnerAWSAccountID != "" && input.QueueOwnerAWSAccountID != s.broker.accountID) {
			return nil, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
		}
		return map[string]any{"QueueUrl": q.URL}, nil
	case "ListQueues":
		var urls []string
		for _, q := range s.broker.ListQueues() {
			if strings.HasPrefix(q.Name, input.QueueNamePrefix) {
				urls = append(urls, q.URL)
			}
		}
		page, next, err := paginateSQSStrings(urls, input.MaxResults, input.NextToken, "queues:"+input.QueueNamePrefix)
		if err != nil {
			return nil, err
		}
		out := map[string]any{"QueueUrls": page}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	case "TagQueue", "ListQueueTags", "UntagQueue", "AddPermission", "RemovePermission", "ListDeadLetterSourceQueues", "StartMessageMoveTask", "ListMessageMoveTasks", "CancelMessageMoveTask":
		return s.executeSQSManagement(action, input)
	}
	switch action {
	case "GetQueueAttributes", "SetQueueAttributes", "SendMessage", "SendMessageBatch", "ReceiveMessage", "DeleteMessage", "DeleteMessageBatch", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "PurgeQueue", "DeleteQueue":
	default:
		return nil, newSQSError("InvalidAction", "Unknown SQS action: "+action)
	}
	q, err := s.queueForURL(input.QueueURL)
	if err != nil {
		return nil, err
	}
	switch action {
	case "DeleteQueue":
		if !s.broker.DeleteQueue(q.Name) {
			return nil, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
		}
		return map[string]any{}, nil
	case "PurgeQueue":
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.deleted {
			return nil, newSQSError("QueueDoesNotExist", "The specified queue does not exist")
		}
		if !q.LastPurgeTimestamp.IsZero() && time.Since(q.LastPurgeTimestamp) < 60*time.Second {
			return nil, newSQSError("PurgeQueueInProgress", "Only one PurgeQueue request is allowed every 60 seconds")
		}
		releaseAllDevSQSCustodyLocked(q)
		q.messages = nil
		q.inFlight = make(map[string]*Message)
		q.attempts = make(map[string]sqsReceiveAttempt)
		q.LastPurgeTimestamp = time.Now()
		notifyQueueLocked(q)
		return map[string]any{}, nil
	case "SetQueueAttributes":
		return map[string]any{}, s.setQueueAttributes(q, input.Attributes)
	case "GetQueueAttributes":
		values := s.queueAttributes(q)
		selected := make(map[string]string)
		for _, name := range input.AttributeNames {
			if name == "All" {
				return map[string]any{"Attributes": values}, nil
			}
			if !sqsReadableAttributes[name] {
				return nil, newSQSError("InvalidAttributeName", "Unknown attribute: "+name)
			}
			if value, ok := values[name]; ok {
				selected[name] = value
			}
		}
		return map[string]any{"Attributes": selected}, nil
	case "SendMessage":
		message, err := input.queueMessageInput()
		if err != nil {
			return nil, err
		}
		result, sendErr := s.broker.SendQueueMessage(q, message)
		if sendErr != nil {
			return nil, sendErr.(*sqsError)
		}
		return sendResultMap(result), nil
	case "ReceiveMessage":
		return s.receiveSQSResponse(ctx, q, input)
	case "DeleteMessage":
		return map[string]any{}, s.deleteSQSReceipt(q, input.ReceiptHandle)
	case "ChangeMessageVisibility":
		return map[string]any{}, s.changeSQSVisibility(q, input.ReceiptHandle, input.VisibilityTimeout)
	case "SendMessageBatch", "DeleteMessageBatch", "ChangeMessageVisibilityBatch":
		return s.executeSQSBatch(q, action, input.Entries)
	}
	return nil, newSQSError("InvalidAction", "Unknown SQS action")
}
func (s *Handler) deleteSQSReceipt(q *Queue, handle string) *sqsError {
	if handle == "" {
		return newSQSError("MissingParameter", "ReceiptHandle is required")
	}
	_, failure := s.broker.deleteSQSReceipt(q, handle)
	return failure
}
func (s *Handler) changeSQSVisibility(q *Queue, handle string, seconds *int) *sqsError {
	if handle == "" || seconds == nil {
		return newSQSError("MissingParameter", "ReceiptHandle and VisibilityTimeout are required")
	}
	if *seconds < 0 || *seconds > 43200 {
		return newSQSError("InvalidParameterValue", "VisibilityTimeout must be between 0 and 43200")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	if q.deleted {
		return newSQSError("QueueDoesNotExist", "The specified queue does not exist")
	}
	message := q.inFlight[handle]
	if message == nil || !now.Before(message.VisibleAt) {
		return newSQSError("ReceiptHandleIsInvalid", "The receipt handle is invalid or expired")
	}
	if now.Add(time.Duration(*seconds) * time.Second).After(message.ReceivedAt.Add(12 * time.Hour)) {
		return newSQSError("InvalidParameterValue", "VisibilityTimeout exceeds the maximum time left")
	}
	message.VisibleAt = now.Add(time.Duration(*seconds) * time.Second)
	invalidateReceiveAttemptsLocked(q, handle)
	notifyQueueLocked(q)
	return nil
}
func (s *Handler) executeSQSBatch(q *Queue, action string, entries []sqsBatchEntry) (map[string]any, *sqsError) {
	if len(entries) == 0 {
		return nil, newSQSError("EmptyBatchRequest", "The batch request must contain at least one entry")
	}
	if len(entries) > 10 {
		return nil, newSQSError("TooManyEntriesInBatchRequest", "The batch request cannot contain more than ten entries")
	}
	ids := make(map[string]bool)
	total := 0
	for _, entry := range entries {
		if !batchIDPattern.MatchString(entry.ID) || len(entry.ID) > 80 {
			return nil, newSQSError("InvalidBatchEntryId", "Batch entry Id must use 1 to 80 letters, digits, hyphens, or underscores")
		}
		if ids[entry.ID] {
			return nil, newSQSError("BatchEntryIdsNotDistinct", "Batch entry Id values must be distinct")
		}
		ids[entry.ID] = true
		if action == "SendMessageBatch" {
			attrs, _ := attributeMap(entry.MessageAttributes)
			total += messageSize(entry.MessageBody, attrs)
		}
	}
	if action == "SendMessageBatch" && total > 1048576 {
		return nil, newSQSError("BatchRequestTooLong", "The combined message batch exceeds 1 MiB")
	}
	successful := make([]map[string]any, 0, len(entries))
	failed := make([]map[string]any, 0)
	for _, entry := range entries {
		var result map[string]any
		var err *sqsError
		switch action {
		case "SendMessageBatch":
			input := sqsRequest{MessageBody: entry.MessageBody, MessageAttributes: entry.MessageAttributes, MessageSystemAttributes: entry.MessageSystemAttributes, DelaySeconds: entry.DelaySeconds, MessageGroupID: entry.MessageGroupID, MessageDeduplicationID: entry.MessageDeduplicationID}
			message, conversionErr := input.queueMessageInput()
			err = conversionErr
			if err == nil {
				sent, sendErr := s.broker.SendQueueMessage(q, message)
				if sendErr != nil {
					err = sendErr.(*sqsError)
				} else {
					result = sendResultMap(sent)
				}
			}
		case "DeleteMessageBatch":
			err = s.deleteSQSReceipt(q, entry.ReceiptHandle)
		case "ChangeMessageVisibilityBatch":
			err = s.changeSQSVisibility(q, entry.ReceiptHandle, entry.VisibilityTimeout)
		}
		if err != nil {
			failed = append(failed, map[string]any{"Id": entry.ID, "Code": err.Code, "Message": err.Message, "SenderFault": err.Code != "ServiceUnavailable"})
		} else {
			if result == nil {
				result = map[string]any{}
			}
			result["Id"] = entry.ID
			successful = append(successful, result)
		}
	}
	return map[string]any{"Successful": successful, "Failed": failed}, nil
}

var sqsSystemAttributeNames = map[string]bool{"SenderId": true, "SentTimestamp": true, "ApproximateReceiveCount": true, "ApproximateFirstReceiveTimestamp": true, "SequenceNumber": true, "MessageDeduplicationId": true, "MessageGroupId": true, "AWSTraceHeader": true, "DeadLetterQueueSourceArn": true}

func (s *Handler) receiveSQSResponse(ctx context.Context, q *Queue, input sqsRequest) (map[string]any, *sqsError) {
	max := 1
	if input.MaxNumberOfMessages != nil {
		max = *input.MaxNumberOfMessages
	}
	if max < 1 || max > 10 {
		return nil, newSQSError("InvalidParameterValue", "MaxNumberOfMessages must be between 1 and 10")
	}
	q.mu.Lock()
	wait, _ := strconv.Atoi(q.Attributes["ReceiveMessageWaitTimeSeconds"])
	fifo := q.fifoLocked()
	q.mu.Unlock()
	if input.WaitTimeSeconds != nil {
		wait = *input.WaitTimeSeconds
	}
	if wait < 0 || wait > 20 {
		return nil, newSQSError("InvalidParameterValue", "WaitTimeSeconds must be between 0 and 20")
	}
	var visibility *time.Duration
	if input.VisibilityTimeout != nil {
		if *input.VisibilityTimeout < 0 || *input.VisibilityTimeout > 43200 {
			return nil, newSQSError("InvalidParameterValue", "VisibilityTimeout must be between 0 and 43200")
		}
		value := time.Duration(*input.VisibilityTimeout) * time.Second
		visibility = &value
	}
	if input.ReceiveRequestAttemptID != "" && (!fifo || (len(input.ReceiveRequestAttemptID) > 128 || !validSQSIdentifier(input.ReceiveRequestAttemptID))) {
		return nil, newSQSError("InvalidParameterValue", "ReceiveRequestAttemptId requires a FIFO queue and at most 128 visible characters")
	}
	systemNames := append(append([]string(nil), input.AttributeNames...), input.MessageSystemAttributeNames...)
	for _, name := range systemNames {
		if name != "All" && !sqsSystemAttributeNames[name] {
			return nil, newSQSError("InvalidAttributeName", "Unknown message system attribute: "+name)
		}
	}
	messages, err := s.broker.receiveSQS(ctx, q, max, time.Duration(wait)*time.Second, visibility, input.ReceiveRequestAttemptID)
	if err != nil {
		return nil, err
	}
	result := make([]map[string]any, 0, len(messages))
	for _, msg := range messages {
		values := sqsMessageSystemValues(msg)
		selected := make(map[string]string)
		for _, name := range systemNames {
			if name == "All" {
				selected = values
				break
			}
			if value, ok := values[name]; ok {
				selected[name] = value
			}
		}
		attrs := make(map[string]sqsMessageAttribute)
		for name, value := range msg.Attributes {
			for _, pattern := range input.MessageAttributeNames {
				if pattern == "All" || pattern == ".*" || pattern == name || (strings.HasSuffix(pattern, ".*") && strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))) {
					attrs[name] = sqsMessageAttribute{DataType: value.DataType, StringValue: value.StringValue, BinaryValue: append([]byte(nil), value.BinaryValue...)}
					break
				}
			}
		}
		item := map[string]any{"MessageId": msg.ID, "ReceiptHandle": msg.ReceiptHandle, "MD5OfBody": md5Body(msg.Body), "Body": msg.Body}
		if len(selected) > 0 {
			item["Attributes"] = selected
		}
		if len(attrs) > 0 {
			item["MessageAttributes"] = attrs
			metadata, _ := attributeMap(attrs)
			item["MD5OfMessageAttributes"] = md5Attributes(metadata)
		}
		result = append(result, item)
	}
	return map[string]any{"Messages": result}, nil
}
func paginateSQSStrings(values []string, maximum *int, token, scope string) ([]string, string, *sqsError) {
	limit := 1000
	if maximum != nil {
		limit = *maximum
	}
	if limit < 1 || limit > 1000 {
		return nil, "", newSQSError("InvalidParameterValue", "MaxResults must be between 1 and 1000")
	}
	start := 0
	if token != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(token)
		var cursor struct{ Scope, Last string }
		if err != nil || json.Unmarshal(decoded, &cursor) != nil || cursor.Scope != scope || cursor.Last == "" {
			return nil, "", newSQSError("InvalidParameterValue", "NextToken is invalid for this request")
		}
		start = sort.SearchStrings(values, cursor.Last)
		if start < len(values) && values[start] == cursor.Last {
			start++
		}
	}
	end := start + limit
	if end > len(values) {
		end = len(values)
	}
	page := append([]string{}, values[start:end]...)
	next := ""
	if maximum != nil && end < len(values) {
		encoded, _ := json.Marshal(struct{ Scope, Last string }{scope, values[end-1]})
		next = base64.RawURLEncoding.EncodeToString(encoded)
	}
	return page, next, nil
}
