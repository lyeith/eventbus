package messaging

import (
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// sqsMoveTask is broker-owned state. sqsBrokerState.mu protects tasks and their
// progress; queue locks protect messages. No task creates an unowned goroutine.
type sqsMoveTask struct {
	Handle         string
	SourceARN      string
	SourceQueue    *Queue
	DestinationARN string
	RequestedRate  *int
	Started        time.Time
	Updated        time.Time
	Status         string
	FailureReason  string
	Moved          int64
	ToMove         int64
	Credits        float64
}

func (s *Handler) executeSQSManagement(action string, input sqsRequest) (map[string]any, *sqsError) {
	switch action {
	case "TagQueue", "ListQueueTags", "UntagQueue":
		return s.sqsQueueTags(action, input)
	case "AddPermission", "RemovePermission":
		return s.sqsQueuePermission(action, input)
	case "ListDeadLetterSourceQueues":
		return s.sqsDeadLetterSources(input)
	case "StartMessageMoveTask":
		return s.sqsStartMessageMove(input)
	case "ListMessageMoveTasks":
		return s.sqsListMessageMoves(input)
	case "CancelMessageMoveTask":
		return s.sqsCancelMessageMove(input)
	default:
		return nil, newSQSError("InvalidAction", "Unknown SQS action: "+action)
	}
}

func validateSQSQueueTags(tags map[string]string) *sqsError {
	for key, value := range tags {
		if !validSQSQueueTagPart(key, 128, false) || !validSQSQueueTagPart(value, 256, true) {
			return newSQSError("InvalidParameterValue", "Tag keys and values must satisfy SQS tag character and length restrictions.")
		}
	}
	return nil
}

func validSQSQueueTagPart(value string, max int, empty bool) bool {
	if !utf8.ValidString(value) || (!empty && value == "") || utf8.RuneCountInString(value) > max || strings.HasPrefix(strings.ToLower(value), "aws:") {
		return false
	}
	for _, r := range value {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && !unicode.IsSpace(r) && !strings.ContainsRune("_.:/=+-@", r) {
			return false
		}
	}
	return true
}

func (s *Handler) sqsQueueTags(action string, input sqsRequest) (map[string]any, *sqsError) {
	q, err := s.queueForURL(input.QueueURL)
	if err != nil {
		return nil, err
	}
	if action == "TagQueue" {
		if input.Tags == nil {
			return nil, newSQSError("MissingParameter", "Tags is required.")
		}
		if err := validateSQSQueueTags(input.Tags); err != nil {
			return nil, err
		}
	}
	if action == "UntagQueue" {
		if input.TagKeys == nil {
			return nil, newSQSError("MissingParameter", "TagKeys is required.")
		}
		for _, key := range input.TagKeys {
			if !validSQSQueueTagPart(key, 128, false) {
				return nil, newSQSError("InvalidParameterValue", "Invalid tag key.")
			}
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.deleted {
		return nil, newSQSError("QueueDoesNotExist", "Queue does not exist.")
	}
	switch action {
	case "ListQueueTags":
		tags := make(map[string]string, len(q.Tags))
		for k, v := range q.Tags {
			tags[k] = v
		}
		return map[string]any{"Tags": tags}, nil
	case "TagQueue":
		if q.Tags == nil {
			q.Tags = make(map[string]string)
		}
		for k, v := range input.Tags {
			q.Tags[k] = v
		}
	case "UntagQueue":
		for _, key := range input.TagKeys {
			delete(q.Tags, key)
		}
	}
	return map[string]any{}, nil
}

// Policy mutation preserves application-supplied statements and fields, including
// conditions. The emulator stores this policy; it does not grant IAM credentials.
func readSQSQueuePolicy(raw string) (map[string]json.RawMessage, []json.RawMessage, *sqsError) {
	policy := make(map[string]json.RawMessage)
	if raw == "" {
		policy["Version"] = json.RawMessage(`"2012-10-17"`)
		return policy, nil, nil
	}
	if json.Unmarshal([]byte(raw), &policy) != nil || policy == nil {
		return nil, nil, newSQSError("InvalidAttributeValue", "Queue Policy is not a JSON policy document.")
	}
	rawStatements, ok := policy["Statement"]
	if !ok {
		return policy, nil, nil
	}
	var statements []json.RawMessage
	if json.Unmarshal(rawStatements, &statements) == nil && statements != nil {
		return policy, statements, nil
	}
	var statement map[string]json.RawMessage
	if json.Unmarshal(rawStatements, &statement) == nil && statement != nil {
		return policy, []json.RawMessage{rawStatements}, nil
	}
	return nil, nil, newSQSError("InvalidAttributeValue", "Queue Policy Statement must be an object or array.")
}

func sqsPolicyStatementLabel(raw json.RawMessage) string {
	var statement struct{ Sid string }
	_ = json.Unmarshal(raw, &statement)
	return statement.Sid
}

func sqsPolicyPrincipalCount(raw json.RawMessage) int {
	var statement struct {
		Principal    any
		NotPrincipal any
	}
	if json.Unmarshal(raw, &statement) != nil {
		return 0
	}
	var count func(any) int
	count = func(value any) int {
		switch v := value.(type) {
		case string:
			return 1
		case []any:
			n := 0
			for _, item := range v {
				n += count(item)
			}
			return n
		case map[string]any:
			n := 0
			for _, item := range v {
				n += count(item)
			}
			return n
		}
		return 0
	}
	return count(statement.Principal) + count(statement.NotPrincipal)
}

func validSQSPermissionLabel(label string) bool {
	if label == "" || len(label) > 80 {
		return false
	}
	for _, r := range label {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return false
		}
	}
	return true
}

func validSQSAccountID(id string) bool {
	if len(id) != 12 {
		return false
	}
	for _, c := range id {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func validSQSPermissionAction(action string) bool {
	switch action {
	case "*", "AddPermission", "CancelMessageMoveTask", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "CreateQueue", "DeleteMessage", "DeleteMessageBatch", "DeleteQueue", "GetQueueAttributes", "GetQueueUrl", "ListDeadLetterSourceQueues", "ListMessageMoveTasks", "ListQueueTags", "ListQueues", "PurgeQueue", "ReceiveMessage", "RemovePermission", "SendMessage", "SendMessageBatch", "SetQueueAttributes", "StartMessageMoveTask", "TagQueue", "UntagQueue":
		return true
	}
	return false
}

func (s *Handler) sqsQueuePermission(action string, input sqsRequest) (map[string]any, *sqsError) {
	q, err := s.queueForURL(input.QueueURL)
	if err != nil {
		return nil, err
	}
	if input.Label == "" {
		return nil, newSQSError("MissingParameter", "Label is required.")
	}
	if !validSQSPermissionLabel(input.Label) {
		return nil, newSQSError("InvalidParameterValue", "Label must contain 1 to 80 alphanumeric, hyphen or underscore characters.")
	}
	if action == "AddPermission" {
		if len(input.Actions) == 0 || len(input.AWSAccountIDs) == 0 {
			return nil, newSQSError("MissingParameter", "Actions and AWSAccountIds are required.")
		}
		if len(input.Actions) > 7 || len(input.AWSAccountIDs) > 50 {
			return nil, newSQSError("OverLimit", "A policy statement supports at most seven actions and fifty principals.")
		}
		for _, account := range input.AWSAccountIDs {
			if !validSQSAccountID(account) {
				return nil, newSQSError("InvalidParameterValue", "AWSAccountIds must contain twelve-digit account numbers.")
			}
		}
		for _, a := range input.Actions {
			if !validSQSPermissionAction(a) {
				return nil, newSQSError("InvalidParameterValue", "Invalid SQS permission action: "+a)
			}
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.deleted {
		return nil, newSQSError("QueueDoesNotExist", "Queue does not exist.")
	}
	policy, statements, err := readSQSQueuePolicy(q.Attributes["Policy"])
	if err != nil {
		return nil, err
	}
	var retained []json.RawMessage
	matched := false
	for _, statement := range statements {
		if sqsPolicyStatementLabel(statement) == input.Label {
			matched = true
			continue
		}
		retained = append(retained, statement)
	}
	if action == "AddPermission" {
		if matched {
			return nil, newSQSError("InvalidParameterValue", "A permission with this Label already exists.")
		}
		if len(statements) >= 20 {
			return nil, newSQSError("OverLimit", "A queue policy supports at most twenty statements.")
		}
		principals := make([]string, 0, len(input.AWSAccountIDs))
		partition := strings.Split(q.ARN, ":")[1]
		for _, account := range input.AWSAccountIDs {
			principals = append(principals, "arn:"+partition+":iam::"+account+":root")
		}
		actions := make([]string, 0, len(input.Actions))
		for _, a := range input.Actions {
			actions = append(actions, "sqs:"+a)
		}
		statement, _ := json.Marshal(map[string]any{"Sid": input.Label, "Effect": "Allow", "Principal": map[string]any{"AWS": principals}, "Action": actions, "Resource": q.ARN})
		retained = append(retained, statement)
		principalCount := 0
		for _, item := range retained {
			principalCount += sqsPolicyPrincipalCount(item)
		}
		if principalCount > 50 {
			return nil, newSQSError("OverLimit", "A queue policy supports at most fifty principals.")
		}
	} else if !matched {
		return map[string]any{}, nil
	}
	if retained == nil {
		retained = make([]json.RawMessage, 0)
	}
	rawStatements, _ := json.Marshal(retained)
	policy["Statement"] = rawStatements
	raw, marshalErr := json.Marshal(policy)
	if marshalErr != nil {
		return nil, newSQSError("InvalidAttributeValue", "Unable to update queue Policy.")
	}
	if len(raw) > 8192 {
		return nil, newSQSError("OverLimit", "A queue policy supports at most 8192 bytes.")
	}
	if q.Attributes == nil {
		q.Attributes = make(map[string]string)
	}
	q.Attributes["Policy"] = string(raw)
	q.LastModifiedTimestamp = time.Now()
	return map[string]any{}, nil
}

func sqsRedriveTarget(raw string) string {
	var policy struct {
		DeadLetterTargetARN string `json:"deadLetterTargetArn"`
	}
	if json.Unmarshal([]byte(raw), &policy) != nil {
		return ""
	}
	return policy.DeadLetterTargetARN
}

func (s *Handler) sqsDeadLetterSources(input sqsRequest) (map[string]any, *sqsError) {
	q, err := s.queueForURL(input.QueueURL)
	if err != nil {
		return nil, err
	}
	urls := make([]string, 0)
	for _, candidate := range s.broker.ListQueues() {
		candidate.mu.Lock()
		target := sqsRedriveTarget(candidate.Attributes["RedrivePolicy"])
		if candidate.deleted {
			target = ""
		}
		candidate.mu.Unlock()
		if target == q.ARN {
			urls = append(urls, candidate.URL)
		}
	}
	sort.Strings(urls)
	page, next, err := paginateSQSStrings(urls, input.MaxResults, input.NextToken, "dlq:"+q.ARN)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"queueUrls": page}
	if next != "" {
		result["NextToken"] = next
	}
	return result, nil
}

func validSQSQueueARN(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) == 6 && parts[0] == "arn" && parts[1] != "" && parts[2] == "sqs" && parts[3] != "" && validSQSAccountID(parts[4]) && parts[5] != ""
}

// Registry lookup releases b.mu before inspecting queue state.
func (b *Broker) sqsIsDeadLetterQueue(arn string) bool {
	for _, q := range b.ListQueues() {
		q.mu.Lock()
		target := sqsRedriveTarget(q.Attributes["RedrivePolicy"])
		if q.deleted {
			target = ""
		}
		q.mu.Unlock()
		if target == arn {
			return true
		}
	}
	return false
}

func sqsQueueIsFIFO(q *Queue) bool { return strings.HasSuffix(q.Name, ".fifo") }

func (s *Handler) sqsStartMessageMove(input sqsRequest) (map[string]any, *sqsError) {
	if input.SourceARN == "" {
		return nil, newSQSError("MissingParameter", "SourceArn is required.")
	}
	if !validSQSQueueARN(input.SourceARN) || input.DestinationARN != "" && !validSQSQueueARN(input.DestinationARN) {
		return nil, newSQSError("InvalidAddress", "SourceArn and DestinationArn must be SQS queue ARNs.")
	}
	if input.MaxNumberOfMessagesPerSecond != nil && (*input.MaxNumberOfMessagesPerSecond < 1 || *input.MaxNumberOfMessagesPerSecond > 500) {
		return nil, newSQSError("InvalidParameterValue", "MaxNumberOfMessagesPerSecond must be between 1 and 500.")
	}
	b := s.broker
	state := b.sqsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	source := b.GetQueueByARN(input.SourceARN)
	if source == nil {
		return nil, newSQSError("ResourceNotFoundException", "Source queue does not exist.")
	}
	if !b.sqsIsDeadLetterQueue(source.ARN) {
		return nil, newSQSError("UnsupportedOperation", "Source queue must be a dead-letter queue for another SQS queue.")
	}
	if input.DestinationARN != "" {
		dest := b.GetQueueByARN(input.DestinationARN)
		if dest == nil {
			return nil, newSQSError("ResourceNotFoundException", "Destination queue does not exist.")
		}
		if source == dest || sqsQueueIsFIFO(source) != sqsQueueIsFIFO(dest) {
			return nil, newSQSError("UnsupportedOperation", "The destination must be a different queue of the same type.")
		}
	}
	active := 0
	for _, task := range state.MoveTasks {
		if task.Status == "RUNNING" || task.Status == "CANCELLING" {
			active++
			if task.SourceARN == source.ARN {
				return nil, newSQSError("UnsupportedOperation", "Only one active message movement task is supported per source queue.")
			}
		}
	}
	if active >= 100 {
		return nil, newSQSError("UnsupportedOperation", "An account supports at most 100 active message movement tasks.")
	}
	b.redriveExpiredSQS(source, time.Now())
	source.mu.Lock()
	if source.deleted {
		source.mu.Unlock()
		return nil, newSQSError("ResourceNotFoundException", "Source queue does not exist.")
	}
	pruneQueueLocked(source, time.Now())
	count := len(source.messages) + len(source.inFlight)
	source.mu.Unlock()
	now := time.Now()
	handlePayload, _ := json.Marshal(map[string]string{"taskId": uuid.NewString(), "sourceArn": source.ARN})
	task := &sqsMoveTask{Handle: base64.StdEncoding.EncodeToString(handlePayload), SourceARN: source.ARN, SourceQueue: source, DestinationARN: input.DestinationARN, Started: now, Updated: now, Status: "RUNNING", ToMove: int64(count)}
	if input.MaxNumberOfMessagesPerSecond != nil {
		rate := *input.MaxNumberOfMessagesPerSecond
		task.RequestedRate = &rate
	}
	state.MoveTasks[task.Handle] = task
	state.pruneSQSMoveHistoryLocked(source.ARN)
	return map[string]any{"TaskHandle": task.Handle}, nil
}

func (s *Handler) sqsListMessageMoves(input sqsRequest) (map[string]any, *sqsError) {
	if input.SourceARN == "" {
		return nil, newSQSError("MissingParameter", "SourceArn is required.")
	}
	if !validSQSQueueARN(input.SourceARN) {
		return nil, newSQSError("InvalidAddress", "SourceArn must be an SQS queue ARN.")
	}
	max := 1
	if input.MaxResults != nil {
		max = *input.MaxResults
	}
	if max < 1 || max > 10 {
		return nil, newSQSError("InvalidParameterValue", "MaxResults must be between 1 and 10.")
	}
	b := s.broker
	state := b.sqsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	if b.GetQueueByARN(input.SourceARN) == nil {
		return nil, newSQSError("ResourceNotFoundException", "Source queue does not exist.")
	}
	tasks := state.sqsMoveHistoryLocked(input.SourceARN)
	if len(tasks) > max {
		tasks = tasks[:max]
	}
	results := make([]map[string]any, 0, len(tasks))
	for _, task := range tasks {
		result := map[string]any{"SourceArn": task.SourceARN, "StartedTimestamp": task.Started.UnixMilli(), "Status": task.Status, "ApproximateNumberOfMessagesMoved": task.Moved, "ApproximateNumberOfMessagesToMove": task.ToMove}
		if task.DestinationARN != "" {
			result["DestinationArn"] = task.DestinationARN
		}
		if task.RequestedRate != nil {
			result["MaxNumberOfMessagesPerSecond"] = *task.RequestedRate
		}
		if task.Status == "RUNNING" {
			result["TaskHandle"] = task.Handle
		}
		if task.Status == "FAILED" {
			result["FailureReason"] = task.FailureReason
		}
		results = append(results, result)
	}
	return map[string]any{"Results": results}, nil
}

func (s *Handler) sqsCancelMessageMove(input sqsRequest) (map[string]any, *sqsError) {
	if input.TaskHandle == "" {
		return nil, newSQSError("MissingParameter", "TaskHandle is required.")
	}
	b := s.broker
	state := b.sqsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	task := state.MoveTasks[input.TaskHandle]
	if task == nil {
		return nil, newSQSError("ResourceNotFoundException", "Message movement task does not exist.")
	}
	if task.Status != "RUNNING" {
		return nil, newSQSError("UnsupportedOperation", "Only RUNNING message movement tasks can be cancelled.")
	}
	task.Status = "CANCELLING"
	return map[string]any{"ApproximateNumberOfMessagesMoved": task.Moved}, nil
}

func (state *sqsBrokerState) sqsMoveHistoryLocked(arn string) []*sqsMoveTask {
	tasks := make([]*sqsMoveTask, 0)
	for _, task := range state.MoveTasks {
		if task.SourceARN == arn {
			tasks = append(tasks, task)
		}
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Started.Equal(tasks[j].Started) {
			return tasks[i].Handle > tasks[j].Handle
		}
		return tasks[i].Started.After(tasks[j].Started)
	})
	return tasks
}

func (state *sqsBrokerState) pruneSQSMoveHistoryLocked(arn string) {
	tasks := state.sqsMoveHistoryLocked(arn)
	for _, task := range tasks[min(10, len(tasks)):] {
		delete(state.MoveTasks, task.Handle)
	}
}

// advanceSQSMessageMoveTasks services movement alongside normal broker work.
// Credits accrue from real elapsed time, so clients observe the same transfers
// whether they poll rapidly or only inspect the queue after a pause.
func (b *Broker) advanceSQSMessageMoveTasks(now time.Time) {
	state := b.sqsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, task := range state.MoveTasks {
		if task.Status == "CANCELLING" {
			task.Status = "CANCELLED"
			continue
		}
		if task.Status != "RUNNING" {
			continue
		}
		if now.Sub(task.Started) >= 36*time.Hour {
			task.Status = "FAILED"
			task.FailureReason = "Message movement task exceeded the 36-hour duration limit."
			continue
		}
		source := b.GetQueueByARN(task.SourceARN)
		if source == nil || source != task.SourceQueue {
			task.Status = "FAILED"
			task.FailureReason = "Source queue does not exist."
			continue
		}
		rate := 500
		if task.RequestedRate != nil {
			rate = *task.RequestedRate
		}
		if now.After(task.Updated) {
			task.Credits += now.Sub(task.Updated).Seconds() * float64(rate)
			task.Updated = now
		}
		b.redriveExpiredSQS(source, now)
		source.mu.Lock()
		if source.deleted {
			source.mu.Unlock()
			task.Status = "FAILED"
			task.FailureReason = "Source queue does not exist."
			continue
		}
		pruneQueueLocked(source, now)
		empty := len(source.messages) == 0 && len(source.inFlight) == 0
		source.mu.Unlock()
		if empty {
			task.Status = "COMPLETED"
			continue
		}
		// Bound a single service turn without dropping accumulated credits.
		budget := int64(task.Credits)
		if budget > 5000 {
			budget = 5000
		}
		for budget > 0 {
			moved, remaining, reason := b.sqsMoveOneLocked(task, source, now)
			if reason != "" {
				task.Status = "FAILED"
				task.FailureReason = reason
				break
			}
			if !remaining {
				task.Status = "COMPLETED"
				break
			}
			if !moved {
				break
			}
			task.Moved++
			task.Credits--
			budget--
		}
		// The final transfer can exhaust the queue exactly at the token boundary.
		if task.Status == "RUNNING" {
			source.mu.Lock()
			empty = len(source.messages) == 0 && len(source.inFlight) == 0
			source.mu.Unlock()
			if empty {
				task.Status = "COMPLETED"
			}
		}
	}
}

// Caller holds sqsBrokerState.mu. Registry lookup finishes before acquiring
// paired queue locks by queue name; b.mu is never held while waiting for them.
func (b *Broker) sqsMoveOneLocked(task *sqsMoveTask, source *Queue, now time.Time) (moved, remaining bool, failure string) {
	source.mu.Lock()
	var selected *Message
	for _, msg := range source.messages {
		if msg.VisibleAt.After(now) {
			continue
		}
		selected = msg
		break
	}
	remaining = len(source.messages) != 0 || len(source.inFlight) != 0
	originalARN := ""
	if selected != nil {
		originalARN = selected.OriginalSourceARN
	}
	source.mu.Unlock()
	if selected == nil {
		return false, remaining, ""
	}
	destinationARN := task.DestinationARN
	if destinationARN == "" {
		destinationARN = originalARN
	}
	if destinationARN == "" {
		return false, true, "Could not determine the original source queue for a message."
	}
	destination := b.GetQueueByARN(destinationARN)
	if destination == nil {
		return false, true, "Destination queue does not exist."
	}
	if destination == source || sqsQueueIsFIFO(destination) != sqsQueueIsFIFO(source) {
		return false, true, "Destination queue must be a different queue of the same type."
	}
	first, second := source, destination
	if second.Name < first.Name {
		first, second = second, first
	}
	first.mu.Lock()
	second.mu.Lock()
	defer first.mu.Unlock()
	defer second.mu.Unlock()
	if source.deleted {
		return false, true, "Source queue does not exist."
	}
	if destination.deleted {
		return false, true, "Destination queue does not exist."
	}
	index := -1
	for i, msg := range source.messages {
		if msg == selected {
			index = i
			break
		}
	}
	// Another consumer may have received the selected message before both queue
	// locks were acquired. It retains ownership until its visibility expires.
	if index < 0 {
		return false, true, ""
	}
	maximum, _ := strconv.Atoi(destination.Attributes["MaximumMessageSize"])
	if maximum > 0 && messageSize(selected.Body, selected.Attributes) > maximum {
		return false, true, "A message exceeds the destination MaximumMessageSize."
	}
	msg := *cloneMessage(selected)
	msg.ID = uuid.NewString()
	msg.ReceiptHandle = ""
	msg.SentTimestamp = now
	msg.VisibleAt = now
	msg.FirstReceivedAt = time.Time{}
	msg.ReceivedAt = time.Time{}
	msg.ReceiveCount = 0
	msg.OriginalSourceARN = ""
	if sqsQueueIsFIFO(destination) {
		// New enqueue identity prevents deduplication of independently redriven items.
		msg.DeduplicationID = selected.ID
		msg.SequenceNumber = destination.nextSequenceLocked()
		dedupKey := msg.DeduplicationID
		if destination.Attributes["DeduplicationScope"] == "messageGroup" {
			dedupKey = msg.GroupID + "\x00" + dedupKey
		}
		if destination.dedup == nil {
			destination.dedup = make(map[string]sqsDedupEntry)
		}
		destination.dedup[dedupKey] = sqsDedupEntry{ID: msg.ID, Sequence: msg.SequenceNumber, Expires: now.Add(5 * time.Minute)}
	}
	delay, _ := strconv.Atoi(destination.Attributes["DelaySeconds"])
	msg.VisibleAt = now.Add(time.Duration(delay) * time.Second)
	source.messages = append(source.messages[:index], source.messages[index+1:]...)
	destination.messages = append(destination.messages, &msg)
	notifyQueueLocked(source)
	notifyQueueLocked(destination)
	return true, true, ""
}
