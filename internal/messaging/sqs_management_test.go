package messaging

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func managementQueue(t *testing.T, b *Broker, name string) *Queue {
	t.Helper()
	return b.CreateQueue(name, time.Second, time.Hour)
}

func managementCall(t *testing.T, h *Handler, action string, input sqsRequest) map[string]any {
	t.Helper()
	out, err := h.executeSQSManagement(action, input)
	if err != nil {
		t.Fatalf("%s: %s: %s", action, err.Code, err.Message)
	}
	return out
}

func managementExpectError(t *testing.T, h *Handler, action string, input sqsRequest, code string) {
	t.Helper()
	_, err := h.executeSQSManagement(action, input)
	if err == nil || err.Code != code {
		t.Fatalf("%s error = %+v, want %s", action, err, code)
	}
}

func managementDLQ(t *testing.T, b *Broker, source, dlq *Queue) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"deadLetterTargetArn": dlq.ARN, "maxReceiveCount": "2"})
	if err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	if source.Attributes == nil {
		source.Attributes = make(map[string]string)
	}
	source.Attributes["RedrivePolicy"] = string(raw)
	source.mu.Unlock()
}

func managementPut(q *Queue, body, originalARN string) *Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg := &Message{ID: fmt.Sprintf("old-%d", len(q.messages)), Body: body, OriginalSourceARN: originalARN, SentTimestamp: time.Now().Add(-time.Minute), ReceiveCount: 4, FirstReceivedAt: time.Now().Add(-time.Minute), Attributes: map[string]MessageAttribute{"binary": {DataType: "Binary", BinaryValue: []byte{0, 255}}}}
	q.messages = append(q.messages, msg)
	notifyQueueLocked(q)
	return msg
}

func managementTask(t *testing.T, h *Handler, source, dest *Queue, rate *int) *sqsMoveTask {
	t.Helper()
	input := sqsRequest{SourceARN: source.ARN, MaxNumberOfMessagesPerSecond: rate}
	if dest != nil {
		input.DestinationARN = dest.ARN
	}
	result := managementCall(t, h, "StartMessageMoveTask", input)
	state := h.broker.sqsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.MoveTasks[result["TaskHandle"].(string)]
}

func TestSQSManagementTagsCopyOverwriteValidation(t *testing.T) {
	b := NewBroker("", "", 4100)
	q := managementQueue(t, b, "tagged")
	h := NewHandler(b)
	managementCall(t, h, "TagQueue", sqsRequest{QueueURL: q.URL, Tags: map[string]string{"team": "one", "项目": "本地"}})
	managementCall(t, h, "TagQueue", sqsRequest{QueueURL: q.URL, Tags: map[string]string{"team": "two"}})
	result := managementCall(t, h, "ListQueueTags", sqsRequest{QueueURL: q.URL})
	tags := result["Tags"].(map[string]string)
	if !reflect.DeepEqual(tags, map[string]string{"team": "two", "项目": "本地"}) {
		t.Fatal(tags)
	}
	tags["team"] = "changed"
	result = managementCall(t, h, "ListQueueTags", sqsRequest{QueueURL: q.URL})
	if result["Tags"].(map[string]string)["team"] != "two" {
		t.Fatal("returned tag map aliases queue state")
	}
	managementCall(t, h, "UntagQueue", sqsRequest{QueueURL: q.URL, TagKeys: []string{"team", "absent"}})
	for _, invalid := range []map[string]string{{"": "v"}, {"aws:reserved": "v"}, {"good": "aws:reserved"}, {strings.Repeat("界", 129): "v"}, {"good": strings.Repeat("界", 257)}, {"bad!": "v"}} {
		managementExpectError(t, h, "TagQueue", sqsRequest{QueueURL: q.URL, Tags: invalid}, "InvalidParameterValue")
	}
	many := make(map[string]string)
	for i := 0; i < 51; i++ {
		many[fmt.Sprintf("key%d", i)] = "value"
	}
	// AWS recommends <=50; its published SQS tag contract does not set a hard 50 limit.
	managementCall(t, h, "TagQueue", sqsRequest{QueueURL: q.URL, Tags: many})
	managementExpectError(t, h, "TagQueue", sqsRequest{QueueURL: q.URL}, "MissingParameter")
	managementExpectError(t, h, "UntagQueue", sqsRequest{QueueURL: q.URL}, "MissingParameter")
}

func TestSQSManagementPermissionPreservesCustomPolicy(t *testing.T) {
	b := NewBroker("", "", 4100)
	q := managementQueue(t, b, "policy")
	h := NewHandler(b)
	custom := `{"Version":"2012-10-17","Id":"custom","Statement":{"Sid":"Keep","Effect":"Deny","Principal":"*","Action":"sqs:SendMessage","Resource":"*","Condition":{"StringNotEquals":{"aws:SourceVpc":"vpc-1"}}}}`
	q.mu.Lock()
	if q.Attributes == nil {
		q.Attributes = make(map[string]string)
	}
	q.Attributes["Policy"] = custom
	q.mu.Unlock()
	input := sqsRequest{QueueURL: q.URL, Label: "Agent_Send", Actions: []string{"SendMessage", "ReceiveMessage"}, AWSAccountIDs: []string{"123456789012"}}
	managementCall(t, h, "AddPermission", input)
	q.mu.Lock()
	raw := q.Attributes["Policy"]
	q.mu.Unlock()
	var policy struct {
		Id        string
		Statement []map[string]any
	}
	if err := json.Unmarshal([]byte(raw), &policy); err != nil {
		t.Fatal(err)
	}
	if policy.Id != "custom" || len(policy.Statement) != 2 || policy.Statement[0]["Condition"] == nil {
		t.Fatalf("custom policy lost: %s", raw)
	}
	added := policy.Statement[1]
	if added["Resource"] != q.ARN || added["Sid"] != "Agent_Send" || added["Effect"] != "Allow" {
		t.Fatal(added)
	}
	principal := added["Principal"].(map[string]any)["AWS"].([]any)
	if principal[0] != "arn:aws:iam::123456789012:root" {
		t.Fatal(principal)
	}
	managementExpectError(t, h, "AddPermission", input, "InvalidParameterValue")
	managementCall(t, h, "RemovePermission", sqsRequest{QueueURL: q.URL, Label: "Agent_Send"})
	managementCall(t, h, "RemovePermission", sqsRequest{QueueURL: q.URL, Label: "absent"})
	q.mu.Lock()
	raw = q.Attributes["Policy"]
	q.mu.Unlock()
	if err := json.Unmarshal([]byte(raw), &policy); err != nil || len(policy.Statement) != 1 || policy.Statement[0]["Condition"] == nil {
		t.Fatalf("remove changed unrelated statement: %s", raw)
	}
	for _, bad := range []sqsRequest{{QueueURL: q.URL, Label: "bad label", Actions: input.Actions, AWSAccountIDs: input.AWSAccountIDs}, {QueueURL: q.URL, Label: "InvalidAccount", Actions: input.Actions, AWSAccountIDs: []string{"arn:aws:iam::123456789012:root"}}, {QueueURL: q.URL, Label: "InvalidAction", Actions: []string{"MadeUp"}, AWSAccountIDs: input.AWSAccountIDs}} {
		managementExpectError(t, h, "AddPermission", bad, "InvalidParameterValue")
	}
	input.Label = "TooManyActions"
	input.Actions = []string{"SendMessage", "ReceiveMessage", "DeleteMessage", "GetQueueUrl", "GetQueueAttributes", "PurgeQueue", "SetQueueAttributes", "ListQueueTags"}
	managementExpectError(t, h, "AddPermission", input, "OverLimit")
}

func TestSQSManagementDeadLetterSourcesPagination(t *testing.T) {
	b := NewBroker("", "", 4100)
	dlq := managementQueue(t, b, "dead")
	h := NewHandler(b)
	sources := make([]*Queue, 3)
	for i := range sources {
		sources[i] = managementQueue(t, b, fmt.Sprintf("source-%d", i))
		managementDLQ(t, b, sources[i], dlq)
	}
	one := 1
	result := managementCall(t, h, "ListDeadLetterSourceQueues", sqsRequest{QueueURL: dlq.URL, MaxResults: &one})
	if _, wrong := result["QueueUrls"]; wrong {
		t.Fatal("SQS requires lowercase queueUrls for this operation")
	}
	if !reflect.DeepEqual(result["queueUrls"], []string{sources[0].URL}) {
		t.Fatal(result)
	}
	next := result["NextToken"].(string)
	result = managementCall(t, h, "ListDeadLetterSourceQueues", sqsRequest{QueueURL: dlq.URL, MaxResults: &one, NextToken: next})
	if !reflect.DeepEqual(result["queueUrls"], []string{sources[1].URL}) {
		t.Fatal(result)
	}
	result = managementCall(t, h, "ListDeadLetterSourceQueues", sqsRequest{QueueURL: dlq.URL})
	if len(result["queueUrls"].([]string)) != 3 || result["NextToken"] != nil {
		t.Fatal(result)
	}
}

func TestSQSMessageMoveRateOriginalSourcesAndNewIdentities(t *testing.T) {
	b := NewBroker("", "", 4100)
	dlq := managementQueue(t, b, "dead")
	first := managementQueue(t, b, "first")
	second := managementQueue(t, b, "second")
	managementDLQ(t, b, first, dlq)
	managementDLQ(t, b, second, dlq)
	old := managementPut(dlq, "one", first.ARN)
	managementPut(dlq, "two", second.ARN)
	managementPut(dlq, "three", first.ARN)
	h := NewHandler(b)
	rate := 4
	task := managementTask(t, h, dlq, nil, &rate)
	start := task.Started
	b.advanceSQSMessageMoveTasks(start.Add(500 * time.Millisecond))
	if waiting, _ := b.QueueDepth(dlq); waiting != 1 {
		t.Fatalf("DLQ depth=%d; rate should move exactly two messages", waiting)
	}
	if waiting, _ := managementDepthAt(first, start.Add(500*time.Millisecond)); waiting != 1 {
		t.Fatal(waiting)
	}
	if waiting, _ := managementDepthAt(second, start.Add(500*time.Millisecond)); waiting != 1 {
		t.Fatal(waiting)
	}
	first.mu.Lock()
	msg := cloneMessage(first.messages[0])
	first.mu.Unlock()
	if msg.ID == old.ID || msg.ReceiveCount != 0 || !msg.FirstReceivedAt.IsZero() || msg.SentTimestamp != start.Add(500*time.Millisecond) || msg.OriginalSourceARN != "" || msg.ReceiptHandle != "" {
		t.Fatalf("redrive failed to assign fresh identity: %+v", msg)
	}
	if !reflect.DeepEqual(msg.Attributes["binary"].BinaryValue, []byte{0, 255}) {
		t.Fatal(msg.Attributes)
	}
	old.Attributes["binary"].BinaryValue[0] = 7
	if msg.Attributes["binary"].BinaryValue[0] != 0 {
		t.Fatal("transfer aliases mutable message attributes")
	}
	running := managementCall(t, h, "ListMessageMoveTasks", sqsRequest{SourceARN: dlq.ARN})["Results"].([]map[string]any)[0]
	if running["TaskHandle"] == nil || running["Status"] != "RUNNING" || running["ApproximateNumberOfMessagesMoved"] != int64(2) || running["ApproximateNumberOfMessagesToMove"] != int64(3) || running["StartedTimestamp"] != start.UnixMilli() || running["DestinationArn"] != nil {
		t.Fatal(running)
	}
	b.advanceSQSMessageMoveTasks(start.Add(time.Second))
	completed := managementCall(t, h, "ListMessageMoveTasks", sqsRequest{SourceARN: dlq.ARN})["Results"].([]map[string]any)[0]
	if completed["Status"] != "COMPLETED" || completed["TaskHandle"] != nil || completed["ApproximateNumberOfMessagesMoved"] != int64(3) {
		t.Fatal(completed)
	}
}

func TestSQSMessageMoveCancelLeavesMovedMessagesAndHistoryBound(t *testing.T) {
	b := NewBroker("", "", 4100)
	dlq := managementQueue(t, b, "dead")
	source := managementQueue(t, b, "source")
	dest := managementQueue(t, b, "dest")
	managementDLQ(t, b, source, dlq)
	for i := 0; i < 3; i++ {
		managementPut(dlq, fmt.Sprint(i), source.ARN)
	}
	h := NewHandler(b)
	rate := 1
	task := managementTask(t, h, dlq, dest, &rate)
	start := task.Started
	managementExpectError(t, h, "StartMessageMoveTask", sqsRequest{SourceARN: dlq.ARN, DestinationARN: dest.ARN}, "UnsupportedOperation")
	b.advanceSQSMessageMoveTasks(start.Add(time.Second))
	result := managementCall(t, h, "CancelMessageMoveTask", sqsRequest{TaskHandle: task.Handle})
	if result["ApproximateNumberOfMessagesMoved"] != int64(1) {
		t.Fatal(result)
	}
	b.advanceSQSMessageMoveTasks(start.Add(5 * time.Second))
	if waiting, _ := managementDepthAt(dest, start.Add(5*time.Second)); waiting != 1 {
		t.Fatal("cancel reverted or continued transfers", waiting)
	}
	if waiting, _ := b.QueueDepth(dlq); waiting != 2 {
		t.Fatal(waiting)
	}
	cancelled := managementCall(t, h, "ListMessageMoveTasks", sqsRequest{SourceARN: dlq.ARN})["Results"].([]map[string]any)[0]
	if cancelled["Status"] != "CANCELLED" || cancelled["TaskHandle"] != nil {
		t.Fatal(cancelled)
	}
	managementExpectError(t, h, "CancelMessageMoveTask", sqsRequest{TaskHandle: task.Handle}, "UnsupportedOperation")
	managementExpectError(t, h, "CancelMessageMoveTask", sqsRequest{TaskHandle: "unknown"}, "ResourceNotFoundException")
	b.PurgeQueue(dlq)
	for i := 0; i < 12; i++ {
		task = managementTask(t, h, dlq, dest, nil)
		b.advanceSQSMessageMoveTasks(task.Started.Add(time.Millisecond))
	}
	ten := 10
	results := managementCall(t, h, "ListMessageMoveTasks", sqsRequest{SourceARN: dlq.ARN, MaxResults: &ten})["Results"].([]map[string]any)
	if len(results) != 10 {
		t.Fatal(len(results))
	}
	state := b.sqsState()
	state.mu.Lock()
	histories := len(state.MoveTasks)
	state.mu.Unlock()
	if histories != 10 {
		t.Fatalf("unbounded retained task history: %d", histories)
	}
}

func TestSQSMessageMoveFailuresValidationAndDelayedMessages(t *testing.T) {
	b := NewBroker("", "", 4100)
	dlq := managementQueue(t, b, "dead")
	source := managementQueue(t, b, "source")
	dest := managementQueue(t, b, "dest")
	fifo := managementQueue(t, b, "dest.fifo")
	h := NewHandler(b)
	managementExpectError(t, h, "StartMessageMoveTask", sqsRequest{SourceARN: source.ARN}, "UnsupportedOperation")
	managementDLQ(t, b, source, dlq)
	for _, rate := range []int{0, -1, 501} {
		managementExpectError(t, h, "StartMessageMoveTask", sqsRequest{SourceARN: dlq.ARN, MaxNumberOfMessagesPerSecond: &rate}, "InvalidParameterValue")
	}
	managementExpectError(t, h, "StartMessageMoveTask", sqsRequest{SourceARN: "not-an-arn"}, "InvalidAddress")
	managementExpectError(t, h, "StartMessageMoveTask", sqsRequest{SourceARN: dlq.ARN, DestinationARN: fifo.ARN}, "UnsupportedOperation")
	managementExpectError(t, h, "StartMessageMoveTask", sqsRequest{SourceARN: dlq.ARN, DestinationARN: dlq.ARN}, "UnsupportedOperation")
	managementPut(dlq, "foreign", "")
	task := managementTask(t, h, dlq, nil, nil)
	b.advanceSQSMessageMoveTasks(task.Started.Add(time.Second))
	failure := managementCall(t, h, "ListMessageMoveTasks", sqsRequest{SourceARN: dlq.ARN})["Results"].([]map[string]any)[0]
	if failure["Status"] != "FAILED" || failure["FailureReason"] == nil || failure["TaskHandle"] != nil {
		t.Fatal(failure)
	}
	if waiting, _ := b.QueueDepth(dlq); waiting != 1 {
		t.Fatal("failed task lost message", waiting)
	}
	task = managementTask(t, h, dlq, dest, nil)
	b.advanceSQSMessageMoveTasks(task.Started.Add(time.Second))
	if waiting, _ := managementDepthAt(dest, task.Started.Add(time.Second)); waiting != 1 {
		t.Fatal("custom destination did not move foreign message", waiting)
	}
	delayed := managementPut(dlq, "delayed", source.ARN)
	task = managementTask(t, h, dlq, dest, nil)
	dlq.mu.Lock()
	delayed.VisibleAt = task.Started.Add(2 * time.Second)
	dlq.mu.Unlock()
	b.advanceSQSMessageMoveTasks(task.Started.Add(time.Second))
	dlq.mu.Lock()
	delayedWaiting := len(dlq.messages)
	dlq.mu.Unlock()
	if delayedWaiting != 1 {
		t.Fatal("redrive moved an invisible message", delayedWaiting)
	}
	b.advanceSQSMessageMoveTasks(task.Started.Add(3 * time.Second))
	if waiting, _ := b.QueueDepth(dlq); waiting != 0 {
		t.Fatal(waiting)
	}
}

func TestSQSMessageMoveRejectsDeletedSourceAndOversizedDestination(t *testing.T) {
	b := NewBroker("", "", 4100)
	dlq := managementQueue(t, b, "dead")
	source := managementQueue(t, b, "source")
	dest := managementQueue(t, b, "small")
	managementDLQ(t, b, source, dlq)
	managementPut(dlq, strings.Repeat("x", 100), source.ARN)
	dest.mu.Lock()
	dest.Attributes["MaximumMessageSize"] = "32"
	dest.mu.Unlock()
	h := NewHandler(b)
	task := managementTask(t, h, dlq, dest, nil)
	b.advanceSQSMessageMoveTasks(task.Started.Add(time.Second))
	if task.Status != "FAILED" || !strings.Contains(task.FailureReason, "MaximumMessageSize") {
		t.Fatal(task)
	}
	if waiting, _ := b.QueueDepth(dlq); waiting != 1 {
		t.Fatal("failed destination removed source message", waiting)
	}
	dest.mu.Lock()
	dest.Attributes["MaximumMessageSize"] = "1048576"
	dest.mu.Unlock()
	task = managementTask(t, h, dlq, dest, nil)
	if !b.DeleteQueue(dlq.Name) {
		t.Fatal("delete failed")
	}
	replacement := managementQueue(t, b, dlq.Name)
	managementPut(replacement, "new queue message", source.ARN)
	b.advanceSQSMessageMoveTasks(task.Started.Add(time.Second))
	if task.Status != "FAILED" {
		t.Fatal("old task followed a recreated source queue", task)
	}
	if waiting, _ := b.QueueDepth(replacement); waiting != 1 {
		t.Fatal(waiting)
	}
}

func TestSQSMessageMoveFIFORefreshesDeduplicationAndSequence(t *testing.T) {
	b := NewBroker("", "", 4100)
	dlq := managementQueue(t, b, "dead.fifo")
	source := managementQueue(t, b, "source.fifo")
	for _, q := range []*Queue{source, dlq} {
		q.mu.Lock()
		q.Attributes["FifoQueue"] = "true"
		q.mu.Unlock()
	}
	managementDLQ(t, b, source, dlq)
	old := managementPut(dlq, "redrive", source.ARN)
	dlq.mu.Lock()
	old.GroupID = "group"
	old.DeduplicationID = "producer-dedup"
	old.SequenceNumber = dlq.nextSequenceLocked()
	dlq.mu.Unlock()
	h := NewHandler(b)
	task := managementTask(t, h, dlq, nil, nil)
	b.advanceSQSMessageMoveTasks(task.Started.Add(time.Second))
	source.mu.Lock()
	if len(source.messages) != 1 {
		source.mu.Unlock()
		t.Fatal("missing FIFO transfer")
	}
	moved := cloneMessage(source.messages[0])
	source.mu.Unlock()
	if moved.GroupID != "group" || moved.DeduplicationID != old.ID || moved.SequenceNumber == "" {
		t.Fatal(moved)
	}
	duplicate, err := b.SendQueueMessage(source, QueueMessageInput{Body: "retry", MessageGroupID: "group", MessageDeduplicationID: old.ID})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.MessageID != moved.ID || duplicate.SequenceNumber != moved.SequenceNumber {
		t.Fatalf("redrive deduplication not registered: %+v", duplicate)
	}
	if waiting, _ := managementDepthAt(source, task.Started.Add(time.Second)); waiting != 1 {
		t.Fatal("duplicate accepted after FIFO redrive", waiting)
	}
}

// Task progress tests use an explicit simulated time. Inspecting visibility
// using wall time would classify their freshly moved messages as still delayed.
func managementDepthAt(q *Queue, now time.Time) (waiting, inFlight int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, message := range q.messages {
		if !now.Before(message.VisibleAt) {
			waiting++
		}
	}
	return waiting, len(q.inFlight)
}
