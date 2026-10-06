package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSNSArchiveRetentionPolicy(t *testing.T) {
	for _, test := range []struct {
		policy string
		days   int
	}{
		{`{}`, 0}, {`{"MessageRetentionPeriod":"1"}`, 1}, {`{"MessageRetentionPeriod":30}`, 30}, {`{"MessageRetentionPeriod":"365"}`, 365},
	} {
		days, err := snsArchiveRetention(test.policy)
		if err != nil || days != test.days {
			t.Fatalf("%s: days=%d err=%v", test.policy, days, err)
		}
	}
	for _, policy := range []string{
		``, `null`, `[]`, `{`, `{ "MessageRetentionPeriod": 0 }`, `{"MessageRetentionPeriod":366}`, `{"MessageRetentionPeriod":-1}`, `{"MessageRetentionPeriod":1.5}`,
		`{"MessageRetentionPeriod":"1.0"}`, `{"MessageRetentionPeriod":true}`, `{"MessageRetentionPeriod":null}`, `{"unknown":1}`, `{"MessageRetentionPeriod":1,"unknown":1}`,
		`{"MessageRetentionPeriod":1,"MessageRetentionPeriod":2}`, `{} {}`, `{"ArchivePolicy":{"MessageRetentionPeriod":"1"}}`,
	} {
		_, err := snsArchiveRetention(policy)
		snsArchiveTestErrorCode(t, err, "InvalidParameter")
	}
}

func TestSNSArchiveReplayPolicy(t *testing.T) {
	start := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	end := start.Add(30 * time.Minute)
	for _, optionalEnd := range []string{"", `,"EndingPoint":""`, fmt.Sprintf(`,"EndingPoint":%q`, end.Format(time.RFC3339))} {
		policy := fmt.Sprintf(`{"PointType":"Timestamp","StartingPoint":%q%s}`, start.Format(time.RFC3339), optionalEnd)
		actualStart, actualEnd, err := snsReplayPolicy(policy)
		if err != nil || !actualStart.Equal(start) || optionalEnd == "" && !actualEnd.IsZero() || strings.Contains(optionalEnd, end.Format(time.RFC3339)) && !actualEnd.Equal(end) {
			t.Fatalf("policy %s: start=%s end=%s err=%v", policy, actualStart, actualEnd, err)
		}
	}
	for _, policy := range []string{
		`{}`, `null`, `[]`, `{"PointType":"Offset","StartingPoint":"2020-01-01T00:00:00Z"}`,
		`{"PointType":"Timestamp"}`, `{"PointType":"Timestamp","StartingPoint":"yesterday"}`,
		`{"PointType":"Timestamp","StartingPoint":42}`, `{"PointType":"Timestamp","StartingPoint":null}`,
		`{"PointType":"Timestamp","StartingPoint":"2020-01-01T00:00:00Z","EndingPoint":null}`,
		`{"PointType":"Timestamp","StartingPoint":"2020-01-01T00:00:00Z","EndingPoint":123}`,
		`{"PointType":"Timestamp","StartingPoint":"2020-01-01T00:00:00Z","EndingPoint":"bad"}`,
		`{"PointType":"Timestamp","StartingPoint":"2020-01-01T00:00:00Z","EndingPoint":"2019-12-31T23:59:59Z"}`,
		`{"PointType":"Timestamp","StartingPoint":"2020-01-01T00:00:00Z","Unknown":true}`,
		`{"PointType":"Timestamp","PointType":"Timestamp","StartingPoint":"2020-01-01T00:00:00Z"}`,
		fmt.Sprintf(`{"PointType":"Timestamp","StartingPoint":%q}`, time.Now().Add(time.Hour).Format(time.RFC3339)),
	} {
		_, _, err := snsReplayPolicy(policy)
		snsArchiveTestErrorCode(t, err, "InvalidParameter")
	}
}

func TestSNSArchivePruningReleasesExpiredRecords(t *testing.T) {
	now := time.Now().UTC()
	cutoff := now.Add(-24 * time.Hour)
	topic := &Topic{archive: []snsArchivedPublication{
		{Input: SNSPublishInput{Message: "expired", Attributes: map[string]MessageAttribute{"binary": {BinaryValue: []byte{1, 2, 3}}}}, PublishedAt: cutoff.Add(-time.Nanosecond), Size: 30},
		{Input: SNSPublishInput{Message: "boundary"}, PublishedAt: cutoff, Size: 40},
		{Input: SNSPublishInput{Message: "recent"}, PublishedAt: now, Size: 50},
	}, archiveBytes: 120}
	backing := topic.archive
	snsPruneArchive(topic, 1, now)
	if len(topic.archive) != 2 || topic.archive[0].Input.Message != "boundary" || topic.archive[1].Input.Message != "recent" || topic.archiveBytes != 90 {
		t.Fatalf("archive pruning: %#v, bytes=%d", topic.archive, topic.archiveBytes)
	}
	if backing[2].Input.Message != "" || backing[2].Input.Attributes != nil {
		t.Fatal("removed archive backing slot still retains request references")
	}
	snsPruneArchive(topic, 0, now)
	if topic.archive != nil || topic.archiveBytes != 0 {
		t.Fatal("disabled archive did not clear its records")
	}
}

func TestSNSArchiveCommitCopiesRequestAttributes(t *testing.T) {
	now := time.Now().UTC()
	input := SNSPublishInput{TopicARN: "arn:aws:sns:us-east-1:000000000000:copy.fifo", Message: "original", Attributes: map[string]MessageAttribute{
		"binary": {DataType: "Binary", BinaryValue: []byte{1, 2, 3}}, "string": {DataType: "String", StringValue: "original"},
	}}
	result := SNSPublishResult{MessageID: "original-id", SequenceNumber: "42"}
	topic := &Topic{}
	if err := snsPrepareArchive(topic, input, 1, now); err != nil {
		t.Fatal(err)
	}
	snsCommitArchive(topic, input, result, now)
	input.Attributes["binary"].BinaryValue[0] = 9
	input.Attributes["string"] = MessageAttribute{DataType: "String", StringValue: "mutated"}
	delete(input.Attributes, "binary")
	input.Message = "mutated"
	archived := topic.archive[0]
	if archived.Input.Message != "original" || archived.Input.Attributes["string"].StringValue != "original" || !bytes.Equal(archived.Input.Attributes["binary"].BinaryValue, []byte{1, 2, 3}) || archived.Result != result || !archived.PublishedAt.Equal(now) {
		t.Fatalf("archive changed through caller data: %#v", archived)
	}
	if archived.Size <= len(archived.Input.Message) || topic.archiveBytes != archived.Size {
		t.Fatalf("archive accounting: size=%d total=%d", archived.Size, topic.archiveBytes)
	}
}

func TestSNSArchiveCapRejectsBeforeDiscardingRetainedRecords(t *testing.T) {
	now := time.Now().UTC()
	input := SNSPublishInput{Message: "next publication"}
	size := snsArchiveSize(input)
	// Model the existing archive's accounting without allocating 64 MiB in a
	// policy/helper test. The production commit derives Size from each request.
	topic := &Topic{archive: []snsArchivedPublication{{Input: SNSPublishInput{Message: "retained"}, PublishedAt: now, Size: snsArchiveMaxBytes - size}}, archiveBytes: snsArchiveMaxBytes - size}
	if err := snsPrepareArchive(topic, input, 1, now); err != nil {
		t.Fatal(err)
	}
	snsCommitArchive(topic, input, SNSPublishResult{MessageID: "id"}, now)
	if topic.archiveBytes != snsArchiveMaxBytes {
		t.Fatalf("archive boundary=%d", topic.archiveBytes)
	}
	err := snsPrepareArchive(topic, input, 1, now)
	snsArchiveTestErrorCode(t, err, "InternalError")
	if len(topic.archive) != 2 || topic.archive[0].Input.Message != "retained" || topic.archiveBytes != snsArchiveMaxBytes {
		t.Fatal("cap overflow discarded an unexpired archive record")
	}
	snsPruneArchive(topic, 1, now.Add(24*time.Hour+time.Second))
	if err := snsPrepareArchive(topic, input, 1, now.Add(24*time.Hour+time.Second)); err != nil || len(topic.archive) != 0 || topic.archiveBytes != 0 {
		t.Fatalf("expired records did not free capacity: %v", err)
	}
}

func TestSNSArchiveReplayPreservesPublicationAndBypassesQueueDedup(t *testing.T) {
	broker := NewBroker("", "", 4100)
	var capture bytes.Buffer
	broker.SetSNSCapture(&SNSCapture{writer: &capture})
	topic, err := broker.CreateTopicWithAttributes("replay.fifo", map[string]string{"FifoTopic": "true", "ArchivePolicy": `{"MessageRetentionPeriod":"1"}`}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	queue := broker.CreateQueue("replay.fifo", 0, 0)
	queue.mu.Lock()
	queue.Attributes["FifoQueue"] = "true"
	queue.mu.Unlock()
	subscription, err := broker.Subscribe(topic.ARN, "sqs", queue.ARN, nil)
	if err != nil {
		t.Fatal(err)
	}
	publishedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	input := SNSPublishInput{TopicARN: topic.ARN, Message: "archived", MessageGroupID: "group", MessageDeduplicationID: "original-dedup"}
	result := SNSPublishResult{MessageID: "original-message-id", SequenceNumber: "37"}
	topic.publishMu.Lock()
	snsCommitArchive(topic, input, result, publishedAt)
	topic.publishMu.Unlock()
	policy := fmt.Sprintf(`{"PointType":"Timestamp","StartingPoint":%q}`, publishedAt.Add(-time.Second).Format(time.RFC3339Nano))
	for index := 0; index < 2; index++ {
		topic.controlMu.Lock()
		topic.publishMu.Lock()
		err := broker.snsReplay(topic, subscription, policy, "SetSubscriptionAttributes", "replay-request")
		topic.publishMu.Unlock()
		topic.controlMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	messages := broker.ReceiveMessages(queue, 1, 0)
	if len(messages) != 1 {
		t.Fatalf("first replay receive count=%d", len(messages))
	}
	for index := 0; index < 2; index++ {
		if index != 0 {
			messages = broker.ReceiveMessages(queue, 1, 0)
			if len(messages) != 1 {
				t.Fatalf("second replay was suppressed by dedup, count=%d", len(messages))
			}
		}
		var envelope struct {
			MessageID      string `json:"MessageId"`
			SequenceNumber string
			Timestamp      string
			Replayed       bool
			Message        string
		}
		if err := json.Unmarshal([]byte(messages[0].Body), &envelope); err != nil {
			t.Fatal(err)
		}
		stamp, err := time.Parse(time.RFC3339Nano, envelope.Timestamp)
		if err != nil || !stamp.Equal(publishedAt) || envelope.MessageID != result.MessageID || envelope.SequenceNumber != result.SequenceNumber || !envelope.Replayed || envelope.Message != "archived" {
			t.Fatalf("replay lost original publication: %#v, timestamp err=%v", envelope, err)
		}
		broker.DeleteMessage(queue, messages[0].ReceiptHandle)
	}
	if len(topic.archive) != 1 {
		t.Fatal("replay itself entered the archive")
	}
	if !strings.Contains(capture.String(), `"operation":"Replay"`) || !strings.Contains(capture.String(), `"published_at"`) || !strings.Contains(capture.String(), `"message_id":"original-message-id"`) {
		t.Fatalf("replay intent missing per-publication evidence: %s", capture.String())
	}
}

func TestSNSArchiveReplayCaptureFailureHasNoQueueSideEffect(t *testing.T) {
	broker := NewBroker("", "", 4100)
	topic, err := broker.CreateTopicWithAttributes("atomic-replay.fifo", map[string]string{"FifoTopic": "true", "ArchivePolicy": `{"MessageRetentionPeriod":"1"}`}, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	queue := broker.CreateQueue("atomic-replay", 0, 0)
	subscription, err := broker.Subscribe(topic.ARN, "sqs", queue.ARN, nil)
	if err != nil {
		t.Fatal(err)
	}
	publishedAt := time.Now().UTC().Add(-time.Minute)
	topic.publishMu.Lock()
	snsCommitArchive(topic, SNSPublishInput{TopicARN: topic.ARN, Message: "atomic", MessageGroupID: "group"}, SNSPublishResult{MessageID: "id"}, publishedAt)
	topic.publishMu.Unlock()
	broker.SetSNSCapture(&SNSCapture{writer: snsArchiveFailWriter{}})
	policy := fmt.Sprintf(`{"PointType":"Timestamp","StartingPoint":%q}`, publishedAt.Add(-time.Second).Format(time.RFC3339Nano))
	topic.controlMu.Lock()
	topic.publishMu.Lock()
	err = broker.snsReplay(topic, subscription, policy, "Subscribe", "request")
	topic.publishMu.Unlock()
	topic.controlMu.Unlock()
	snsArchiveTestErrorCode(t, err, "InternalError")
	if len(broker.ReceiveMessages(queue, 10, 0)) != 0 {
		t.Fatal("replay sent a queue message before durable capture")
	}
}

func snsArchiveTestErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var failure *snsError
	if !errors.As(err, &failure) || failure.Code != code {
		t.Fatalf("want SNS %s, got %v", code, err)
	}
}

type snsArchiveFailWriter struct{}

func (snsArchiveFailWriter) Write([]byte) (int, error) {
	return 0, errors.New("test archive capture failure")
}
