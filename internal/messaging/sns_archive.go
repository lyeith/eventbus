package messaging

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Archives are local, in-memory fixtures. Cap serialized request bytes plus a
// fixed metadata allowance at 64 MiB per topic. Never evict unexpired records to
// make room: reject the new publication before capture instead. Go allocation
// overhead is additional; clearing/pruning releases removed request references.
const snsArchiveMaxBytes = 64 * 1024 * 1024
const snsArchiveMetadataBytes = 512

// Topic.publishMu owns the archive, its byte accounting, and publication order.
type snsArchivedPublication struct {
	Input       SNSPublishInput
	Result      SNSPublishResult
	PublishedAt time.Time
	Size        int
}

// snsArchiveObject accepts exactly one JSON object and rejects duplicate keys;
// policy field handling below then rejects unknown names and incorrect types.
func snsArchiveObject(raw, name string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil, snsInvalid(name + " must be a JSON object")
	}
	values := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		key, valid := token.(string)
		if err != nil || !valid {
			return nil, snsInvalid("Invalid " + name + " JSON")
		}
		if _, exists := values[key]; exists {
			return nil, snsInvalid(name + " must not contain duplicate fields")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, snsInvalid("Invalid " + name + " JSON")
		}
		values[key] = value
	}
	if closing, err := decoder.Token(); err != nil || closing != json.Delim('}') {
		return nil, snsInvalid("Invalid " + name + " JSON")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, snsInvalid(name + " must contain exactly one JSON object")
	}
	return values, nil
}

// An empty object disables archiving and clears existing records. AWS examples
// encode the day count as a string; integer JSON values are accepted as well.
func snsArchiveRetention(policy string) (int, error) {
	values, err := snsArchiveObject(policy, "ArchivePolicy")
	if err != nil {
		return 0, err
	}
	if len(values) == 0 {
		return 0, nil
	}
	if len(values) != 1 || values["MessageRetentionPeriod"] == nil {
		return 0, snsInvalid("ArchivePolicy supports only MessageRetentionPeriod")
	}
	value := values["MessageRetentionPeriod"]
	var days int
	if value[0] == '"' {
		var text string
		if json.Unmarshal(value, &text) != nil {
			return 0, snsInvalid("Invalid MessageRetentionPeriod")
		}
		days, err = strconv.Atoi(text)
	} else {
		err = json.Unmarshal(value, &days)
	}
	if err != nil || days < 1 || days > 365 {
		return 0, snsInvalid("MessageRetentionPeriod must be an integer from 1 to 365 days; use {} to disable archiving")
	}
	return days, nil
}

func snsReplayPolicy(raw string) (time.Time, time.Time, error) {
	values, err := snsArchiveObject(raw, "ReplayPolicy")
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	for name := range values {
		if name != "PointType" && name != "StartingPoint" && name != "EndingPoint" {
			return time.Time{}, time.Time{}, snsInvalid("Unknown ReplayPolicy field: " + name)
		}
	}
	var pointType, startingPoint, endingPoint string
	if json.Unmarshal(values["PointType"], &pointType) != nil || pointType != "Timestamp" {
		return time.Time{}, time.Time{}, snsInvalid("ReplayPolicy PointType must be Timestamp")
	}
	if json.Unmarshal(values["StartingPoint"], &startingPoint) != nil || startingPoint == "" {
		return time.Time{}, time.Time{}, snsInvalid("ReplayPolicy requires StartingPoint")
	}
	start, err := time.Parse(time.RFC3339Nano, startingPoint)
	if err != nil || start.After(time.Now()) {
		return time.Time{}, time.Time{}, snsInvalid("ReplayPolicy StartingPoint must be an RFC3339 timestamp no later than now")
	}
	if endingValue, exists := values["EndingPoint"]; exists {
		if json.Unmarshal(endingValue, &endingPoint) != nil || strings.TrimSpace(string(endingValue)) == "null" {
			return time.Time{}, time.Time{}, snsInvalid("ReplayPolicy EndingPoint must be an RFC3339 timestamp")
		}
	}
	var end time.Time
	if endingPoint != "" {
		end, err = time.Parse(time.RFC3339Nano, endingPoint)
		if err != nil || end.Before(start) {
			return time.Time{}, time.Time{}, snsInvalid("ReplayPolicy EndingPoint must be an RFC3339 timestamp no earlier than StartingPoint")
		}
	}
	return start.UTC(), end.UTC(), nil
}

func snsPruneArchive(topic *Topic, days int, now time.Time) {
	if days <= 0 {
		topic.archive = nil
		topic.archiveBytes = 0
		return
	}
	cutoff := now.Add(-time.Duration(days) * 24 * time.Hour)
	kept := topic.archive[:0]
	bytes := 0
	for _, publication := range topic.archive {
		if publication.PublishedAt.Before(cutoff) {
			continue
		}
		kept = append(kept, publication)
		bytes += publication.Size
	}
	clear(topic.archive[len(kept):])
	topic.archive = kept
	topic.archiveBytes = bytes
	if len(kept) == 0 {
		topic.archive = nil
	}
}

func snsArchiveSize(input SNSPublishInput) int {
	// The wire input is entirely JSON-serializable: strings and binary values.
	encoded, _ := json.Marshal(input)
	return len(encoded) + snsArchiveMetadataBytes
}

func snsPrepareArchive(topic *Topic, input SNSPublishInput, retention int, now time.Time) error {
	snsPruneArchive(topic, retention, now)
	if retention <= 0 {
		return nil
	}
	size := snsArchiveSize(input)
	if size > snsArchiveMaxBytes || topic.archiveBytes > snsArchiveMaxBytes-size {
		return snsInternal("FIFO archive exceeds EventBus's 64 MiB per-topic fixture limit; shorten retention or disable ArchivePolicy to clear it")
	}
	return nil
}

func snsCommitArchive(topic *Topic, input SNSPublishInput, result SNSPublishResult, now time.Time) {
	input.Attributes = cloneMessageAttributes(input.Attributes)
	publication := snsArchivedPublication{Input: input, Result: result, PublishedAt: now.UTC(), Size: snsArchiveSize(input)}
	topic.archive = append(topic.archive, publication)
	topic.archiveBytes += publication.Size
}

// snsReplay runs under publishMu and controlMu, with Topic.mu released. All
// replay intent is captured before queue delivery. Subscription state is owned
// by the caller, which commits ReplayPolicy/ReplayState only after success.
func (b *Broker) snsReplay(topic *Topic, subscription *Subscription, policy, operation, requestID string) error {
	start, end, err := snsReplayPolicy(policy)
	if err != nil {
		return err
	}
	topic.mu.RLock()
	fifo := topic.Attributes["FifoTopic"] == "true"
	archivePolicy := topic.Attributes["ArchivePolicy"]
	topic.mu.RUnlock()
	if !fifo {
		return snsInvalid("ReplayPolicy requires a FIFO topic")
	}
	retention, err := snsArchiveRetention(archivePolicy)
	if err != nil || retention == 0 {
		return snsInvalid("ReplayPolicy requires an active ArchivePolicy")
	}
	now := time.Now().UTC()
	snsPruneArchive(topic, retention, now)
	type replayPublication struct {
		publication snsArchivedPublication
		plans       []snsPlannedDelivery
	}
	selected := make([]replayPublication, 0)
	deliveries := make([]SNSCaptureDelivery, 0)
	records := make([]SNSCaptureRecord, 0)
	for _, publication := range topic.archive {
		if publication.PublishedAt.Before(start) || !end.IsZero() && publication.PublishedAt.After(end) || publication.PublishedAt.After(now) {
			continue
		}
		plans, plannedDeliveries := b.snsPlanDeliveries(publication.Input, publication.Result, []*Subscription{subscription}, publication.PublishedAt, true)
		for index := range plannedDeliveries {
			plannedDeliveries[index].MessageID = publication.Result.MessageID
		}
		record := snsCapturePublication(publication.Input, publication.Result, plannedDeliveries, map[string]any{"replayed": true, "published_at": publication.PublishedAt})
		record.Operation = "Replay"
		record.RequestID = requestID
		record.CapturedAt = now
		records = append(records, record)
		deliveries = append(deliveries, plannedDeliveries...)
		selected = append(selected, replayPublication{publication: publication, plans: plans})
	}
	details := map[string]any{"source_operation": operation, "subscription_arn": subscription.ARN, "starting_point": start, "replayed": true, "publications": records}
	if !end.IsZero() {
		details["ending_point"] = end
	}
	if err := b.CaptureSNS(SNSCaptureRecord{Operation: "Replay", RequestID: requestID, TargetARN: topic.ARN, Deliveries: deliveries, Details: details}); err != nil {
		return snsInternal("Unable to capture SNS replay intent")
	}
	for _, replay := range selected {
		input := replay.publication.Input
		input.RequestID = requestID
		input.Operation = "Replay"
		// Queue deduplication must not suppress a deliberate replay immediately
		// after the original publication, or a second replay of that message.
		dedupID := "replay-" + uuid.NewString()
		if err := b.snsSendDeliveries(input, replay.publication.Result, replay.plans, dedupID, true); err != nil {
			return fmt.Errorf("SNS replay delivery: %w", err)
		}
	}
	return nil
}
