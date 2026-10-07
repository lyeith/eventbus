package messaging

import (
	"crypto/md5"
	"fmt"
	"strconv"
	"strings"

	"github.com/lyeith/eventbus/internal/sqsevent"
)

// BuildSQSLambdaEvent projects owned receive snapshots into the one native event
// schema. Legacy consumers and native mappings share this projection; lease,
// execution and settlement policy remain with their respective owners.
func BuildSQSLambdaEvent(messages []*Message, sourceARN string) sqsevent.Event {
	var region string
	if parts := strings.Split(sourceARN, ":"); len(parts) > 3 {
		region = parts[3]
	}
	event := sqsevent.Event{Records: make([]sqsevent.Record, 0, len(messages))}
	for _, message := range messages {
		attributes := map[string]string{
			"ApproximateReceiveCount": strconv.Itoa(message.ReceiveCount),
			"SenderId":                message.SenderID,
			"SentTimestamp":           strconv.FormatInt(message.SentTimestamp.UnixMilli(), 10),
		}
		if !message.FirstReceivedAt.IsZero() {
			attributes["ApproximateFirstReceiveTimestamp"] = strconv.FormatInt(message.FirstReceivedAt.UnixMilli(), 10)
		}
		for name, value := range map[string]string{
			"MessageGroupId": message.GroupID, "MessageDeduplicationId": message.DeduplicationID,
			"SequenceNumber": message.SequenceNumber, "DeadLetterQueueSourceArn": message.OriginalSourceARN,
		} {
			if value != "" {
				attributes[name] = value
			}
		}
		for name, value := range message.SystemAttributes {
			attributes[name] = value.StringValue
		}
		custom := make(map[string]sqsevent.MessageAttribute, len(message.Attributes))
		for name, value := range message.Attributes {
			attribute := sqsevent.MessageAttribute{DataType: value.DataType, StringListValues: []string{}, BinaryListValues: [][]byte{}}
			if strings.HasPrefix(value.DataType, "Binary") {
				attribute.BinaryValue = append([]byte(nil), value.BinaryValue...)
			} else {
				text := value.StringValue
				attribute.StringValue = &text
			}
			custom[name] = attribute
		}
		event.Records = append(event.Records, sqsevent.Record{
			MessageID: message.ID, ReceiptHandle: message.ReceiptHandle, Body: message.Body,
			Attributes: attributes, MessageAttributes: custom,
			MD5OfBody:   fmt.Sprintf("%x", md5.Sum([]byte(message.Body))),
			EventSource: "aws:sqs", EventSourceARN: sourceARN, AWSRegion: region,
		})
	}
	return event
}
