package messaging

import (
	"strings"

	"github.com/lyeith/eventbus/internal/sqsevent"
)

// BuildSQSLambdaEvent projects owned receive snapshots into the one native event
// schema. Legacy consumers and native mappings share this projection; lease,
// execution and settlement policy remain with their respective owners.
func BuildSQSLambdaEvent(messages []*Message, sourceARN string) sqsevent.Event {
	region := sqsRegionFromARN(sourceARN)
	event := sqsevent.Event{Records: make([]sqsevent.Record, 0, len(messages))}
	for _, message := range messages {
		event.Records = append(event.Records, buildSQSLambdaRecord(message, sourceARN, region))
	}
	return event
}

func sqsRegionFromARN(sourceARN string) string {
	if parts := strings.Split(sourceARN, ":"); len(parts) > 3 {
		return parts[3]
	}
	return ""
}

func buildSQSLambdaRecord(message *Message, sourceARN, region string) sqsevent.Record {
	attributes := sqsMessageSystemValues(message)
	// Preserve the existing typed consumer projection for configured system
	// values; native HTTP only exposes its supported/selected system names.
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
	return sqsevent.Record{
		MessageID: message.ID, ReceiptHandle: message.ReceiptHandle, Body: message.Body,
		Attributes: attributes, MessageAttributes: custom, MD5OfBody: md5Body(message.Body),
		EventSource: "aws:sqs", EventSourceARN: sourceARN, AWSRegion: region,
	}
}
