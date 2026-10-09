package messaging

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
)

// Native receive and Lambda events share the same SQS-owned system values.
// Each adapter still selects fields and encodes its own wire representation.
func sqsMessageSystemValues(message *Message) map[string]string {
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
	if trace, ok := message.SystemAttributes["AWSTraceHeader"]; ok {
		attributes["AWSTraceHeader"] = trace.StringValue
	}
	return attributes
}

func md5Body(body string) string { sum := md5.Sum([]byte(body)); return hex.EncodeToString(sum[:]) }
func md5Attributes(attributes map[string]MessageAttribute) string {
	if len(attributes) == 0 {
		return ""
	}
	names := make([]string, 0, len(attributes))
	for name := range attributes {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := md5.New()
	encode := func(value []byte) {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(value)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write(value)
	}
	for _, name := range names {
		attribute := attributes[name]
		encode([]byte(name))
		encode([]byte(attribute.DataType))
		if strings.SplitN(attribute.DataType, ".", 2)[0] == "Binary" {
			_, _ = hash.Write([]byte{2})
			encode(attribute.BinaryValue)
		} else {
			_, _ = hash.Write([]byte{1})
			encode([]byte(attribute.StringValue))
		}
	}
	return hex.EncodeToString(hash.Sum(nil))
}
