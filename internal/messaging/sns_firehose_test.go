package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type testFirehoseDelivery struct {
	arn     string
	records [][]byte
	failure error
}

func (delivery *testFirehoseDelivery) PutFirehoseRecord(_ context.Context, arn string, data []byte) (string, error) {
	delivery.arn = arn
	delivery.records = append(delivery.records, bytes.Clone(data))
	return "firehose-record", delivery.failure
}

func TestSNSFirehosePortAppliesFiltersAndNativeRawOrEnvelope(t *testing.T) {
	for _, raw := range []string{"true", "false"} {
		t.Run(raw, func(t *testing.T) {
			broker := NewBroker("us-east-1", "000000000000", 0)
			delivery := &testFirehoseDelivery{}
			broker.SetFirehoseDelivery(delivery)
			topic := broker.CreateTopic("native")
			endpoint := "arn:aws:firehose:us-east-1:000000000000:deliverystream/native"
			_, err := broker.subscribeSNS(topic.ARN, "firehose", endpoint, map[string]string{"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns-firehose", "RawMessageDelivery": raw, "FilterPolicy": `{"kind":["accepted"]}`}, "")
			require.NoError(t, err)
			attrs := map[string]MessageAttribute{"kind": {DataType: "String", StringValue: "refused"}}
			_, err = broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: `{"id":1}`, Attributes: attrs})
			require.NoError(t, err)
			require.Empty(t, delivery.records)
			attrs["kind"] = MessageAttribute{DataType: "String", StringValue: "accepted"}
			result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: `{"id":2}`, Attributes: attrs})
			require.NoError(t, err)
			require.Equal(t, endpoint, delivery.arn)
			require.Len(t, delivery.records, 1)
			if raw == "true" {
				require.Equal(t, `{"id":2}`, string(delivery.records[0]))
			} else {
				var envelope map[string]interface{}
				require.NoError(t, json.Unmarshal(delivery.records[0], &envelope))
				require.Equal(t, "Notification", envelope["Type"])
				require.Equal(t, result.MessageID, envelope["MessageId"])
				require.Equal(t, topic.ARN, envelope["TopicArn"])
				require.Equal(t, `{"id":2}`, envelope["Message"])
				require.Contains(t, envelope, "MessageAttributes")
			}
		})
	}
}

func TestSNSFirehoseAdmissionFailureUsesExistingOutcomeCaptureAndRedrive(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	delivery := &testFirehoseDelivery{failure: errors.New("stream unavailable")}
	broker.SetFirehoseDelivery(delivery)
	var capture bytes.Buffer
	broker.SetSNSCapture(NewSNSCapture(&capture))
	topic := broker.CreateTopic("native")
	queue := broker.CreateQueue("deadletters", 0, 0)
	_, err := broker.subscribeSNS(topic.ARN, "firehose", "arn:aws:firehose:us-east-1:000000000000:deliverystream/native", map[string]string{"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns-firehose", "RawMessageDelivery": "true", "RedrivePolicy": `{"deadLetterTargetArn":"` + queue.ARN + `"}`}, "")
	require.NoError(t, err)
	result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "original"})
	require.NoError(t, err)
	require.NotEmpty(t, result.MessageID)
	messages := broker.ReceiveMessages(queue, 1, 0)
	require.Len(t, messages, 1)
	require.Equal(t, "original", messages[0].Body)
	lines := bytes.Split(bytes.TrimSpace(capture.Bytes()), []byte{'\n'})
	require.Len(t, lines, 2)
	var outcome SNSCaptureRecord
	require.NoError(t, json.Unmarshal(lines[1], &outcome))
	require.Equal(t, "DeliveryFailure", outcome.Operation)
	require.Equal(t, "dead_lettered", outcome.Deliveries[0].Status)
}

func TestSNSFirehoseRejectsForeignMalformedEndpointsRolesAndFIFO(t *testing.T) {
	broker := NewBroker("us-east-1", "000000000000", 0)
	topic := broker.CreateTopic("native")
	role := "arn:aws:iam::000000000000:role/sns-firehose"
	for _, endpoint := range []string{"arn:aws:firehose:us-west-2:000000000000:deliverystream/native", "arn:aws:firehose:us-east-1:111111111111:deliverystream/native", "arn:aws:firehose:us-east-1:000000000000:native", "arn:aws:firehose:us-east-1:000000000000:deliverystream/bad/name"} {
		_, err := broker.subscribeSNS(topic.ARN, "firehose", endpoint, map[string]string{"SubscriptionRoleArn": role}, "")
		require.Error(t, err)
	}
	for _, role := range []string{"", "arn:aws:iam::x:role/native", "arn:aws:iam::000000000000:user/native"} {
		_, err := broker.subscribeSNS(topic.ARN, "firehose", "arn:aws:firehose:us-east-1:000000000000:deliverystream/native", map[string]string{"SubscriptionRoleArn": role}, "")
		require.Error(t, err)
	}
	fifo := broker.CreateTopic("native.fifo")
	_, err := broker.subscribeSNS(fifo.ARN, "firehose", "arn:aws:firehose:us-east-1:000000000000:deliverystream/native", map[string]string{"SubscriptionRoleArn": role}, "")
	require.Error(t, err)
}
