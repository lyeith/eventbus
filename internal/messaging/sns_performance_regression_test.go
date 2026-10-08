package messaging

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func snsPerformanceRegressionSubscription(t *testing.T, broker *Broker, topic *Topic, name string, attributes map[string]string) (*Queue, *Subscription) {
	t.Helper()
	queue := broker.CreateQueue(name, time.Hour, 0)
	subscription, err := broker.subscribeSNS(topic.ARN, "sqs", queue.ARN, attributes, "")
	require.NoError(t, err)
	return queue, subscription
}

func snsPerformanceRegressionReceive(t *testing.T, broker *Broker, queue *Queue) *Message {
	t.Helper()
	messages := broker.ReceiveMessages(queue, 1, 0)
	require.Len(t, messages, 1)
	require.Equal(t, 1, messages[0].ReceiveCount)
	require.True(t, broker.DeleteMessage(queue, messages[0].ReceiptHandle))
	return messages[0]
}

func snsPerformanceRegressionEnvelope(t *testing.T, body string) snsEnvelope {
	t.Helper()
	var envelope snsEnvelope
	require.NoError(t, json.Unmarshal([]byte(body), &envelope))
	return envelope
}

func TestSNSSharedEnvelopePreservesRawFilteringAndSubscriptionSpecificLambdaEvents(t *testing.T) {
	broker := newTestBroker()
	var capture bytes.Buffer
	broker.SetSNSCapture(NewSNSCapture(&capture))
	lambda := &snsTestLambdaDelivery{}
	broker.SetLambdaDelivery(lambda)
	topic := broker.CreateTopic("shared-envelope")
	// Put Lambda before SQS so wrapper type conversion cannot mutate a reused
	// notification subsequently delivered to another protocol.
	var lambdaSubscriptions []*Subscription
	for _, qualifier := range []string{"first", "second"} {
		subscription, err := broker.Subscribe(topic.ARN, "lambda", "arn:aws:lambda:us-east-1:000000000000:function:handler:"+qualifier, nil)
		require.NoError(t, err)
		lambdaSubscriptions = append(lambdaSubscriptions, subscription)
	}
	wrappedOne, _ := snsPerformanceRegressionSubscription(t, broker, topic, "wrapped-one", nil)
	wrappedTwo, _ := snsPerformanceRegressionSubscription(t, broker, topic, "wrapped-two", nil)
	raw, _ := snsPerformanceRegressionSubscription(t, broker, topic, "raw", map[string]string{"RawMessageDelivery": "true"})
	filtered, filteredSubscription := snsPerformanceRegressionSubscription(t, broker, topic, "filtered", map[string]string{"FilterPolicy": `{"kind":["blocked"]}`})
	message := "original <>&\n\"text\""
	input := SNSPublishInput{TopicARN: topic.ARN, Subject: "subject", Message: message, RequestID: "mixed-request", Attributes: map[string]MessageAttribute{
		"kind": {DataType: "String", StringValue: "allowed"}, "number": {DataType: "Number", StringValue: "001.23000"},
		"array": {DataType: "String.Array", StringValue: `["one",2]`}, "binary": {DataType: "Binary.fixture", BinaryValue: []byte{0, 1, 255}},
	}}
	result, err := broker.PublishSNS(input)
	require.NoError(t, err)
	one := snsPerformanceRegressionReceive(t, broker, wrappedOne)
	two := snsPerformanceRegressionReceive(t, broker, wrappedTwo)
	require.Equal(t, one.Body, two.Body, "the exact publication notification must be identical across wrapped destinations")
	require.Empty(t, one.Attributes)
	require.Empty(t, two.Attributes)
	envelope := snsPerformanceRegressionEnvelope(t, one.Body)
	expected := map[string]any{
		"Type": "Notification", "MessageId": result.MessageID, "TopicArn": topic.ARN, "Subject": "subject", "Message": message, "Timestamp": envelope.Timestamp,
		"MessageAttributes": map[string]any{
			"kind": map[string]string{"Type": "String", "Value": "allowed"}, "number": map[string]string{"Type": "Number", "Value": "1.23"},
			"array": map[string]string{"Type": "String.Array", "Value": `["one",2]`}, "binary": map[string]string{"Type": "Binary.fixture", "Value": "AAH/"},
		},
	}
	encoded, err := json.Marshal(expected)
	require.NoError(t, err)
	require.JSONEq(t, string(encoded), one.Body)
	require.Contains(t, one.Body, `\u003c\u003e\u0026`)
	_, err = time.Parse(time.RFC3339Nano, envelope.Timestamp)
	require.NoError(t, err)
	rawMessage := snsPerformanceRegressionReceive(t, broker, raw)
	require.Equal(t, message, rawMessage.Body)
	require.Equal(t, MessageAttribute{DataType: "Number", StringValue: "1.23"}, rawMessage.Attributes["number"])
	require.Equal(t, input.Attributes["binary"], rawMessage.Attributes["binary"])
	require.Equal(t, input.Attributes["kind"], rawMessage.Attributes["kind"])
	require.Empty(t, broker.ReceiveMessages(filtered, 10, 0))
	require.Len(t, lambda.payloads, 2)
	for index, payload := range lambda.payloads {
		var event struct {
			Records []snsLambdaRecord `json:"Records"`
		}
		require.NoError(t, json.Unmarshal(payload, &event))
		require.Len(t, event.Records, 1)
		record := event.Records[0]
		require.Equal(t, lambdaSubscriptions[index].ARN, record.EventSubscriptionARN)
		require.Equal(t, result.MessageID, record.SNS.MessageID)
		require.Equal(t, envelope.Timestamp, record.SNS.Timestamp)
		require.Equal(t, message, record.SNS.Message)
		require.Equal(t, "String", record.SNS.MessageAttributes["number"].Type)
		require.Equal(t, "String", record.SNS.MessageAttributes["array"].Type)
		require.Equal(t, fmt.Sprintf("http://localhost:%d/?Action=Unsubscribe&SubscriptionArn=%s", broker.port, url.QueryEscape(lambdaSubscriptions[index].ARN)), record.SNS.UnsubscribeURL)
	}
	records := snsLambdaCaptureRecords(t, &capture)
	require.Len(t, records, 3, "publication intent plus each distinct Lambda admission must remain captured")
	require.Equal(t, "Publish", records[0].Operation)
	require.Len(t, records[0].Deliveries, 6)
	for _, delivery := range records[0].Deliveries {
		status := "scheduled"
		if delivery.SubscriptionARN == filteredSubscription.ARN {
			status = "filtered"
		}
		require.Equal(t, status, delivery.Status)
		require.Equal(t, result.MessageID, delivery.MessageID)
	}
	for index, record := range records[1:] {
		require.Equal(t, "DeliveryAdmission", record.Operation)
		require.Equal(t, lambdaSubscriptions[index].ARN, record.Deliveries[0].SubscriptionARN)
		require.Equal(t, "mixed-request", record.RequestID)
	}
}

func TestSNSProtocolEnvelopeCacheIsPerPublicationAndKeepsAttributeStripping(t *testing.T) {
	broker := newTestBroker()
	var capture bytes.Buffer
	broker.SetSNSCapture(NewSNSCapture(&capture))
	lambda := &snsTestLambdaDelivery{}
	firehose := &testFirehoseDelivery{}
	broker.SetLambdaDelivery(lambda)
	broker.SetFirehoseDelivery(firehose)
	topic := broker.CreateTopic("protocol-envelope")
	wrappedOne, _ := snsPerformanceRegressionSubscription(t, broker, topic, "protocol-one", nil)
	wrappedTwo, _ := snsPerformanceRegressionSubscription(t, broker, topic, "protocol-two", nil)
	raw, _ := snsPerformanceRegressionSubscription(t, broker, topic, "protocol-raw", map[string]string{"RawMessageDelivery": "true"})
	filtered, _ := snsPerformanceRegressionSubscription(t, broker, topic, "protocol-filtered", map[string]string{"FilterPolicy": `{"kind":["blocked"]}`, "FilterPolicyScope": "MessageBody"})
	_, err := broker.Subscribe(topic.ARN, "lambda", snsTestLambdaARN, nil)
	require.NoError(t, err)
	_, err = broker.subscribeSNS(topic.ARN, "firehose", "arn:aws:firehose:us-east-1:000000000000:deliverystream/protocol", map[string]string{"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns-firehose"}, "")
	require.NoError(t, err)
	var previousID string
	for index := range 2 {
		selected := fmt.Sprintf(`{"kind":"allowed","text":"sqs-%d<>&"}`, index)
		lambdaText := fmt.Sprintf("lambda-%d", index)
		firehoseText := fmt.Sprintf("firehose-%d", index)
		protocols, err := json.Marshal(map[string]string{"default": fmt.Sprintf("default-%d", index), "sqs": selected, "lambda": lambdaText, "firehose": firehoseText})
		require.NoError(t, err)
		input := SNSPublishInput{TopicARN: topic.ARN, Subject: fmt.Sprintf("subject-%d", index), Message: string(protocols), MessageStructure: "json", Attributes: map[string]MessageAttribute{"kind": {DataType: "String", StringValue: "original-attribute"}}}
		result, err := broker.PublishSNS(input)
		require.NoError(t, err)
		require.NotEqual(t, previousID, result.MessageID, "cache lifetime cannot cross publications")
		previousID = result.MessageID
		one := snsPerformanceRegressionReceive(t, broker, wrappedOne)
		two := snsPerformanceRegressionReceive(t, broker, wrappedTwo)
		require.Equal(t, one.Body, two.Body)
		envelope := snsPerformanceRegressionEnvelope(t, one.Body)
		expected := map[string]any{"Type": "Notification", "MessageId": result.MessageID, "TopicArn": topic.ARN, "Subject": input.Subject, "Message": selected, "Timestamp": envelope.Timestamp}
		encoded, err := json.Marshal(expected)
		require.NoError(t, err)
		require.JSONEq(t, string(encoded), one.Body)
		require.Empty(t, envelope.MessageAttributes)
		rawMessage := snsPerformanceRegressionReceive(t, broker, raw)
		require.Equal(t, selected, rawMessage.Body)
		require.Empty(t, rawMessage.Attributes)
		require.Empty(t, broker.ReceiveMessages(filtered, 10, 0))
		firehoseEnvelope := snsPerformanceRegressionEnvelope(t, string(firehose.records[index]))
		require.Equal(t, firehoseText, firehoseEnvelope.Message)
		require.Equal(t, result.MessageID, firehoseEnvelope.MessageID)
		require.Equal(t, envelope.Timestamp, firehoseEnvelope.Timestamp)
		require.Empty(t, firehoseEnvelope.MessageAttributes)
		var lambdaEvent struct {
			Records []snsLambdaRecord `json:"Records"`
		}
		require.NoError(t, json.Unmarshal(lambda.payloads[index], &lambdaEvent))
		require.Len(t, lambdaEvent.Records, 1)
		require.Equal(t, lambdaText, lambdaEvent.Records[0].SNS.Message)
		require.Equal(t, result.MessageID, lambdaEvent.Records[0].SNS.MessageID)
		require.Equal(t, envelope.Timestamp, lambdaEvent.Records[0].SNS.Timestamp)
		require.Empty(t, lambdaEvent.Records[0].SNS.MessageAttributes)
	}
	records := snsLambdaCaptureRecords(t, &capture)
	require.Len(t, records, 4)
	for _, record := range records {
		if record.Operation == "Publish" {
			require.Equal(t, "original-attribute", record.MessageAttributes["kind"].StringValue, "native capture keeps the caller's original request attributes")
		}
	}
}
