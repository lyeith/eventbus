package messaging

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSNSPreparedFiltersKeepPublicationAndPolicyIsolation(t *testing.T) {
	broker := newTestBroker()
	topic := broker.CreateTopic("prepared-policy-isolation")
	tests := []struct {
		name, policy, scope string
		want                [2]bool
	}{
		{"body-east", `{"Records":{"kind":["east"],"amount":[{"numeric":["=",1.5]}]}}`, "MessageBody", [2]bool{true, false}},
		{"body-west", `{"Records":{"kind":["west"],"amount":[2]}}`, "MessageBody", [2]bool{false, true}},
		{"attribute-east", `{"tags":["east"],"amount":[1.5]}`, "MessageAttributes", [2]bool{true, false}},
		{"attribute-west", `{"tags":["west"],"amount":[{"numeric":["=",2]}]}`, "MessageAttributes", [2]bool{false, true}},
		{"empty-body-policy", "{}", "MessageBody", [2]bool{true, true}},
	}
	queues := make([]*Queue, len(tests))
	for index, test := range tests {
		queues[index], _ = snsPerformanceRegressionSubscription(t, broker, topic, test.name, map[string]string{
			"RawMessageDelivery": "true", "FilterPolicy": test.policy, "FilterPolicyScope": test.scope,
		})
	}
	bodies := []string{`{"Records":[[{"kind":"east","amount":1.50000}]]}`, `{"Records":[{"kind":"west","amount":2e0}]}`}
	for publication, body := range bodies {
		tag, number := "east", "1.50000"
		if publication == 1 {
			tag, number = "west", "2e0"
		}
		_, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: body, Attributes: map[string]MessageAttribute{
			"tags":   {DataType: "String.Array", StringValue: `["` + tag + `",true]`},
			"amount": {DataType: "Number.custom", StringValue: number},
		}})
		require.NoError(t, err)
		for index, test := range tests {
			messages := broker.ReceiveMessages(queues[index], 10, 0)
			if !test.want[publication] {
				require.Empty(t, messages, "%s publication %d", test.name, publication)
				continue
			}
			require.Len(t, messages, 1, "%s publication %d", test.name, publication)
			require.Equal(t, body, messages[0].Body)
			require.Equal(t, `["`+tag+`",true]`, messages[0].Attributes["tags"].StringValue)
			require.True(t, broker.DeleteMessage(queues[index], messages[0].ReceiptHandle))
		}
	}
}

func TestSNSPreparedFiltersKeepSelectedProtocolBodyAndAttributeStripping(t *testing.T) {
	broker := newTestBroker()
	topic := broker.CreateTopic("prepared-protocol-bodies")
	lambda := &snsTestLambdaDelivery{}
	firehose := &testFirehoseDelivery{}
	broker.SetLambdaDelivery(lambda)
	broker.SetFirehoseDelivery(firehose)
	var capture bytes.Buffer
	broker.SetSNSCapture(NewSNSCapture(&capture))
	queue, _ := snsPerformanceRegressionSubscription(t, broker, topic, "prepared-protocol-queue", map[string]string{
		"FilterPolicyScope": "MessageBody", "FilterPolicy": `{"Records":{"kind":["queue"],"amount":[1.5]}}`,
	})
	wrongBody, _ := snsPerformanceRegressionSubscription(t, broker, topic, "prepared-protocol-wrong", map[string]string{
		"FilterPolicyScope": "MessageBody", "FilterPolicy": `{"Records":{"kind":["lambda"]}}`,
	})
	stripped, _ := snsPerformanceRegressionSubscription(t, broker, topic, "prepared-protocol-attributes", map[string]string{
		"FilterPolicy": `{"kind":["supplied"]}`,
	})
	_, err := broker.subscribeSNS(topic.ARN, "lambda", snsTestLambdaARN, map[string]string{
		"FilterPolicyScope": "MessageBody", "FilterPolicy": `{"Records":{"kind":["lambda"],"amount":[{"numeric":["=",1.5]}]}}`,
	}, "")
	require.NoError(t, err)
	_, err = broker.subscribeSNS(topic.ARN, "firehose", "arn:aws:firehose:us-east-1:000000000000:deliverystream/prepared", map[string]string{
		"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/sns-firehose",
		"FilterPolicyScope":   "MessageBody", "FilterPolicy": `{"Records":{"kind":["firehose"],"amount":[1.5]}}`,
	}, "")
	require.NoError(t, err)
	bodyQueue := `{"Records":[[{"kind":"queue","amount":1.50000}]]}`
	bodyLambda := `{"Records":[{"kind":"lambda","amount":1.5}]}`
	bodyFirehose := `{"Records":[{"kind":"firehose","amount":15e-1}]}`
	protocols, err := json.Marshal(map[string]string{"default": bodyLambda, "sqs": bodyQueue, "firehose": bodyFirehose})
	require.NoError(t, err)
	attrs := map[string]MessageAttribute{"kind": {DataType: "String", StringValue: "supplied"}}
	result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: string(protocols), MessageStructure: "json", Attributes: attrs})
	require.NoError(t, err)
	envelope := snsPerformanceRegressionEnvelope(t, snsPerformanceRegressionReceive(t, broker, queue).Body)
	require.Equal(t, bodyQueue, envelope.Message)
	require.Empty(t, envelope.MessageAttributes)
	require.Empty(t, broker.ReceiveMessages(wrongBody, 10, 0))
	require.Empty(t, broker.ReceiveMessages(stripped, 10, 0))
	require.Len(t, lambda.payloads, 1)
	var event struct {
		Records []snsLambdaRecord `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(lambda.payloads[0], &event))
	require.Len(t, event.Records, 1)
	require.Equal(t, bodyLambda, event.Records[0].SNS.Message, "missing protocol uses the selected default")
	require.Empty(t, event.Records[0].SNS.MessageAttributes)
	require.Equal(t, result.MessageID, event.Records[0].SNS.MessageID)
	require.Len(t, firehose.records, 1)
	envelope = snsPerformanceRegressionEnvelope(t, string(firehose.records[0]))
	require.Equal(t, bodyFirehose, envelope.Message)
	require.Empty(t, envelope.MessageAttributes)
	require.Equal(t, result.MessageID, envelope.MessageID)
	records := snsLambdaCaptureRecords(t, &capture)
	require.Len(t, records, 2)
	require.Equal(t, attrs, records[0].MessageAttributes, "capture preserves original attributes")

	// Invalid/nonobject bodies reject nonempty filters, while empty policies
	// keep accepting. Preparing inputs cannot turn absent JSON into an object.
	empty, _ := snsPerformanceRegressionSubscription(t, broker, topic, "prepared-empty-policy", map[string]string{
		"FilterPolicyScope": "MessageBody", "FilterPolicy": "{}", "RawMessageDelivery": "true",
	})
	for _, invalid := range []string{"not JSON", "null", "[]"} {
		_, err = broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: invalid})
		require.NoError(t, err)
		require.Equal(t, invalid, snsPerformanceRegressionReceive(t, broker, empty).Body)
		require.Empty(t, broker.ReceiveMessages(queue, 10, 0))
		require.Empty(t, broker.ReceiveMessages(wrongBody, 10, 0))
	}
	require.Len(t, lambda.payloads, 1)
	require.Len(t, firehose.records, 1)
}
