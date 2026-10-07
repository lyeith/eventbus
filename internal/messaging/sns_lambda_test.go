package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const snsTestLambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:handler:live"

type snsTestLambdaDelivery struct {
	validated, admitted []string
	payloads            [][]byte
	validationErr       error
	admissionErr        error
}

func (delivery *snsTestLambdaDelivery) ValidateLambdaTarget(_ context.Context, target string) error {
	delivery.validated = append(delivery.validated, target)
	return delivery.validationErr
}
func (delivery *snsTestLambdaDelivery) AdmitSNSLambda(ctx context.Context, target string, payload []byte) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	delivery.admitted = append(delivery.admitted, target)
	delivery.payloads = append(delivery.payloads, bytes.Clone(payload))
	return "local-invocation", delivery.admissionErr
}

func snsLambdaCaptureRecords(t *testing.T, capture *bytes.Buffer) []SNSCaptureRecord {
	t.Helper()
	var records []SNSCaptureRecord
	for _, line := range bytes.Split(bytes.TrimSpace(capture.Bytes()), []byte{'\n'}) {
		var record SNSCaptureRecord
		require.NoError(t, json.Unmarshal(line, &record))
		records = append(records, record)
	}
	return records
}

func TestSNSLambdaUsesNativeEventAndSeparateAdmissionEvidence(t *testing.T) {
	broker := newTestBroker()
	var capture bytes.Buffer
	broker.SetSNSCapture(NewSNSCapture(&capture))
	delivery := &snsTestLambdaDelivery{}
	broker.SetLambdaDelivery(delivery)
	topic := broker.CreateTopic("native-lambda")
	subscription, err := broker.Subscribe(topic.ARN, "lambda", snsTestLambdaARN, nil)
	require.NoError(t, err)
	before := time.Now().UTC()
	result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Subject: "subject", Message: `{"id":1}`, RequestID: "publish-request", Attributes: map[string]MessageAttribute{
		"text": {DataType: "String", StringValue: "value"}, "number": {DataType: "Number", StringValue: "001.23000"},
		"array": {DataType: "String.Array", StringValue: `["one",2]`}, "binary": {DataType: "Binary", BinaryValue: []byte{0, 1, 255}},
	}})
	require.NoError(t, err)
	require.Equal(t, []string{snsTestLambdaARN}, delivery.validated)
	require.Equal(t, []string{snsTestLambdaARN}, delivery.admitted)
	require.Len(t, delivery.payloads, 1)
	var event struct {
		Records []snsLambdaRecord `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(delivery.payloads[0], &event))
	require.Len(t, event.Records, 1)
	record := event.Records[0]
	require.Equal(t, "aws:sns", record.EventSource)
	require.Equal(t, "1.0", record.EventVersion)
	require.Equal(t, subscription.ARN, record.EventSubscriptionARN)
	require.Equal(t, "Notification", record.SNS.Type)
	require.Equal(t, result.MessageID, record.SNS.MessageID)
	require.Equal(t, topic.ARN, record.SNS.TopicArn)
	require.Equal(t, "subject", record.SNS.Subject)
	require.Equal(t, `{"id":1}`, record.SNS.Message)
	publishedAt, err := time.Parse(time.RFC3339Nano, record.SNS.Timestamp)
	require.NoError(t, err)
	require.False(t, publishedAt.Before(before))
	require.Equal(t, time.UTC, publishedAt.Location())
	require.Equal(t, snsEnvelopeAttr{Type: "String", Value: "1.23"}, record.SNS.MessageAttributes["number"])
	require.Equal(t, snsEnvelopeAttr{Type: "String", Value: `["one",2]`}, record.SNS.MessageAttributes["array"])
	require.Equal(t, snsEnvelopeAttr{Type: "String", Value: "value"}, record.SNS.MessageAttributes["text"])
	require.Equal(t, snsEnvelopeAttr{Type: "Binary", Value: "AAH/"}, record.SNS.MessageAttributes["binary"])
	require.Contains(t, record.SNS.UnsubscribeURL, "http://localhost:")
	require.NotContains(t, string(delivery.payloads[0]), "SigningCertURL")
	evidence := snsLambdaCaptureRecords(t, &capture)
	require.Len(t, evidence, 2)
	require.Equal(t, "Publish", evidence[0].Operation)
	require.Equal(t, "scheduled", evidence[0].Deliveries[0].Status)
	require.Equal(t, "DeliveryAdmission", evidence[1].Operation)
	require.Equal(t, "publish-request", evidence[1].RequestID)
	require.Equal(t, result.MessageID, evidence[1].MessageID)
	require.Equal(t, "admitted", evidence[1].Deliveries[0].Status)
	require.Equal(t, "local-invocation", evidence[1].Deliveries[0].InvocationRequestID)
	require.Equal(t, subscription.ARN, evidence[1].Deliveries[0].SubscriptionARN)
}

func TestSNSLambdaFiltersBeforeResolvingTargetAndChoosesProtocolMessage(t *testing.T) {
	for _, scope := range []string{"MessageAttributes", "MessageBody"} {
		t.Run(scope, func(t *testing.T) {
			broker := newTestBroker()
			delivery := &snsTestLambdaDelivery{validationErr: errors.New("filtered target does not exist")}
			broker.SetLambdaDelivery(delivery)
			topic := broker.CreateTopic("lambda-filter")
			_, err := broker.subscribeSNS(topic.ARN, "lambda", snsTestLambdaARN, map[string]string{"FilterPolicyScope": scope, "FilterPolicy": `{"kind":["accepted"]}`}, "")
			require.NoError(t, err)
			_, err = broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: `{"kind":"refused"}`, Attributes: map[string]MessageAttribute{"kind": {DataType: "String", StringValue: "refused"}}})
			require.NoError(t, err)
			require.Empty(t, delivery.validated)
			delivery.validationErr = nil
			input := SNSPublishInput{TopicARN: topic.ARN, Message: `{"kind":"accepted"}`, Attributes: map[string]MessageAttribute{"kind": {DataType: "String", StringValue: "accepted"}}}
			if scope == "MessageBody" {
				input.MessageStructure = "json"
				input.Message = `{"default":"default","lambda":"{\"kind\":\"accepted\"}"}`
			}
			_, err = broker.PublishSNS(input)
			require.NoError(t, err)
			require.Len(t, delivery.payloads, 1)
			var event map[string][]map[string]any
			require.NoError(t, json.Unmarshal(delivery.payloads[0], &event))
			notification := event["Records"][0]["Sns"].(map[string]any)
			require.Equal(t, `{"kind":"accepted"}`, notification["Message"])
			if scope == "MessageBody" {
				require.NotContains(t, notification, "MessageAttributes")
			}
		})
	}
}

func TestSNSLambdaDeliveryFailuresCaptureAndUseSNSAdmissionDLQ(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		delivery *snsTestLambdaDelivery
	}{
		{"unconfigured", nil},
		{"unknown function or alias", &snsTestLambdaDelivery{validationErr: errors.New("missing target")}},
		{"admission pressure", &snsTestLambdaDelivery{admissionErr: errors.New("bounded admission exhausted")}},
	} {
		for _, redrive := range []bool{false, true} {
			t.Run(scenario.name+"/redrive="+map[bool]string{false: "false", true: "true"}[redrive], func(t *testing.T) {
				broker := newTestBroker()
				if scenario.delivery != nil {
					broker.SetLambdaDelivery(scenario.delivery)
				}
				var capture bytes.Buffer
				broker.SetSNSCapture(NewSNSCapture(&capture))
				topic := broker.CreateTopic("lambda-errors")
				queue := broker.CreateQueue("deadletters", 0, 0)
				attributes := map[string]string{}
				if redrive {
					attributes["RedrivePolicy"] = `{"deadLetterTargetArn":"` + queue.ARN + `"}`
				}
				subscription, err := broker.subscribeSNS(topic.ARN, "lambda", snsTestLambdaARN, attributes, "")
				require.NoError(t, err)
				result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "original"})
				require.NoError(t, err, "publication acceptance is independent of fanout failure")
				evidence := snsLambdaCaptureRecords(t, &capture)
				require.Len(t, evidence, 2)
				require.Equal(t, "DeliveryFailure", evidence[1].Operation)
				require.Equal(t, result.MessageID, evidence[1].MessageID)
				require.Equal(t, subscription.ARN, evidence[1].Deliveries[0].SubscriptionARN)
				require.Equal(t, result.MessageID, evidence[1].Deliveries[0].MessageID)
				require.NotEmpty(t, evidence[1].Deliveries[0].Error)
				messages := broker.ReceiveMessages(queue, 1, 0)
				if redrive {
					require.Equal(t, "dead_lettered", evidence[1].Deliveries[0].Status)
					require.Len(t, messages, 1)
					var envelope snsEnvelope
					require.NoError(t, json.Unmarshal([]byte(messages[0].Body), &envelope))
					require.Equal(t, result.MessageID, envelope.MessageID)
					require.Equal(t, "original", envelope.Message)
				} else {
					require.Equal(t, "dropped", evidence[1].Deliveries[0].Status)
					require.Empty(t, messages)
				}
			})
		}
	}
}

func TestSNSLambdaRequiresOwnedEndpointAndRejectsUnsupportedDeliveryOptions(t *testing.T) {
	broker := newTestBroker()
	topic := broker.CreateTopic("lambda-owned")
	for _, endpoint := range []string{
		"arn:aws:lambda:us-west-2:000000000000:function:handler", "arn:aws:lambda:us-east-1:111111111111:function:handler",
		"arn:aws-cn:lambda:us-east-1:000000000000:function:handler", "arn:aws:lambda:us-east-1:000000000000:handler",
		"arn:aws:lambda:us-east-1:000000000000:function:bad/name", "arn:aws:lambda:us-east-1:000000000000:function:handler:alias:extra",
		"arn:aws:lambda:us-east-1:000000000000:function:" + strings.Repeat("a", 65),
	} {
		_, err := broker.Subscribe(topic.ARN, "lambda", endpoint, nil)
		require.Error(t, err, endpoint)
	}
	for _, attributes := range []map[string]string{
		{"RawMessageDelivery": "true"}, {"DeliveryPolicy": "{}"}, {"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/native"},
		{"ReplayPolicy": `{"StartingPoint":0}`}, {"Unsupported": "true"},
	} {
		_, err := broker.subscribeSNS(topic.ARN, "lambda", snsTestLambdaARN, attributes, "")
		require.Error(t, err, attributes)
	}
	fifo := broker.CreateTopic("lambda-owned.fifo")
	_, err := broker.Subscribe(fifo.ARN, "lambda", snsTestLambdaARN, nil)
	require.Error(t, err)
	for _, suffix := range []string{"", ":1", ":live", ":$LATEST"} {
		_, err := broker.Subscribe(topic.ARN, "lambda", strings.TrimSuffix(snsTestLambdaARN, ":live")+suffix, nil)
		require.NoError(t, err)
	}
}

func TestSNSLambdaAdmissionNeverRunsWhenInitialCaptureFails(t *testing.T) {
	broker := newTestBroker()
	delivery := &snsTestLambdaDelivery{}
	broker.SetLambdaDelivery(delivery)
	broker.SetSNSCapture(NewSNSCapture(&snsFailingWriter{failure: errors.New("disk full")}))
	topic := broker.CreateTopic("capture-first")
	_, err := broker.Subscribe(topic.ARN, "lambda", snsTestLambdaARN, nil)
	require.NoError(t, err)
	_, err = broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "body"})
	require.Error(t, err)
	require.Empty(t, delivery.admitted)
}
