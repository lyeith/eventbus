package messaging

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

func TestSNSConfirmedSubscriptionRemainsMutableSDK(t *testing.T) {
	for _, authenticated := range []bool{false, true} {
		t.Run(fmt.Sprint(authenticated), func(t *testing.T) {
			broker, _, client, _ := setupTestServer(t)
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "sns.jsonl")
			capture, err := OpenSNSCapture(path)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, capture.Close()) })
			broker.SetSNSCapture(capture)
			topic := broker.CreateTopic("confirmation")
			subscribed, err := client.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic.ARN), Protocol: aws.String("https"), Endpoint: aws.String("https://example.test/events"), ReturnSubscriptionArn: true})
			require.NoError(t, err)
			token := smsReadCapture(t, path)[0].Details["token"].(string)
			confirmed, err := client.ConfirmSubscription(ctx, &sns.ConfirmSubscriptionInput{TopicArn: aws.String(topic.ARN), Token: aws.String(token), AuthenticateOnUnsubscribe: aws.String(fmt.Sprint(authenticated))})
			require.NoError(t, err)
			require.Equal(t, subscribed.SubscriptionArn, confirmed.SubscriptionArn)
			_, err = client.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: confirmed.SubscriptionArn, AttributeName: aws.String("FilterPolicy"), AttributeValue: aws.String(`{"kind":["allowed"]}`)})
			require.NoError(t, err)
			got, err := client.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: confirmed.SubscriptionArn})
			require.NoError(t, err)
			require.Equal(t, fmt.Sprint(authenticated), got.Attributes["ConfirmationWasAuthenticated"])
			require.JSONEq(t, `{"kind":["allowed"]}`, got.Attributes["FilterPolicy"])
			_, err = client.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: confirmed.SubscriptionArn})
			require.NoError(t, err)
			records := smsReadCapture(t, path)
			restored, err := client.ConfirmSubscription(ctx, &sns.ConfirmSubscriptionInput{TopicArn: aws.String(topic.ARN), Token: aws.String(records[1].Details["token"].(string))})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(restored.SubscriptionArn))
		})
	}
}

func TestSNSConfiguredLargeTopicTargetAndBatchSDK(t *testing.T) {
	broker, _, client, _ := setupTestServer(t)
	ctx := context.Background()
	topic, err := broker.CreateTopicWithAttributes("large", map[string]string{"MaximumMessageSize": "1048576"}, nil, "")
	require.NoError(t, err)
	large := strings.Repeat("x", 300000)
	_, err = client.Publish(ctx, &sns.PublishInput{TargetArn: aws.String(topic.ARN), Message: aws.String(large)})
	require.NoError(t, err)
	batch, err := client.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(topic.ARN), PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{{Id: aws.String("one"), Message: aws.String(large)}, {Id: aws.String("two"), Message: aws.String(large)}}})
	require.NoError(t, err)
	require.Len(t, batch.Successful, 2)
	require.Empty(t, batch.Failed)
	_, err = client.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: aws.String(topic.ARN), PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{{Id: aws.String("one"), Message: aws.String(strings.Repeat("x", 600000))}, {Id: aws.String("two"), Message: aws.String(strings.Repeat("x", 600000))}}})
	require.ErrorContains(t, err, "BatchRequestTooLong")
	_, err = client.AddPermission(ctx, &sns.AddPermissionInput{TopicArn: aws.String(topic.ARN), Label: aws.String("modern"), AWSAccountId: []string{"123456789012"}, ActionName: []string{"PublishBatch", "PutDataProtectionPolicy"}})
	require.NoError(t, err)
	attrs, err := client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: aws.String(topic.ARN)})
	require.NoError(t, err)
	require.Contains(t, attrs.Attributes["Policy"], "SNS:PublishBatch")
}

func TestSNSArchiveReplayAndPauseSDK(t *testing.T) {
	broker, _, client, queues := setupTestServer(t)
	ctx := context.Background()
	created, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("archive.fifo"), Attributes: map[string]string{"FifoTopic": "true", "ArchivePolicy": `{"MessageRetentionPeriod":"1"}`}})
	require.NoError(t, err)
	queue, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("archive.fifo"), Attributes: map[string]string{"FifoQueue": "true"}})
	require.NoError(t, err)
	q := broker.GetQueue("archive.fifo")
	sub, err := client.Subscribe(ctx, &sns.SubscribeInput{TopicArn: created.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(q.ARN)})
	require.NoError(t, err)
	start := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano)
	published, err := client.Publish(ctx, &sns.PublishInput{TopicArn: created.TopicArn, Message: aws.String("archived"), MessageGroupId: aws.String("group"), MessageDeduplicationId: aws.String("original")})
	require.NoError(t, err)
	original, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue.QueueUrl})
	require.NoError(t, err)
	require.Len(t, original.Messages, 1)
	var before map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(original.Messages[0].Body)), &before))
	require.Equal(t, aws.ToString(published.SequenceNumber), before["SequenceNumber"])
	_, err = queues.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: queue.QueueUrl, ReceiptHandle: original.Messages[0].ReceiptHandle})
	require.NoError(t, err)
	policy := fmt.Sprintf(`{"PointType":"Timestamp","StartingPoint":%q,"EndingPoint":%q}`, start, time.Now().UTC().Format(time.RFC3339Nano))
	_, err = client.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn, AttributeName: aws.String("ReplayPolicy"), AttributeValue: aws.String(policy)})
	require.NoError(t, err)
	replay, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue.QueueUrl})
	require.NoError(t, err)
	require.Len(t, replay.Messages, 1)
	var after map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(replay.Messages[0].Body)), &after))
	require.Equal(t, before["MessageId"], after["MessageId"])
	require.Equal(t, before["Timestamp"], after["Timestamp"])
	require.Equal(t, true, after["Replayed"])
	_, err = queues.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: queue.QueueUrl, ReceiptHandle: replay.Messages[0].ReceiptHandle})
	require.NoError(t, err)
	_, err = client.Publish(ctx, &sns.PublishInput{TopicArn: created.TopicArn, Message: aws.String("paused"), MessageGroupId: aws.String("group"), MessageDeduplicationId: aws.String("second")})
	require.NoError(t, err)
	require.Empty(t, broker.ReceiveMessages(q, 1, 0))
	_, err = client.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: created.TopicArn})
	require.ErrorContains(t, err, "InvalidState")
	_, err = client.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{TopicArn: created.TopicArn, AttributeName: aws.String("ArchivePolicy"), AttributeValue: aws.String(`{}`)})
	require.NoError(t, err)
	_, err = client.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: created.TopicArn})
	require.NoError(t, err)
}

func TestSNSMissingQueueRedrivesAndBinaryEnvelope(t *testing.T) {
	broker := newTestBroker()
	topic := broker.CreateTopic("redrive")
	source := broker.CreateQueue("gone", 0, 0)
	dlq := broker.CreateQueue("deadletters", 0, 0)
	_, err := broker.subscribeSNS(topic.ARN, "sqs", source.ARN, map[string]string{"RedrivePolicy": fmt.Sprintf(`{"deadLetterTargetArn":%q}`, dlq.ARN)}, "")
	require.NoError(t, err)
	broker.DeleteQueue(source.Name)
	binary := []byte{0, 1, 255}
	result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "data", Attributes: map[string]MessageAttribute{"blob": {DataType: "Binary.fixture", BinaryValue: binary}}})
	require.NoError(t, err)
	messages := broker.ReceiveMessages(dlq, 1, 0)
	require.Len(t, messages, 1)
	var body snsEnvelope
	require.NoError(t, json.Unmarshal([]byte(messages[0].Body), &body))
	require.Equal(t, result.MessageID, body.MessageID)
	require.Equal(t, base64.StdEncoding.EncodeToString(binary), body.MessageAttributes["blob"].Value)
}

func TestSNSRedrivePolicyIdentityAndAtomicUpdatesSDK(t *testing.T) {
	broker, _, client, _ := setupTestServer(t)
	ctx := context.Background()
	for _, fifo := range []bool{false, true} {
		t.Run(fmt.Sprint(fifo), func(t *testing.T) {
			name := "redrive-validation"
			dlqName := "deadletters"
			if fifo {
				name += ".fifo"
				dlqName += ".fifo"
			}
			topic := broker.CreateTopic(name)
			endpoint := "arn:aws:sqs:us-east-1:000000000000:destination"
			if fifo {
				endpoint += ".fifo"
			}
			valid := fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:%s"}`, dlqName)
			sub, err := client.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic.ARN), Protocol: aws.String("sqs"), Endpoint: aws.String(endpoint), Attributes: map[string]string{"RedrivePolicy": valid}})
			require.NoError(t, err)
			wrongType := dlqName + ".fifo"
			if fifo {
				wrongType = "deadletters"
			}
			invalid := []string{
				fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-west-2:000000000000:%s"}`, dlqName),
				fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:111111111111:%s"}`, dlqName),
				fmt.Sprintf(`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:%s"}`, wrongType),
				`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:invalid/name"}`,
			}
			for _, policy := range invalid {
				_, err = client.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn, AttributeName: aws.String("RedrivePolicy"), AttributeValue: aws.String(policy)})
				require.ErrorContains(t, err, "InvalidParameter")
				attrs, err := client.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn})
				require.NoError(t, err)
				require.Equal(t, valid, attrs.Attributes["RedrivePolicy"])
				_, err = client.Subscribe(ctx, &sns.SubscribeInput{TopicArn: aws.String(topic.ARN), Protocol: aws.String("sqs"), Endpoint: aws.String(endpoint + "other"), Attributes: map[string]string{"RedrivePolicy": policy}})
				require.ErrorContains(t, err, "InvalidParameter")
			}
			_, err = client.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: sub.SubscriptionArn, AttributeName: aws.String("RedrivePolicy"), AttributeValue: aws.String("")})
			require.NoError(t, err)
		})
	}
}

func TestSNSPublicationCaptureFailureDoesNotCommitFIFO(t *testing.T) {
	broker := newTestBroker()
	topic := broker.CreateTopic("capture.fifo")
	capture, err := OpenSNSCapture(filepath.Join(t.TempDir(), "capture.jsonl"))
	require.NoError(t, err)
	require.NoError(t, capture.Close())
	broker.SetSNSCapture(capture)
	input := SNSPublishInput{TopicARN: topic.ARN, Message: "must capture", MessageGroupID: "group", MessageDeduplicationID: "retry"}
	_, err = broker.PublishSNS(input)
	var failure *snsError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "InternalError", failure.Code)
	broker.SetSNSCapture(nil)
	result, err := broker.PublishSNS(input)
	require.NoError(t, err)
	require.Equal(t, "1", result.SequenceNumber)
	duplicate, err := broker.PublishSNS(input)
	require.NoError(t, err)
	require.Equal(t, result, duplicate)
}

type snsAcceptedThenFailWriter struct{ calls int }

func (writer *snsAcceptedThenFailWriter) Write(data []byte) (int, error) {
	writer.calls++
	if writer.calls == 1 {
		return len(data), nil
	}
	return 0, errors.New("outcome disk failure")
}
func TestSNSTerminalOutcomeCapturePreservesAcceptedPublish(t *testing.T) {
	broker := newTestBroker()
	topic := broker.CreateTopic("accepted")
	delivered := broker.CreateQueue("working", 0, 0)
	_, err := broker.subscribeSNS(topic.ARN, "sqs", delivered.ARN, nil, "")
	require.NoError(t, err)
	_, err = broker.subscribeSNS(topic.ARN, "sqs", "arn:aws:sqs:us-east-1:000000000000:missing", nil, "")
	require.NoError(t, err)
	writer := &snsAcceptedThenFailWriter{}
	broker.SetSNSCapture(&SNSCapture{writer: writer})
	result, err := broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "accepted"})
	require.NoError(t, err)
	require.NotEmpty(t, result.MessageID)
	require.Len(t, broker.ReceiveMessages(delivered, 1, 0), 1)
	require.Equal(t, 2, writer.calls)
	_, err = broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: "must not accept"})
	var failure *snsError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "InternalError", failure.Code)
	require.Empty(t, broker.ReceiveMessages(delivered, 1, 0))
	require.Equal(t, 2, writer.calls)
}

func TestSNSTopicSMSProtocolLimitDoesNotBlockSQS(t *testing.T) {
	broker := newTestBroker()
	topic := broker.CreateTopic("multi")
	queue := broker.CreateQueue("multi", 0, 0)
	path := filepath.Join(t.TempDir(), "sns.jsonl")
	capture, err := OpenSNSCapture(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, capture.Close()) })
	broker.SetSNSCapture(capture)
	_, err = broker.subscribeSNS(topic.ARN, "sqs", queue.ARN, nil, "")
	require.NoError(t, err)
	_, err = broker.subscribeSNS(topic.ARN, "sms", "+12065550100", nil, "")
	require.NoError(t, err)
	message, _ := json.Marshal(map[string]string{"default": "short", "sms": strings.Repeat("x", 1601)})
	_, err = broker.PublishSNS(SNSPublishInput{TopicARN: topic.ARN, Message: string(message), MessageStructure: "json"})
	require.NoError(t, err)
	require.Len(t, broker.ReceiveMessages(queue, 1, 0), 1)
	records := smsReadCapture(t, path)
	require.Len(t, records, 2)
	found := false
	for _, delivery := range records[0].Deliveries {
		if delivery.Protocol == "sms" {
			found = true
			require.Equal(t, "dropped", delivery.Status)
			require.Contains(t, delivery.Error, "1600")
		}
	}
	require.True(t, found)
}
