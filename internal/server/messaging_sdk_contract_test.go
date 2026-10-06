package server_test

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func messagingSDKServer(t *testing.T) (*httptest.Server, *sns.Client, *sqs.Client, string) {
	t.Helper()
	listener := httptest.NewUnstartedServer(nil)
	broker := messaging.NewBroker("us-east-1", "000000000000", listener.Listener.Addr().(*net.TCPAddr).Port)
	capturePath := filepath.Join(t.TempDir(), "sns.jsonl")
	capture, err := messaging.OpenSNSCapture(capturePath)
	require.NoError(t, err)
	broker.SetSNSCapture(capture)
	listener.Config.Handler = server.New(server.Services{Messaging: messaging.NewHandler(broker)})
	listener.Start()
	t.Cleanup(func() { listener.Close(); require.NoError(t, capture.Close()) })
	cfg := aws.Config{Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), HTTPClient: &http.Client{Timeout: 10 * time.Second}, RetryMaxAttempts: 1}
	notifications := sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = aws.String(listener.URL) })
	queues := sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String(listener.URL) })
	return listener, notifications, queues, capturePath
}

// Inventories are from the AWS API operation indexes, not emulator dispatch:
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_Operations.html
// https://docs.aws.amazon.com/sns/latest/api/API_Operations.html
// These requests deliberately omit required fields: a supported operation must
// validate its own input, rather than disappear at the real server dispatcher.
func TestAWSMessagingOperationInventoriesReachTheirService(t *testing.T) {
	listener, _, _, _ := messagingSDKServer(t)
	operations := map[string][]string{
		"SQS": {"AddPermission", "CancelMessageMoveTask", "ChangeMessageVisibility", "ChangeMessageVisibilityBatch", "CreateQueue", "DeleteMessage", "DeleteMessageBatch", "DeleteQueue", "GetQueueAttributes", "GetQueueUrl", "ListDeadLetterSourceQueues", "ListMessageMoveTasks", "ListQueues", "ListQueueTags", "PurgeQueue", "ReceiveMessage", "RemovePermission", "SendMessage", "SendMessageBatch", "SetQueueAttributes", "StartMessageMoveTask", "TagQueue", "UntagQueue"},
		"SNS": {"AddPermission", "CheckIfPhoneNumberIsOptedOut", "ConfirmSubscription", "CreatePlatformApplication", "CreatePlatformEndpoint", "CreateSMSSandboxPhoneNumber", "CreateTopic", "DeleteEndpoint", "DeletePlatformApplication", "DeleteSMSSandboxPhoneNumber", "DeleteTopic", "GetDataProtectionPolicy", "GetEndpointAttributes", "GetPlatformApplicationAttributes", "GetSMSAttributes", "GetSMSSandboxAccountStatus", "GetSubscriptionAttributes", "GetTopicAttributes", "ListEndpointsByPlatformApplication", "ListOriginationNumbers", "ListPhoneNumbersOptedOut", "ListPlatformApplications", "ListSMSSandboxPhoneNumbers", "ListSubscriptions", "ListSubscriptionsByTopic", "ListTagsForResource", "ListTopics", "OptInPhoneNumber", "Publish", "PublishBatch", "PutDataProtectionPolicy", "RemovePermission", "SetEndpointAttributes", "SetPlatformApplicationAttributes", "SetSMSAttributes", "SetSubscriptionAttributes", "SetTopicAttributes", "Subscribe", "TagResource", "Unsubscribe", "UntagResource", "VerifySMSSandboxPhoneNumber"},
	}
	for service, actions := range operations {
		for _, action := range actions {
			t.Run(service+"/"+action, func(t *testing.T) {
				var request *http.Request
				if service == "SQS" {
					request, _ = http.NewRequest(http.MethodPost, listener.URL, strings.NewReader(`{}`))
					request.Header.Set("Content-Type", "application/x-amz-json-1.0")
					request.Header.Set("X-Amz-Target", "AmazonSQS."+action)
				} else {
					data := url.Values{"Action": {action}, "Version": {"2010-03-31"}}.Encode()
					request, _ = http.NewRequest(http.MethodPost, listener.URL, strings.NewReader(data))
					request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				}
				response, err := listener.Client().Do(request)
				require.NoError(t, err)
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.NotEmpty(t, body)
				require.NotContains(t, string(body), "InvalidAction")
				require.NotContains(t, string(body), "UnknownOperation")
				require.NotContains(t, string(body), "not implemented")
				require.Less(t, response.StatusCode, 500)
			})
		}
	}
}

func TestDirectSQSAndSNSRawFanoutThroughAWSClients(t *testing.T) {
	_, notifications, queues, _ := messagingSDKServer(t)
	ctx := context.Background()
	created, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("contract-queue"), Attributes: map[string]string{"VisibilityTimeout": "30", "ReceiveMessageWaitTimeSeconds": "0"}, Tags: map[string]string{"owner": "agent-proof"}})
	require.NoError(t, err)
	sent, err := queues.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: created.QueueUrl, MessageBody: aws.String("direct body"), MessageAttributes: map[string]sqstypes.MessageAttributeValue{
		"kind":  {DataType: aws.String("String"), StringValue: aws.String("direct")},
		"bytes": {DataType: aws.String("Binary"), BinaryValue: []byte{0, 1, 255}},
	}})
	require.NoError(t, err)
	digest := md5.Sum([]byte("direct body"))
	require.Equal(t, hex.EncodeToString(digest[:]), aws.ToString(sent.MD5OfMessageBody))
	require.NotEmpty(t, aws.ToString(sent.MD5OfMessageAttributes))
	received, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MessageAttributeNames: []string{"All"}, MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll}})
	require.NoError(t, err)
	require.Len(t, received.Messages, 1)
	first := received.Messages[0]
	require.Equal(t, aws.ToString(sent.MessageId), aws.ToString(first.MessageId))
	require.Equal(t, "direct body", aws.ToString(first.Body))
	require.Equal(t, []byte{0, 1, 255}, first.MessageAttributes["bytes"].BinaryValue)
	require.Equal(t, "1", first.Attributes["ApproximateReceiveCount"])
	changed, err := queues.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: created.QueueUrl, Entries: []sqstypes.ChangeMessageVisibilityBatchRequestEntry{{Id: aws.String("ready"), ReceiptHandle: first.ReceiptHandle, VisibilityTimeout: 0}}})
	require.NoError(t, err)
	require.Len(t, changed.Successful, 1)
	again, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll}})
	require.NoError(t, err)
	require.Len(t, again.Messages, 1)
	require.Equal(t, "2", again.Messages[0].Attributes["ApproximateReceiveCount"])
	require.NotEqual(t, aws.ToString(first.ReceiptHandle), aws.ToString(again.Messages[0].ReceiptHandle))
	deleted, err := queues.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []sqstypes.DeleteMessageBatchRequestEntry{{Id: aws.String("valid"), ReceiptHandle: again.Messages[0].ReceiptHandle}, {Id: aws.String("invalid"), ReceiptHandle: aws.String("not-a-receipt")}}})
	require.NoError(t, err)
	require.Len(t, deleted.Successful, 1)
	require.Len(t, deleted.Failed, 1)
	require.True(t, deleted.Failed[0].SenderFault)
	tags, err := queues.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: created.QueueUrl})
	require.NoError(t, err)
	require.Equal(t, "agent-proof", tags.Tags["owner"])
	attributes, err := queues.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: created.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	require.NoError(t, err)
	topic, err := notifications.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("contract-topic")})
	require.NoError(t, err)
	subscribed, err := notifications.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(attributes.Attributes["QueueArn"]), Attributes: map[string]string{"RawMessageDelivery": "true"}})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(subscribed.SubscriptionArn))
	publication, err := notifications.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("raw SNS body"), MessageAttributes: map[string]snstypes.MessageAttributeValue{"bytes": {DataType: aws.String("Binary"), BinaryValue: []byte{0, 128, 255}}}})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(publication.MessageId))
	fanout, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MessageAttributeNames: []string{"All"}})
	require.NoError(t, err)
	require.Len(t, fanout.Messages, 1)
	require.Equal(t, "raw SNS body", aws.ToString(fanout.Messages[0].Body))
	require.Equal(t, []byte{0, 128, 255}, fanout.Messages[0].MessageAttributes["bytes"].BinaryValue)
	_, err = queues.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: created.QueueUrl, ReceiptHandle: fanout.Messages[0].ReceiptHandle})
	require.NoError(t, err)
	batch, err := notifications.PublishBatch(ctx, &sns.PublishBatchInput{TopicArn: topic.TopicArn, PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{{Id: aws.String("valid"), Message: aws.String("batch body")}, {Id: aws.String("invalid"), Message: aws.String("")}}})
	require.NoError(t, err)
	require.Len(t, batch.Successful, 1)
	require.Len(t, batch.Failed, 1)
	require.True(t, batch.Failed[0].SenderFault)
}

func TestSNSMobilePushIntentCapturedThroughAWSClient(t *testing.T) {
	_, notifications, _, path := messagingSDKServer(t)
	ctx := context.Background()
	application, err := notifications.CreatePlatformApplication(ctx, &sns.CreatePlatformApplicationInput{Name: aws.String("local-platform"), Platform: aws.String("GCM"), Attributes: map[string]string{"PlatformCredential": "synthetic-local-credential"}})
	require.NoError(t, err)
	endpoint, err := notifications.CreatePlatformEndpoint(ctx, &sns.CreatePlatformEndpointInput{PlatformApplicationArn: application.PlatformApplicationArn, Token: aws.String("synthetic-device-token")})
	require.NoError(t, err)
	result, err := notifications.Publish(ctx, &sns.PublishInput{TargetArn: endpoint.EndpointArn, Message: aws.String("local push intent")})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(result.MessageId))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var record messaging.SNSCaptureRecord
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if record.MessageID == aws.ToString(result.MessageId) {
			require.Equal(t, aws.ToString(endpoint.EndpointArn), record.TargetARN)
			require.Equal(t, "local push intent", record.Message)
			require.Equal(t, "eventbus.sns.capture.v1", record.SchemaVersion)
			found = true
		}
	}
	require.True(t, found, "accepted external push must have a complete capture record")
}

func TestSQSBatchAndLegacyQuerySendsShareTheQueue(t *testing.T) {
	listener, _, queues, _ := messagingSDKServer(t)
	ctx := context.Background()
	created, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("query-and-batch")})
	require.NoError(t, err)
	batch, err := queues.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: created.QueueUrl, Entries: []sqstypes.SendMessageBatchRequestEntry{{Id: aws.String("valid"), MessageBody: aws.String("batch")}, {Id: aws.String("invalid"), MessageBody: aws.String("")}}})
	require.NoError(t, err)
	require.Len(t, batch.Successful, 1)
	require.Len(t, batch.Failed, 1)
	require.True(t, batch.Failed[0].SenderFault)
	values := url.Values{"Action": {"SendMessage"}, "Version": {"2012-11-05"}, "QueueUrl": {aws.ToString(created.QueueUrl)}, "MessageBody": {"query"}, "MessageAttribute.1.Name": {"binary"}, "MessageAttribute.1.Value.DataType": {"Binary"}, "MessageAttribute.1.Value.BinaryValue": {"AAF/"}}
	response, err := listener.Client().PostForm(listener.URL, values)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, 200, response.StatusCode, string(body))
	require.Contains(t, string(body), "SendMessageResponse")
	require.Contains(t, string(body), "http://queue.amazonaws.com/doc/2012-11-05/")
	received, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: created.QueueUrl, MaxNumberOfMessages: 10, MessageAttributeNames: []string{"All"}})
	require.NoError(t, err)
	require.Len(t, received.Messages, 2)
	found := false
	for _, message := range received.Messages {
		if aws.ToString(message.Body) == "query" {
			require.Equal(t, []byte{0, 1, 127}, message.MessageAttributes["binary"].BinaryValue)
			found = true
		}
	}
	require.True(t, found)
}

func TestSNSTopicAndSubscriptionAdministrationThroughAWSClient(t *testing.T) {
	_, notifications, queues, _ := messagingSDKServer(t)
	ctx := context.Background()
	topic, err := notifications.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("administration"), Tags: []snstypes.Tag{{Key: aws.String("owner"), Value: aws.String("agent")}}})
	require.NoError(t, err)
	_, err = notifications.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{TopicArn: topic.TopicArn, AttributeName: aws.String("DisplayName"), AttributeValue: aws.String("Local agents")})
	require.NoError(t, err)
	attrs, err := notifications.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: topic.TopicArn})
	require.NoError(t, err)
	require.Equal(t, "Local agents", attrs.Attributes["DisplayName"])
	_, err = notifications.TagResource(ctx, &sns.TagResourceInput{ResourceArn: topic.TopicArn, Tags: []snstypes.Tag{{Key: aws.String("scenario"), Value: aws.String("sdk")}}})
	require.NoError(t, err)
	_, err = notifications.UntagResource(ctx, &sns.UntagResourceInput{ResourceArn: topic.TopicArn, TagKeys: []string{"owner"}})
	require.NoError(t, err)
	tags, err := notifications.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{ResourceArn: topic.TopicArn})
	require.NoError(t, err)
	require.Equal(t, []snstypes.Tag{{Key: aws.String("scenario"), Value: aws.String("sdk")}}, tags.Tags)
	policy := `{"Name":"agent-policy","Description":"Local audit fixture","Version":"2021-06-01","Statement":[{"DataDirection":"Inbound","Principal":["*"],"DataIdentifier":["arn:aws:dataprotection::aws:data-identifier/CreditCardNumber"],"Operation":{"Audit":{"SampleRate":"99","FindingsDestination":{"CloudWatchLogs":{"LogGroup":"local-fixture"}}}}}]}`
	_, err = notifications.PutDataProtectionPolicy(ctx, &sns.PutDataProtectionPolicyInput{ResourceArn: topic.TopicArn, DataProtectionPolicy: aws.String(policy)})
	require.NoError(t, err)
	protection, err := notifications.GetDataProtectionPolicy(ctx, &sns.GetDataProtectionPolicyInput{ResourceArn: topic.TopicArn})
	require.NoError(t, err)
	require.JSONEq(t, policy, aws.ToString(protection.DataProtectionPolicy))
	_, err = notifications.AddPermission(ctx, &sns.AddPermissionInput{TopicArn: topic.TopicArn, Label: aws.String("reader"), AWSAccountId: []string{"123456789012"}, ActionName: []string{"Subscribe"}})
	require.NoError(t, err)
	attrs, err = notifications.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: topic.TopicArn})
	require.NoError(t, err)
	require.Contains(t, attrs.Attributes["Policy"], "reader")
	_, err = notifications.RemovePermission(ctx, &sns.RemovePermissionInput{TopicArn: topic.TopicArn, Label: aws.String("reader")})
	require.NoError(t, err)
	attrs, err = notifications.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{TopicArn: topic.TopicArn})
	require.NoError(t, err)
	require.NotContains(t, attrs.Attributes["Policy"], "reader")
	queue, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("administration")})
	require.NoError(t, err)
	queueAttrs, err := queues.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	require.NoError(t, err)
	subscription, err := notifications.Subscribe(ctx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("sqs"), Endpoint: aws.String(queueAttrs.Attributes["QueueArn"])})
	require.NoError(t, err)
	subscriptions, err := notifications.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})
	require.NoError(t, err)
	require.Len(t, subscriptions.Subscriptions, 1)
	require.Equal(t, subscription.SubscriptionArn, subscriptions.Subscriptions[0].SubscriptionArn)
	_, err = notifications.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: subscription.SubscriptionArn})
	require.NoError(t, err)
	subscriptions, err = notifications.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})
	require.NoError(t, err)
	require.Empty(t, subscriptions.Subscriptions)
}
