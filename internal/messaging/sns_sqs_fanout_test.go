package messaging

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snsNotification represents the SNS envelope that wraps messages delivered to SQS.
type snsNotification struct {
	Type              string                     `json:"Type"`
	MessageId         string                     `json:"MessageId"`
	TopicArn          string                     `json:"TopicArn"`
	Message           string                     `json:"Message"`
	Timestamp         string                     `json:"Timestamp"`
	MessageAttributes map[string]json.RawMessage `json:"MessageAttributes,omitempty"`
}

// createTopicAndQueue is a helper that creates an SNS topic and SQS queue via the SDK,
// then subscribes the queue to the topic. Returns topicArn, queueUrl, queueArn.
func createTopicAndQueue(
	t *testing.T,
	ctx context.Context,
	snsClient *sns.Client,
	sqsClient *sqs.Client,
	topicName, queueName string,
	filterPolicy *string,
) (topicArn, queueUrl, queueArn string) {
	t.Helper()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicArn = *topicResult.TopicArn

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueUrl = *queueResult.QueueUrl

	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueUrl),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn = attrResult.Attributes["QueueArn"]

	subInput := &sns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	}
	if filterPolicy != nil {
		subInput.Attributes = map[string]string{
			"FilterPolicy": *filterPolicy,
		}
	}
	_, err = snsClient.Subscribe(ctx, subInput)
	require.NoError(t, err)

	return topicArn, queueUrl, queueArn
}

// receiveOne receives a single message from the queue. Returns nil if no message arrives.
func receiveOne(t *testing.T, ctx context.Context, sqsClient *sqs.Client, queueUrl string, waitSeconds int32) *sqstypes.Message {
	t.Helper()

	result, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueUrl),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     waitSeconds,
	})
	require.NoError(t, err)

	if len(result.Messages) == 0 {
		return nil
	}
	return &result.Messages[0]
}

func TestFanoutPublishToQueue(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	topicArn, queueUrl, _ := createTopicAndQueue(t, ctx, snsClient, sqsClient,
		"fanout-topic", "fanout-queue", nil)

	_, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("hello fanout"),
	})
	require.NoError(t, err)

	msg := receiveOne(t, ctx, sqsClient, queueUrl, 2)
	require.NotNil(t, msg, "expected a message in the queue")

	var envelope snsNotification
	err = json.Unmarshal([]byte(*msg.Body), &envelope)
	require.NoError(t, err)

	assert.Equal(t, "Notification", envelope.Type)
	assert.Equal(t, "hello fanout", envelope.Message)
	assert.Equal(t, topicArn, envelope.TopicArn)
	assert.NotEmpty(t, envelope.MessageId)
	assert.NotEmpty(t, envelope.Timestamp)
}

func TestFanoutWithFilterPolicyMatch(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	filter := `{"event_type":["import.created"]}`
	topicArn, queueUrl, _ := createTopicAndQueue(t, ctx, snsClient, sqsClient,
		"filter-match-topic", "filter-match-queue", &filter)

	_, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("matching message"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event_type": {
				DataType:    aws.String("String"),
				StringValue: aws.String("import.created"),
			},
		},
	})
	require.NoError(t, err)

	msg := receiveOne(t, ctx, sqsClient, queueUrl, 2)
	require.NotNil(t, msg, "expected message to pass filter")

	var envelope snsNotification
	err = json.Unmarshal([]byte(*msg.Body), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "matching message", envelope.Message)
}

func TestFanoutWithFilterPolicyNoMatch(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	filter := `{"event_type":["import.created"]}`
	topicArn, queueUrl, _ := createTopicAndQueue(t, ctx, snsClient, sqsClient,
		"filter-nomatch-topic", "filter-nomatch-queue", &filter)

	_, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("non-matching message"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event_type": {
				DataType:    aws.String("String"),
				StringValue: aws.String("export.completed"),
			},
		},
	})
	require.NoError(t, err)

	msg := receiveOne(t, ctx, sqsClient, queueUrl, 1)
	assert.Nil(t, msg, "message should not pass the filter policy")
}

func TestFullMessageLifecycle(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	// Create topic + queue + subscription
	topicArn, queueUrl, _ := createTopicAndQueue(t, ctx, snsClient, sqsClient,
		"lifecycle-topic", "lifecycle-queue", nil)

	// Publish via SNS
	_, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("lifecycle test message"),
	})
	require.NoError(t, err)

	// Receive via SQS
	msg := receiveOne(t, ctx, sqsClient, queueUrl, 2)
	require.NotNil(t, msg, "expected message after publish")

	var envelope snsNotification
	err = json.Unmarshal([]byte(*msg.Body), &envelope)
	require.NoError(t, err)
	assert.Equal(t, "lifecycle test message", envelope.Message)

	// Delete via SQS
	_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueUrl),
		ReceiptHandle: msg.ReceiptHandle,
	})
	require.NoError(t, err)

	// Receive again should return empty
	msg2 := receiveOne(t, ctx, sqsClient, queueUrl, 1)
	assert.Nil(t, msg2, "queue should be empty after delete")
}

func TestFanoutMultipleSubscribers(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("multi-sub-topic"),
	})
	require.NoError(t, err)
	topicArn := *topicResult.TopicArn

	var queueUrls []string
	for _, qName := range []string{"multi-sub-q1", "multi-sub-q2"} {
		qResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(qName),
		})
		require.NoError(t, err)
		queueUrl := *qResult.QueueUrl
		queueUrls = append(queueUrls, queueUrl)

		attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       aws.String(queueUrl),
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
		})
		require.NoError(t, err)
		queueArn := attrResult.Attributes["QueueArn"]

		_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn: aws.String(topicArn),
			Protocol: aws.String("sqs"),
			Endpoint: aws.String(queueArn),
		})
		require.NoError(t, err)
	}

	_, err = snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("broadcast to all"),
	})
	require.NoError(t, err)

	for i, queueUrl := range queueUrls {
		msg := receiveOne(t, ctx, sqsClient, queueUrl, 2)
		require.NotNilf(t, msg, "expected message in queue %d", i)

		var envelope snsNotification
		err := json.Unmarshal([]byte(*msg.Body), &envelope)
		require.NoError(t, err)
		assert.Equal(t, "broadcast to all", envelope.Message)
	}
}
