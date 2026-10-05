package main

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSNSCreateTopic(t *testing.T) {
	_, _, snsClient, _ := setupTestServer(t)
	ctx := context.Background()

	result, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("test-topic"),
	})

	require.NoError(t, err)
	require.NotNil(t, result.TopicArn)
	assert.Contains(t, *result.TopicArn, "test-topic")
	assert.Contains(t, *result.TopicArn, "arn:aws:sns:us-east-1:000000000000:test-topic")
}

func TestSNSCreateTopicIdempotent(t *testing.T) {
	_, _, snsClient, _ := setupTestServer(t)
	ctx := context.Background()

	r1, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("idempotent-topic"),
	})
	require.NoError(t, err)

	r2, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("idempotent-topic"),
	})
	require.NoError(t, err)

	assert.Equal(t, *r1.TopicArn, *r2.TopicArn)
}

func TestSNSSubscribe(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("sub-topic"),
	})
	require.NoError(t, err)

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("sub-queue"),
	})
	require.NoError(t, err)

	// Get queue ARN for the subscription endpoint
	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueResult.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn := attrResult.Attributes["QueueArn"]

	subResult, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicResult.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	})

	require.NoError(t, err)
	require.NotNil(t, subResult.SubscriptionArn)
	assert.NotEmpty(t, *subResult.SubscriptionArn)
}

// The SDK sees SNS's answers: the same subscription for the same endpoint,
// and InvalidParameter for the same endpoint under another filter policy.
func TestSNSSubscribeAgainAnswersAsSNSDoes(t *testing.T) {
	broker, _, snsClient, _ := setupTestServer(t)
	ctx := context.Background()
	topic := broker.CreateTopic("resubscribe-topic")
	queue := broker.CreateQueue("resubscribe-queue", 0, 0)
	subscribe := func(filterPolicy string) (*sns.SubscribeOutput, error) {
		return snsClient.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn:   aws.String(topic.ARN),
			Protocol:   aws.String("sqs"),
			Endpoint:   aws.String(queue.ARN),
			Attributes: map[string]string{"FilterPolicy": filterPolicy},
		})
	}

	first, err := subscribe(`{"event_type":["import.created"]}`)
	require.NoError(t, err)
	again, err := subscribe(`{"event_type":["import.created"]}`)
	require.NoError(t, err)
	assert.Equal(t, *first.SubscriptionArn, *again.SubscriptionArn)

	_, err = subscribe(`{"event_type":["data-hub.changed"]}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameter")
}

func TestSNSSubscribeWithFilterPolicy(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("filter-topic"),
	})
	require.NoError(t, err)

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("filter-queue"),
	})
	require.NoError(t, err)

	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueResult.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn := attrResult.Attributes["QueueArn"]

	subResult, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicResult.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
		Attributes: map[string]string{
			"FilterPolicy": `{"event_type":["import.created"]}`,
		},
	})

	require.NoError(t, err)
	require.NotNil(t, subResult.SubscriptionArn)
	assert.NotEmpty(t, *subResult.SubscriptionArn)
}

func TestSNSPublish(t *testing.T) {
	_, _, snsClient, _ := setupTestServer(t)
	ctx := context.Background()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("publish-topic"),
	})
	require.NoError(t, err)

	pubResult, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: topicResult.TopicArn,
		Message:  aws.String("hello world"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event_type": {
				DataType:    aws.String("String"),
				StringValue: aws.String("import.created"),
			},
		},
	})

	require.NoError(t, err)
	require.NotNil(t, pubResult.MessageId)
	assert.NotEmpty(t, *pubResult.MessageId)
}

func TestSNSPublishToNonexistentTopic(t *testing.T) {
	_, _, snsClient, _ := setupTestServer(t)
	ctx := context.Background()

	_, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:nonexistent"),
		Message:  aws.String("hello"),
	})

	assert.Error(t, err)
}

func TestSNSListTopics(t *testing.T) {
	_, _, snsClient, _ := setupTestServer(t)
	ctx := context.Background()

	names := []string{"list-topic-a", "list-topic-b", "list-topic-c"}
	expectedArns := make(map[string]bool)
	for _, name := range names {
		result, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
			Name: aws.String(name),
		})
		require.NoError(t, err)
		expectedArns[*result.TopicArn] = true
	}

	listResult, err := snsClient.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)

	foundArns := make(map[string]bool)
	for _, topic := range listResult.Topics {
		foundArns[*topic.TopicArn] = true
	}

	for arn := range expectedArns {
		assert.True(t, foundArns[arn], "expected topic ARN %s in list", arn)
	}
}

func TestSNSListSubscriptionsByTopic(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("listsub-topic"),
	})
	require.NoError(t, err)

	// Create two queues and subscribe both
	for _, qName := range []string{"listsub-q1", "listsub-q2"} {
		qResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
			QueueName: aws.String(qName),
		})
		require.NoError(t, err)

		attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl:       qResult.QueueUrl,
			AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
		})
		require.NoError(t, err)
		queueArn := attrResult.Attributes["QueueArn"]

		_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
			TopicArn: topicResult.TopicArn,
			Protocol: aws.String("sqs"),
			Endpoint: aws.String(queueArn),
		})
		require.NoError(t, err)
	}

	listResult, err := snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
		TopicArn: topicResult.TopicArn,
	})
	require.NoError(t, err)
	assert.Len(t, listResult.Subscriptions, 2)

	for _, sub := range listResult.Subscriptions {
		assert.Equal(t, *topicResult.TopicArn, *sub.TopicArn)
		assert.Equal(t, "sqs", *sub.Protocol)
		assert.NotEmpty(t, *sub.SubscriptionArn)
		assert.NotEmpty(t, *sub.Endpoint)
	}
}

func TestSNSDeleteTopic(t *testing.T) {
	_, _, snsClient, _ := setupTestServer(t)
	ctx := context.Background()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("delete-me"),
	})
	require.NoError(t, err)

	_, err = snsClient.DeleteTopic(ctx, &sns.DeleteTopicInput{
		TopicArn: topicResult.TopicArn,
	})
	require.NoError(t, err)

	listResult, err := snsClient.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)

	for _, topic := range listResult.Topics {
		assert.NotEqual(t, *topicResult.TopicArn, *topic.TopicArn, "deleted topic should not appear in list")
	}
}

func TestSNSDeleteTopicWithSubscriptions(t *testing.T) {
	_, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	topicResult, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String("delete-with-subs"),
	})
	require.NoError(t, err)

	qResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("orphan-queue"),
	})
	require.NoError(t, err)

	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       qResult.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn := attrResult.Attributes["QueueArn"]

	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicResult.TopicArn,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	})
	require.NoError(t, err)

	// Delete the topic (should succeed even with subscriptions)
	_, err = snsClient.DeleteTopic(ctx, &sns.DeleteTopicInput{
		TopicArn: topicResult.TopicArn,
	})
	require.NoError(t, err)

	// Verify topic is gone
	listResult, err := snsClient.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)
	for _, topic := range listResult.Topics {
		assert.NotEqual(t, *topicResult.TopicArn, *topic.TopicArn)
	}
}
