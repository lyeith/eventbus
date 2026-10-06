package messaging

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQSCreateQueue(t *testing.T) {
	_, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	result, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("test-queue"),
	})

	require.NoError(t, err)
	require.NotNil(t, result.QueueUrl)
	assert.NotEmpty(t, *result.QueueUrl)
	assert.Contains(t, *result.QueueUrl, "test-queue")
}

func TestSQSCreateQueueWithAttributes(t *testing.T) {
	_, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	result, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("attr-queue"),
		Attributes: map[string]string{
			"VisibilityTimeout":      "60",
			"MessageRetentionPeriod": "86400",
		},
	})

	require.NoError(t, err)
	require.NotNil(t, result.QueueUrl)

	// Verify attributes were set by reading them back
	attrResult, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: result.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameVisibilityTimeout,
			sqstypes.QueueAttributeNameMessageRetentionPeriod,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", attrResult.Attributes["VisibilityTimeout"])
	assert.Equal(t, "86400", attrResult.Attributes["MessageRetentionPeriod"])
}

func TestSQSGetQueueAttributes(t *testing.T) {
	_, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	createResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("attr-get-queue"),
	})
	require.NoError(t, err)

	result, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       createResult.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})

	require.NoError(t, err)
	queueArn := result.Attributes["QueueArn"]
	assert.Contains(t, queueArn, "arn:aws:sqs:us-east-1:000000000000:attr-get-queue")
}

// A local caller waits for a consumer to finish its queue by these two counts,
// so a received message counts until it is deleted.
func TestSQSGetQueueAttributesCountsWaitingAndInFlightMessages(t *testing.T) {
	broker, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	created, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("depth-queue"),
	})
	require.NoError(t, err)
	q := broker.GetQueue("depth-queue")
	require.NotNil(t, q)
	broker.enqueueMessage(q, `{"n":1}`)
	broker.enqueueMessage(q, `{"n":2}`)

	depth := func() (string, string) {
		t.Helper()
		result, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
			QueueUrl: created.QueueUrl,
			AttributeNames: []sqstypes.QueueAttributeName{
				sqstypes.QueueAttributeNameApproximateNumberOfMessages,
				sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		require.NoError(t, err)
		return result.Attributes["ApproximateNumberOfMessages"],
			result.Attributes["ApproximateNumberOfMessagesNotVisible"]
	}

	waiting, inFlight := depth()
	assert.Equal(t, "2", waiting)
	assert.Equal(t, "0", inFlight)

	received, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            created.QueueUrl,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, received.Messages, 1)
	waiting, inFlight = depth()
	assert.Equal(t, "1", waiting)
	assert.Equal(t, "1", inFlight)

	_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      created.QueueUrl,
		ReceiptHandle: received.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)
	waiting, inFlight = depth()
	assert.Equal(t, "1", waiting)
	assert.Equal(t, "0", inFlight)
}

// The query protocol reports the same counts, in its XML.
func TestSQSGetQueueAttributesQueryProtocolCountsMessages(t *testing.T) {
	broker, ts, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	created, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("depth-xml-queue"),
	})
	require.NoError(t, err)
	broker.enqueueMessage(broker.GetQueue("depth-xml-queue"), `{"n":1}`)

	resp, err := http.PostForm(ts.URL, url.Values{
		"Action":   {"GetQueueAttributes"},
		"QueueUrl": {*created.QueueUrl},
	})
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/xml")
	assert.Contains(t, string(body), "<GetQueueAttributesResponse")
	assert.Contains(t, string(body), "<Name>ApproximateNumberOfMessages</Name>\n      <Value>1</Value>")
	assert.Contains(t, string(body), "<Name>ApproximateNumberOfMessagesNotVisible</Name>\n      <Value>0</Value>")
}

func TestSQSGetQueueUrl(t *testing.T) {
	_, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	createResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("url-queue"),
	})
	require.NoError(t, err)

	result, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("url-queue"),
	})

	require.NoError(t, err)
	assert.Equal(t, *createResult.QueueUrl, *result.QueueUrl)
}

func TestSQSGetQueueUrlNotFound(t *testing.T) {
	_, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	_, err := sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("nonexistent-queue"),
	})

	assert.Error(t, err)
}

func TestSQSReceiveMessageImmediate(t *testing.T) {
	broker, _, snsClient, sqsClient := setupTestServer(t)
	ctx := context.Background()

	// Create queue and enqueue a message directly via broker
	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("recv-immediate-queue"),
	})
	require.NoError(t, err)

	q := broker.GetQueue("recv-immediate-queue")
	require.NotNil(t, q)
	broker.enqueueMessage(q, `{"test":"immediate"}`)

	// Receive without long-polling
	result, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueResult.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, result.Messages, 1)
	assert.Contains(t, *result.Messages[0].Body, `{"test":"immediate"}`)
	assert.NotEmpty(t, *result.Messages[0].ReceiptHandle)
	assert.NotEmpty(t, *result.Messages[0].MessageId)

	// Also verify via full SNS pipeline
	_ = snsClient // ensure snsClient is available for other tests
}

func TestSQSReceiveMessageLongPollTimeout(t *testing.T) {
	_, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("longpoll-timeout-queue"),
	})
	require.NoError(t, err)

	start := time.Now()
	result, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueResult.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, result.Messages, "empty queue should return no messages")
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "should wait approximately 1 second")
	assert.Less(t, elapsed, 3*time.Second, "should not wait much longer than requested")
}

func TestSQSReceiveMessageMaxMessages(t *testing.T) {
	broker, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("maxmsg-queue"),
	})
	require.NoError(t, err)

	q := broker.GetQueue("maxmsg-queue")
	require.NotNil(t, q)
	for i := 0; i < 5; i++ {
		broker.enqueueMessage(q, fmt.Sprintf(`{"msg":%d}`, i))
	}

	result, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueResult.QueueUrl,
		MaxNumberOfMessages: 3,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Len(t, result.Messages, 3, "should return exactly MaxNumberOfMessages")

	// Remaining 2 messages should still be in the queue
	result2, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueResult.QueueUrl,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Len(t, result2.Messages, 2, "remaining messages should be available")
}

func TestSQSDeleteMessage(t *testing.T) {
	broker, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("delete-msg-queue"),
	})
	require.NoError(t, err)

	q := broker.GetQueue("delete-msg-queue")
	require.NotNil(t, q)
	broker.enqueueMessage(q, `{"delete":"me"}`)

	// Receive the message
	recvResult, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueResult.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, recvResult.Messages, 1)
	receiptHandle := recvResult.Messages[0].ReceiptHandle

	// Delete by receipt handle
	_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueResult.QueueUrl,
		ReceiptHandle: receiptHandle,
	})
	require.NoError(t, err)

	// Receive again should return empty (message is deleted, not just in-flight)
	recvResult2, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueResult.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, recvResult2.Messages, "deleted message should not reappear")
}

func TestSQSPurgeQueue(t *testing.T) {
	broker, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("purge-queue"),
	})
	require.NoError(t, err)

	q := broker.GetQueue("purge-queue")
	require.NotNil(t, q)
	for i := 0; i < 5; i++ {
		broker.enqueueMessage(q, fmt.Sprintf(`{"msg":%d}`, i))
	}

	_, err = sqsClient.PurgeQueue(ctx, &sqs.PurgeQueueInput{
		QueueUrl: queueResult.QueueUrl,
	})
	require.NoError(t, err)

	recvResult, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueResult.QueueUrl,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, recvResult.Messages, "purged queue should have no messages")
}

func TestSQSDeleteQueue(t *testing.T) {
	_, _, _, sqsClient := setupTestServer(t)
	ctx := context.Background()

	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("doomed-queue"),
	})
	require.NoError(t, err)
	require.NotNil(t, queueResult.QueueUrl)

	_, err = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: queueResult.QueueUrl,
	})
	require.NoError(t, err)

	// GetQueueUrl should fail for the deleted queue
	_, err = sqsClient.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String("doomed-queue"),
	})
	assert.Error(t, err, "GetQueueUrl should fail for a deleted queue")
}
