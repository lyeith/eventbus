package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func TestSQSCompleteSDKOperations(t *testing.T) {
	b, _, _, client := setupTestServer(t)
	ctx := context.Background()
	create, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("complete"), Tags: map[string]string{"owner": "agent"}})
	require.NoError(t, err)
	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: create.QueueUrl, Attributes: map[string]string{"VisibilityTimeout": "0", "ReceiveMessageWaitTimeSeconds": "0"}})
	require.NoError(t, err)
	attr, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: create.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll}})
	require.NoError(t, err)
	require.Equal(t, "0", attr.Attributes["VisibilityTimeout"])
	_, err = client.TagQueue(ctx, &sqs.TagQueueInput{QueueUrl: create.QueueUrl, Tags: map[string]string{"purpose": "eval"}})
	require.NoError(t, err)
	tags, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{QueueUrl: create.QueueUrl})
	require.NoError(t, err)
	require.Equal(t, "agent", tags.Tags["owner"])
	_, err = client.UntagQueue(ctx, &sqs.UntagQueueInput{QueueUrl: create.QueueUrl, TagKeys: []string{"owner"}})
	require.NoError(t, err)
	_, err = client.AddPermission(ctx, &sqs.AddPermissionInput{QueueUrl: create.QueueUrl, Label: aws.String("reader"), AWSAccountIds: []string{"123456789012"}, Actions: []string{"ReceiveMessage"}})
	require.NoError(t, err)
	_, err = client.RemovePermission(ctx, &sqs.RemovePermissionInput{QueueUrl: create.QueueUrl, Label: aws.String("reader")})
	require.NoError(t, err)
	urlResult, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String("complete")})
	require.NoError(t, err)
	require.Equal(t, *create.QueueUrl, *urlResult.QueueUrl)
	listed, err := client.ListQueues(ctx, &sqs.ListQueuesInput{QueueNamePrefix: aws.String("comp"), MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Equal(t, []string{*create.QueueUrl}, listed.QueueUrls)
	attributes := map[string]sqstypes.MessageAttributeValue{"binary": {DataType: aws.String("Binary.custom"), BinaryValue: []byte{0, 1, 255}}, "count": {DataType: aws.String("Number"), StringValue: aws.String("001.2300")}, "label": {DataType: aws.String("String"), StringValue: aws.String("capture")}}
	sent, err := client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: create.QueueUrl, MessageBody: aws.String("payload"), MessageAttributes: attributes, MessageGroupId: aws.String("tenant")})
	require.NoError(t, err)
	require.Equal(t, md5Body("payload"), *sent.MD5OfMessageBody)
	require.NotEmpty(t, *sent.MD5OfMessageAttributes)
	got, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: create.QueueUrl, VisibilityTimeout: 60, MessageAttributeNames: []string{"All"}, MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{sqstypes.MessageSystemAttributeNameAll}})
	require.NoError(t, err)
	require.Len(t, got.Messages, 1)
	require.Equal(t, "1.23", *got.Messages[0].MessageAttributes["count"].StringValue)
	require.Equal(t, []byte{0, 1, 255}, got.Messages[0].MessageAttributes["binary"].BinaryValue)
	require.Equal(t, *sent.MD5OfMessageAttributes, *got.Messages[0].MD5OfMessageAttributes)
	_, err = client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{QueueUrl: create.QueueUrl, ReceiptHandle: got.Messages[0].ReceiptHandle, VisibilityTimeout: 30})
	require.NoError(t, err)
	changed, err := client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: create.QueueUrl, Entries: []sqstypes.ChangeMessageVisibilityBatchRequestEntry{{Id: aws.String("good"), ReceiptHandle: got.Messages[0].ReceiptHandle, VisibilityTimeout: 30}, {Id: aws.String("bad"), ReceiptHandle: aws.String("unknown"), VisibilityTimeout: 30}}})
	require.NoError(t, err)
	require.Len(t, changed.Successful, 1)
	require.Len(t, changed.Failed, 1)
	_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: create.QueueUrl, ReceiptHandle: got.Messages[0].ReceiptHandle})
	require.NoError(t, err)
	batch, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: create.QueueUrl, Entries: []sqstypes.SendMessageBatchRequestEntry{{Id: aws.String("one"), MessageBody: aws.String("one")}, {Id: aws.String("two"), MessageBody: aws.String("two")}}})
	require.NoError(t, err)
	require.Len(t, batch.Successful, 2)
	got, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: create.QueueUrl, MaxNumberOfMessages: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, got.Messages, 2)
	deleted, err := client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: create.QueueUrl, Entries: []sqstypes.DeleteMessageBatchRequestEntry{{Id: aws.String("one"), ReceiptHandle: got.Messages[0].ReceiptHandle}, {Id: aws.String("two"), ReceiptHandle: got.Messages[1].ReceiptHandle}, {Id: aws.String("bad"), ReceiptHandle: aws.String("unknown")}}})
	require.NoError(t, err)
	require.Len(t, deleted.Successful, 2)
	require.Len(t, deleted.Failed, 1)
	dlq, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("complete-dlq")})
	require.NoError(t, err)
	dlqAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{QueueUrl: dlq.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn}})
	require.NoError(t, err)
	redrive, _ := json.Marshal(map[string]any{"deadLetterTargetArn": dlqAttrs.Attributes["QueueArn"], "maxReceiveCount": 1})
	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{QueueUrl: create.QueueUrl, Attributes: map[string]string{"RedrivePolicy": string(redrive)}})
	require.NoError(t, err)
	sources, err := client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{QueueUrl: dlq.QueueUrl})
	require.NoError(t, err)
	require.Equal(t, []string{*create.QueueUrl}, sources.QueueUrls)
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: create.QueueUrl, MessageBody: aws.String("redrive")})
	require.NoError(t, err)
	got, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: create.QueueUrl, VisibilityTimeout: 0})
	require.NoError(t, err)
	require.Len(t, got.Messages, 1)
	got, err = client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: dlq.QueueUrl, VisibilityTimeout: 0})
	require.NoError(t, err)
	require.Len(t, got.Messages, 0) // another source operation triggers lazy DLQ transition
	b.RequeueExpired(b.GetQueue("complete"))
	_, err = b.SendQueueMessage(b.GetQueue("complete-dlq"), QueueMessageInput{Body: "extra"})
	require.NoError(t, err)
	_, err = b.SendQueueMessage(b.GetQueue("complete-dlq"), QueueMessageInput{Body: "extra two"})
	require.NoError(t, err)
	moved, err := client.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{SourceArn: aws.String(dlqAttrs.Attributes["QueueArn"]), MaxNumberOfMessagesPerSecond: aws.Int32(1)})
	require.NoError(t, err)
	tasks, err := client.ListMessageMoveTasks(ctx, &sqs.ListMessageMoveTasksInput{SourceArn: aws.String(dlqAttrs.Attributes["QueueArn"]), MaxResults: aws.Int32(10)})
	require.NoError(t, err)
	require.NotEmpty(t, tasks.Results)
	// Keep cancellation test running by adding two more DLQ messages after start.
	_, err = b.SendQueueMessage(b.GetQueue("complete-dlq"), QueueMessageInput{Body: "extra"})
	require.NoError(t, err)
	_, err = client.CancelMessageMoveTask(ctx, &sqs.CancelMessageMoveTaskInput{TaskHandle: moved.TaskHandle})
	require.NoError(t, err)
	_, err = client.PurgeQueue(ctx, &sqs.PurgeQueueInput{QueueUrl: create.QueueUrl})
	require.NoError(t, err)
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: create.QueueUrl})
	require.NoError(t, err)
}

func TestSQSQueryAndJSONShareWireContracts(t *testing.T) {
	b, server, _, _ := setupTestServer(t)
	q := b.CreateQueue("query-wire", time.Minute, 0)
	post := func(form url.Values) string {
		t.Helper()
		form.Set("Version", "2012-11-05")
		resp, err := http.PostForm(server.URL, form)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode, string(body))
		return string(body)
	}
	body := post(url.Values{"Action": {"SendMessageBatch"}, "QueueUrl": {q.URL}, "SendMessageBatchRequestEntry.1.Id": {"first"}, "SendMessageBatchRequestEntry.1.MessageBody": {"<captured>"}, "SendMessageBatchRequestEntry.1.MessageAttribute.1.Name": {"bytes"}, "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.DataType": {"Binary"}, "SendMessageBatchRequestEntry.1.MessageAttribute.1.Value.BinaryValue": {"AAH/"}})
	require.Contains(t, body, "<SendMessageBatchResultEntry>")
	require.Contains(t, body, "<MD5OfMessageAttributes>")
	body = post(url.Values{"Action": {"ReceiveMessage"}, "QueueUrl": {q.URL}, "MessageAttributeName.1": {"All"}, "MessageSystemAttributeName.1": {"All"}})
	require.Contains(t, body, "<MD5OfBody>")
	require.Contains(t, body, "<BinaryValue>AAH/</BinaryValue>")
	require.Contains(t, body, "&lt;captured&gt;")
	request, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader(`{"QueueUrl":"`+q.URL+`","AttributeNames":["VisibilityTimeout"]}`))
	require.NoError(t, err)
	request.Header.Set("X-Amz-Target", "AmazonSQS.GetQueueAttributes")
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	var attrs map[string]map[string]string
	require.NoError(t, json.NewDecoder(response.Body).Decode(&attrs))
	require.Equal(t, map[string]string{"VisibilityTimeout": "60"}, attrs["Attributes"])
}

func TestSQSValidationAndFIFOState(t *testing.T) {
	b := NewBroker("us-east-1", "000000000000", 4100)
	handler := NewHandler(b)
	q, err := b.createSQSQueue("ordered.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}, nil)
	require.Nil(t, err)
	send := func(body, group string) QueueSendResult {
		t.Helper()
		result, err := b.SendQueueMessage(q, QueueMessageInput{Body: body, MessageGroupID: group})
		require.NoError(t, err)
		return result
	}
	one := send("one", "a")
	duplicate := send("one", "a")
	require.Equal(t, one.MessageID, duplicate.MessageID)
	send("two", "a")
	send("three", "b")
	got, apiErr := b.receiveSQS(context.Background(), q, 1, 0, nil, "attempt")
	require.Nil(t, apiErr)
	require.Len(t, got, 1)
	retry, apiErr := b.receiveSQS(context.Background(), q, 1, 0, nil, "attempt")
	require.Nil(t, apiErr)
	require.Equal(t, got[0].ReceiptHandle, retry[0].ReceiptHandle)
	other := b.ReceiveMessages(q, 10, 0)
	require.Len(t, other, 1)
	require.Equal(t, "b", other[0].GroupID)
	require.Nil(t, handler.deleteSQSReceipt(q, got[0].ReceiptHandle))
	_, apiErr = b.receiveSQS(context.Background(), q, 1, 0, nil, "attempt")
	require.NotNil(t, apiErr)
	got = b.ReceiveMessages(q, 10, 0)
	require.Len(t, got, 1)
	require.Equal(t, "two", got[0].Body)
	_, sendErr := b.SendQueueMessage(q, QueueMessageInput{Body: "invalid\x00", MessageGroupID: "a"})
	require.Error(t, sendErr)
	for _, number := range []string{"001.2300", "1e-128", "1e126", "-0"} {
		_, apiErr := normalizeSQSNumber(number)
		require.Nil(t, apiErr, number)
	}
	for _, number := range []string{"1e-129", "1e127", "123456789012345678901234567890123456789"} {
		_, apiErr := normalizeSQSNumber(number)
		require.NotNil(t, apiErr, number)
	}
	seconds := 1
	require.Error(t, handler.changeSQSVisibility(q, "unknown", &seconds))
	_, apiErr = handler.executeSQS(context.Background(), "SendMessageBatch", sqsRequest{QueueURL: q.URL, Entries: []sqsBatchEntry{{ID: "same", MessageBody: "a"}, {ID: "same", MessageBody: "b"}}})
	require.Equal(t, "BatchEntryIdsNotDistinct", apiErr.Code)
}

func TestSQSLongPollCancellationAndSnapshotOwnership(t *testing.T) {
	b := NewBroker("us-east-1", "000000000000", 4100)
	q := b.CreateQueue("cancel", time.Minute, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	got, err := b.receiveSQS(ctx, q, 1, 20*time.Second, nil, "")
	require.Nil(t, err)
	require.Empty(t, got)
	require.Less(t, time.Since(started), time.Second)
	_, sendErr := b.SendQueueMessage(q, QueueMessageInput{Body: "owned", Attributes: map[string]MessageAttribute{"binary": {DataType: "Binary", BinaryValue: []byte{1}}}})
	require.NoError(t, sendErr)
	got = b.ReceiveMessages(q, 1, 0)
	got[0].Attributes["binary"].BinaryValue[0] = 9
	deadline := got[0].VisibleAt
	require.True(t, b.ExtendMessageVisibility(q, got[0].ReceiptHandle, time.Second))
	require.Equal(t, deadline, got[0].VisibleAt)
	q.mu.Lock()
	require.Equal(t, byte(1), q.inFlight[got[0].ReceiptHandle].Attributes["binary"].BinaryValue[0])
	q.mu.Unlock()
}

func TestSQSJSONRejectsWrongTypesAndLargeDecodedMessage(t *testing.T) {
	_, server, _, _ := setupTestServer(t)
	for _, body := range []string{`{"QueueName":123}`, `{"QueueName":"ok"} {}`, `{"QueueName":null,"Attributes":null}`} {
		req, err := http.NewRequest(http.MethodPost, server.URL, bytes.NewBufferString(body))
		require.NoError(t, err)
		req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	}
}

func TestSQSQueueURLIdentity(t *testing.T) {
	b := NewBroker("us-east-1", "000000000000", 4100)
	q := b.CreateQueue("identity", 0, 0)
	handler := NewHandler(b)
	for _, url := range []string{q.URL, "http://eventbus:4100/queue/identity", "https://sqs.us-east-1.amazonaws.com/000000000000/identity", "https://sqs.us-east-1.amazonaws.com/000000000000/identity/", q.URL + "/"} {
		got, err := handler.queueForURL(url)
		require.Nil(t, err, url)
		require.Same(t, q, got)
	}
	for _, url := range []string{"ftp://localhost/queue/identity", "http://user:pass@localhost/queue/identity", "http:///queue/identity", "http://localhost/elsewhere/identity", "http://localhost/123456789012/identity", "http://localhost/queue/identity?x=1", "http://localhost/queue/identity#x", "http://localhost/queue/identity//", "http://localhost/queue/%69dentity"} {
		_, err := handler.queueForURL(url)
		require.NotNil(t, err, url)
		require.Equal(t, "InvalidAddress", err.Code, url)
	}
}

func TestSQSReceiveAttemptRestoresUnchangedExpiredDelivery(t *testing.T) {
	b := NewBroker("us-east-1", "000000000000", 4100)
	q, err := b.createSQSQueue("attempt-expiry.fifo", map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}, nil)
	require.Nil(t, err)
	_, sendErr := b.SendQueueMessage(q, QueueMessageInput{Body: "retried", MessageGroupID: "group"})
	require.NoError(t, sendErr)
	got, err := b.receiveSQS(context.Background(), q, 1, 0, nil, "retry")
	require.Nil(t, err)
	require.Len(t, got, 1)
	receipt := got[0].ReceiptHandle
	// Expiry is not a DeleteMessage/ChangeMessageVisibility modification.
	q.mu.Lock()
	q.inFlight[receipt].VisibleAt = time.Now().Add(-time.Second)
	q.mu.Unlock()
	require.Equal(t, 1, b.RequeueExpired(q))
	retry, err := b.receiveSQS(context.Background(), q, 1, 0, nil, "retry")
	require.Nil(t, err)
	require.Len(t, retry, 1)
	require.Equal(t, receipt, retry[0].ReceiptHandle)
	require.Equal(t, 1, retry[0].ReceiveCount)
	require.True(t, retry[0].VisibleAt.After(time.Now()))
	q.mu.Lock()
	q.inFlight[receipt].VisibleAt = time.Now().Add(-time.Second)
	q.mu.Unlock()
	b.RequeueExpired(q)
	other, err := b.receiveSQS(context.Background(), q, 1, 0, nil, "another")
	require.Nil(t, err)
	require.Len(t, other, 1)
	require.NotEqual(t, receipt, other[0].ReceiptHandle)
	_, err = b.receiveSQS(context.Background(), q, 1, 0, nil, "retry")
	require.NotNil(t, err)
	q.mu.Lock()
	q.inFlight[other[0].ReceiptHandle].VisibleAt = time.Now().Add(-time.Second)
	q.mu.Unlock()
	b.RequeueExpired(q)
	_, err = b.receiveSQS(context.Background(), q, 1, 0, nil, "retry")
	require.NotNil(t, err, "a later delivery must not resurrect the old receipt after expiry")
}

func TestSQSOfficialAttributeDigestAndQueuePathExample(t *testing.T) {
	// Golden value is the AWS SendMessage API reference example, independent of
	// this implementation: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
	attributes := map[string]MessageAttribute{"my_attribute_name_1": {DataType: "String", StringValue: "my_attribute_value_1"}, "my_attribute_name_2": {DataType: "String", StringValue: "my_attribute_value_2"}}
	require.Equal(t, "c48838208d2b4e14e3ca0093a8443f09", md5Attributes(attributes))
	require.Equal(t, "fafb00f5732ab283681e124bf8747ed1", md5Body("This is a test message"))
	b, server, _, _ := setupTestServer(t)
	q := b.CreateQueue("MyQueue", 0, 0)
	form := url.Values{"Action": {"SendMessage"}, "Version": {"2012-11-05"}, "MessageBody": {"This is a test message"}, "MessageAttribute.1.Name": {"my_attribute_name_1"}, "MessageAttribute.1.Value.StringValue": {"my_attribute_value_1"}, "MessageAttribute.1.Value.DataType": {"String"}, "MessageAttribute.2.Name": {"my_attribute_name_2"}, "MessageAttribute.2.Value.StringValue": {"my_attribute_value_2"}, "MessageAttribute.2.Value.DataType": {"String"}}
	response, err := http.PostForm(server.URL+"/000000000000/MyQueue/", form)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, 200, response.StatusCode, string(body))
	require.Contains(t, string(body), "<MD5OfMessageAttributes>c48838208d2b4e14e3ca0093a8443f09</MD5OfMessageAttributes>")
	require.Len(t, b.ReceiveMessages(q, 1, 0), 1)
}

func TestSQSQueueDelayChangesAreRetroactiveOnlyForInitialFIFOEnqueues(t *testing.T) {
	b := NewBroker("us-east-1", "000000000000", 4100)
	handler := NewHandler(b)
	for _, fifo := range []bool{false, true} {
		name := "delay-standard"
		attrs := map[string]string{"DelaySeconds": "600"}
		if fifo {
			name = "delay-fifo.fifo"
			attrs["FifoQueue"] = "true"
			attrs["ContentBasedDeduplication"] = "true"
		}
		q, err := b.createSQSQueue(name, attrs, nil)
		require.Nil(t, err)
		_, sendErr := b.SendQueueMessage(q, QueueMessageInput{Body: "initial", MessageGroupID: "group"})
		require.NoError(t, sendErr)
		require.Empty(t, b.ReceiveMessages(q, 1, 0))
		require.Nil(t, handler.setQueueAttributes(q, map[string]string{"DelaySeconds": "0"}))
		got := b.ReceiveMessages(q, 1, 0)
		if !fifo {
			require.Empty(t, got, "standard queued messages must retain their original delay")
			q.mu.Lock()
			require.Equal(t, q.messages[0].SentTimestamp.Add(600*time.Second), q.messages[0].VisibleAt)
			q.mu.Unlock()
			continue
		}
		require.Len(t, got, 1, "reducing a FIFO delay must release an initial waiting message")
		originalDeadline := got[0].VisibleAt
		require.Nil(t, handler.setQueueAttributes(q, map[string]string{"DelaySeconds": "600"}))
		q.mu.Lock()
		require.Equal(t, originalDeadline, q.inFlight[got[0].ReceiptHandle].VisibleAt, "a queue delay must not replace an in-flight visibility timeout")
		q.mu.Unlock()
		require.True(t, b.ExtendMessageVisibility(q, got[0].ReceiptHandle, -time.Second))
		b.RequeueExpired(q)
		require.Nil(t, handler.setQueueAttributes(q, map[string]string{"DelaySeconds": "600"}))
		require.Len(t, b.ReceiveMessages(q, 1, 0), 1, "previously received work uses visibility, not initial queue delay")
		require.Nil(t, handler.setQueueAttributes(q, map[string]string{"DelaySeconds": "0"}))
		_, sendErr = b.SendQueueMessage(q, QueueMessageInput{Body: "new initial", MessageGroupID: "other"})
		require.NoError(t, sendErr)
		require.Nil(t, handler.setQueueAttributes(q, map[string]string{"DelaySeconds": "600"}))
		require.Empty(t, b.ReceiveMessages(q, 1, 0), "increasing a FIFO delay must delay initial waiting messages")
	}
}

func TestSQSQueryScalarAttributeNameAndIndexedPermissionActions(t *testing.T) {
	b, server, _, _ := setupTestServer(t)
	q := b.CreateQueue("scalar-attributes", 0, 0)
	_, err := b.SendQueueMessage(q, QueueMessageInput{Body: "legacy"})
	require.NoError(t, err)
	form := url.Values{"Action": {"ReceiveMessage"}, "Version": {"2012-11-05"}, "AttributeName": {"All"}}
	response, err := http.PostForm(server.URL+"/000000000000/scalar-attributes/", form)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, 200, response.StatusCode, string(body))
	require.Contains(t, string(body), "<Name>SenderId</Name>")
	require.Contains(t, string(body), "<Name>ApproximateReceiveCount</Name>")
	// Action=AddPermission must never be interpreted as a permission action.
	input, apiErr := parseSQSQuery(url.Values{"Action": {"AddPermission"}, "Action.1": {"SendMessage"}, "AWSAccountId.1": {"123456789012"}, "AttributeName": {"All"}, "AttributeName.1": {"QueueArn"}}, "AddPermission")
	require.Nil(t, apiErr)
	require.Equal(t, []string{"SendMessage"}, input.Actions)
	require.Equal(t, []string{"All", "QueueArn"}, input.AttributeNames)
}
