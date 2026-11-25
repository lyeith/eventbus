package messaging

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
)

type snsHTTPFixtureTransport struct {
	client   *http.Client
	endpoint string
}

func (fixture snsHTTPFixtureTransport) Allows(endpoint string) bool {
	return endpoint == fixture.endpoint
}

func (fixture snsHTTPFixtureTransport) Post(ctx context.Context, endpoint string, headers http.Header, body string) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header = headers
	response, err := fixture.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

func TestSNSHTTPNativeSDKDelivery(t *testing.T) {
	broker, api, client, queues := setupTestServer(t)
	require.NoError(t, broker.SetSNSRetryOptions(SNSRetryOptions{RetryDelayScale: 0.001}))
	broker.SetSNSHTTPCallbackOrigin(api.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	type observed struct {
		headers http.Header
		body    string
		method  string
	}
	notifications := make(chan observed, 24)
	callbackErrors := make(chan error, 8)
	var replies atomic.Int32
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(400)
			return
		}
		notifications <- observed{r.Header.Clone(), string(data), r.Method}
		switch r.Header.Get("X-Amz-Sns-Message-Type") {
		case "SubscriptionConfirmation":
			var value map[string]string
			if err := json.Unmarshal(data, &value); err != nil {
				callbackErrors <- err
				w.WriteHeader(400)
				return
			}
			response, err := client.ConfirmSubscription(r.Context(), &sns.ConfirmSubscriptionInput{
				TopicArn: aws.String(value["TopicArn"]), Token: aws.String(value["Token"]),
			})
			if err == nil && response.SubscriptionArn == nil {
				err = io.ErrUnexpectedEOF
			}
			callbackErrors <- err
			if err != nil {
				w.WriteHeader(400)
				return
			}
		case "Notification":
			if r.Header.Get("X-Amz-Sns-Rawdelivery") == "true" {
				if string(data) == `{"kind":"keep","permanent":true}` {
					w.WriteHeader(400)
					return
				}
				if string(data) == `{"kind":"keep","exhausted":true}` {
					w.WriteHeader(500)
					return
				}
			} else {
				switch replies.Add(1) {
				case 1:
					w.WriteHeader(500)
					return
				case 2:
					w.WriteHeader(429)
					return
				}
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer fixture.Close()
	defer func() {
		drain, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		require.NoError(t, broker.CloseSNSDeliveries(drain))
	}()
	broker.SetSNSHTTPTransport(snsHTTPFixtureTransport{fixture.Client(), fixture.URL})
	event := func() observed {
		select {
		case observation := <-notifications:
			require.Equal(t, http.MethodPost, observation.method)
			return observation
		case <-ctx.Done():
			t.Fatal("SNS fixture did not receive its webhook")
			return observed{}
		}
	}
	drain := func() { require.NoError(t, broker.DrainSNSDeliveries(ctx)) }
	empty := func() {
		select {
		case unexpected := <-notifications:
			t.Fatalf("unexpected webhook: %v", unexpected.headers)
		default:
		}
	}

	topic, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("local-webhook"), Attributes: map[string]string{
		"DeliveryPolicy": `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":2,"maxDelayTarget":5,"numRetries":2},"defaultRequestPolicy":{"headerContentType":"application/json"}}}`,
	}})
	require.NoError(t, err)
	dlq, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("local-webhook-dlq")})
	require.NoError(t, err)
	queueARN := broker.GetQueue("local-webhook-dlq").ARN
	subscription, err := client.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Endpoint: aws.String(fixture.URL), Protocol: aws.String("http"), ReturnSubscriptionArn: true,
		Attributes: map[string]string{
			"DeliveryPolicy":    `{"healthyRetryPolicy":{"numNoDelayRetries":2}}`,
			"FilterPolicyScope": "MessageBody", "FilterPolicy": `{"kind":["keep"]}`,
			"RedrivePolicy": `{"deadLetterTargetArn":` + strconv.Quote(queueARN) + `}`,
		},
	})
	require.NoError(t, err)

	confirm := event()
	require.Equal(t, "SubscriptionConfirmation", confirm.headers.Get("X-Amz-Sns-Message-Type"))
	require.Equal(t, "application/json", confirm.headers.Get("Content-Type"))
	var confirmation map[string]string
	require.NoError(t, json.Unmarshal([]byte(confirm.body), &confirmation))
	require.Equal(t, confirm.headers.Get("X-Amz-Sns-Message-Id"), confirmation["MessageId"])
	require.NotEmpty(t, confirmation["Timestamp"])
	confirmationURL, err := url.Parse(confirmation["SubscribeURL"])
	require.NoError(t, err)
	require.Equal(t, strings.TrimPrefix(api.URL, "http://"), confirmationURL.Host)
	require.Equal(t, confirmation["Token"], confirmationURL.Query().Get("Token"))
	require.Equal(t, "ConfirmSubscription", confirmationURL.Query().Get("Action"))
	select {
	case err := <-callbackErrors:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("webhook callback could not confirm")
	}
	drain()
	attrs, err := client.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: subscription.SubscriptionArn})
	require.NoError(t, err)
	require.Equal(t, "false", attrs.Attributes["PendingConfirmation"])
	require.JSONEq(t, `{"healthyRetryPolicy":{"minDelayTarget":2,"maxDelayTarget":5,"numRetries":2,"numNoDelayRetries":2,"numMinDelayRetries":0,"numMaxDelayRetries":0,"backoffFunction":"linear"},"requestPolicy":{"headerContentType":"application/json"}}`, attrs.Attributes["EffectiveDeliveryPolicy"])

	publish := func(message string) *sns.PublishOutput {
		result, err := client.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String(message),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{"kind": {DataType: aws.String("String"), StringValue: aws.String("keep")}}})
		require.NoError(t, err)
		return result
	}
	publish(`{"kind":"gone"}`)
	drain()
	empty()
	wrapped := publish(`{"kind":"keep"}`)
	for range 3 {
		notification := event()
		require.Equal(t, "Notification", notification.headers.Get("X-Amz-Sns-Message-Type"))
		require.Equal(t, aws.ToString(wrapped.MessageId), notification.headers.Get("X-Amz-Sns-Message-Id"))
		require.Equal(t, aws.ToString(subscription.SubscriptionArn), notification.headers.Get("X-Amz-Sns-Subscription-Arn"))
		var envelope map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(notification.body), &envelope))
		require.NotEmpty(t, envelope["MessageAttributes"])
		require.Contains(t, string(envelope["UnsubscribeURL"]), "Action=Unsubscribe")
	}
	drain()
	empty()

	set := func(name, value string) {
		_, err := client.SetSubscriptionAttributes(ctx, &sns.SetSubscriptionAttributesInput{SubscriptionArn: subscription.SubscriptionArn,
			AttributeName: aws.String(name), AttributeValue: aws.String(value)})
		require.NoError(t, err)
	}
	set("RawMessageDelivery", "true")
	set("DeliveryPolicy", `{"healthyRetryPolicy":{"numRetries":1,"numNoDelayRetries":1},"requestPolicy":{"headerContentType":"application/octet-stream"}}`)
	for _, scenario := range []struct {
		body     string
		attempts int
	}{
		{`{"kind":"keep","permanent":true}`, 1},
		{`{"kind":"keep","exhausted":true}`, 2},
	} {
		published := publish(scenario.body)
		for range scenario.attempts {
			raw := event()
			require.Equal(t, scenario.body, raw.body)
			require.Equal(t, "true", raw.headers.Get("X-Amz-Sns-Rawdelivery"))
			require.Equal(t, "application/octet-stream", raw.headers.Get("Content-Type"))
			require.Equal(t, aws.ToString(published.MessageId), raw.headers.Get("X-Amz-Sns-Message-Id"))
		}
		drain()
		empty()
		redriven, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: dlq.QueueUrl, MaxNumberOfMessages: 10})
		require.NoError(t, err)
		require.Len(t, redriven.Messages, 1)
		require.Equal(t, scenario.body, aws.ToString(redriven.Messages[0].Body))
		_, err = queues.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: dlq.QueueUrl, ReceiptHandle: redriven.Messages[0].ReceiptHandle})
		require.NoError(t, err)
	}

	_, err = client.Unsubscribe(ctx, &sns.UnsubscribeInput{SubscriptionArn: subscription.SubscriptionArn})
	require.NoError(t, err)
	unsubscribe := event()
	require.Equal(t, "UnsubscribeConfirmation", unsubscribe.headers.Get("X-Amz-Sns-Message-Type"))
	require.Empty(t, unsubscribe.headers.Get("X-Amz-Sns-Rawdelivery"))
	require.Equal(t, snsDefaultContentType, unsubscribe.headers.Get("Content-Type"))
	var restore map[string]string
	require.NoError(t, json.Unmarshal([]byte(unsubscribe.body), &restore))
	restored, err := client.ConfirmSubscription(ctx, &sns.ConfirmSubscriptionInput{TopicArn: topic.TopicArn, Token: aws.String(restore["Token"])})
	require.NoError(t, err)
	require.Equal(t, subscription.SubscriptionArn, restored.SubscriptionArn)
	drain()
	empty()
}

func TestSNSHTTPPolicyValidationAndThrottle(t *testing.T) {
	topic := `{"http":{"defaultHealthyRetryPolicy":{"minDelayTarget":1,"maxDelayTarget":8,"numRetries":7,"numNoDelayRetries":1,"numMinDelayRetries":1,"numMaxDelayRetries":1},"defaultThrottlePolicy":{"maxReceivesPerSecond":2},"disableSubscriptionOverrides":true}}`
	policy, disabled, err := effectiveSNSHTTPPolicy(topic, `{"healthyRetryPolicy":{"numRetries":9},"throttlePolicy":{"maxReceivesPerSecond":50}}`, false)
	require.NoError(t, err)
	require.True(t, disabled)
	want := []time.Duration{0, time.Second, time.Second, 3333333333, 5666666666, 8 * time.Second, 8 * time.Second}
	for i, expected := range want {
		delay, allowed := policy.Retry.retryDelay(i + 1)
		require.True(t, allowed)
		require.InDelta(t, expected, delay, 1)
	}
	_, allowed := policy.Retry.retryDelay(8)
	require.False(t, allowed)
	for _, invalid := range []string{
		`{"healthyRetryPolicy":{"numRetries":-1}}`,
		`{"healthyRetryPolicy":{"numRetries":2,"numNoDelayRetries":3}}`,
		`{"healthyRetryPolicy":{"minDelayTarget":30,"maxDelayTarget":20}}`,
		`{"healthyRetryPolicy":{"minDelayTarget":3600,"maxDelayTarget":3600,"numRetries":2}}`,
		`{"requestPolicy":{"headerContentType":"text/csv"}}`,
		`{"throttlePolicy":{"maxReceivesPerSecond":0}}`,
	} {
		_, _, err = effectiveSNSHTTPPolicy("", invalid, false)
		require.Error(t, err, invalid)
	}
	now := time.Now()
	first := snsWebhookDelivery{policy: policy, throttle: &snsHTTPThrottle{}}
	other := first
	require.Equal(t, now, first.earliestAttempt(now))
	next := now.Add(500 * time.Millisecond)
	require.Equal(t, next, other.earliestAttempt(now))
	require.Equal(t, next, other.earliestAttempt(next))
	require.Equal(t, next.Add(500*time.Millisecond), first.earliestAttempt(next))
}

type snsHTTPDeadlineFixture struct {
	cancel context.CancelFunc
	status int
}

func (snsHTTPDeadlineFixture) Allows(string) bool { return true }
func (f snsHTTPDeadlineFixture) Post(context.Context, string, http.Header, string) (int, error) {
	f.cancel()
	return f.status, nil
}

func TestSNSHTTPJoinedResponseTakesPrecedenceOverConcurrentCancellation(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusBadRequest} {
		ctx, cancel := context.WithCancel(t.Context())
		delivery := snsWebhookDelivery{transport: snsHTTPDeadlineFixture{cancel, status}}
		err := delivery.attempt(ctx)
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		if status == http.StatusNoContent {
			require.NoError(t, err)
		} else {
			var failure snsHTTPDeliveryError
			require.ErrorAs(t, err, &failure)
			require.False(t, failure.Retryable())
		}
	}
}

func TestSNSHTTPConfirmationRetainsOriginalRedrivePolicyAcrossCallback(t *testing.T) {
	broker, _, client, queues := setupTestServer(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	queue, err := queues.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("original-confirmation-dlq")})
	require.NoError(t, err)
	queueARN := broker.GetQueue("original-confirmation-dlq").ARN
	callback := make(chan error, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var confirmation map[string]string
		err := json.NewDecoder(r.Body).Decode(&confirmation)
		if err == nil {
			var result *sns.ConfirmSubscriptionOutput
			result, err = client.ConfirmSubscription(r.Context(), &sns.ConfirmSubscriptionInput{
				TopicArn: aws.String(confirmation["TopicArn"]), Token: aws.String(confirmation["Token"]),
			})
			if err == nil {
				_, err = client.SetSubscriptionAttributes(r.Context(), &sns.SetSubscriptionAttributesInput{
					SubscriptionArn: result.SubscriptionArn, AttributeName: aws.String("RedrivePolicy"), AttributeValue: aws.String(""),
				})
			}
		}
		callback <- err
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer receiver.Close()
	broker.SetSNSHTTPTransport(snsHTTPFixtureTransport{receiver.Client(), receiver.URL})
	defer func() {
		drain, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		require.NoError(t, broker.CloseSNSDeliveries(drain))
	}()
	topic, err := client.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("confirmation-policy-snapshot")})
	require.NoError(t, err)
	subscription, err := client.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Endpoint: aws.String(receiver.URL), Protocol: aws.String("http"), ReturnSubscriptionArn: true,
		Attributes: map[string]string{"RedrivePolicy": `{"deadLetterTargetArn":` + strconv.Quote(queueARN) + "}"},
	})
	require.NoError(t, err)
	select {
	case err := <-callback:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("confirmation callback did not finish")
	}
	require.NoError(t, broker.DrainSNSDeliveries(ctx))
	updated, err := client.GetSubscriptionAttributes(ctx, &sns.GetSubscriptionAttributesInput{SubscriptionArn: subscription.SubscriptionArn})
	require.NoError(t, err)
	require.Empty(t, updated.Attributes["RedrivePolicy"])
	redriven, err := queues.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: queue.QueueUrl})
	require.NoError(t, err)
	require.Len(t, redriven.Messages, 1)
	require.Contains(t, aws.ToString(redriven.Messages[0].Body), "SubscriptionConfirmation")
}
