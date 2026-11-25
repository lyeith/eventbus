package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// SNSHTTPTransport sends SNS POSTs to explicitly owned endpoints. Allow checks
// are side-effect free. The app rechecks ownership when it sends each attempt.
type SNSHTTPTransport interface {
	Allows(endpoint string) bool
	Post(context.Context, string, http.Header, string) (int, error)
}

// SetSNSHTTPTransport attaches the optional local webhook transport. Configure
// it before serving. An absent transport keeps HTTP/S delivery capture-only.
func (b *Broker) SetSNSHTTPTransport(transport SNSHTTPTransport) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.snsHTTPTransport = transport
}

// SetSNSHTTPCallbackOrigin selects the local SNS API origin reachable by
// accepted webhooks, including during a retained owner's drain.
func (b *Broker) SetSNSHTTPCallbackOrigin(origin string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.snsHTTPCallbackOrigin = origin
}

func isSNSHTTP(protocol string) bool { return protocol == "http" || protocol == "https" }

type snsHTTPThrottle struct {
	mu   sync.Mutex
	next time.Time
}

type snsWebhookDelivery struct {
	transport SNSHTTPTransport
	endpoint  string
	headers   http.Header
	body      string
	policy    snsHTTPPolicy
	throttle  *snsHTTPThrottle
	failure   error
}

func (d *snsWebhookDelivery) attempt(ctx context.Context) error {
	if d.failure != nil {
		return d.failure
	}
	status, err := d.transport.Post(ctx, d.endpoint, d.headers.Clone(), d.body)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return snsHTTPDeliveryError{message: "SNS HTTP/S delivery could not reach the endpoint", retry: true}
	}
	if status >= 200 && status < 300 {
		return nil
	}
	return snsHTTPDeliveryError{
		message: "SNS HTTP/S delivery returned status " + strconv.Itoa(status),
		retry:   status == 429 || status >= 500 && status < 600,
	}
}

type snsHTTPDeliveryError struct {
	message string
	retry   bool
}

func (e snsHTTPDeliveryError) Error() string   { return e.message }
func (e snsHTTPDeliveryError) Retryable() bool { return e.retry }

func (d *snsWebhookDelivery) retryDelay(attempt int) (time.Duration, bool) {
	return d.policy.Retry.retryDelay(attempt)
}

func (d *snsWebhookDelivery) payloadSize() int {
	size := len(d.endpoint) + len(d.body)
	for key, values := range d.headers {
		size += len(key)
		for _, value := range values {
			size += len(value)
		}
	}
	return size
}

// A due attempt claims one per-subscription slot. A deferred attempt rechecks
// the shared rate gate when it is due. This does not spend a retry attempt.
func (d *snsWebhookDelivery) earliestAttempt(now time.Time) time.Time {
	if d.policy.Throttle == nil || d.throttle == nil {
		return now
	}
	d.throttle.mu.Lock()
	defer d.throttle.mu.Unlock()
	if d.throttle.next.After(now) {
		return d.throttle.next
	}
	interval := max(time.Nanosecond, time.Second/time.Duration(d.policy.Throttle.MaxReceivesPerSecond))
	d.throttle.next = now.Add(interval)
	return now
}

func (b *Broker) planSNSWebhook(input SNSPublishInput, result SNSPublishResult, subscription *Subscription, body string, topicAttributes map[string]string) *snsWebhookDelivery {
	raw := subscription.Attributes["RawMessageDelivery"] == "true"
	return b.newSNSWebhook("Notification", input.TopicARN, result.MessageID, subscription, body, topicAttributes["DeliveryPolicy"], raw)
}

func (b *Broker) newSNSWebhook(kind, topicARN, messageID string, subscription *Subscription, body, topicPolicy string, raw bool) *snsWebhookDelivery {
	if !isSNSHTTP(subscription.Protocol) {
		return nil
	}
	b.mu.RLock()
	transport := b.snsHTTPTransport
	b.mu.RUnlock()
	if transport == nil || !transport.Allows(subscription.Endpoint) {
		return nil
	}
	policy, _, err := effectiveSNSHTTPPolicy(topicPolicy, subscription.Attributes["DeliveryPolicy"], subscription.Attributes["RawMessageDelivery"] == "true")
	contentType := policy.Request.HeaderContentType
	if !raw {
		// Control messages are always JSON, including on raw subscriptions.
		mediaType, _, _ := mime.ParseMediaType(contentType)
		if mediaType != "application/json" && mediaType != "text/plain" {
			contentType = snsDefaultContentType
		}
	}
	headers := make(http.Header)
	headers.Set("Content-Type", contentType)
	headers.Set("User-Agent", "Amazon Simple Notification Service Agent")
	headers.Set("X-Amz-Sns-Message-Type", kind)
	headers.Set("X-Amz-Sns-Message-Id", messageID)
	headers.Set("X-Amz-Sns-Topic-Arn", topicARN)
	headers.Set("X-Amz-Sns-Subscription-Arn", subscription.ARN)
	if raw {
		headers.Set("X-Amz-Sns-Rawdelivery", "true")
	} else if kind == "Notification" && err == nil {
		// The wrapped message, selected protocol payload, and attributes are
		// already encoded by buildSNSNotification. Add only HTTP's own link.
		var envelope map[string]json.RawMessage
		if decodeErr := json.Unmarshal([]byte(body), &envelope); decodeErr != nil || envelope == nil {
			err = snsInternal("SNS HTTP/S notification could not be encoded")
		} else {
			envelope["UnsubscribeURL"], _ = json.Marshal(b.snsControlURL(subscription.Endpoint, "Unsubscribe", url.Values{"SubscriptionArn": {subscription.ARN}}))
			encoded, _ := json.Marshal(envelope)
			body = string(encoded)
		}
	}
	return &snsWebhookDelivery{transport: transport, endpoint: subscription.Endpoint, headers: headers, body: body, policy: policy, throttle: subscription.httpThrottle, failure: err}
}

func (b *Broker) snsControlURL(endpoint, action string, params url.Values) string {
	params.Set("Action", action)
	params.Set("Version", "2010-03-31")
	b.mu.RLock()
	origin, transport := b.snsHTTPCallbackOrigin, b.snsHTTPTransport
	b.mu.RUnlock()
	if origin == "" || transport == nil || !transport.Allows(endpoint) {
		origin = "http://localhost:" + strconv.Itoa(b.port)
	}
	return strings.TrimRight(origin, "/") + "/?" + params.Encode()
}

// Control IDs and the timestamp are assigned before delivery so retries carry
// the same envelope. Only the application-owned transport can send them.
func (b *Broker) snsConfirmation(kind string, subscription *Subscription, token, topicPolicy string) (SNSPublishResult, string, *snsWebhookDelivery) {
	result := SNSPublishResult{MessageID: uuid.NewString()}
	message := fmt.Sprintf("Confirm this subscription to %s using the SubscribeURL or Token.", subscription.TopicARN)
	if kind == "UnsubscribeConfirmation" {
		message = fmt.Sprintf("Restore subscription %s using the SubscribeURL or Token.", subscription.ARN)
	}
	body, _ := json.Marshal(map[string]string{
		"Type": kind, "MessageId": result.MessageID, "TopicArn": subscription.TopicARN,
		"Message": message, "Token": token, "Timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"SubscribeURL": b.snsControlURL(subscription.Endpoint, "ConfirmSubscription", url.Values{"TopicArn": {subscription.TopicARN}, "Token": {token}}),
	})
	wire := string(body)
	return result, wire, b.newSNSWebhook(kind, subscription.TopicARN, result.MessageID, subscription, wire, topicPolicy, false)
}

// Prepare reserves bounded scheduling capacity and records the existing control
// intent. The caller commits the task only after publishing subscription state.
func (b *Broker) prepareSNSConfirmation(operation, requestID, kind string, subscription *Subscription, token, topicPolicy string) (commit, cancel func(), err error) {
	// The callback may confirm or update subscription state during delivery.
	subscription = cloneSNSSubscription(subscription)
	result, body, webhook := b.snsConfirmation(kind, subscription, token, topicPolicy)
	input := SNSPublishInput{Operation: operation, RequestID: requestID, TopicARN: subscription.TopicARN}
	commit, cancel, err = b.prepareSNSWebhook(input, result, subscription, body, webhook)
	if err != nil {
		return nil, nil, err
	}
	status := "captured"
	if webhook != nil {
		status = "scheduled"
	}
	record := SNSCaptureRecord{
		Operation: operation, RequestID: requestID, TargetARN: subscription.TopicARN, MessageID: result.MessageID, Message: body,
		Deliveries: []SNSCaptureDelivery{{Protocol: subscription.Protocol, Endpoint: subscription.Endpoint, Status: status, MessageID: result.MessageID, SubscriptionARN: subscription.ARN}},
		Details:    map[string]any{"token": token, "subscription_arn": subscription.ARN},
	}
	if err := b.CaptureSNS(record); err != nil {
		cancel()
		message := "Unable to capture subscription confirmation"
		if kind == "UnsubscribeConfirmation" {
			message = "Unable to capture unsubscribe confirmation"
		}
		return nil, nil, snsInternal(message)
	}
	return commit, cancel, nil
}
