package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// LambdaDelivery belongs to SNS, the event producer. App composition adapts an
// explicitly owned local Lambda registry and its bounded asynchronous admission.
// A returned request ID proves admission only; Lambda owns execution evidence,
// handler retries, deadlines and joining accepted work during shutdown.
type LambdaDelivery interface {
	ValidateLambdaTarget(context.Context, string) error
	AdmitSNSLambda(context.Context, string, []byte) (string, error)
}

func (broker *Broker) SetLambdaDelivery(delivery LambdaDelivery) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	broker.lambda = delivery
}

func (broker *Broker) lambdaDelivery() LambdaDelivery {
	broker.mu.RLock()
	defer broker.mu.RUnlock()
	return broker.lambda
}

var snsLambdaFunctionRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}(?::[A-Za-z0-9_-]{1,128}|:\$LATEST)?$`)

func (broker *Broker) validateLambdaEndpoint(endpoint string) error {
	prefix := fmt.Sprintf("arn:aws:lambda:%s:%s:function:", broker.region, broker.accountID)
	if !strings.HasPrefix(endpoint, prefix) || !snsLambdaFunctionRE.MatchString(strings.TrimPrefix(endpoint, prefix)) {
		return snsInvalid("Lambda endpoint must identify a local function in this topic's region and account")
	}
	return nil
}

type snsLambdaRecord struct {
	EventSource          string                `json:"EventSource"`
	EventVersion         string                `json:"EventVersion"`
	EventSubscriptionARN string                `json:"EventSubscriptionArn"`
	SNS                  snsLambdaNotification `json:"Sns"`
}

type snsLambdaNotification struct {
	snsEnvelope
	UnsubscribeURL string `json:"UnsubscribeUrl"`
}

func (broker *Broker) buildSNSLambdaEvent(subscription *Subscription, body string) ([]byte, error) {
	var envelope snsEnvelope
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		return nil, err
	}
	// SNS evaluates filters using the original native attribute types. Lambda
	// receives Number and String.Array as String after matching, per AWS.
	for name, attribute := range envelope.MessageAttributes {
		base := strings.SplitN(attribute.Type, ".", 2)[0]
		if base == "Number" || attribute.Type == "String.Array" {
			attribute.Type = "String"
			envelope.MessageAttributes[name] = attribute
		}
	}
	unsubscribe := fmt.Sprintf("http://localhost:%d/?Action=Unsubscribe&SubscriptionArn=%s", broker.port, url.QueryEscape(subscription.ARN))
	event := struct {
		Records []snsLambdaRecord `json:"Records"`
	}{Records: []snsLambdaRecord{{
		EventSource: "aws:sns", EventVersion: "1.0", EventSubscriptionARN: subscription.ARN,
		SNS: snsLambdaNotification{snsEnvelope: envelope, UnsubscribeURL: unsubscribe},
	}}}
	return json.Marshal(event)
}

func (broker *Broker) captureSNSLambdaAdmission(input SNSPublishInput, result SNSPublishResult, subscription *Subscription, requestID string) error {
	return broker.CaptureSNS(SNSCaptureRecord{
		Operation: "DeliveryAdmission", RequestID: input.RequestID, MessageID: result.MessageID, TargetARN: input.TopicARN,
		Deliveries: []SNSCaptureDelivery{{Protocol: "lambda", Endpoint: subscription.Endpoint, SubscriptionARN: subscription.ARN,
			Status: "admitted", MessageID: result.MessageID, InvocationRequestID: requestID}},
	})
}
