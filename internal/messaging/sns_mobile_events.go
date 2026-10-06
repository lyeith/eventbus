package messaging

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"
)

// mobilePrepareEvent records event intent before endpoint state is committed.
// The exact same JSON is subsequently sent through ordinary SNS topic fanout.
func (b *Broker) mobilePrepareEvent(application *mobileApplication, endpoint *mobileEndpoint, eventType string) (*SNSPublishInput, error) {
	topicARN := application.attributes["Event"+eventType]
	if topicARN == "" {
		return nil, nil
	}
	event := map[string]string{
		"EndpointArn": endpoint.arn, "EventType": eventType, "Resource": application.arn,
		"Service": "SNS", "Time": time.Now().UTC().Format(time.RFC3339Nano), "Type": eventType,
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	message := string(encoded)
	if err := b.CaptureSNS(SNSCaptureRecord{
		Operation: eventType, TargetARN: topicARN, Message: message,
		Details: map[string]any{"endpoint_arn": endpoint.arn, "platform_application_arn": application.arn},
	}); err != nil {
		return nil, &snsError{Status: http.StatusInternalServerError, Code: "InternalError", Message: "Failed to capture SNS application event: " + err.Error()}
	}
	return &SNSPublishInput{TopicARN: topicARN, Message: message, Operation: eventType}, nil
}

// Events are an asynchronous AWS side effect. A failed fanout cannot undo an
// endpoint operation that has already committed, but remains visible locally.
func (b *Broker) mobileDispatchEvent(input *SNSPublishInput) {
	if input == nil {
		return
	}
	if _, err := b.PublishSNS(*input); err != nil {
		captureErr := b.CaptureSNS(SNSCaptureRecord{
			Operation: input.Operation, TargetARN: input.TopicARN, Message: input.Message,
			Deliveries: []SNSCaptureDelivery{{Protocol: "topic", Endpoint: input.TopicARN, Status: "failed", Error: err.Error()}},
		})
		log.Error().Err(err).AnErr("captureError", captureErr).Str("topicArn", input.TopicARN).Str("eventType", input.Operation).Msg("SNS application event fanout failed")
	}
}
