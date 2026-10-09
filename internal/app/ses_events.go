package app

import (
	"context"
	"errors"
	"strings"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/ses"
)

// SES owns event selection and payloads. This adapter forwards them to the
// single SNS owner, which retains native envelopes, filtering and delivery.
type sesSNSPublisher struct{ broker *messaging.Broker }

var _ ses.EventPublisher = sesSNSPublisher{}

func (publisher sesSNSPublisher) ValidateTopic(arn string) error {
	if publisher.broker == nil || strings.HasSuffix(arn, ".fifo") {
		return errors.New("SES events require an existing standard SNS topic")
	}
	topic := publisher.broker.GetTopic(arn)
	if topic == nil || topic.ARN != arn {
		return errors.New("SES events require an existing standard SNS topic")
	}
	return nil
}

func (publisher sesSNSPublisher) PublishEvent(ctx context.Context, arn, message, requestID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if publisher.broker == nil {
		return errors.New("SNS event publishing is unavailable")
	}
	_, err := publisher.broker.PublishSNS(messaging.SNSPublishInput{
		Operation: "Publish", RequestID: requestID, TopicARN: arn, Message: message,
	})
	return err
}
