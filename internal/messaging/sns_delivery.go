package messaging

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Planning has no delivery side effects. A durable intent is recorded before
// the caller commits FIFO/archive state or submits the planned queue messages.
type snsPlannedDelivery struct {
	subscription *Subscription
	body         string
	queue        *Queue
	firehose     FirehoseDelivery
	lambda       LambdaDelivery
	lambdaEvent  []byte
	attributes   map[string]MessageAttribute
	failure      error
}

func snsDeliveryAttributes(source map[string]MessageAttribute) map[string]MessageAttribute {
	attributes := cloneMessageAttributes(source)
	for name, value := range attributes {
		if strings.SplitN(value.DataType, ".", 2)[0] == "Number" {
			if number, ok := new(big.Rat).SetString(value.StringValue); ok {
				value.StringValue = strings.TrimRight(strings.TrimRight(number.FloatString(5), "0"), ".")
				if value.StringValue == "-0" {
					value.StringValue = "0"
				}
				attributes[name] = value
			}
		}
	}
	return attributes
}

func (b *Broker) snsPlanDeliveries(input SNSPublishInput, result SNSPublishResult, subscriptions []*Subscription, publishedAt time.Time, replayed bool) ([]snsPlannedDelivery, []SNSCaptureDelivery) {
	var protocolMessages map[string]string
	if input.MessageStructure == "json" {
		protocolMessages, _ = snsProtocolMessages(input.Message)
	}
	plans := make([]snsPlannedDelivery, 0, len(subscriptions))
	deliveries := make([]SNSCaptureDelivery, 0, len(subscriptions))
	deliveredAttributes := snsDeliveryAttributes(input.Attributes)
	// All envelope fields except the selected protocol message are shared by
	// this publication. Raw delivery needs no envelope; Lambda's event wrapper
	// remains subscription-specific and is built from this immutable body.
	var envelopes map[string]string
	for _, subscription := range subscriptions {
		delivery := SNSCaptureDelivery{Protocol: subscription.Protocol, Endpoint: subscription.Endpoint, SubscriptionARN: subscription.ARN, Status: "captured", MessageID: result.MessageID}
		if subscription.Pending || subscription.Paused && !replayed {
			delivery.Status = "pending_confirmation"
			if subscription.Paused {
				delivery.Status = "paused"
			}
			deliveries = append(deliveries, delivery)
			continue
		}
		message := input.Message
		if protocolMessages != nil {
			if value, exists := protocolMessages[subscription.Protocol]; exists {
				message = value
			} else {
				message = protocolMessages["default"]
			}
		}
		messageAttributes := deliveredAttributes
		if protocolMessages != nil {
			messageAttributes = nil
		}
		if subscription.FilterPolicy != nil && !subscription.FilterPolicy.MatchesMessage(messageAttributes, message, subscription.Attributes["FilterPolicyScope"]) {
			delivery.Status = "filtered"
			deliveries = append(deliveries, delivery)
			continue
		}
		body := message
		if subscription.Attributes["RawMessageDelivery"] != "true" {
			var exists bool
			body, exists = envelopes[message]
			if !exists {
				body, _ = buildSNSNotification(input, result, message, messageAttributes, publishedAt, replayed)
				if envelopes == nil {
					envelopes = make(map[string]string)
				}
				envelopes[message] = body
			}
		}
		plan := snsPlannedDelivery{subscription: subscription, body: body}
		switch subscription.Protocol {
		case "sqs":
			if subscription.Attributes["RawMessageDelivery"] == "true" {
				plan.attributes = messageAttributes
			}
			if len(plan.attributes) > 10 {
				plan.failure = snsInvalid("Raw SQS delivery supports at most 10 message attributes")
			} else {
				plan.queue = b.GetQueueByARN(subscription.Endpoint)
				if plan.queue == nil {
					plan.failure = snsNotFound("Queue does not exist")
				} else {
					delivery.Status = "scheduled"
				}
			}
			plans = append(plans, plan)
		case "firehose":
			plan.firehose = b.firehoseDelivery()
			if plan.firehose == nil {
				plan.failure = snsInternal("Firehose delivery is not configured")
			} else {
				delivery.Status = "scheduled"
			}
			plans = append(plans, plan)
		case "lambda":
			plan.lambda = b.lambdaDelivery()
			if plan.lambda == nil {
				plan.failure = snsInternal("Local Lambda delivery is not configured")
			} else if err := plan.lambda.ValidateLambdaTarget(context.Background(), subscription.Endpoint); err != nil {
				plan.failure = snsNotFound("Local Lambda function or qualifier is unavailable")
			} else {
				plan.lambdaEvent, plan.failure = b.buildSNSLambdaEvent(subscription, body)
				if plan.failure == nil {
					delivery.Status = "scheduled"
				}
			}
			plans = append(plans, plan)
		case "sms":
			_, plan.failure = smsDeliveryMessage(message, "")
			if plan.failure == nil {
				state := b.snsState()
				state.mu.Lock()
				plan.failure = smsCheckDeliveryLocked(&state.sms, subscription.Endpoint, messageAttributes)
				state.mu.Unlock()
			}
			if plan.failure != nil {
				plans = append(plans, plan)
			}
		case "application":
			attributes, err := b.mobileGetEndpointAttributes(subscription.Endpoint)
			if err != nil {
				plan.failure = err
			} else if attributes["Enabled"] != "true" {
				plan.failure = &snsError{400, "EndpointDisabled", "Endpoint is disabled"}
			}
			if plan.failure != nil {
				plans = append(plans, plan)
			}
		}
		if plan.failure != nil {
			delivery.Status = "dropped"
			delivery.Error = plan.failure.Error()
		}
		deliveries = append(deliveries, delivery)
	}
	return plans, deliveries
}

func (b *Broker) snsSendDeliveries(input SNSPublishInput, result SNSPublishResult, plans []snsPlannedDelivery, dedupID string, replayed bool) error {
	// A single publication has one bounded admission budget across destination
	// subscriptions; expensive queries cannot extend it once per subscriber.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, plan := range plans {
		err := plan.failure
		queueDedup := ""
		if plan.queue != nil && strings.HasSuffix(plan.queue.Name, ".fifo") {
			queueDedup = dedupID
			if replayed {
				queueDedup = "replay-" + uuid.NewString()
			}
		}
		if err == nil && plan.lambda != nil {
			var requestID string
			requestID, err = plan.lambda.AdmitSNSLambda(ctx, plan.subscription.Endpoint, plan.lambdaEvent)
			if err == nil {
				if captureErr := b.captureSNSLambdaAdmission(input, result, plan.subscription, requestID); captureErr != nil {
					return snsInternal("Publication accepted, but Lambda admission outcome capture failed")
				}
			}
		} else if err == nil && plan.firehose != nil {
			_, err = plan.firehose.PutFirehoseRecord(ctx, plan.subscription.Endpoint, []byte(plan.body))
		} else if err == nil {
			_, err = b.SendQueueMessage(plan.queue, QueueMessageInput{Body: plan.body, Attributes: plan.attributes, MessageGroupID: input.MessageGroupID, MessageDeduplicationID: queueDedup, SenderID: b.accountID})
		}
		if err == nil {
			continue
		}
		failure := SNSCaptureRecord{Operation: "DeliveryFailure", RequestID: input.RequestID, MessageID: result.MessageID, TargetARN: input.TopicARN, Deliveries: []SNSCaptureDelivery{{Protocol: plan.subscription.Protocol, Endpoint: plan.subscription.Endpoint, SubscriptionARN: plan.subscription.ARN, MessageID: result.MessageID, Status: "dropped", Error: err.Error()}}}
		if redrive := plan.subscription.Attributes["RedrivePolicy"]; redrive != "" {
			var policy struct {
				DeadLetterTargetARN string `json:"deadLetterTargetArn"`
			}
			_ = json.Unmarshal([]byte(redrive), &policy)
			queue := b.GetQueueByARN(policy.DeadLetterTargetARN)
			if queue != nil {
				dlqDedup := ""
				if strings.HasSuffix(queue.Name, ".fifo") {
					dlqDedup = dedupID
					if dlqDedup == "" || replayed {
						dlqDedup = uuid.NewString()
					}
				}
				_, redriveErr := b.SendQueueMessage(queue, QueueMessageInput{Body: plan.body, Attributes: plan.attributes, MessageGroupID: input.MessageGroupID, MessageDeduplicationID: dlqDedup, SenderID: b.accountID})
				if redriveErr == nil {
					failure.Deliveries[0].Status = "dead_lettered"
				} else {
					failure.Deliveries[0].Error += "; redrive: " + redriveErr.Error()
				}
			} else {
				failure.Deliveries[0].Error += "; redrive: queue does not exist"
			}
		}
		if captureErr := b.CaptureSNS(failure); captureErr != nil {
			return snsInternal("Publication accepted, but delivery outcome capture failed")
		}
	}
	return nil
}
