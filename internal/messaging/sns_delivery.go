package messaging

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"time"
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
	webhook      *snsWebhookDelivery
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

func (b *Broker) snsPlanDeliveries(input SNSPublishInput, result SNSPublishResult, subscriptions []*Subscription, topicAttributes map[string]string, publishedAt time.Time, replayed bool) ([]snsPlannedDelivery, []SNSCaptureDelivery) {
	var protocolMessages map[string]string
	if input.MessageStructure == "json" {
		protocolMessages, _ = snsProtocolMessages(input.Message)
	}
	plans := make([]snsPlannedDelivery, 0, len(subscriptions))
	deliveries := make([]SNSCaptureDelivery, 0, len(subscriptions))
	deliveredAttributes := snsDeliveryAttributes(input.Attributes)
	filterInputs := snsFilterInputs{attributes: deliveredAttributes}
	if protocolMessages != nil {
		// Protocol-specific publications strip attributes for every delivery,
		// including subscription filtering.
		filterInputs.attributes = nil
	}
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
		if subscription.FilterPolicy != nil && !subscription.FilterPolicy.matchesInput(&filterInputs, message, subscription.Attributes["FilterPolicyScope"]) {
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
		case "http", "https":
			plan.webhook = b.planSNSWebhook(input, result, subscription, body, topicAttributes)
			if plan.webhook != nil {
				delivery.Status = "scheduled"
				plans = append(plans, plan)
			}
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

// Only immutable delivery inputs and identifiers survive after Publish returns.
func snsDeliveryTasks(input SNSPublishInput, result SNSPublishResult, plans []snsPlannedDelivery, dedupID string) []*snsDeliveryTask {
	ownedInput := SNSPublishInput{TopicARN: input.TopicARN, RequestID: input.RequestID, MessageGroupID: input.MessageGroupID}
	tasks := make([]*snsDeliveryTask, 0, len(plans))
	for _, plan := range plans {
		tasks = append(tasks, snsDeliveryTaskForPlan(ownedInput, result, plan, dedupID))
	}
	return tasks
}

func snsDeliveryTaskForPlan(input SNSPublishInput, result SNSPublishResult, plan snsPlannedDelivery, dedupID string) *snsDeliveryTask {
	// Keep the immutable dispatch fields, rather than retaining the compiled
	// filter and mutable subscription settings for the lifetime of a retry.
	sub := plan.subscription
	redrive := sub.Attributes["RedrivePolicy"]
	plan.subscription = &Subscription{ARN: sub.ARN, Protocol: sub.Protocol, Endpoint: sub.Endpoint}
	if redrive != "" {
		plan.subscription.Attributes = map[string]string{"RedrivePolicy": redrive}
	}
	size := len(plan.body) + len(plan.lambdaEvent) + len(plan.subscription.Endpoint) + len(plan.subscription.ARN) + len(redrive)
	for name, value := range plan.attributes {
		size += len(name) + len(value.DataType) + len(value.BinaryValue) + len(value.StringValue)
	}
	if plan.webhook != nil {
		size += plan.webhook.payloadSize()
	}
	task := &snsDeliveryTask{input: input, result: result, plan: plan, dedupID: dedupID, size: size}
	if strings.HasSuffix(input.TopicARN, ".fifo") && input.MessageGroupID != "" {
		task.key = snsRetryKey{subscription: plan.subscription.ARN, group: input.MessageGroupID}
	}
	return task
}

func (b *Broker) prepareSNSWebhook(input SNSPublishInput, result SNSPublishResult, sub *Subscription, body string, delivery *snsWebhookDelivery) (commit, cancel func(), err error) {
	if delivery == nil {
		return func() {}, func() {}, nil
	}
	owner := b.snsRetryOwner()
	tasks := snsDeliveryTasks(input, result, []snsPlannedDelivery{{subscription: sub, body: body, webhook: delivery}}, "")
	if err = owner.reserve(tasks); err != nil {
		return nil, nil, err
	}
	var committed bool
	return func() {
			committed = true
			_ = owner.submit(owner.ctx, tasks[0])
		}, func() {
			if !committed {
				owner.abandon(tasks)
			}
		}, nil
}

func (o *snsRetryOwner) send(tasks []*snsDeliveryTask) error {
	// One publication shares an inline admission budget across destinations.
	ctx, cancel := context.WithTimeout(o.ctx, snsAttemptTimeout)
	defer cancel()
	var evidenceErr error
	for _, task := range tasks {
		if err := o.submit(ctx, task); err != nil && evidenceErr == nil {
			evidenceErr = err
		}
	}
	return evidenceErr
}

func (b *Broker) snsAttempt(ctx context.Context, t *snsDeliveryTask) (failure, evidenceErr error) {
	plan := t.plan
	if plan.failure != nil {
		return plan.failure, nil
	}
	if err := ctx.Err(); err != nil {
		return err, nil
	}
	if plan.lambda != nil {
		requestID, err := plan.lambda.AdmitSNSLambda(ctx, plan.subscription.Endpoint, plan.lambdaEvent)
		if err != nil {
			return err, nil
		}
		// Admission is final, even when the outcome capture itself fails.
		return nil, b.captureSNSLambdaAdmission(t.input, t.result, plan.subscription, requestID)
	}
	if plan.firehose != nil {
		_, err := plan.firehose.PutFirehoseRecord(ctx, plan.subscription.Endpoint, []byte(plan.body))
		return err, nil
	}
	if plan.webhook != nil {
		if err := plan.webhook.attempt(ctx); err != nil {
			return err, nil
		}
	} else {
		_, err := b.SendQueueMessage(plan.queue, t.queueInput(plan.queue))
		if err != nil {
			return err, nil
		}
	}
	if plan.webhook != nil || t.attempts > 1 {
		return nil, b.snsCaptureTaskOutcome(t, "DeliverySuccess", "delivered", "")
	}
	return nil, nil
}

func (t *snsDeliveryTask) queueInput(queue *Queue) QueueMessageInput {
	var dedupID string
	if queue != nil && strings.HasSuffix(queue.Name, ".fifo") {
		dedupID = t.dedupID
		if dedupID == "" {
			dedupID = t.result.MessageID
		}
	}
	body := t.plan.body
	if t.plan.webhook != nil {
		body = t.plan.webhook.body
	}
	return QueueMessageInput{Body: body, Attributes: t.plan.attributes,
		MessageGroupID: t.input.MessageGroupID, MessageDeduplicationID: dedupID}
}

func (b *Broker) snsFailDelivery(t *snsDeliveryTask, cause error) error {
	status, message := "dropped", cause.Error()
	if redrive := t.plan.subscription.Attributes["RedrivePolicy"]; redrive != "" {
		var policy struct {
			DeadLetterTargetARN string `json:"deadLetterTargetArn"`
		}
		_ = json.Unmarshal([]byte(redrive), &policy)
		if queue := b.GetQueueByARN(policy.DeadLetterTargetARN); queue != nil {
			if _, err := b.SendQueueMessage(queue, t.queueInput(queue)); err == nil {
				status = "dead_lettered"
			} else {
				message += "; redrive: " + err.Error()
			}
		} else {
			message += "; redrive: queue does not exist"
		}
	}
	return b.snsCaptureTaskOutcome(t, "DeliveryFailure", status, message)
}

func (b *Broker) snsCaptureTaskOutcome(t *snsDeliveryTask, operation, status, message string) error {
	sub := t.plan.subscription
	return b.CaptureSNS(SNSCaptureRecord{
		Operation: operation, RequestID: t.input.RequestID, MessageID: t.result.MessageID, TargetARN: t.input.TopicARN,
		Deliveries: []SNSCaptureDelivery{{Protocol: sub.Protocol, Endpoint: sub.Endpoint, SubscriptionARN: sub.ARN,
			MessageID: t.result.MessageID, Status: status, Error: message}},
		Details: map[string]any{"attempts": t.attempts},
	})
}
