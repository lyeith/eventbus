package ses

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/mail"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// Correlation retains only event metadata, never MIME bodies or attachments.
// These are local harness limits, not SES send quotas; the original capture is
// durable regardless of correlation eviction.
const acceptedMessageTTL = 24 * time.Hour
const acceptedMessageLimit = 10_000
const acceptedMessageByteLimit = 64 << 20

type acceptedMessage struct {
	id            string
	sentAt        time.Time
	retainedAt    time.Time
	configuration *configurationSet
	mail          json.RawMessage
	recipients    []string
	size          int
	order         *list.Element
}

type preparedMessage struct {
	accepted   *acceptedMessage
	sendTopics []string
}

type eventAdmission struct {
	TopicARN string `json:"topic_arn"`
	Error    string `json:"error,omitempty"`
}

// Prepare captures an immutable native routing snapshot before durable email
// acceptance. Later updates cannot mutate an admitted event's destination list.
func (manager *SESManager) prepareEvents(api string, input map[string]any, emails []map[string]any) ([]preparedMessage, *sesAPIError) {
	now := time.Now().UTC()
	prepared := make([]preparedMessage, 0, len(emails))
	for _, email := range emails {
		id := sesString(email["message_id"])
		if id == "" {
			continue
		} // A failed bulk entry cannot produce events.
		configurationName := sesString(input["ConfigurationSetName"])
		if api == "v1" && sesString(email["content_type"]) == "raw" {
			configurationName = sesString(email["configuration_set"])
		}
		if configurationName == "" {
			continue
		}
		recipients := eventRecipients(sesObject(email["destination"]))
		payload := manager.eventMail(api, input, email, configurationName, now, recipients)
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, &sesAPIError{Code: "InternalFailure", Message: "Unable to prepare SES event metadata", Status: http.StatusInternalServerError}
		}
		size := len(encoded) + len(id)
		for _, recipient := range recipients {
			size += len(recipient)
		}
		accepted := &acceptedMessage{id: id, sentAt: now, mail: encoded, recipients: recipients, size: size}
		manager.mu.Lock()
		if configurationName != "" {
			accepted.configuration = manager.configurationSets[configurationName]
			if accepted.configuration == nil {
				manager.mu.Unlock()
				if api == "v1" {
					return nil, missingConfigurationSet(configurationName)
				}
				return nil, &sesAPIError{Code: "NotFoundException", Message: "Configuration set does not exist: " + configurationName, Status: 404}
			}
		}
		topics := eventTopics(accepted.configuration, "send")
		manager.mu.Unlock()
		prepared = append(prepared, preparedMessage{accepted: accepted, sendTopics: topics})
	}
	return prepared, nil
}

func eventRecipients(destination map[string]any) []string {
	recipients := make([]string, 0)
	for _, name := range []string{"ToAddresses", "CcAddresses", "BccAddresses"} {
		for _, recipient := range sesStrings(destination[name]) {
			if parsed, err := mail.ParseAddress(recipient); err == nil {
				recipient = parsed.Address
			}
			recipients = append(recipients, recipient)
		}
	}
	return recipients
}

func (manager *SESManager) eventMail(api string, input, email map[string]any, configuration string, sentAt time.Time, recipients []string) map[string]any {
	source := sesString(email["from"])
	if parsed, err := mail.ParseAddress(source); err == nil {
		source = parsed.Address
	}
	sourceARN := sesString(input["SourceArn"])
	if api == "v2" {
		sourceARN = sesString(input["FromEmailAddressIdentityArn"])
	}
	if sourceARN == "" {
		sourceARN = fmt.Sprintf("arn:aws:ses:%s:%s:identity/%s", manager.region, manager.accountID, source)
	}
	headers, truncated := eventHeaders(email)
	common := map[string]any{"from": []string{sesString(email["from"])}, "messageId": email["message_id"]}
	for key, field := range map[string]string{"ToAddresses": "to", "CcAddresses": "cc", "BccAddresses": "bcc"} {
		if values := sesStrings(sesObject(email["destination"])[key]); len(values) != 0 {
			common[field] = slices.Clone(values)
		}
	}
	if subject, present := email["subject"]; present {
		common["subject"] = subject
	}
	tags := map[string][]string{}
	for _, key := range []string{"tags", "replacement_tags"} {
		mergeEventTags(tags, email[key])
	}
	if api == "v2" {
		mergeEventTags(tags, input["EmailTags"])
		if index, ok := email["entry_index"].(int); ok {
			mergeEventTags(tags, input["DefaultEmailTags"])
			entries, _ := input["BulkEmailEntries"].([]any)
			if index < len(entries) {
				mergeEventTags(tags, sesObject(entries[index])["ReplacementTags"])
			}
		}
	}
	if configuration != "" {
		tags["ses:configuration-set"] = []string{configuration}
	}
	if index := strings.LastIndex(source, "@"); index >= 0 {
		tags["ses:from-domain"] = []string{source[index+1:]}
	}
	return map[string]any{"timestamp": sentAt.Format(time.RFC3339Nano), "messageId": email["message_id"],
		"source": source, "sourceArn": sourceARN, "sendingAccountId": manager.accountID,
		"destination": slices.Clone(recipients), "headersTruncated": truncated, "headers": headers, "commonHeaders": common, "tags": tags}
}

func mergeEventTags(target map[string][]string, value any) {
	tags, _ := value.([]any)
	for _, value := range tags {
		tag := sesObject(value)
		name := sesString(tag["Name"])
		if name != "" {
			target[name] = []string{sesString(tag["Value"])}
		}
	}
}

func eventHeaders(email map[string]any) ([]map[string]string, bool) {
	all := make([]map[string]string, 0)
	if headers := sesObject(email["headers"]); headers != nil {
		names := make([]string, 0, len(headers))
		for name := range headers {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			for _, value := range sesStrings(headers[name]) {
				all = append(all, map[string]string{"name": name, "value": value})
			}
		}
	} else if headers, ok := email["headers"].([]any); ok {
		for _, value := range headers {
			header := sesObject(value)
			all = append(all, map[string]string{"name": sesString(header["Name"]), "value": sesString(header["Value"])})
		}
	}
	if sesString(email["content_type"]) != "raw" {
		all = append(all, map[string]string{"name": "From", "value": sesString(email["from"])})
		for key, header := range map[string]string{"ToAddresses": "To", "CcAddresses": "Cc"} {
			if values := sesStrings(sesObject(email["destination"])[key]); len(values) > 0 {
				all = append(all, map[string]string{"name": header, "value": strings.Join(values, ", ")})
			}
		}
		all = append(all, map[string]string{"name": "Subject", "value": sesString(email["subject"])})
	}
	size := 0
	for index, header := range all {
		size += len(header["name"]) + len(header["value"]) + 4
		if size > 10*1024 {
			return all[:index], true
		}
	}
	return all, false
}

// Matching is native SES configuration (lowercase enums); published eventType
// uses AWS's capitalized payload spelling. Distinct destinations each publish.
func eventTopics(configuration *configurationSet, eventType string) []string {
	topics := make([]string, 0)
	if configuration == nil {
		return topics
	}
	names := make([]string, 0, len(configuration.destinations))
	for name := range configuration.destinations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		destination := configuration.destinations[name]
		if destination.enabled && slices.Contains(destination.matchingTypes, eventType) {
			topics = append(topics, destination.topicARN)
		}
	}
	return topics
}

func (manager *SESManager) acceptEvents(ctx context.Context, requestID string, prepared []preparedMessage) {
	manager.mu.Lock()
	now := time.Now().UTC()
	manager.pruneAccepted(now)
	for _, message := range prepared {
		accepted := message.accepted
		if accepted.size > acceptedMessageByteLimit {
			continue
		}
		accepted.retainedAt = now
		accepted.order = manager.acceptedOrder.PushBack(accepted)
		manager.accepted[accepted.id] = accepted
		manager.acceptedBytes += accepted.size
		for len(manager.accepted) > acceptedMessageLimit || manager.acceptedBytes > acceptedMessageByteLimit {
			manager.removeAccepted(manager.acceptedOrder.Front())
		}
	}
	manager.mu.Unlock()
	for _, message := range prepared {
		manager.publishEvent(ctx, requestID, message.accepted, "Send", map[string]any{}, message.sendTopics)
	}
}

// Caller holds manager.mu. Admission order is retention-time order, not outcome-time
// order, so expired correlations and oldest overflow are removed in bounded work.
func (manager *SESManager) pruneAccepted(now time.Time) {
	for element := manager.acceptedOrder.Front(); element != nil; element = manager.acceptedOrder.Front() {
		if now.Sub(element.Value.(*acceptedMessage).retainedAt) < acceptedMessageTTL {
			break
		}
		manager.removeAccepted(element)
	}
}
func (manager *SESManager) removeAccepted(element *list.Element) {
	accepted := element.Value.(*acceptedMessage)
	delete(manager.accepted, accepted.id)
	manager.acceptedBytes -= accepted.size
	manager.acceptedOrder.Remove(element)
}

func (manager *SESManager) publishEvent(ctx context.Context, requestID string, accepted *acceptedMessage, eventType string, details map[string]any, topics []string) []eventAdmission {
	admissions := make([]eventAdmission, 0, len(topics))
	if len(topics) == 0 {
		return admissions
	}
	payload := map[string]any{"eventType": eventType, "mail": accepted.mail, strings.ToLower(eventType): details}
	encoded, err := json.Marshal(payload)
	if err != nil {
		for _, topic := range topics {
			admissions = append(admissions, eventAdmission{TopicARN: topic, Error: "Unable to encode SES event: " + err.Error()})
		}
		return admissions
	}
	for _, topic := range topics {
		result := eventAdmission{TopicARN: topic}
		if err := manager.eventPublisher.PublishEvent(ctx, topic, string(encoded), requestID); err != nil {
			result.Error = err.Error()
			// The accepted email cannot be undone by a downstream notification
			// failure. Keep the failure explicit and correlated in private logs.
			log.Error().Err(err).Str("topicArn", topic).Str("eventType", eventType).Str("messageId", accepted.id).
				Str("request_id", requestID).Msg("SES configuration-set event admission failed")
		}
		admissions = append(admissions, result)
	}
	return admissions
}
