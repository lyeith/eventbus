package messaging

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/awsprotocol"
)

// serveSNSQuery recognizes the complete SNS operation set. The shared handler
// remains responsible for selecting SNS/SQS and rejecting unknown actions.
func (s *Handler) serveSNSQuery(w http.ResponseWriter, r *http.Request, action string) bool {
	snsRequestID(w)
	switch action {
	case "CreateTopic":
		s.handleCreateTopic(w, r)
	case "DeleteTopic":
		s.handleDeleteTopic(w, r)
	case "ListTopics":
		s.handleListTopics(w, r)
	case "GetTopicAttributes", "SetTopicAttributes", "GetDataProtectionPolicy", "PutDataProtectionPolicy", "AddPermission", "RemovePermission", "TagResource", "UntagResource", "ListTagsForResource":
		s.handleSNSTopicControl(w, r, action)
	case "Subscribe":
		s.handleSubscribe(w, r)
	case "ConfirmSubscription", "Unsubscribe", "GetSubscriptionAttributes", "SetSubscriptionAttributes":
		s.handleSNSSubscriptionControl(w, r, action)
	case "ListSubscriptions", "ListSubscriptionsByTopic":
		s.handleSNSListSubscriptions(w, r, action)
	case "Publish":
		s.handlePublish(w, r)
	case "PublishBatch":
		s.handleSNSPublishBatch(w, r)
	default:
		if s.handleSNSSMSQuery(w, r, action) || s.handleSNSMobileQuery(w, r, action) {
			return true
		}
		return false
	}
	return true
}

func (s *Handler) handleCreateTopic(w http.ResponseWriter, r *http.Request) {
	tags, err := snsReadTags(r, "Tags")
	if err != nil {
		snsWriteError(w, err)
		return
	}
	topic, err := s.broker.CreateTopicWithAttributes(r.FormValue("Name"), formMapValues(r, "Attributes"), tags, r.FormValue("DataProtectionPolicy"))
	if err != nil {
		snsWriteError(w, err)
		return
	}
	snsResponse(w, "CreateTopic", "<TopicArn>"+awsprotocol.XMLEscape(topic.ARN)+"</TopicArn>")
}

func (s *Handler) handleDeleteTopic(w http.ResponseWriter, r *http.Request) {
	arn := r.FormValue("TopicArn")
	if !snsTopicARNValid(arn) {
		snsWriteError(w, snsInvalid("Invalid TopicArn"))
		return
	}
	topic := s.broker.GetTopic(arn)
	if topic != nil {
		topic.controlMu.Lock()
		defer topic.controlMu.Unlock()
		topic.publishMu.Lock()
		defer topic.publishMu.Unlock()
		topic.mu.RLock()
		retention, _ := snsArchiveRetention(topic.Attributes["ArchivePolicy"])
		topic.mu.RUnlock()
		if retention > 0 {
			snsWriteError(w, &snsError{400, "InvalidState", "Disable ArchivePolicy before deleting an archived topic"})
			return
		}
		s.broker.DeleteTopic(arn)
	}
	snsResponse(w, "DeleteTopic", "")
}

func (s *Handler) handleListTopics(w http.ResponseWriter, r *http.Request) {
	after, err := snsParsePageToken(r.FormValue("NextToken"), "ListTopics", s.broker.accountID+":"+s.broker.region)
	if err != nil {
		snsWriteError(w, err)
		return
	}
	topics := s.broker.ListTopics()
	var members strings.Builder
	count := 0
	last := ""
	next := ""
	for _, topic := range topics {
		if topic.ARN <= after {
			continue
		}
		if count == 100 {
			next = snsPageToken("ListTopics", s.broker.accountID+":"+s.broker.region, last)
			break
		}
		fmt.Fprintf(&members, "<member><TopicArn>%s</TopicArn></member>", awsprotocol.XMLEscape(topic.ARN))
		last = topic.ARN
		count++
	}
	result := "<Topics>" + members.String() + "</Topics>"
	if next != "" {
		result += "<NextToken>" + next + "</NextToken>"
	}
	snsResponse(w, "ListTopics", result)
}

func snsReadTags(r *http.Request, prefix string) (map[string]string, error) {
	tags := make(map[string]string)
	for index := 1; index <= 100; index++ {
		base := fmt.Sprintf("%s.member.%d.", prefix, index)
		key := r.FormValue(base + "Key")
		value := r.FormValue(base + "Value")
		if key == "" {
			if value != "" {
				return nil, snsInvalid("Tag key must not be empty")
			}
			break
		}
		if _, exists := tags[key]; exists {
			return nil, snsInvalid("Tag keys must be unique")
		}
		tags[key] = value
	}
	return tags, validateSNSTags(tags)
}

func (s *Handler) handleSNSTopicControl(w http.ResponseWriter, r *http.Request, action string) {
	arn := r.FormValue("TopicArn")
	if action == "ListTagsForResource" || action == "TagResource" || action == "UntagResource" {
		arn = r.FormValue("ResourceArn")
	}
	if action == "GetDataProtectionPolicy" || action == "PutDataProtectionPolicy" {
		arn = r.FormValue("ResourceArn")
	}
	if !snsTopicARNValid(arn) {
		snsWriteError(w, snsInvalid("Invalid topic resource ARN"))
		return
	}
	topic := s.broker.GetTopic(arn)
	if topic == nil {
		snsWriteError(w, snsNotFound("Topic does not exist"))
		return
	}
	if action == "GetTopicAttributes" {
		snsResponse(w, action, "<Attributes>"+snsMapXML(s.broker.topicAttributes(topic))+"</Attributes>")
		return
	}
	topic.controlMu.Lock()
	defer topic.controlMu.Unlock()
	topic.publishMu.Lock()
	defer topic.publishMu.Unlock()
	topic.mu.Lock()
	defer topic.mu.Unlock()
	var result string
	switch action {
	case "SetTopicAttributes":
		name, value := r.FormValue("AttributeName"), r.FormValue("AttributeValue")
		if err := validateSNSTopicAttribute(name, value, topic.Attributes["FifoTopic"] == "true", false); err != nil {
			snsWriteError(w, err)
			return
		}
		if name == "MaximumMessageSize" {
			size, _ := strconv.Atoi(value)
			if size > 262144 {
				if len(topic.Subscriptions) > 100 {
					snsWriteError(w, snsInvalid("Large topics support at most 100 subscriptions"))
					return
				}
				for _, subscription := range topic.Subscriptions {
					if subscription.Protocol != "sqs" && subscription.Protocol != "lambda" && subscription.Protocol != "firehose" {
						snsWriteError(w, snsInvalid("Large topics support only SQS, Lambda or Firehose subscriptions"))
						return
					}
				}
			}
		}
		topic.Attributes[name] = value
		if name == "ArchivePolicy" {
			retention, _ := snsArchiveRetention(value)
			snsPruneArchive(topic, retention, time.Now())
		}
	case "GetDataProtectionPolicy":
		result = "<DataProtectionPolicy>" + awsprotocol.XMLEscape(topic.DataProtectionPolicy) + "</DataProtectionPolicy>"
	case "PutDataProtectionPolicy":
		if topic.Attributes["FifoTopic"] == "true" {
			snsWriteError(w, snsInvalid("DataProtectionPolicy is supported only for standard topics"))
			return
		}
		policy := r.FormValue("DataProtectionPolicy")
		if err := validateSNSProtectionPolicy(policy); err != nil {
			snsWriteError(w, err)
			return
		}
		topic.DataProtectionPolicy = policy
	case "TagResource":
		tags, err := snsReadTags(r, "Tags")
		if err != nil {
			snsWriteError(w, err)
			return
		}
		if len(tags) == 0 {
			snsWriteError(w, snsInvalid("Tags are required"))
			return
		}
		combined := maps.Clone(topic.Tags)
		for key, value := range tags {
			combined[key] = value
		}
		if err := validateSNSTags(combined); err != nil {
			snsWriteError(w, err)
			return
		}
		topic.Tags = combined
	case "UntagResource":
		keys := snsReadList(r, "TagKeys")
		if len(keys) == 0 {
			snsWriteError(w, snsInvalid("TagKeys are required"))
			return
		}
		for _, key := range keys {
			delete(topic.Tags, key)
		}
	case "ListTagsForResource":
		keys := make([]string, 0, len(topic.Tags))
		for key := range topic.Tags {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var tags strings.Builder
		for _, key := range keys {
			fmt.Fprintf(&tags, "<member><Key>%s</Key><Value>%s</Value></member>", awsprotocol.XMLEscape(key), awsprotocol.XMLEscape(topic.Tags[key]))
		}
		result = "<Tags>" + tags.String() + "</Tags>"
	case "AddPermission", "RemovePermission":
		if err := snsMutatePermission(topic, r, action); err != nil {
			snsWriteError(w, err)
			return
		}
	}
	snsResponse(w, action, result)
}

func snsReadList(request *http.Request, prefix string) []string {
	var values []string
	for index := 1; index <= 1000; index++ {
		value := request.FormValue(fmt.Sprintf("%s.member.%d", prefix, index))
		if value == "" {
			value = request.FormValue(fmt.Sprintf("%s.%d", prefix, index))
		}
		if value == "" {
			break
		}
		values = append(values, value)
	}
	return values
}

func snsMutatePermission(topic *Topic, request *http.Request, action string) error {
	label := request.FormValue("Label")
	if label == "" || len(label) > 100 {
		return snsInvalid("A Label of at most 100 characters is required")
	}
	var policy map[string]any
	if err := json.Unmarshal([]byte(topic.Attributes["Policy"]), &policy); err != nil {
		return snsInternal("Stored policy is invalid")
	}
	var statements []any
	switch value := policy["Statement"].(type) {
	case []any:
		statements = value
	case map[string]any:
		statements = []any{value}
	}
	position := -1
	for index, value := range statements {
		if statement, ok := value.(map[string]any); ok && statement["Sid"] == label {
			position = index
			break
		}
	}
	if action == "RemovePermission" {
		if position >= 0 {
			statements = append(statements[:position], statements[position+1:]...)
		}
	} else {
		if position >= 0 {
			return snsInvalid("Policy statement already exists")
		}
		accounts := snsReadList(request, "AWSAccountId")
		actions := snsReadList(request, "ActionName")
		if len(accounts) == 0 || len(actions) == 0 {
			return snsInvalid("AWSAccountId and ActionName are required")
		}
		principals := make([]string, len(accounts))
		for index, account := range accounts {
			if len(account) != 12 || strings.Trim(account, "0123456789") != "" {
				return snsInvalid("Invalid AWSAccountId")
			}
			principals[index] = "arn:aws:iam::" + account + ":root"
		}
		for index, value := range actions {
			if !snsKnownPermissionAction(value) {
				return snsInvalid("Invalid SNS permission action")
			}
			actions[index] = "SNS:" + value
		}
		statements = append(statements, map[string]any{"Sid": label, "Effect": "Allow", "Principal": map[string]any{"AWS": principals}, "Action": actions, "Resource": topic.ARN})
	}
	policy["Statement"] = statements
	encoded, err := json.Marshal(policy)
	if err != nil {
		return snsInternal("Cannot serialize topic policy")
	}
	topic.Attributes["Policy"] = string(encoded)
	return nil
}

func (s *Handler) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	returnARN := r.FormValue("ReturnSubscriptionArn")
	if returnARN != "" && returnARN != "true" && returnARN != "false" {
		snsWriteError(w, snsInvalid("ReturnSubscriptionArn must be true or false"))
		return
	}
	subscription, err := s.broker.subscribeSNS(r.FormValue("TopicArn"), r.FormValue("Protocol"), r.FormValue("Endpoint"), formMapValues(r, "Attributes"), snsRequestID(w))
	if errors.Is(err, errSubscriptionAttributesDiffer) {
		err = snsInvalid("Subscription already exists with different attributes")
	}
	if err != nil {
		snsWriteError(w, err)
		return
	}
	arn := subscription.ARN
	if subscription.Pending && returnARN != "true" {
		arn = "pending confirmation"
	}
	snsResponse(w, "Subscribe", "<SubscriptionArn>"+awsprotocol.XMLEscape(arn)+"</SubscriptionArn>")
}

func (s *Handler) handleSNSSubscriptionControl(w http.ResponseWriter, r *http.Request, action string) {
	if action == "ConfirmSubscription" {
		arn, token := r.FormValue("TopicArn"), r.FormValue("Token")
		if !snsTopicARNValid(arn) || token == "" {
			snsWriteError(w, snsInvalid("Valid TopicArn and Token are required"))
			return
		}
		topic := s.broker.GetTopic(arn)
		if topic == nil {
			snsWriteError(w, snsNotFound("Topic does not exist"))
			return
		}
		authenticate := r.FormValue("AuthenticateOnUnsubscribe")
		if authenticate != "" && authenticate != "true" && authenticate != "false" {
			snsWriteError(w, snsInvalid("AuthenticateOnUnsubscribe must be true or false"))
			return
		}
		topic.controlMu.Lock()
		defer topic.controlMu.Unlock()
		topic.publishMu.Lock()
		defer topic.publishMu.Unlock()
		topic.mu.Lock()
		defer topic.mu.Unlock()
		for _, subscription := range topic.Subscriptions {
			if subscription.Token == token && time.Now().Before(subscription.TokenExpires) {
				subscription.Pending = false
				subscription.Attributes["ConfirmationWasAuthenticated"] = strconv.FormatBool(authenticate == "true")
				snsResponse(w, action, "<SubscriptionArn>"+awsprotocol.XMLEscape(subscription.ARN)+"</SubscriptionArn>")
				return
			}
		}
		if subscription := topic.deletedSubscriptions[token]; subscription != nil && time.Now().Before(subscription.TokenExpires) {
			subscription.Pending = false
			subscription.Attributes["ConfirmationWasAuthenticated"] = strconv.FormatBool(authenticate == "true")
			topic.Subscriptions = append(topic.Subscriptions, subscription)
			delete(topic.deletedSubscriptions, token)
			snsResponse(w, action, "<SubscriptionArn>"+awsprotocol.XMLEscape(subscription.ARN)+"</SubscriptionArn>")
			return
		}
		snsWriteError(w, snsInvalid("Invalid or expired subscription token"))
		return
	}
	arn := r.FormValue("SubscriptionArn")
	topic, snapshot := s.broker.findSNSSubscription(arn)
	if topic == nil {
		if action == "Unsubscribe" && strings.Count(arn, ":") == 6 {
			snsResponse(w, action, "")
			return
		}
		snsWriteError(w, snsNotFound("Subscription does not exist"))
		return
	}
	topic.controlMu.Lock()
	defer topic.controlMu.Unlock()
	topic.publishMu.Lock()
	defer topic.publishMu.Unlock()
	topic.mu.Lock()
	var subscription *Subscription
	position := -1
	for index, value := range topic.Subscriptions {
		if value.ARN == arn {
			subscription = value
			position = index
			break
		}
	}
	if subscription == nil {
		topic.mu.Unlock()
		snsWriteError(w, snsNotFound("Subscription does not exist"))
		return
	}
	snapshot = cloneSNSSubscription(subscription)
	if action == "GetSubscriptionAttributes" {
		attributes := maps.Clone(subscription.Attributes)
		attributes["SubscriptionArn"] = subscription.ARN
		attributes["TopicArn"] = subscription.TopicARN
		attributes["Protocol"] = subscription.Protocol
		attributes["Endpoint"] = subscription.Endpoint
		attributes["Owner"] = s.broker.accountID
		attributes["PendingConfirmation"] = strconv.FormatBool(subscription.Pending)
		if attributes["ConfirmationWasAuthenticated"] == "" {
			attributes["ConfirmationWasAuthenticated"] = "false"
		}
		topic.mu.Unlock()
		snsResponse(w, action, "<Attributes>"+snsMapXML(attributes)+"</Attributes>")
		return
	}
	if action == "SetSubscriptionAttributes" {
		name, value := r.FormValue("AttributeName"), r.FormValue("AttributeValue")
		attributes := maps.Clone(subscription.Attributes)
		delete(attributes, "ConfirmationWasAuthenticated")
		delete(attributes, "ReplayStatus")
		attributes[name] = value
		filter, err := validateSNSSubscriptionAttributes(attributes, subscription.Protocol)
		if err == nil {
			err = s.broker.validateSNSRedrivePolicy(topic.ARN, attributes["RedrivePolicy"])
		}
		if err == nil && name == "ReplayPolicy" && topic.Attributes["FifoTopic"] != "true" {
			err = snsInvalid("ReplayPolicy requires a FIFO topic")
		}
		if err != nil {
			topic.mu.Unlock()
			snsWriteError(w, err)
			return
		}
		if name == "ReplayPolicy" {
			candidate := cloneSNSSubscription(subscription)
			candidate.Attributes = attributes
			candidate.FilterPolicy = filter
			topic.mu.Unlock()
			err = s.broker.snsReplay(topic, candidate, value, action, snsRequestID(w))
			if err != nil {
				topic.mu.Lock()
				subscription.Attributes["ReplayStatus"] = "Failed"
				topic.mu.Unlock()
				snsWriteError(w, err)
				return
			}
			topic.mu.Lock()
			_, end, _ := snsReplayPolicy(value)
			subscription.Paused = !end.IsZero()
			attributes["ReplayStatus"] = "Completed"
		} else if status := subscription.Attributes["ReplayStatus"]; status != "" {
			attributes["ReplayStatus"] = status
		}
		if confirmation := subscription.Attributes["ConfirmationWasAuthenticated"]; confirmation != "" {
			attributes["ConfirmationWasAuthenticated"] = confirmation
		}
		subscription.Attributes = attributes
		subscription.FilterPolicy = filter
		topic.mu.Unlock()
		snsResponse(w, action, "")
		return
	}
	// Control serialization lets capture happen outside the state mutex while
	// preserving a capture failure's no-delete guarantee.
	topic.mu.Unlock()
	var token string
	if snapshot.Protocol == "http" || snapshot.Protocol == "https" || snapshot.Protocol == "email" || snapshot.Protocol == "email-json" {
		token = uuid.NewString()
		message, _ := json.Marshal(map[string]string{"Type": "UnsubscribeConfirmation", "TopicArn": topic.ARN, "Token": token})
		if err := s.broker.CaptureSNS(SNSCaptureRecord{Operation: "Unsubscribe", RequestID: snsRequestID(w), TargetARN: topic.ARN, Message: string(message), Deliveries: []SNSCaptureDelivery{{Protocol: snapshot.Protocol, Endpoint: snapshot.Endpoint, Status: "captured"}}, Details: map[string]any{"token": token, "subscription_arn": arn}}); err != nil {
			snsWriteError(w, snsInternal("Unable to capture unsubscribe confirmation"))
			return
		}
	}
	topic.mu.Lock()
	if token != "" {
		if topic.deletedSubscriptions == nil {
			topic.deletedSubscriptions = make(map[string]*Subscription)
		}
		for key, deleted := range topic.deletedSubscriptions {
			if !time.Now().Before(deleted.TokenExpires) {
				delete(topic.deletedSubscriptions, key)
			}
		}
		snapshot.Token = token
		snapshot.TokenExpires = time.Now().Add(48 * time.Hour)
		topic.deletedSubscriptions[token] = snapshot
	}
	topic.Subscriptions = append(topic.Subscriptions[:position], topic.Subscriptions[position+1:]...)
	topic.mu.Unlock()
	snsResponse(w, action, "")
}

func (s *Handler) handleListSubscriptionsByTopic(w http.ResponseWriter, r *http.Request) {
	s.handleSNSListSubscriptions(w, r, "ListSubscriptionsByTopic")
}
func (s *Handler) handleSNSListSubscriptions(w http.ResponseWriter, r *http.Request, action string) {
	resource := s.broker.accountID + ":" + s.broker.region
	var subscriptions []*Subscription
	if action == "ListSubscriptionsByTopic" {
		resource = r.FormValue("TopicArn")
		if !snsTopicARNValid(resource) {
			snsWriteError(w, snsInvalid("Invalid TopicArn"))
			return
		}
		var err error
		subscriptions, err = s.broker.ListSubscriptionsByTopic(resource)
		if err != nil {
			snsWriteError(w, err)
			return
		}
	} else {
		for _, topic := range s.broker.ListTopics() {
			values, err := s.broker.ListSubscriptionsByTopic(topic.ARN)
			if err == nil {
				subscriptions = append(subscriptions, values...)
			}
		}
		sort.Slice(subscriptions, func(i, j int) bool { return subscriptions[i].ARN < subscriptions[j].ARN })
	}
	after, err := snsParsePageToken(r.FormValue("NextToken"), action, resource)
	if err != nil {
		snsWriteError(w, err)
		return
	}
	var members strings.Builder
	count := 0
	last := ""
	next := ""
	for _, subscription := range subscriptions {
		if subscription.ARN <= after {
			continue
		}
		if count == 100 {
			next = snsPageToken(action, resource, last)
			break
		}
		arn := subscription.ARN
		if subscription.Pending {
			arn = "PendingConfirmation"
		}
		fmt.Fprintf(&members, "<member><TopicArn>%s</TopicArn><Protocol>%s</Protocol><SubscriptionArn>%s</SubscriptionArn><Endpoint>%s</Endpoint><Owner>%s</Owner></member>", awsprotocol.XMLEscape(subscription.TopicARN), awsprotocol.XMLEscape(subscription.Protocol), awsprotocol.XMLEscape(arn), awsprotocol.XMLEscape(subscription.Endpoint), s.broker.accountID)
		last = subscription.ARN
		count++
	}
	result := "<Subscriptions>" + members.String() + "</Subscriptions>"
	if next != "" {
		result += "<NextToken>" + next + "</NextToken>"
	}
	snsResponse(w, action, result)
}

func snsReadPublish(r *http.Request, prefix string) (SNSPublishInput, error) {
	attrs, err := parseSNSMessageAttributes(r, prefix+"MessageAttributes")
	if err != nil {
		return SNSPublishInput{}, err
	}
	return SNSPublishInput{TopicARN: r.FormValue(prefix + "TopicArn"), TargetARN: r.FormValue(prefix + "TargetArn"), PhoneNumber: r.FormValue(prefix + "PhoneNumber"), Message: r.FormValue(prefix + "Message"), Subject: r.FormValue(prefix + "Subject"), MessageStructure: r.FormValue(prefix + "MessageStructure"), MessageGroupID: r.FormValue(prefix + "MessageGroupId"), MessageDeduplicationID: r.FormValue(prefix + "MessageDeduplicationId"), Attributes: attrs}, nil
}
func (s *Handler) handlePublish(w http.ResponseWriter, r *http.Request) {
	input, err := snsReadPublish(r, "")
	if err != nil {
		snsWriteError(w, err)
		return
	}
	input.Operation = "Publish"
	input.RequestID = snsRequestID(w)
	result, err := s.broker.PublishSNS(input)
	if err != nil {
		snsWriteError(w, err)
		return
	}
	body := "<MessageId>" + awsprotocol.XMLEscape(result.MessageID) + "</MessageId>"
	if result.SequenceNumber != "" {
		body += "<SequenceNumber>" + result.SequenceNumber + "</SequenceNumber>"
	}
	snsResponse(w, "Publish", body)
}

func (s *Handler) handleSNSPublishBatch(w http.ResponseWriter, r *http.Request) {
	topicARN := r.FormValue("TopicArn")
	if !snsTopicARNValid(topicARN) {
		snsWriteError(w, snsInvalid("Invalid TopicArn"))
		return
	}
	topic := s.broker.GetTopic(topicARN)
	if topic == nil {
		snsWriteError(w, snsNotFound("Topic does not exist"))
		return
	}
	topic.mu.RLock()
	maxSize, _ := strconv.Atoi(topic.Attributes["MaximumMessageSize"])
	topic.mu.RUnlock()
	var entries []SNSPublishInput
	var ids []string
	seen := make(map[string]bool)
	total := 0
	for index := 1; index <= 11; index++ {
		prefix := fmt.Sprintf("PublishBatchRequestEntries.member.%d.", index)
		id := r.FormValue(prefix + "Id")
		if id == "" && r.FormValue(prefix+"Message") == "" {
			break
		}
		if index == 11 {
			snsWriteError(w, &snsError{400, "TooManyEntriesInBatchRequest", "PublishBatch supports at most 10 entries"})
			return
		}
		if !snsBatchID.MatchString(id) {
			snsWriteError(w, &snsError{400, "InvalidBatchEntryId", "Batch entry Id must be 1..80 alphanumeric, hyphen or underscore characters"})
			return
		}
		if seen[id] {
			snsWriteError(w, &snsError{400, "BatchEntryIdsNotDistinct", "Batch entry IDs must be unique"})
			return
		}
		seen[id] = true
		input, err := snsReadPublish(r, prefix)
		if err != nil {
			snsWriteError(w, err)
			return
		}
		input.TopicARN = topicARN
		input.Operation = "PublishBatch"
		input.RequestID = snsRequestID(w)
		entries = append(entries, input)
		ids = append(ids, id)
		total += snsMessageSize(input)
	}
	if len(entries) == 0 {
		snsWriteError(w, &snsError{400, "EmptyBatchRequest", "PublishBatch requires entries"})
		return
	}
	if total > maxSize {
		snsWriteError(w, &snsError{400, "BatchRequestTooLong", fmt.Sprintf("Batch payload exceeds %d bytes", maxSize)})
		return
	}
	var successful, failed strings.Builder
	for index, input := range entries {
		result, err := s.broker.PublishSNS(input)
		if err != nil {
			var failure *snsError
			if !errors.As(err, &failure) {
				failure = snsInternal("Publication failed")
			}
			fmt.Fprintf(&failed, "<member><Id>%s</Id><Code>%s</Code><Message>%s</Message><SenderFault>%t</SenderFault></member>", awsprotocol.XMLEscape(ids[index]), failure.Code, awsprotocol.XMLEscape(failure.Message), failure.Status < 500)
		} else {
			fmt.Fprintf(&successful, "<member><Id>%s</Id><MessageId>%s</MessageId>", awsprotocol.XMLEscape(ids[index]), awsprotocol.XMLEscape(result.MessageID))
			if result.SequenceNumber != "" {
				fmt.Fprintf(&successful, "<SequenceNumber>%s</SequenceNumber>", result.SequenceNumber)
			}
			successful.WriteString("</member>")
		}
	}
	snsResponse(w, "PublishBatch", "<Successful>"+successful.String()+"</Successful><Failed>"+failed.String()+"</Failed>")
}
