package ses

import (
	"context"
	"net/http"
	"slices"
	"sort"
	"strings"
)

// EventPublisher is the SES-owned SNS admission port. Composition supplies the
// actual SNS core; SES never constructs envelopes, executes handlers or retries.
type EventPublisher interface {
	ValidateTopic(topicARN string) error
	PublishEvent(ctx context.Context, topicARN, message, requestID string) error
}

type ManagerOption func(*SESManager)

// WithEventPublisher binds publishing and the native mail identity at startup.
func WithEventPublisher(publisher EventPublisher, region, accountID string) ManagerOption {
	return func(manager *SESManager) {
		manager.eventPublisher = publisher
		manager.region, manager.accountID = region, accountID
	}
}

type configurationSet struct {
	name         string
	destinations map[string]*eventDestination
}

type eventDestination struct {
	name          string
	enabled       bool
	matchingTypes []string
	topicARN      string
}

func sesV1ConfigurationAction(action string) bool {
	switch action {
	case "CreateConfigurationSet", "CreateConfigurationSetEventDestination", "UpdateConfigurationSetEventDestination",
		"DeleteConfigurationSetEventDestination", "DescribeConfigurationSet", "DeleteConfigurationSet":
		return true
	}
	return false
}

func sesConfigurationNameValid(name string) bool {
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for _, character := range name {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '_' || character == '-') {
			return false
		}
	}
	return true
}

func sesConfigurationError(code, message, name, destination string) *sesAPIError {
	fields := map[string]any{}
	if name != "" {
		fields["ConfigurationSetName"] = name
	}
	if destination != "" {
		fields["EventDestinationName"] = destination
	}
	return &sesAPIError{Code: code, Message: message, Status: http.StatusBadRequest, Fields: fields}
}

func missingConfigurationSet(name string) *sesAPIError {
	return sesConfigurationError("ConfigurationSetDoesNotExist", "Configuration set does not exist: "+name, name, "")
}

func missingEventDestination(name, destination string) *sesAPIError {
	return sesConfigurationError("EventDestinationDoesNotExist", "Event destination does not exist: "+destination, name, destination)
}

// configurationV1 commits native resource mutations independently of the email
// capture sink: management is not an email-send request and does not assert SMTP.
func (manager *SESManager) configurationV1(action string, input map[string]any) (map[string]any, *sesAPIError) {
	name := sesString(input["ConfigurationSetName"])
	if action == "CreateConfigurationSet" {
		object := sesObject(input["ConfigurationSet"])
		name = sesString(object["Name"])
		if !sesConfigurationNameValid(name) {
			return nil, sesConfigurationError("InvalidConfigurationSet", "ConfigurationSet.Name must contain 1 to 64 ASCII letters, numbers, underscores or dashes", name, "")
		}
		manager.mu.Lock()
		defer manager.mu.Unlock()
		if manager.configurationSets[name] != nil {
			return nil, sesConfigurationError("ConfigurationSetAlreadyExists", "Configuration set already exists: "+name, name, "")
		}
		manager.configurationSets[name] = &configurationSet{name: name, destinations: make(map[string]*eventDestination)}
		return map[string]any{}, nil
	}
	if _, ok := input["ConfigurationSetName"].(string); !ok || name == "" {
		return nil, sesInvalid("v1", "ConfigurationSetName is required and must be a string")
	}
	manager.mu.Lock()
	configuration := manager.configurationSets[name]
	if configuration == nil {
		manager.mu.Unlock()
		return nil, missingConfigurationSet(name)
	}
	switch action {
	case "DeleteConfigurationSet":
		delete(manager.configurationSets, name)
		manager.mu.Unlock()
		return map[string]any{}, nil
	case "DescribeConfigurationSet":
		attributes := sesStrings(input["ConfigurationSetAttributeNames"])
		if err := sesV1StringList(input["ConfigurationSetAttributeNames"], "ConfigurationSetAttributeNames"); err != nil {
			manager.mu.Unlock()
			return nil, err
		}
		for _, attribute := range attributes {
			if !slices.Contains([]string{"eventDestinations", "trackingOptions", "deliveryOptions", "reputationOptions"}, attribute) {
				manager.mu.Unlock()
				return nil, sesInvalid("v1", "Invalid configuration set attribute: "+attribute)
			}
		}
		output := map[string]any{"ConfigurationSet": map[string]any{"Name": name}}
		if slices.Contains(attributes, "eventDestinations") {
			names := make([]string, 0, len(configuration.destinations))
			for destination := range configuration.destinations {
				names = append(names, destination)
			}
			sort.Strings(names)
			destinations := make([]map[string]any, 0, len(names))
			for _, name := range names {
				destinations = append(destinations, configuration.destinations[name].wire())
			}
			output["EventDestinations"] = destinations
		}
		manager.mu.Unlock()
		return output, nil
	case "DeleteConfigurationSetEventDestination":
		destination, ok := input["EventDestinationName"].(string)
		if !ok || destination == "" {
			manager.mu.Unlock()
			return nil, sesInvalid("v1", "EventDestinationName is required and must be a string")
		}
		if configuration.destinations[destination] == nil {
			manager.mu.Unlock()
			return nil, missingEventDestination(name, destination)
		}
		delete(configuration.destinations, destination)
		manager.mu.Unlock()
		return map[string]any{}, nil
	}
	// Validate the external resource outside our mutex. Recheck the exact native
	// generation before committing so a delete/recreate cannot receive this write.
	originalDestination := configuration.destinations[sesString(sesObject(input["EventDestination"])["Name"])]
	manager.mu.Unlock()
	destination, apiErr := manager.parseEventDestination(name, input["EventDestination"])
	if apiErr != nil {
		return nil, apiErr
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.configurationSets[name] != configuration {
		return nil, missingConfigurationSet(name)
	}
	current := configuration.destinations[destination.name]
	if action == "CreateConfigurationSetEventDestination" {
		if current != nil {
			return nil, sesConfigurationError("EventDestinationAlreadyExists", "Event destination already exists: "+destination.name, name, destination.name)
		}
	} else {
		if originalDestination == nil || current != originalDestination {
			return nil, missingEventDestination(name, destination.name)
		}
	}
	if action == "CreateConfigurationSetEventDestination" {
		configuration.destinations[destination.name] = destination
	} else {
		// The entity generation survives ordinary updates. Concurrent valid
		// updates serialize here; only delete/recreate changes its identity.
		*current = *destination
	}
	return map[string]any{}, nil
}

func (destination *eventDestination) wire() map[string]any {
	types := make([]any, len(destination.matchingTypes))
	for index, eventType := range destination.matchingTypes {
		types[index] = eventType
	}
	return map[string]any{"Name": destination.name, "Enabled": destination.enabled,
		"MatchingEventTypes": types, "SNSDestination": map[string]any{"TopicARN": destination.topicARN}}
}

func (manager *SESManager) parseEventDestination(configurationName string, value any) (*eventDestination, *sesAPIError) {
	object := sesObject(value)
	if object == nil {
		return nil, sesInvalid("v1", "EventDestination is required and must be an object")
	}
	name := sesString(object["Name"])
	if !sesConfigurationNameValid(name) {
		return nil, sesInvalid("v1", "EventDestination.Name must contain 1 to 64 ASCII letters, numbers, underscores or dashes")
	}
	enabled := false
	if value, present := object["Enabled"]; present {
		switch value {
		case "true", true:
			enabled = true
		case "false", false:
			enabled = false
		default:
			return nil, sesInvalid("v1", "EventDestination.Enabled must be a boolean")
		}
	}
	if err := sesV1StringList(object["MatchingEventTypes"], "EventDestination.MatchingEventTypes"); err != nil {
		return nil, err
	}
	types := sesStrings(object["MatchingEventTypes"])
	if len(types) == 0 {
		return nil, sesInvalid("v1", "EventDestination.MatchingEventTypes must not be empty")
	}
	for _, eventType := range types {
		if !slices.Contains([]string{"send", "reject", "bounce", "complaint", "delivery", "open", "click", "renderingFailure"}, eventType) {
			return nil, sesInvalid("v1", "Invalid matching event type: "+eventType)
		}
	}
	destinationCount := 0
	for _, key := range []string{"SNSDestination", "CloudWatchDestination", "KinesisFirehoseDestination"} {
		if _, present := object[key]; present {
			destinationCount++
		}
	}
	if destinationCount != 1 {
		return nil, sesInvalid("v1", "Specify exactly one event destination")
	}
	if _, present := object["CloudWatchDestination"]; present {
		return nil, sesConfigurationError("InvalidCloudWatchDestination", "CloudWatch event destinations are not implemented", configurationName, name)
	}
	if _, present := object["KinesisFirehoseDestination"]; present {
		return nil, sesConfigurationError("InvalidFirehoseDestination", "Firehose event destinations are not implemented", configurationName, name)
	}
	topicARN := sesString(sesObject(object["SNSDestination"])["TopicARN"])
	parts := strings.SplitN(topicARN, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || !slices.Contains([]string{"aws", "aws-cn", "aws-us-gov"}, parts[1]) ||
		parts[2] != "sns" || parts[3] != manager.region || parts[4] != manager.accountID || parts[5] == "" ||
		strings.ContainsAny(parts[5], ":/") || strings.HasSuffix(parts[5], ".fifo") || manager.eventPublisher == nil {
		return nil, sesConfigurationError("InvalidSNSDestination", "SNSDestination.TopicARN must identify an available standard SNS topic in this account and region", configurationName, name)
	}
	if err := manager.eventPublisher.ValidateTopic(topicARN); err != nil {
		return nil, sesConfigurationError("InvalidSNSDestination", "Invalid SNS destination: "+err.Error(), configurationName, name)
	}
	return &eventDestination{name: name, enabled: enabled, matchingTypes: slices.Clone(types), topicARN: topicARN}, nil
}
