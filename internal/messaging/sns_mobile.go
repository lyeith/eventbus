package messaging

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
)

// mobileState belongs to SNS and is protected by snsState.mu. Credentials are
// retained only in application state, never in Get/List responses or captures.
type mobileState struct {
	applications map[string]*mobileApplication
	endpoints    map[string]*mobileEndpoint
}

type mobileApplication struct {
	arn, name, platform string
	attributes          map[string]string
}

type mobileEndpoint struct {
	arn, applicationARN string
	attributes          map[string]string
}

var mobileNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)

func (state *mobileState) initialize() {
	if state.applications == nil {
		state.applications = make(map[string]*mobileApplication)
	}
	if state.endpoints == nil {
		state.endpoints = make(map[string]*mobileEndpoint)
	}
}

func mobilePlatformSupported(platform string) bool {
	switch platform {
	case "ADM", "APNS", "APNS_SANDBOX", "GCM", "BAIDU", "MPNS", "WNS":
		return true
	}
	return false
}

func mobileCopyAttributes(attributes map[string]string) map[string]string {
	copy := make(map[string]string, len(attributes))
	for key, value := range attributes {
		copy[key] = value
	}
	return copy
}

func mobilePublicApplicationAttributes(application *mobileApplication) map[string]string {
	attributes := mobileCopyAttributes(application.attributes)
	delete(attributes, "PlatformPrincipal")
	delete(attributes, "PlatformCredential")
	attributes["AllowEndpointPolicies"] = "false"
	switch {
	case strings.HasPrefix(application.platform, "APNS"):
		attributes["AuthenticationMethod"] = "Certificate"
		if application.attributes["ApplePlatformTeamID"] != "" {
			attributes["AuthenticationMethod"] = "Token"
		}
	case application.platform == "GCM":
		attributes["AuthenticationMethod"] = "Key"
		var credentials map[string]json.RawMessage
		if json.Unmarshal([]byte(application.attributes["PlatformCredential"]), &credentials) == nil && credentials != nil {
			attributes["AuthenticationMethod"] = "Token"
		}
	}
	return attributes
}

func (b *Broker) mobileValidateARN(arn, kind string) error {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "sns" || parts[3] != b.region || parts[4] != b.accountID {
		return snsInvalid("Invalid parameter: ARN must identify an SNS resource in this region and account")
	}
	resource := strings.Split(parts[5], "/")
	want := 3
	if kind == "endpoint" {
		want = 4
	}
	if len(resource) != want || resource[0] != kind || !mobilePlatformSupported(resource[1]) || !mobileNamePattern.MatchString(resource[2]) {
		return snsInvalid("Invalid parameter: invalid " + kind + " ARN")
	}
	if kind == "endpoint" {
		if _, err := uuid.Parse(resource[3]); err != nil {
			return snsInvalid("Invalid parameter: invalid endpoint ARN")
		}
	}
	return nil
}

func (b *Broker) mobileValidateApplicationAttributes(platform string, attributes map[string]string, requireCredentials bool) error {
	if len(attributes) == 0 {
		return snsInvalid("Attributes must not be empty")
	}
	for key, value := range attributes {
		if !utf8.ValidString(value) {
			return snsInvalid("Invalid parameter: " + key + " must be UTF-8")
		}
		switch key {
		case "PlatformCredential", "PlatformPrincipal":
			if value == "" {
				return snsInvalid("Invalid parameter: " + key + " must not be empty")
			}
		case "ApplePlatformTeamID", "ApplePlatformBundleID":
			if !strings.HasPrefix(platform, "APNS") {
				return snsInvalid("Invalid parameter: " + key + " is only valid for APNS")
			}
		case "EventEndpointCreated", "EventEndpointDeleted", "EventEndpointUpdated", "EventDeliveryFailure", "EventDeliveryAttemptFailure":
			// The empty string removes a configured event topic.
			if value != "" {
				if !snsTopicARNValid(value) || b.GetTopic(value) == nil || strings.HasSuffix(value, ".fifo") {
					return snsInvalid("Invalid parameter: " + key + " must reference an existing standard SNS topic")
				}
			}
		case "SuccessFeedbackRoleArn", "FailureFeedbackRoleArn":
			if value != "" {
				parts := strings.SplitN(value, ":", 6)
				if len(parts) != 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "iam" || parts[3] != "" || parts[4] != b.accountID || !strings.HasPrefix(parts[5], "role/") || len(parts[5]) == 5 {
					return snsInvalid("Invalid parameter: " + key + " must be an IAM role ARN")
				}
			}
		case "SuccessFeedbackSampleRate":
			rate, err := strconv.Atoi(value)
			if err != nil || rate < 0 || rate > 100 {
				return snsInvalid("Invalid parameter: SuccessFeedbackSampleRate must be between 0 and 100")
			}
		default:
			return snsInvalid("Invalid parameter: unsupported platform application attribute " + key)
		}
	}
	if requireCredentials {
		if attributes["PlatformCredential"] == "" {
			return snsInvalid("Invalid parameter: PlatformCredential is required")
		}
		if platform != "GCM" && attributes["PlatformPrincipal"] == "" {
			return snsInvalid("Invalid parameter: PlatformPrincipal is required")
		}
	}
	return nil
}

func mobileNormalizeToken(platform, token string) string {
	if strings.HasPrefix(platform, "APNS") {
		return strings.ToLower(token)
	}
	return token
}

func mobileValidateEndpointAttributes(platform string, attributes map[string]string) error {
	for key, value := range attributes {
		if !utf8.ValidString(value) {
			return snsInvalid("Invalid parameter: " + key + " must be UTF-8")
		}
		switch key {
		case "CustomUserData":
			if len(value) >= 2048 {
				return snsInvalid("Invalid parameter: CustomUserData must be less than 2048 bytes")
			}
		case "Enabled":
			if value != "true" && value != "false" {
				return snsInvalid("Invalid parameter: Enabled must be true or false")
			}
		case "Token":
			if value == "" {
				return snsInvalid("Invalid parameter: Token must not be empty")
			}
		case "ChannelId", "UserId":
			if platform != "BAIDU" || value == "" {
				return snsInvalid("Invalid parameter: " + key + " is only valid for BAIDU and must not be empty")
			}
		default:
			return snsInvalid("Invalid parameter: unsupported endpoint attribute " + key)
		}
	}
	return nil
}

func (b *Broker) mobileCreateApplication(name, platform string, attributes map[string]string) (string, error) {
	if !mobileNamePattern.MatchString(name) {
		return "", snsInvalid("Invalid parameter: Name must be 1 to 256 ASCII letters, numbers, underscores, hyphens or periods")
	}
	if !mobilePlatformSupported(platform) {
		return "", snsInvalid("Invalid parameter: Platform is not supported")
	}
	if err := b.mobileValidateApplicationAttributes(platform, attributes, true); err != nil {
		return "", err
	}
	arn := fmt.Sprintf("arn:aws:sns:%s:%s:app/%s/%s", b.region, b.accountID, platform, name)
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	state.mobile.initialize()
	if existing := state.mobile.applications[arn]; existing != nil {
		if len(existing.attributes) != len(attributes) {
			return "", snsInvalid("Platform application already exists with different attributes")
		}
		for key, value := range attributes {
			if existing.attributes[key] != value {
				return "", snsInvalid("Platform application already exists with different attributes")
			}
		}
		return arn, nil
	}
	state.mobile.applications[arn] = &mobileApplication{arn: arn, name: name, platform: platform, attributes: mobileCopyAttributes(attributes)}
	return arn, nil
}

func (b *Broker) mobileCreateEndpoint(applicationARN, token string, customUserData *string, attributes map[string]string) (string, error) {
	if err := b.mobileValidateARN(applicationARN, "app"); err != nil {
		return "", err
	}
	state := b.snsState()
	state.mu.Lock()
	var event *SNSPublishInput
	defer func() { state.mu.Unlock(); b.mobileDispatchEvent(event) }()
	state.mobile.initialize()
	application := state.mobile.applications[applicationARN]
	if application == nil {
		return "", snsNotFound("Platform application does not exist")
	}
	token = mobileNormalizeToken(application.platform, token)
	requested := mobileCopyAttributes(attributes)
	if value, exists := requested["Token"]; exists && mobileNormalizeToken(application.platform, value) != token {
		return "", snsInvalid("Invalid parameter: Token and Attributes.Token disagree")
	}
	requested["Token"] = token
	if customUserData != nil {
		if value, exists := requested["CustomUserData"]; exists && value != *customUserData {
			return "", snsInvalid("Invalid parameter: CustomUserData values disagree")
		}
		requested["CustomUserData"] = *customUserData
	}
	if _, exists := requested["CustomUserData"]; !exists {
		requested["CustomUserData"] = ""
	}
	if err := mobileValidateEndpointAttributes(application.platform, requested); err != nil {
		return "", err
	}
	if application.platform == "BAIDU" && (requested["ChannelId"] == "" || requested["UserId"] == "" || requested["ChannelId"] != token) {
		return "", snsInvalid("Invalid parameter: BAIDU endpoints require ChannelId and UserId; Token must equal ChannelId")
	}
	for _, endpoint := range state.mobile.endpoints {
		if endpoint.applicationARN != applicationARN || endpoint.attributes["Token"] != token {
			continue
		}
		for key, value := range requested {
			if endpoint.attributes[key] != value {
				return "", snsInvalid("Invalid parameter: Token Reason: Endpoint " + endpoint.arn + " already exists with the same Token, but different attributes")
			}
		}
		// Creating an existing endpoint does not implicitly re-enable it.
		return endpoint.arn, nil
	}
	if _, exists := requested["Enabled"]; !exists {
		requested["Enabled"] = "true"
	}
	if _, exists := requested["CustomUserData"]; !exists {
		requested["CustomUserData"] = ""
	}
	arn := fmt.Sprintf("arn:aws:sns:%s:%s:endpoint/%s/%s/%s", b.region, b.accountID, application.platform, application.name, uuid.NewString())
	endpoint := &mobileEndpoint{arn: arn, applicationARN: applicationARN, attributes: requested}
	var err error
	event, err = b.mobilePrepareEvent(application, endpoint, "EndpointCreated")
	if err != nil {
		return "", err
	}
	state.mobile.endpoints[arn] = endpoint
	return arn, nil
}

func (b *Broker) mobileDeleteEndpoint(arn string) error {
	if err := b.mobileValidateARN(arn, "endpoint"); err != nil {
		return err
	}
	state := b.snsState()
	state.mu.Lock()
	var event *SNSPublishInput
	defer func() { state.mu.Unlock(); b.mobileDispatchEvent(event) }()
	if endpoint := state.mobile.endpoints[arn]; endpoint != nil {
		if application := state.mobile.applications[endpoint.applicationARN]; application != nil {
			var err error
			event, err = b.mobilePrepareEvent(application, endpoint, "EndpointDeleted")
			if err != nil {
				return err
			}
		}
	}
	delete(state.mobile.endpoints, arn)
	// AWS retains any topic subscriptions; callers must unsubscribe separately.
	return nil
}

func (b *Broker) mobileDeleteApplication(arn string) error {
	if err := b.mobileValidateARN(arn, "app"); err != nil {
		return err
	}
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.mobile.applications, arn)
	for endpointARN, endpoint := range state.mobile.endpoints {
		if endpoint.applicationARN == arn {
			delete(state.mobile.endpoints, endpointARN)
		}
	}
	return nil
}

func (b *Broker) mobileGetEndpointAttributes(arn string) (map[string]string, error) {
	if err := b.mobileValidateARN(arn, "endpoint"); err != nil {
		return nil, err
	}
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	endpoint := state.mobile.endpoints[arn]
	if endpoint == nil {
		return nil, snsNotFound("Endpoint does not exist")
	}
	return mobileCopyAttributes(endpoint.attributes), nil
}

func (b *Broker) mobileGetApplicationAttributes(arn string) (map[string]string, error) {
	if err := b.mobileValidateARN(arn, "app"); err != nil {
		return nil, err
	}
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	application := state.mobile.applications[arn]
	if application == nil {
		return nil, snsNotFound("Platform application does not exist")
	}
	return mobilePublicApplicationAttributes(application), nil
}

func (b *Broker) mobileSetEndpointAttributes(arn string, attributes map[string]string) error {
	if err := b.mobileValidateARN(arn, "endpoint"); err != nil {
		return err
	}
	if len(attributes) == 0 {
		return snsInvalid("Attributes must not be empty")
	}
	state := b.snsState()
	state.mu.Lock()
	var event *SNSPublishInput
	defer func() { state.mu.Unlock(); b.mobileDispatchEvent(event) }()
	endpoint := state.mobile.endpoints[arn]
	if endpoint == nil {
		return snsNotFound("Endpoint does not exist")
	}
	application := state.mobile.applications[endpoint.applicationARN]
	if application == nil {
		return snsNotFound("Platform application does not exist")
	}
	attributes = mobileCopyAttributes(attributes)
	if token, exists := attributes["Token"]; exists {
		attributes["Token"] = mobileNormalizeToken(application.platform, token)
	}
	if err := mobileValidateEndpointAttributes(application.platform, attributes); err != nil {
		return err
	}
	if token, exists := attributes["Token"]; exists {
		for _, existing := range state.mobile.endpoints {
			if existing.arn != arn && existing.applicationARN == endpoint.applicationARN && existing.attributes["Token"] == token {
				return snsInvalid("Invalid parameter: Token is already registered to a different endpoint")
			}
		}
	}
	updated := mobileCopyAttributes(endpoint.attributes)
	for key, value := range attributes {
		updated[key] = value
	}
	if application.platform == "BAIDU" && (updated["ChannelId"] == "" || updated["UserId"] == "" || updated["ChannelId"] != updated["Token"]) {
		return snsInvalid("Invalid parameter: BAIDU Token must equal ChannelId")
	}
	changed := false
	for key, value := range attributes {
		if endpoint.attributes[key] != value {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}
	updatedEndpoint := &mobileEndpoint{arn: endpoint.arn, applicationARN: endpoint.applicationARN, attributes: updated}
	var err error
	event, err = b.mobilePrepareEvent(application, updatedEndpoint, "EndpointUpdated")
	if err != nil {
		return err
	}
	endpoint.attributes = updated
	return nil
}

func (b *Broker) mobileSetApplicationAttributes(arn string, attributes map[string]string) error {
	if err := b.mobileValidateARN(arn, "app"); err != nil {
		return err
	}
	// Validate topic references before taking SNS state's lock: the topic owner
	// may itself need snsState.mu while publishing to an application endpoint.
	platform := strings.Split(strings.SplitN(arn, ":", 6)[5], "/")[1]
	if err := b.mobileValidateApplicationAttributes(platform, attributes, false); err != nil {
		return err
	}
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	application := state.mobile.applications[arn]
	if application == nil {
		return snsNotFound("Platform application does not exist")
	}
	for key, value := range attributes {
		if value == "" {
			delete(application.attributes, key)
		} else {
			application.attributes[key] = value
		}
	}
	return nil
}

func (b *Broker) validateSNSApplicationEndpoint(arn string) error {
	_, err := b.mobileGetEndpointAttributes(arn)
	return err
}

// publishMobile captures intent synchronously. State remains locked through the
// append so a concurrent disable or delete cannot invalidate an accepted send.
func (b *Broker) publishMobile(input SNSPublishInput) (SNSPublishResult, error) {
	if err := b.mobileValidateARN(input.TargetARN, "endpoint"); err != nil {
		return SNSPublishResult{}, err
	}
	state := b.snsState()
	state.mu.Lock()
	defer state.mu.Unlock()
	endpoint := state.mobile.endpoints[input.TargetARN]
	if endpoint == nil {
		return SNSPublishResult{}, snsNotFound("Endpoint does not exist")
	}
	application := state.mobile.applications[endpoint.applicationARN]
	if application == nil {
		return SNSPublishResult{}, snsNotFound("Platform application does not exist")
	}
	if application.attributes["PlatformCredential"] == "" {
		return SNSPublishResult{}, &snsError{Status: http.StatusBadRequest, Code: "PlatformApplicationDisabled", Message: "Platform application is disabled"}
	}
	if endpoint.attributes["Enabled"] != "true" {
		return SNSPublishResult{}, &snsError{Status: http.StatusBadRequest, Code: "EndpointDisabled", Message: "Endpoint is disabled"}
	}
	resolvedMessage := input.Message
	if input.MessageStructure == "json" {
		messages, err := snsProtocolMessages(input.Message)
		if err != nil {
			return SNSPublishResult{}, err
		}
		resolvedMessage = messages["default"]
		if value, exists := messages[application.platform]; exists {
			resolvedMessage = value
		}
	}
	operation := input.Operation
	if operation == "" {
		operation = "Publish"
	}
	messageID := uuid.NewString()
	err := b.CaptureSNS(SNSCaptureRecord{
		Operation: operation, RequestID: input.RequestID, MessageID: messageID,
		TargetARN: input.TargetARN, Subject: input.Subject, Message: input.Message,
		MessageStructure: input.MessageStructure, MessageAttributes: input.Attributes,
		MessageGroupID: input.MessageGroupID, MessageDeduplicationID: input.MessageDeduplicationID,
		Deliveries: []SNSCaptureDelivery{{Protocol: "application", Endpoint: input.TargetARN, Status: "captured", MessageID: messageID}},
		Details:    map[string]any{"platform": application.platform, "resolved_message": resolvedMessage},
	})
	if err != nil {
		return SNSPublishResult{}, &snsError{Status: http.StatusInternalServerError, Code: "InternalError", Message: "Failed to capture SNS message: " + err.Error()}
	}
	return SNSPublishResult{MessageID: messageID}, nil
}

// mobilePage uses ARN cursors rather than offsets, so deletions before a later
// page do not cause remaining records to be skipped. Tokens bind to list scope.
func mobilePage(keys []string, nextToken, scope string) ([]string, string, error) {
	sort.Strings(keys)
	start := 0
	if nextToken != "" {
		after, err := snsParsePageToken(nextToken, scope, "")
		if err != nil {
			return nil, "", err
		}
		start = sort.Search(len(keys), func(index int) bool { return keys[index] > after })
	}
	end := start + 100
	if end >= len(keys) {
		return keys[start:], "", nil
	}
	return keys[start:end], snsPageToken(scope, "", keys[end-1]), nil
}
