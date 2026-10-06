package messaging

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

func snsKnownPermissionAction(action string) bool {
	if action == "*" {
		return true
	}
	for _, known := range strings.Fields("AddPermission CheckIfPhoneNumberIsOptedOut ConfirmSubscription CreatePlatformApplication CreatePlatformEndpoint CreateSMSSandboxPhoneNumber CreateTopic DeleteEndpoint DeletePlatformApplication DeleteSMSSandboxPhoneNumber DeleteTopic GetDataProtectionPolicy GetEndpointAttributes GetPlatformApplicationAttributes GetSMSAttributes GetSMSSandboxAccountStatus GetSubscriptionAttributes GetTopicAttributes ListEndpointsByPlatformApplication ListOriginationNumbers ListPhoneNumbersOptedOut ListPlatformApplications ListSMSSandboxPhoneNumbers ListSubscriptions ListSubscriptionsByTopic ListTagsForResource ListTopics OptInPhoneNumber Publish PublishBatch PutDataProtectionPolicy RemovePermission SetEndpointAttributes SetPlatformApplicationAttributes SetSMSAttributes SetSubscriptionAttributes SetTopicAttributes Subscribe TagResource Unsubscribe UntagResource VerifySMSSandboxPhoneNumber") {
		if action == known {
			return true
		}
	}
	return false
}

type snsState struct {
	mu     sync.Mutex
	sms    smsState
	mobile mobileState
}

func (b *Broker) snsState() *snsState {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sns == nil {
		b.sns = &snsState{}
	}
	return b.sns
}

type snsError struct {
	Status        int
	Code, Message string
}

func (err *snsError) Error() string { return err.Message }
func snsInvalid(message string) *snsError {
	return &snsError{Status: http.StatusBadRequest, Code: "InvalidParameter", Message: message}
}
func snsNotFound(message string) *snsError {
	return &snsError{Status: http.StatusNotFound, Code: "NotFound", Message: message}
}
func snsInternal(message string) *snsError {
	return &snsError{Status: http.StatusInternalServerError, Code: "InternalError", Message: message}
}

func snsWriteError(writer http.ResponseWriter, err error) {
	var failure *snsError
	if !errors.As(err, &failure) {
		failure = snsInternal("Internal SNS service error")
	}
	awsprotocol.XMLError(writer, failure.Status, failure.Code, failure.Message)
}

func snsRequestID(writer http.ResponseWriter) string {
	id := writer.Header().Get("X-Amzn-RequestId")
	if id == "" {
		id = awsprotocol.RequestID()
		writer.Header().Set("X-Amzn-RequestId", id)
	}
	return id
}

func snsResponse(writer http.ResponseWriter, operation, result string) {
	requestID := snsRequestID(writer)
	// Tag/Untag have modeled empty output structures: SDKs still require Result.
	if result != "" || operation == "TagResource" || operation == "UntagResource" {
		result = "<" + operation + "Result>" + result + "</" + operation + "Result>"
	}
	awsprotocol.XMLResponse(writer, http.StatusOK, "<"+operation+"Response xmlns=\"http://sns.amazonaws.com/doc/2010-03-31/\">"+result+"<ResponseMetadata><RequestId>"+requestID+"</RequestId></ResponseMetadata></"+operation+"Response>")
}

func snsMapXML(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var output strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&output, "<entry><key>%s</key><value>%s</value></entry>", awsprotocol.XMLEscape(key), awsprotocol.XMLEscape(values[key]))
	}
	return output.String()
}

type SNSPublishInput struct {
	Operation, RequestID                                                 string
	TopicARN, TargetARN, PhoneNumber, Message, Subject, MessageStructure string
	MessageGroupID, MessageDeduplicationID                               string
	Attributes                                                           map[string]MessageAttribute
}

type SNSPublishResult struct{ MessageID, SequenceNumber string }

var snsTopicName = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:\.fifo)?$`)
var snsAttributeName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)
var snsBatchID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
var snsNumber = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

func snsTopicARNValid(arn string) bool {
	parts := strings.Split(arn, ":")
	return len(parts) == 6 && parts[0] == "arn" && strings.HasPrefix(parts[1], "aws") && parts[2] == "sns" && parts[3] != "" && len(parts[4]) == 12 && strings.Trim(parts[4], "0123456789") == "" && len(parts[5]) <= 256 && snsTopicName.MatchString(parts[5])
}

func snsIdentifierValid(value string) bool {
	if len(value) < 1 || len(value) > 128 {
		return false
	}
	for _, character := range value {
		if character < 33 || character > 126 {
			return false
		}
	}
	return true
}

func snsMessageSize(input SNSPublishInput) int {
	size := len(input.Message)
	for name, attribute := range input.Attributes {
		size += len(name) + len(attribute.DataType) + len(attribute.StringValue) + len(attribute.BinaryValue)
	}
	return size
}

func validateSNSPublish(input SNSPublishInput, maxSize int) error {
	if input.Message == "" || !utf8.ValidString(input.Message) {
		return snsInvalid("Message must be nonempty UTF-8 text")
	}
	if snsMessageSize(input) > maxSize {
		return snsInvalid("Message exceeds the topic's MaximumMessageSize")
	}
	if utf8.RuneCountInString(input.Subject) >= 100 || !utf8.ValidString(input.Subject) {
		return snsInvalid("Subject must contain fewer than 100 UTF-8 characters")
	}
	for _, character := range input.Subject {
		if unicode.IsControl(character) {
			return snsInvalid("Subject must not contain control characters")
		}
	}
	if input.MessageStructure != "" && input.MessageStructure != "json" {
		return snsInvalid("MessageStructure must be json when supplied")
	}
	if input.MessageStructure == "json" {
		if _, err := snsProtocolMessages(input.Message); err != nil {
			return err
		}
	}
	if input.MessageGroupID != "" && !snsIdentifierValid(input.MessageGroupID) {
		return snsInvalid("Invalid MessageGroupId")
	}
	if input.MessageDeduplicationID != "" && !snsIdentifierValid(input.MessageDeduplicationID) {
		return snsInvalid("Invalid MessageDeduplicationId")
	}
	for name, attribute := range input.Attributes {
		lower := strings.ToLower(name)
		reserved := strings.HasPrefix(lower, "aws.") || strings.HasPrefix(lower, "amazon.")
		mobileReserved := strings.HasPrefix(name, "AWS.SNS.MOBILE.")
		smsReserved := name == "AWS.SNS.SMS.SenderID" || name == "AWS.SNS.SMS.SMSType" || name == "AWS.SNS.SMS.MaxPrice" || name == "AWS.MM.SMS.OriginationNumber" || name == "AWS.MM.SMS.EntityId" || name == "AWS.MM.SMS.TemplateId"
		if !snsAttributeName.MatchString(name) || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") || (reserved && !mobileReserved && !smsReserved) {
			return snsInvalid("Invalid message attribute name: " + name)
		}
		logicalType := strings.SplitN(attribute.DataType, ".", 2)[0]
		if attribute.DataType == "String.Array" {
			logicalType = "String.Array"
		}
		switch logicalType {
		case "String":
			if attribute.StringValue == "" || !utf8.ValidString(attribute.StringValue) || len(attribute.BinaryValue) > 0 {
				return snsInvalid("Invalid String message attribute: " + name)
			}
		case "String.Array":
			var values []any
			if json.Unmarshal([]byte(attribute.StringValue), &values) != nil || values == nil || len(attribute.BinaryValue) > 0 {
				return snsInvalid("Invalid String.Array message attribute: " + name)
			}
			for _, value := range values {
				switch value.(type) {
				case string, float64, bool, nil:
				default:
					return snsInvalid("Invalid String.Array message attribute: " + name)
				}
			}
		case "Number":
			if len(attribute.StringValue) > 100 || !snsNumber.MatchString(attribute.StringValue) || len(attribute.BinaryValue) > 0 {
				return snsInvalid("Invalid Number message attribute: " + name)
			}
			// Bound exponent magnitude before big.Rat to prevent an invalid
			// decimal string from allocating arbitrarily large integers.
			if position := strings.IndexAny(attribute.StringValue, "eE"); position >= 0 {
				exponent := strings.TrimLeft(attribute.StringValue[position+1:], "+-0")
				if len(exponent) > 2 {
					return snsInvalid("Invalid Number message attribute: " + name)
				}
			}
			value, ok := new(big.Rat).SetString(attribute.StringValue)
			if !ok || new(big.Rat).Abs(value).Cmp(big.NewRat(1000000000, 1)) > 0 {
				return snsInvalid("Invalid Number message attribute: " + name)
			}
			if !new(big.Rat).Mul(value, big.NewRat(100000, 1)).IsInt() {
				return snsInvalid("Number message attributes support five decimal places")
			}
		case "Binary":
			if len(attribute.BinaryValue) == 0 || attribute.StringValue != "" {
				return snsInvalid("Invalid Binary message attribute: " + name)
			}
		default:
			return snsInvalid("Invalid message attribute data type: " + attribute.DataType)
		}
	}
	return nil
}

// Decode a protocol-specific message without silently accepting duplicate keys.
func snsProtocolMessages(message string) (map[string]string, error) {
	decoder := json.NewDecoder(strings.NewReader(message))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil, snsInvalid("MessageStructure json requires a JSON object")
	}
	values := make(map[string]string)
	seen := make(map[string]bool)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil, snsInvalid("Invalid protocol message JSON")
		}
		key, ok := keyToken.(string)
		if !ok || seen[key] {
			return nil, snsInvalid("Duplicate protocol message key")
		}
		seen[key] = true
		var value any
		if decoder.Decode(&value) != nil {
			return nil, snsInvalid("Invalid protocol message JSON")
		}
		if text, ok := value.(string); ok {
			values[key] = text
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, snsInvalid("Invalid protocol message JSON")
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return nil, snsInvalid("Protocol message JSON must contain one object")
	}
	if _, exists := values["default"]; !exists {
		return nil, snsInvalid("MessageStructure json requires a default string")
	}
	return values, nil
}

func parseSNSMessageAttributes(request *http.Request, prefix string) (map[string]MessageAttribute, error) {
	if prefix == "" {
		prefix = "MessageAttributes"
	}
	attributes := make(map[string]MessageAttribute)
	for index := 1; index <= 10000; index++ {
		base := fmt.Sprintf("%s.entry.%d.", prefix, index)
		name := request.FormValue(base + "Name")
		if name == "" {
			break
		}
		if _, exists := attributes[name]; exists {
			return nil, snsInvalid("Message attribute names must be unique")
		}
		attribute := MessageAttribute{DataType: request.FormValue(base + "Value.DataType"), StringValue: request.FormValue(base + "Value.StringValue")}
		if raw := request.FormValue(base + "Value.BinaryValue"); raw != "" {
			value, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return nil, snsInvalid("Invalid base64 BinaryValue")
			}
			attribute.BinaryValue = value
		}
		attributes[name] = attribute
	}
	return attributes, nil
}

type snsCursor struct {
	Operation string `json:"operation"`
	Resource  string `json:"resource"`
	After     string `json:"after"`
}

func snsPageToken(operation, resource, after string) string {
	data, _ := json.Marshal(snsCursor{operation, resource, after})
	return base64.RawURLEncoding.EncodeToString(data)
}
func snsParsePageToken(raw, operation, resource string) (string, error) {
	if raw == "" {
		return "", nil
	}
	encoded, err := base64.RawURLEncoding.DecodeString(raw)
	var cursor snsCursor
	if err != nil || json.Unmarshal(encoded, &cursor) != nil || cursor.Operation != operation || cursor.Resource != resource || cursor.After == "" {
		return "", snsInvalid("Invalid NextToken")
	}
	return cursor.After, nil
}
