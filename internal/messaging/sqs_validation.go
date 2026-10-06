package messaging

import (
	"encoding/json"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var queueNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(?:\.fifo)?$`)
var attributeNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
var batchIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,80}$`)
var numericAttributePattern = regexp.MustCompile(`^([+-]?)([0-9]+)(?:\.([0-9]+))?(?:[eE]([+-]?[0-9]+))?$`)
var traceRootPattern = regexp.MustCompile(`^1-[0-9a-fA-F]{8}-[0-9a-fA-F]{24}$`)
var traceParentPattern = regexp.MustCompile(`^[0-9a-fA-F]{16}$`)

type sqsRedrivePolicy struct {
	DeadLetterTargetARN string
	MaxReceiveCount     int
}

func parseRedrivePolicy(raw string) (sqsRedrivePolicy, *sqsError) {
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &fields) != nil {
		return sqsRedrivePolicy{}, newSQSError("InvalidAttributeValue", "RedrivePolicy must be JSON")
	}
	var result sqsRedrivePolicy
	if json.Unmarshal(fields["deadLetterTargetArn"], &result.DeadLetterTargetARN) != nil || result.DeadLetterTargetARN == "" {
		return result, newSQSError("InvalidAttributeValue", "RedrivePolicy requires deadLetterTargetArn")
	}
	var count any
	if json.Unmarshal(fields["maxReceiveCount"], &count) != nil {
		return result, newSQSError("InvalidAttributeValue", "RedrivePolicy requires maxReceiveCount")
	}
	switch value := count.(type) {
	case string:
		result.MaxReceiveCount, _ = strconv.Atoi(value)
	case float64:
		if value != float64(int(value)) {
			return result, newSQSError("InvalidAttributeValue", "maxReceiveCount must be an integer")
		}
		result.MaxReceiveCount = int(value)
	}
	if result.MaxReceiveCount < 1 || result.MaxReceiveCount > 1000 || !strings.HasPrefix(result.DeadLetterTargetARN, "arn:aws:sqs:") {
		return result, newSQSError("InvalidAttributeValue", "Invalid RedrivePolicy")
	}
	return result, nil
}
func validateRedriveAllowPolicy(raw string) *sqsError {
	var fields struct {
		Permission string   `json:"redrivePermission"`
		Sources    []string `json:"sourceQueueArns"`
	}
	if json.Unmarshal([]byte(raw), &fields) != nil {
		return newSQSError("InvalidAttributeValue", "RedriveAllowPolicy must be JSON")
	}
	switch fields.Permission {
	case "allowAll", "denyAll":
		if len(fields.Sources) > 0 {
			return newSQSError("InvalidAttributeValue", "sourceQueueArns only applies to byQueue")
		}
	case "byQueue":
		if len(fields.Sources) == 0 || len(fields.Sources) > 10 {
			return newSQSError("InvalidAttributeValue", "byQueue requires 1..10 sourceQueueArns")
		}
		for _, arn := range fields.Sources {
			if !strings.HasPrefix(arn, "arn:aws:sqs:") {
				return newSQSError("InvalidAttributeValue", "Invalid sourceQueueArn")
			}
		}
	default:
		return newSQSError("InvalidAttributeValue", "Invalid redrivePermission")
	}
	return nil
}
func validateQueueAttributes(attributes map[string]string, creation bool) *sqsError {
	bounds := map[string][2]int{"VisibilityTimeout": {0, 43200}, "MessageRetentionPeriod": {60, 1209600}, "DelaySeconds": {0, 900}, "MaximumMessageSize": {1024, 1048576}, "ReceiveMessageWaitTimeSeconds": {0, 20}, "KmsDataKeyReusePeriodSeconds": {60, 86400}}
	for name, value := range attributes {
		if limits, ok := bounds[name]; ok {
			number, err := strconv.Atoi(value)
			if err != nil || number < limits[0] || number > limits[1] {
				return newSQSError("InvalidAttributeValue", "Invalid "+name)
			}
			continue
		}
		switch name {
		case "FifoQueue":
			if !creation {
				return newSQSError("InvalidAttributeName", "FifoQueue cannot be changed")
			}
			fallthrough
		case "ContentBasedDeduplication", "SqsManagedSseEnabled":
			if value != "true" && value != "false" {
				return newSQSError("InvalidAttributeValue", "Invalid "+name)
			}
		case "Policy":
			if value != "" {
				var document map[string]any
				if !json.Valid([]byte(value)) || json.Unmarshal([]byte(value), &document) != nil || document == nil {
					return newSQSError("InvalidAttributeValue", "Policy must be a JSON object")
				}
				if len(value) > 8192 {
					return newSQSError("OverLimit", "Policy exceeds 8192 bytes")
				}
			}
		case "RedrivePolicy":
			if value != "" {
				if _, err := parseRedrivePolicy(value); err != nil {
					return err
				}
			}
		case "RedriveAllowPolicy":
			if value != "" {
				if err := validateRedriveAllowPolicy(value); err != nil {
					return err
				}
			}
		case "KmsMasterKeyId":
			if len(value) > 256 {
				return newSQSError("InvalidAttributeValue", "Invalid KmsMasterKeyId")
			}
		case "DeduplicationScope":
			if value != "queue" && value != "messageGroup" {
				return newSQSError("InvalidAttributeValue", "Invalid DeduplicationScope")
			}
		case "FifoThroughputLimit":
			if value != "perQueue" && value != "perMessageGroupId" {
				return newSQSError("InvalidAttributeValue", "Invalid FifoThroughputLimit")
			}
		default:
			return newSQSError("InvalidAttributeName", "Unknown queue attribute "+name)
		}
	}
	return nil
}
func validateQueueCombination(attributes map[string]string) *sqsError {
	fifo := attributes["FifoQueue"] == "true"
	for _, name := range []string{"ContentBasedDeduplication", "DeduplicationScope", "FifoThroughputLimit"} {
		if attributes[name] != "" && !fifo {
			return newSQSError("InvalidAttributeName", name+" only applies to FIFO queues")
		}
	}
	if attributes["FifoThroughputLimit"] == "perMessageGroupId" && attributes["DeduplicationScope"] != "messageGroup" {
		return newSQSError("InvalidAttributeValue", "perMessageGroupId requires messageGroup deduplication")
	}
	if attributes["KmsMasterKeyId"] != "" && attributes["SqsManagedSseEnabled"] == "true" {
		return newSQSError("InvalidAttributeValue", "Only one server-side encryption option is supported")
	}
	return nil
}
func validateMessageInput(input QueueMessageInput) *sqsError {
	if input.Body == "" {
		return newSQSError("MissingParameter", "MessageBody is required")
	}
	if !validSQSCharacters(input.Body) {
		return newSQSError("InvalidMessageContents", "Message contains unsupported characters")
	}
	if input.DelaySeconds != nil && (*input.DelaySeconds < 0 || *input.DelaySeconds > 900) {
		return newSQSError("InvalidParameterValue", "DelaySeconds must be 0..900")
	}
	for _, value := range []string{input.MessageGroupID, input.MessageDeduplicationID} {
		if value != "" && (len(value) > 128 || !validSQSIdentifier(value)) {
			return newSQSError("InvalidParameterValue", "Invalid message group or deduplication ID")
		}
	}
	if len(input.Attributes) > 10 {
		return newSQSError("InvalidParameterValue", "At most 10 message attributes are supported")
	}
	for name, attribute := range input.Attributes {
		if !attributeNamePattern.MatchString(name) || len(name) > 256 || strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") || strings.Contains(name, "..") {
			return newSQSError("InvalidParameterValue", "Invalid message attribute name")
		}
		if err := validateMessageAttribute(attribute); err != nil {
			return err
		}
	}
	for name, attribute := range input.SystemAttributes {
		if name != "AWSTraceHeader" || attribute.DataType != "String" || attribute.BinaryValue != nil || !validTraceHeader(attribute.StringValue) {
			return newSQSError("InvalidParameterValue", "Only a valid String AWSTraceHeader system attribute is supported")
		}
	}
	return nil
}
func validateMessageAttribute(attribute MessageAttribute) *sqsError {
	if len(attribute.DataType) == 0 || len(attribute.DataType) > 256 || !validSQSCharacters(attribute.DataType) {
		return newSQSError("InvalidParameterValue", "Invalid message attribute data type")
	}
	datatype := strings.SplitN(attribute.DataType, ".", 2)[0]
	switch datatype {
	case "String", "Number":
		if attribute.StringValue == "" || attribute.BinaryValue != nil || !validSQSCharacters(attribute.StringValue) {
			return newSQSError("InvalidParameterValue", "Invalid string message attribute")
		}
		if datatype == "Number" {
			if _, err := normalizeSQSNumber(attribute.StringValue); err != nil {
				return err
			}
		}
	case "Binary":
		if len(attribute.BinaryValue) == 0 || attribute.StringValue != "" {
			return newSQSError("InvalidParameterValue", "Invalid binary message attribute")
		}
	default:
		return newSQSError("InvalidParameterValue", "Unknown message attribute data type")
	}
	return nil
}
func validSQSCharacters(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if character == 9 || character == 10 || character == 13 || character >= 0x20 && character <= 0xD7FF || character >= 0xE000 && character <= 0xFFFD || character >= 0x10000 && character <= 0x10FFFF {
			continue
		}
		return false
	}
	return true
}
func validSQSIdentifier(value string) bool {
	for _, character := range value {
		if character < 33 || character > 126 {
			return false
		}
	}
	return true
}
func messageSize(body string, attributes map[string]MessageAttribute) int {
	size := len(body)
	for name, attribute := range attributes {
		size += len(name) + len(attribute.DataType) + len(attribute.StringValue) + len(attribute.BinaryValue)
	}
	return size
}
func validTraceHeader(value string) bool {
	fields := make(map[string]string)
	for _, part := range strings.Split(value, ";") {
		name, item, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || fields[name] != "" {
			return false
		}
		fields[name] = item
	}
	if !traceRootPattern.MatchString(fields["Root"]) {
		return false
	}
	if parent := fields["Parent"]; parent != "" && !traceParentPattern.MatchString(parent) {
		return false
	}
	if sampled := fields["Sampled"]; sampled != "" && sampled != "0" && sampled != "1" && sampled != "?" {
		return false
	}
	return true
}
func normalizeSQSNumber(value string) (string, *sqsError) {
	parts := numericAttributePattern.FindStringSubmatch(value)
	if parts == nil {
		return "", newSQSError("InvalidParameterValue", "Invalid Number message attribute")
	}
	exponent := 0
	if parts[4] != "" {
		parsed, err := strconv.Atoi(parts[4])
		if err != nil || parsed < -1000 || parsed > 1000 {
			return "", newSQSError("InvalidParameterValue", "Number attribute is outside the supported range")
		}
		exponent = parsed
	}
	digits := strings.TrimLeft(parts[2]+parts[3], "0")
	exponent -= len(parts[3])
	if digits == "" {
		return "0", nil
	}
	for strings.HasSuffix(digits, "0") {
		digits = strings.TrimSuffix(digits, "0")
		exponent++
	}
	if len(digits) > 38 {
		return "", newSQSError("InvalidParameterValue", "Number attributes support 38 digits of precision")
	}
	rational, ok := new(big.Rat).SetString(value)
	if !ok {
		return "", newSQSError("InvalidParameterValue", "Invalid Number attribute")
	}
	rational.Abs(rational)
	minimum, _ := new(big.Rat).SetString("1e-128")
	maximum, _ := new(big.Rat).SetString("1e126")
	if rational.Cmp(minimum) < 0 || rational.Cmp(maximum) > 0 {
		return "", newSQSError("InvalidParameterValue", "Number attribute is outside the supported range")
	}
	if exponent >= 0 {
		digits += strings.Repeat("0", exponent)
	} else {
		point := len(digits) + exponent
		if point > 0 {
			digits = digits[:point] + "." + digits[point:]
		} else {
			digits = "0." + strings.Repeat("0", -point) + digits
		}
	}
	if parts[1] == "-" {
		digits = "-" + digits
	}
	return digits, nil
}
