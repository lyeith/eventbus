package messaging

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/lyeith/eventbus/internal/awsprotocol"
)

func (s *Handler) serveSQSQuery(w http.ResponseWriter, r *http.Request, action string) {
	if err := r.ParseForm(); err != nil {
		s.writeSQSError(w, newSQSError("InvalidParameterValue", "Invalid Query request"), false)
		return
	}
	input, err := parseSQSQuery(r.Form, action)
	if err != nil {
		s.writeSQSError(w, err, false)
		return
	}
	if input.QueueURL == "" && (strings.HasPrefix(r.URL.Path, "/queue/") || strings.HasPrefix(r.URL.Path, "/"+s.broker.accountID+"/")) {
		input.QueueURL = "http://" + r.Host + r.URL.Path
	}
	result, apiErr := s.executeSQS(r.Context(), action, input)
	if apiErr != nil {
		s.writeSQSError(w, apiErr, false)
		return
	}
	var body strings.Builder
	fmt.Fprintf(&body, `<%sResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">`, action)
	if len(result) > 0 {
		fmt.Fprintf(&body, "<%sResult>", action)
		encodeSQSXMLMap(&body, result, action)
		fmt.Fprintf(&body, "</%sResult>", action)
	}
	fmt.Fprintf(&body, "<ResponseMetadata><RequestId>%s</RequestId></ResponseMetadata></%sResponse>", awsprotocol.EnsureRequestID(w), action)
	awsprotocol.XMLResponse(w, http.StatusOK, body.String())
}
func parseSQSQuery(form url.Values, action string) (sqsRequest, *sqsError) {
	input := sqsRequest{QueueURL: form.Get("QueueUrl"), QueueName: form.Get("QueueName"), QueueOwnerAWSAccountID: form.Get("QueueOwnerAWSAccountId"), QueueNamePrefix: form.Get("QueueNamePrefix"), NextToken: form.Get("NextToken"), Label: form.Get("Label"), SourceARN: form.Get("SourceArn"), DestinationARN: form.Get("DestinationArn"), TaskHandle: form.Get("TaskHandle"), MessageBody: form.Get("MessageBody"), ReceiptHandle: form.Get("ReceiptHandle"), MessageGroupID: form.Get("MessageGroupId"), MessageDeduplicationID: form.Get("MessageDeduplicationId"), ReceiveRequestAttemptID: form.Get("ReceiveRequestAttemptId")}
	integers := map[string]**int{"MaxResults": &input.MaxResults, "MaxNumberOfMessagesPerSecond": &input.MaxNumberOfMessagesPerSecond, "DelaySeconds": &input.DelaySeconds, "VisibilityTimeout": &input.VisibilityTimeout, "MaxNumberOfMessages": &input.MaxNumberOfMessages, "WaitTimeSeconds": &input.WaitTimeSeconds}
	for name, target := range integers {
		value, err := sqsQueryInteger(form, name)
		if err != nil {
			return input, err
		}
		*target = value
	}
	input.Attributes = parseSQSQueryMap(form, "Attribute", "Name", "Value")
	input.Tags = parseSQSQueryMap(form, "Tag", "Key", "Value")
	input.AttributeNames = parseSQSQueryList(form, "AttributeName")
	input.MessageSystemAttributeNames = parseSQSQueryList(form, "MessageSystemAttributeName")
	input.MessageAttributeNames = parseSQSQueryList(form, "MessageAttributeName")
	input.TagKeys = parseSQSQueryList(form, "TagKey")
	input.AWSAccountIDs = parseSQSQueryList(form, "AWSAccountId")
	input.Actions = parseSQSQueryList(form, "Action")
	var err *sqsError
	input.MessageAttributes, err = parseSQSQueryMessageAttributes(form, "MessageAttribute")
	if err != nil {
		return input, err
	}
	input.MessageSystemAttributes, err = parseSQSQueryMessageAttributes(form, "MessageSystemAttribute")
	if err != nil {
		return input, err
	}
	prefix := ""
	switch action {
	case "SendMessageBatch":
		prefix = "SendMessageBatchRequestEntry"
	case "DeleteMessageBatch":
		prefix = "DeleteMessageBatchRequestEntry"
	case "ChangeMessageVisibilityBatch":
		prefix = "ChangeMessageVisibilityBatchRequestEntry"
	}
	if prefix != "" {
		for _, index := range sqsQueryIndices(form, prefix) {
			key := fmt.Sprintf("%s.%d.", prefix, index)
			entry := sqsBatchEntry{ID: form.Get(key + "Id"), MessageBody: form.Get(key + "MessageBody"), ReceiptHandle: form.Get(key + "ReceiptHandle"), MessageGroupID: form.Get(key + "MessageGroupId"), MessageDeduplicationID: form.Get(key + "MessageDeduplicationId")}
			entry.DelaySeconds, err = sqsQueryInteger(form, key+"DelaySeconds")
			if err != nil {
				return input, err
			}
			entry.VisibilityTimeout, err = sqsQueryInteger(form, key+"VisibilityTimeout")
			if err != nil {
				return input, err
			}
			entry.MessageAttributes, err = parseSQSQueryMessageAttributes(form, key+"MessageAttribute")
			if err != nil {
				return input, err
			}
			entry.MessageSystemAttributes, err = parseSQSQueryMessageAttributes(form, key+"MessageSystemAttribute")
			if err != nil {
				return input, err
			}
			input.Entries = append(input.Entries, entry)
		}
	}
	return input, nil
}
func sqsQueryInteger(form url.Values, name string) (*int, *sqsError) {
	raw, exists := form[name]
	if !exists {
		return nil, nil
	}
	if len(raw) != 1 {
		return nil, newSQSError("InvalidParameterValue", "Repeated "+name)
	}
	number, err := strconv.Atoi(raw[0])
	if err != nil {
		return nil, newSQSError("InvalidParameterValue", name+" must be an integer")
	}
	return &number, nil
}
func sqsQueryIndices(form url.Values, prefix string) []int {
	seen := make(map[int]bool)
	for key := range form {
		if rest, ok := strings.CutPrefix(key, prefix+"."); ok {
			index, _, _ := strings.Cut(rest, ".")
			number, err := strconv.Atoi(index)
			if err == nil && number > 0 {
				seen[number] = true
			}
		}
	}
	indices := make([]int, 0, len(seen))
	for index := range seen {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	return indices
}
func parseSQSQueryList(form url.Values, prefix string) []string {
	var values []string
	// AWS's legacy examples also use AttributeName=All. Action itself names
	// the operation, so only its indexed Action.N values are a permission list.
	if prefix != "Action" {
		values = append(values, form[prefix]...)
	}
	for _, index := range sqsQueryIndices(form, prefix) {
		values = append(values, form.Get(fmt.Sprintf("%s.%d", prefix, index)))
	}
	return values
}
func parseSQSQueryMap(form url.Values, prefix, keyName, valueName string) map[string]string {
	values := make(map[string]string)
	for _, index := range sqsQueryIndices(form, prefix) {
		key := fmt.Sprintf("%s.%d.", prefix, index)
		values[form.Get(key+keyName)] = form.Get(key + valueName)
	}
	return values
}
func parseSQSQueryMessageAttributes(form url.Values, prefix string) (map[string]sqsMessageAttribute, *sqsError) {
	attributes := make(map[string]sqsMessageAttribute)
	for _, index := range sqsQueryIndices(form, prefix) {
		key := fmt.Sprintf("%s.%d.", prefix, index)
		value := sqsMessageAttribute{DataType: form.Get(key + "Value.DataType"), StringValue: form.Get(key + "Value.StringValue"), StringListValues: parseSQSQueryList(form, key+"Value.StringListValue")}
		if binary, exists := form[key+"Value.BinaryValue"]; exists {
			if len(binary) != 1 {
				return nil, newSQSError("InvalidParameterValue", "Repeated BinaryValue")
			}
			decoded, err := base64.StdEncoding.DecodeString(binary[0])
			if err != nil {
				return nil, newSQSError("InvalidParameterValue", "BinaryValue must be base64")
			}
			value.BinaryValue = decoded
		}
		for _, binary := range parseSQSQueryList(form, key+"Value.BinaryListValue") {
			decoded, err := base64.StdEncoding.DecodeString(binary)
			if err != nil {
				return nil, newSQSError("InvalidParameterValue", "BinaryListValue must be base64")
			}
			value.BinaryListValues = append(value.BinaryListValues, decoded)
		}
		attributes[form.Get(key+"Name")] = value
	}
	return attributes, nil
}
func xmlSQSValue(body *strings.Builder, name string, value any) {
	fmt.Fprintf(body, "<%s>%s</%s>", name, awsprotocol.XMLEscape(fmt.Sprint(value)), name)
}
func encodeSQSXMLMap(body *strings.Builder, values map[string]any, action string) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := values[key]
		switch items := value.(type) {
		case []string:
			name := key
			if key == "QueueUrls" || key == "queueUrls" {
				name = "QueueUrl"
			}
			for _, item := range items {
				xmlSQSValue(body, name, item)
			}
		case map[string]string:
			tag := "Attribute"
			keyTag := "Name"
			if key == "Tags" {
				tag = "Tag"
				keyTag = "Key"
			}
			names := make([]string, 0, len(items))
			for name := range items {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				fmt.Fprintf(body, "<%s>", tag)
				xmlSQSValue(body, keyTag, name)
				xmlSQSValue(body, "Value", items[name])
				fmt.Fprintf(body, "</%s>", tag)
			}
		case map[string]sqsMessageAttribute:
			names := make([]string, 0, len(items))
			for name := range items {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				attribute := items[name]
				body.WriteString("<MessageAttribute>")
				xmlSQSValue(body, "Name", name)
				body.WriteString("<Value>")
				xmlSQSValue(body, "DataType", attribute.DataType)
				if attribute.BinaryValue != nil {
					xmlSQSValue(body, "BinaryValue", base64.StdEncoding.EncodeToString(attribute.BinaryValue))
				} else {
					xmlSQSValue(body, "StringValue", attribute.StringValue)
				}
				body.WriteString("</Value></MessageAttribute>")
			}
		case []map[string]any:
			name := "Result"
			switch key {
			case "Messages":
				name = "Message"
			case "Successful":
				name = action + "ResultEntry"
			case "Failed":
				name = "BatchResultErrorEntry"
			}
			for _, item := range items {
				fmt.Fprintf(body, "<%s>", name)
				encodeSQSXMLMap(body, item, action)
				fmt.Fprintf(body, "</%s>", name)
			}
		default:
			xmlSQSValue(body, key, value)
		}
	}
}
