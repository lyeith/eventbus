package main

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// extractAction determines the AWS action from the request.
// Supports Query protocol (form-encoded Action param).
func extractAction(r *http.Request) string {
	if err := r.ParseForm(); err != nil {
		return ""
	}
	return r.FormValue("Action")
}

// formValues returns all values for a form parameter prefix like "Attribute.N.Key".
func formMapValues(r *http.Request, prefix string) map[string]string {
	result := make(map[string]string)
	for i := 1; i <= 100; i++ {
		key := r.FormValue(fmt.Sprintf("%s.entry.%d.key", prefix, i))
		val := r.FormValue(fmt.Sprintf("%s.entry.%d.value", prefix, i))
		if key == "" {
			// Try alternate format: Attribute.N.Name / Attribute.N.Value
			key = r.FormValue(fmt.Sprintf("%s.%d.Name", prefix, i))
			val = r.FormValue(fmt.Sprintf("%s.%d.Value", prefix, i))
		}
		if key == "" {
			break
		}
		result[key] = val
	}
	return result
}

// parseMessageAttributes extracts MessageAttributes from form params.
// Format: MessageAttributes.entry.N.Name, MessageAttributes.entry.N.Value.DataType, MessageAttributes.entry.N.Value.StringValue
func parseMessageAttributes(r *http.Request) map[string]MessageAttribute {
	attrs := make(map[string]MessageAttribute)

	for i := 1; i <= 100; i++ {
		name := r.FormValue(fmt.Sprintf("MessageAttributes.entry.%d.Name", i))
		if name == "" {
			break
		}
		dataType := r.FormValue(fmt.Sprintf("MessageAttributes.entry.%d.Value.DataType", i))
		stringValue := r.FormValue(fmt.Sprintf("MessageAttributes.entry.%d.Value.StringValue", i))
		attrs[name] = MessageAttribute{
			DataType:    dataType,
			StringValue: stringValue,
		}
	}

	return attrs
}

// --- XML response helpers ---

func xmlResponse(w http.ResponseWriter, statusCode int, body string) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(statusCode)
	_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`)
	_, _ = fmt.Fprint(w, body)
}

func requestID() string {
	return uuid.New().String()
}

func xmlError(w http.ResponseWriter, statusCode int, code, message string) {
	body := fmt.Sprintf(`
<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <Error>
    <Type>Sender</Type>
    <Code>%s</Code>
    <Message>%s</Message>
  </Error>
  <RequestId>%s</RequestId>
</ErrorResponse>`, xmlEscape(code), xmlEscape(message), requestID())
	xmlResponse(w, statusCode, body)
}

func xmlEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
