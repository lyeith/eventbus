package messaging

import (
	"fmt"
	"net/http"
)

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
