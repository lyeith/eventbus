package ses

import (
	"net/http"
	"net/mail"
	"strings"

	"github.com/google/uuid"
)

func sesObject(value any) map[string]any { object, _ := value.(map[string]any); return object }
func sesString(value any) string         { text, _ := value.(string); return text }
func sesStrings(value any) []string {
	if values, ok := value.([]string); ok {
		return values
	}
	values, _ := value.([]any)
	result := make([]string, 0, len(values))
	for _, value := range values {
		if text, ok := value.(string); ok {
			result = append(result, text)
		}
	}
	return result
}
func sesMessageID() string { return uuid.NewString() }
func sesInvalid(api, message string) *sesAPIError {
	code := "BadRequestException"
	if api == "v1" {
		code = "InvalidParameterValue"
	}
	return &sesAPIError{Code: code, Message: message, Status: http.StatusBadRequest}
}
func sesValidateAddress(api, field, address string) *sesAPIError {
	if address == "" || strings.ContainsAny(address, "\r\n") {
		return sesInvalid(api, field+" must be a valid email address")
	}
	for _, character := range address {
		if character > 127 {
			return sesInvalid(api, field+" must use ASCII email addresses and MIME-encoded display names")
		}
	}
	parsed, err := mail.ParseAddress(address)
	if err != nil || !strings.Contains(parsed.Address, "@") {
		return sesInvalid(api, field+" must be a valid email address")
	}
	return nil
}
func sesValidateRecipients(api string, destination map[string]any) *sesAPIError {
	count := 0
	for _, field := range []string{"ToAddresses", "CcAddresses", "BccAddresses"} {
		if value, present := destination[field]; present {
			values, ok := value.([]any)
			if !ok {
				if stringsValue, yes := value.([]string); yes {
					values = make([]any, len(stringsValue))
					for i, text := range stringsValue {
						values[i] = text
					}
				} else {
					return sesInvalid(api, field+" must be a list")
				}
			}
			for _, value := range values {
				address, ok := value.(string)
				if !ok {
					return sesInvalid(api, field+" must contain email addresses")
				}
				if err := sesValidateAddress(api, field, address); err != nil {
					return err
				}
				count++
			}
		}
	}
	if count < 1 || count > 50 {
		return sesInvalid(api, "A message requires between 1 and 50 recipients")
	}
	return nil
}
