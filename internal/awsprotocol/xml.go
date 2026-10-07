package awsprotocol

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

const (
	SNSNamespace = "http://sns.amazonaws.com/doc/2010-03-31/"
	SQSNamespace = "http://queue.amazonaws.com/doc/2012-11-05/"
	SESNamespace = "http://ses.amazonaws.com/doc/2010-12-01/"
)

// QueryNamespace identifies only known Query service versions. Callers retain
// operation admission and use an empty namespace for unidentifiable requests.
func QueryNamespace(version string) (string, bool) {
	switch version {
	case "2010-03-31":
		return SNSNamespace, true
	case "2012-11-05":
		return SQSNamespace, true
	case "2010-12-01":
		return SESNamespace, true
	default:
		return "", false
	}
}

func RequestID() string {
	return uuid.New().String()
}

// EnsureRequestID preserves a service-assigned ID across its header and body.
func EnsureRequestID(w http.ResponseWriter) string {
	requestID := w.Header().Get("X-Amzn-RequestId")
	if requestID == "" {
		requestID = RequestID()
		w.Header().Set("X-Amzn-RequestId", requestID)
	}
	return requestID
}

func XMLResponse(w http.ResponseWriter, statusCode int, body string) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	EnsureRequestID(w)
	w.WriteHeader(statusCode)
	_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`)
	_, _ = fmt.Fprint(w, body)
}

func QueryError(w http.ResponseWriter, statusCode int, namespace, code, message string) {
	requestID := EnsureRequestID(w)
	errorType := "Sender"
	if statusCode >= http.StatusInternalServerError {
		errorType = "Receiver"
	}
	attribute := ""
	if namespace != "" {
		attribute = ` xmlns="` + XMLEscape(namespace) + `"`
	}
	body := fmt.Sprintf(`<ErrorResponse%s><Error><Type>%s</Type><Code>%s</Code><Message>%s</Message></Error><RequestId>%s</RequestId></ErrorResponse>`,
		attribute, errorType, XMLEscape(code), XMLEscape(message), XMLEscape(requestID))
	XMLResponse(w, statusCode, body)
}

// XMLError preserves the historical SNS choice for its existing callers.
func XMLError(w http.ResponseWriter, statusCode int, code, message string) {
	QueryError(w, statusCode, SNSNamespace, code, message)
}

func XMLEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
