package awsprotocol

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// --- XML response helpers ---

func XMLResponse(w http.ResponseWriter, statusCode int, body string) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.WriteHeader(statusCode)
	_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?>`)
	_, _ = fmt.Fprint(w, body)
}

func RequestID() string {
	return uuid.New().String()
}

func XMLError(w http.ResponseWriter, statusCode int, code, message string) {
	body := fmt.Sprintf(`
<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <Error>
    <Type>Sender</Type>
    <Code>%s</Code>
    <Message>%s</Message>
  </Error>
  <RequestId>%s</RequestId>
</ErrorResponse>`, XMLEscape(code), XMLEscape(message), RequestID())
	XMLResponse(w, statusCode, body)
}

func XMLEscape(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "'", "&apos;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
