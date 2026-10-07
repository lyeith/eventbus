package gateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"unicode/utf8"
)

type proxyResponse struct {
	status  int
	headers http.Header
	body    []byte
}

func (gateway *Gateway) serveLambda(w http.ResponseWriter, request *http.Request, route RouteConfig, event requestEvent, authorization authorizerResponse) {
	var body []byte
	var err error
	if request.Body != nil {
		body, err = io.ReadAll(io.LimitReader(request.Body, maxInvokePayload+1))
	}
	if err != nil {
		writeGatewayError(w, http.StatusBadRequest)
		return
	}
	if len(body) > maxInvokePayload {
		writeGatewayError(w, http.StatusRequestEntityTooLarge)
		return
	}
	text, encoded := requestBody(body, request.Header.Get("Content-Type"))
	event = integrationRequestEvent(event, route.Integration.RemoveHeaders)
	payload, err := json.Marshal(integrationEvent(event, route.Integration.PayloadFormatVersion, text, encoded, authorization, route.Authorizer != "" && !gateway.options.NoAuth))
	// The synchronous Lambda envelope has a smaller budget than the gateway's
	// HTTP body limit. Reject before Invoke rather than truncating the event.
	if err != nil {
		writeGatewayError(w, http.StatusInternalServerError)
		return
	}
	if len(payload) > maxInvokePayload {
		writeGatewayError(w, http.StatusRequestEntityTooLarge)
		return
	}
	invoked, failure := invokeLambda(request.Context(), gateway.invokeClient, route.Integration.InvokeURL, route.Integration.Timeout, payload)
	switch failure {
	case invokeTimedOut:
		writeLambdaError(w, http.StatusGatewayTimeout)
		return
	case invokeRejected:
		writeLambdaError(w, http.StatusInternalServerError)
		return
	case invokeResultTooLarge:
		writeLambdaError(w, http.StatusBadGateway)
		return
	}
	if invoked.functionError {
		writeLambdaError(w, http.StatusBadGateway)
		return
	}
	response, err := parseProxyResponse(invoked.payload, route.Integration.PayloadFormatVersion)
	if err != nil {
		writeLambdaError(w, http.StatusBadGateway)
		return
	}
	for name, values := range response.headers {
		// The gateway owns this ID; a function cannot replace it.
		if strings.EqualFold(name, "X-Request-Id") {
			continue
		}
		w.Header()[name] = values
	}
	w.WriteHeader(response.status)
	if request.Method != http.MethodHead && response.status != http.StatusNoContent && response.status != http.StatusNotModified {
		_, _ = w.Write(response.body)
	}
}

func requestBody(body []byte, contentType string) (string, bool) {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if contentType == "" {
		mediaType = ""
		err = nil
	}
	text := err == nil && (mediaType == "" || strings.HasPrefix(mediaType, "text/") || strings.HasSuffix(mediaType, "+json") || strings.HasSuffix(mediaType, "+xml") || mediaType == "application/json" || mediaType == "application/xml" || mediaType == "application/javascript" || mediaType == "application/x-www-form-urlencoded")
	if utf8.Valid(body) && text {
		return string(body), false
	}
	return base64.StdEncoding.EncodeToString(body), true
}

func parseProxyResponse(data []byte, version string) (proxyResponse, error) {
	if !json.Valid(data) {
		return proxyResponse{}, fmt.Errorf("invalid JSON response")
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	if _, explicit := fields["statusCode"]; !explicit && version == "2.0" {
		body := bytes.TrimSpace(data)
		var text string
		if json.Unmarshal(data, &text) == nil && !bytes.Equal(body, []byte("null")) {
			body = []byte(text)
		}
		return proxyResponse{status: http.StatusOK, headers: http.Header{"Content-Type": {"application/json"}}, body: body}, nil
	}
	if err := validateProxyStrings(fields); err != nil {
		return proxyResponse{}, err
	}
	var raw struct {
		StatusCode        *int                `json:"statusCode"`
		Headers           map[string]string   `json:"headers"`
		MultiValueHeaders map[string][]string `json:"multiValueHeaders"`
		Cookies           []string            `json:"cookies"`
		Body              *string             `json:"body"`
		IsBase64Encoded   bool                `json:"isBase64Encoded"`
	}
	if err := json.Unmarshal(data, &raw); err != nil || raw.StatusCode == nil || *raw.StatusCode < 200 || *raw.StatusCode > 599 {
		return proxyResponse{}, fmt.Errorf("invalid proxy response")
	}
	if version == "2.0" && raw.MultiValueHeaders != nil {
		return proxyResponse{}, fmt.Errorf("multiValueHeaders require format 1.0")
	}
	if version == "1.0" && raw.Cookies != nil {
		return proxyResponse{}, fmt.Errorf("cookies require format 2.0")
	}
	headers := make(http.Header)
	for name, values := range raw.MultiValueHeaders {
		if !validHeaderName(name) {
			return proxyResponse{}, fmt.Errorf("invalid response header")
		}
		for _, value := range values {
			if !validHeaderValue(value) {
				return proxyResponse{}, fmt.Errorf("invalid response header")
			}
			headers.Add(name, value)
		}
	}
	for name, value := range raw.Headers {
		if !validHeaderName(name) || !validHeaderValue(value) {
			return proxyResponse{}, fmt.Errorf("invalid response header")
		}
		duplicate := false
		for _, existing := range headers.Values(name) {
			if existing == value {
				duplicate = true
				break
			}
		}
		if !duplicate {
			headers.Add(name, value)
		}
	}
	for _, cookie := range raw.Cookies {
		if !validHeaderValue(cookie) {
			return proxyResponse{}, fmt.Errorf("invalid response cookie")
		}
		headers.Add("Set-Cookie", cookie)
	}
	// Never let a function dictate framing or connection ownership.
	for _, value := range headers.Values("Connection") {
		for _, name := range strings.Split(value, ",") {
			headers.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range []string{"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade", "Content-Length"} {
		headers.Del(name)
	}
	var body []byte
	if raw.Body != nil {
		body = []byte(*raw.Body)
	}
	if raw.IsBase64Encoded {
		var err error
		body, err = base64.StdEncoding.Strict().DecodeString(string(body))
		if err != nil {
			return proxyResponse{}, fmt.Errorf("invalid base64 response")
		}
	}
	return proxyResponse{status: *raw.StatusCode, headers: headers, body: body}, nil
}

func validHeaderValue(value string) bool {
	for _, character := range value {
		if character == 127 || character < 32 && character != '\t' {
			return false
		}
	}
	return true
}

func writeLambdaError(w http.ResponseWriter, status int) {
	// AWS distinguishes Invoke API failures, function/malformed response failures
	// and timeouts, while keeping function error details out of the HTTP response.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": "Internal server error"})
}

// encoding/json accepts null into a Go string. Native header/cookie elements
// must actually be JSON strings, so validate these elements before decoding.
func validateProxyStrings(fields map[string]json.RawMessage) error {
	isString := func(value json.RawMessage) bool {
		value = bytes.TrimSpace(value)
		return len(value) > 0 && value[0] == '"'
	}
	var headers map[string]json.RawMessage
	if value, present := fields["headers"]; present {
		if err := json.Unmarshal(value, &headers); err != nil {
			return err
		}
		for _, item := range headers {
			if !isString(item) {
				return fmt.Errorf("response headers must be strings")
			}
		}
	}
	var multi map[string][]json.RawMessage
	if value, present := fields["multiValueHeaders"]; present {
		if err := json.Unmarshal(value, &multi); err != nil {
			return err
		}
		for _, values := range multi {
			for _, item := range values {
				if !isString(item) {
					return fmt.Errorf("response multiValueHeaders must contain strings")
				}
			}
		}
	}
	var cookies []json.RawMessage
	if value, present := fields["cookies"]; present {
		if err := json.Unmarshal(value, &cookies); err != nil {
			return err
		}
		for _, item := range cookies {
			if !isString(item) {
				return fmt.Errorf("response cookies must be strings")
			}
		}
	}
	return nil
}
