package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

type invokeResult struct {
	payload       []byte
	functionError bool
}
type invokeFailure int

const (
	invokeOK invokeFailure = iota
	invokeRejected
	invokeTimedOut
	invokeResultTooLarge
)

// The gateway owns the HTTP Invoke exchange; service-specific response and
// failure policies remain with the authorizer and integration consumers.
func invokeLambda(ctx context.Context, client *http.Client, endpoint string, timeout time.Duration, payload []byte) (invokeResult, invokeFailure) {
	invokeContext, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(invokeContext, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return invokeResult{}, invokeRejected
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Amz-Invocation-Type", "RequestResponse")
	response, err := client.Do(request)
	if err != nil {
		if errors.Is(invokeContext.Err(), context.DeadlineExceeded) {
			return invokeResult{}, invokeTimedOut
		}
		return invokeResult{}, invokeRejected
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, maxInvokePayload+1))
	if errors.Is(invokeContext.Err(), context.DeadlineExceeded) {
		return invokeResult{}, invokeTimedOut
	}
	if err != nil || response.StatusCode != http.StatusOK {
		return invokeResult{}, invokeRejected
	}
	if len(data) > maxInvokePayload {
		return invokeResult{}, invokeResultTooLarge
	}
	return invokeResult{payload: data, functionError: response.Header.Get("X-Amz-Function-Error") != ""}, invokeOK
}
