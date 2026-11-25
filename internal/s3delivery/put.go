// Package s3delivery sends signed, path-style PUT requests to a configured S3
// endpoint. The caller owns delivery scheduling, retries and completion.
package s3delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// Credentials are explicit static credentials. The transport never consults
// the environment or default AWS credentials chain.
type Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// Target fixes the destination origin and signing identity at construction.
// Callers supply an HTTP client so its connection pool can share their lifetime.
type Target struct {
	endpoint, region string
	credentials      Credentials
}

func NewTarget(endpoint, region string, credentials Credentials) Target {
	return Target{endpoint: endpoint, region: region, credentials: credentials}
}

func NewHTTPClient(transport http.RoundTripper) *http.Client {
	return &http.Client{
		Transport: transport,
		Timeout:   10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse // Credentials and body belong to the configured endpoint.
		},
	}
}

// StatusError reports a non-success response from the configured endpoint.
type StatusError struct {
	Status int
}

func (failure *StatusError) Error() string {
	return fmt.Sprintf("destination returned status %d", failure.Status)
}

func (target Target) PutObject(ctx context.Context, client *http.Client, bucket, key, contentType string, data []byte) error {
	endpoint, err := s3ObjectURL(target.endpoint, bucket, key)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create delivery request: %w", err)
	}
	request.Header.Set("Content-Type", contentType)
	hash := sha256.Sum256(data)
	payloadHash := hex.EncodeToString(hash[:])
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)
	credentials := aws.Credentials{
		AccessKeyID: target.credentials.AccessKeyID, SecretAccessKey: target.credentials.SecretAccessKey,
		SessionToken: target.credentials.SessionToken,
	}
	if err := v4.NewSigner().SignHTTP(ctx, credentials, request, payloadHash, "s3", target.region, time.Now(), func(options *v4.SignerOptions) {
		options.DisableURIPathEscaping = true
	}); err != nil {
		return fmt.Errorf("sign delivery: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("send delivery: %w", err)
	}
	_, readErr := io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	closeErr := response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.Join(&StatusError{Status: response.StatusCode}, readErr, closeErr)
	}
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("complete destination response: %w", err)
	}
	return nil
}
