package app

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/google/uuid"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/s3delivery"
)

var lambdaDestinationFunctionName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

type lambdaDestinationS3 struct {
	target s3delivery.Target
	client *http.Client
}

func newLambdaDestinationS3(cfg config) (*lambdaDestinationS3, error) {
	if cfg.s3Endpoint == "" {
		return nil, nil
	}
	endpoint, err := url.Parse(cfg.s3Endpoint)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil ||
		(endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Fragment != "" {
		return nil, errors.New("Lambda S3 destination requires a configured HTTP or HTTPS S3 endpoint")
	}
	// Use an owned connection pool. Invocation records and configured
	// credentials travel only to the explicitly configured endpoint.
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("Lambda S3 destination requires an owned HTTP transport")
	}
	transport := base.Clone()
	transport.Proxy = nil
	target := s3delivery.NewTarget(cfg.s3Endpoint, cfg.region, s3delivery.Credentials{
		AccessKeyID: cfg.s3AccessKey, SecretAccessKey: cfg.s3SecretKey, SessionToken: cfg.s3SessionToken,
	})
	return &lambdaDestinationS3{target: target, client: s3delivery.NewHTTPClient(transport)}, nil
}

func (destination *lambdaDestinationS3) CloseIdleConnections() {
	if destination != nil {
		destination.client.CloseIdleConnections()
	}
}

func (destination *lambdaDestinationS3) Put(ctx context.Context, bucket string, record lambdaservice.AsyncInvocationRecord, payload []byte) error {
	if destination == nil {
		return errors.New("Lambda S3 destination is unavailable")
	}
	function, err := arn.Parse(record.RequestContext.FunctionARN)
	if err != nil || function.Service != "lambda" {
		return errors.New("invalid Lambda invocation function ARN")
	}
	resource, ok := strings.CutPrefix(function.Resource, "function:")
	name, _, _ := strings.Cut(resource, ":")
	if !ok || !lambdaDestinationFunctionName.MatchString(name) {
		return errors.New("invalid Lambda invocation function name")
	}
	id, err := uuid.Parse(record.RequestContext.RequestID)
	if err != nil || record.Timestamp.IsZero() {
		return errors.New("invalid Lambda invocation identity or timestamp")
	}
	// The invocation UUID makes the documented AWS key layout unique while
	// deriving the key deterministically from the accepted record.
	key := "aws/lambda/async/" + name + "/" + record.Timestamp.UTC().Format("2006/01/02/2006-01-02T15.04.05") + "-" + id.String()
	return destination.target.PutObject(ctx, destination.client, bucket, key, "application/json", payload)
}
