package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
)

type lambdaDestinations struct {
	broker         *messaging.Broker
	functions      *lambdaservice.Service // Set at startup before the HTTP listener starts.
	storage        *lambdaDestinationS3
	eventsEndpoint *url.URL
	client         *http.Client
	region         string
}

var _ lambdaservice.AsyncDestinationSink = (*lambdaDestinations)(nil)
var bucketDestinationName = regexp.MustCompile("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
var eventBusDestinationName = regexp.MustCompile("^[A-Za-z0-9_.-]{1,256}$")
var unavailableAsyncDestination = errors.New("local asynchronous destination is unavailable")

func (d *lambdaDestinations) ValidateAsyncDestination(ref string, onFailure bool) error {
	target, err := arn.Parse(ref)
	if err != nil {
		return unavailableAsyncDestination
	}
	switch target.Service {
	case "sqs":
		if !strings.HasSuffix(target.Resource, ".fifo") {
			_, err := d.broker.QueueInfo(d.broker.GetQueueByARN(ref))
			return err
		}
	case "sns":
		if !strings.HasSuffix(target.Resource, ".fifo") && d.broker.GetTopic(ref) != nil {
			return nil
		}
	case "lambda":
		if d.functions != nil {
			return d.functions.ValidateTarget(ref, "")
		}
	case "s3":
		if onFailure && d.storage != nil && bucketDestinationName.MatchString(target.Resource) {
			return nil
		}
	case "events":
		name, ok := strings.CutPrefix(target.Resource, "event-bus/")
		if d.eventsEndpoint != nil && ok && eventBusDestinationName.MatchString(name) {
			return nil
		}
	}
	return unavailableAsyncDestination
}

func (d *lambdaDestinations) DeliverAsyncDestination(ctx context.Context, ref string, record lambdaservice.AsyncInvocationRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.ValidateAsyncDestination(ref, record.RequestContext.Condition != "Success"); err != nil {
		return err
	}
	target, _ := arn.Parse(ref)
	payload, err := json.Marshal(record)
	if err != nil {
		return err
	}
	switch target.Service {
	case "sqs":
		_, err = d.broker.SendQueueMessage(d.broker.GetQueueByARN(ref), messaging.QueueMessageInput{Body: string(payload)})
	case "sns":
		_, err = d.broker.PublishSNS(messaging.SNSPublishInput{Operation: "Publish", TopicARN: ref,
			RequestID: record.RequestContext.RequestID, Message: string(payload)})
	case "lambda":
		_, err = admitLambdaTarget(ctx, d.functions, ref, payload)
	case "s3":
		err = d.storage.Put(ctx, target.Resource, record, payload)
	case "events":
		err = d.putEvents(ctx, ref, record, payload)
	default:
		err = unavailableAsyncDestination
	}
	return err
}

func (d *lambdaDestinations) putEvents(ctx context.Context, bus string, record lambdaservice.AsyncInvocationRecord, detail []byte) error {
	kind := "Lambda Function Invocation Result - Failure"
	if record.RequestContext.Condition == "Success" {
		kind = "Lambda Function Invocation Result - Success"
	}
	input, err := json.Marshal(map[string]any{"Entries": []any{map[string]any{
		"EventBusName": bus, "Time": float64(record.Timestamp.UnixMilli()) / 1000, "Source": "lambda",
		"DetailType": kind, "Resources": []string{record.RequestContext.FunctionARN, bus}, "Detail": string(detail),
	}}})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, d.eventsEndpoint.String(), bytes.NewReader(input))
	if err != nil {
		return unavailableAsyncDestination
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", "AWSEvents.PutEvents")
	hash := sha256.Sum256(input)
	// The explicitly selected loopback peer receives fixed fixture credentials.
	if err := v4.NewSigner().SignHTTP(ctx, aws.Credentials{AccessKeyID: "eventbus-local", SecretAccessKey: "eventbus-local"}, request,
		hex.EncodeToString(hash[:]), "events", d.region, time.Now()); err != nil {
		return unavailableAsyncDestination
	}
	response, err := d.client.Do(request)
	if err != nil {
		return unavailableAsyncDestination
	}
	defer response.Body.Close()
	var result struct {
		FailedEntryCount *int `json:"FailedEntryCount"`
		Entries          []struct {
			EventID string `json:"EventId"`
		} `json:"Entries"`
	}
	if response.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(response.Body, 64<<10)).Decode(&result) != nil ||
		result.FailedEntryCount == nil || *result.FailedEntryCount != 0 || len(result.Entries) != 1 || result.Entries[0].EventID == "" {
		return unavailableAsyncDestination
	}
	return nil
}
