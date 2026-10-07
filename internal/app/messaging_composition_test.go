package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/eventsource"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// This test executable is a real provided runtime. The production app owns its
// process, private Runtime API and event delivery; the handler only records the
// event it received and acknowledges it through the native Runtime API.
func TestMessagingCompositionProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_MESSAGING_COMPOSITION_PROCESS") != "1" {
		return
	}
	if err := messagingCompositionInvocation(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

type messagingCompositionObservation struct {
	FunctionARN string          `json:"function_arn"`
	Handler     string          `json:"handler"`
	RequestID   string          `json:"request_id"`
	Event       json.RawMessage `json:"event"`
}

func messagingCompositionInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 8 * time.Second}
	endpoint := "http://" + os.Getenv("AWS_LAMBDA_RUNTIME_API") + "/2018-06-01/runtime/invocation/"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+"next", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	payload, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	id := response.Header.Get("Lambda-Runtime-Aws-Request-Id")
	if readErr != nil || response.StatusCode != http.StatusOK || id == "" || !json.Valid(payload) {
		return errors.New("provided handler received an invalid invocation")
	}
	var source struct {
		Records []struct {
			EventSource string `json:"EventSource"`
		} `json:"Records"`
	}
	if err := json.Unmarshal(payload, &source); err != nil || len(source.Records) != 1 {
		return errors.New("provided handler expected one native record")
	}
	directory := os.Getenv("MESSAGING_OBSERVATIONS")
	if source.Records[0].EventSource == "aws:sns" {
		if err := os.WriteFile(filepath.Join(directory, "sns-started"), []byte(id), 0600); err != nil {
			return err
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(filepath.Join(directory, "sns-release")); err == nil {
				break
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	observation := messagingCompositionObservation{
		FunctionARN: response.Header.Get("Lambda-Runtime-Invoked-Function-Arn"),
		Handler:     os.Getenv("MESSAGING_HANDLER"), RequestID: id, Event: payload,
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return err
	}
	filename := filepath.Join(directory, id+".json")
	if err := os.WriteFile(filename+".tmp", encoded, 0600); err != nil {
		return err
	}
	if err := os.Rename(filename+".tmp", filename); err != nil {
		return err
	}
	reply, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+id+"/response", strings.NewReader(`{"processed":true}`))
	if err != nil {
		return err
	}
	ack, err := client.Do(reply)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("runtime acknowledgment returned %d", ack.StatusCode)
	}
	return nil
}

func messagingCompositionObservations(directory string) ([]messagingCompositionObservation, error) {
	files, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	var observations []messagingCompositionObservation
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(directory, file.Name()))
		if err != nil {
			return nil, err
		}
		var observation messagingCompositionObservation
		if err := json.Unmarshal(data, &observation); err != nil {
			return nil, err
		}
		observations = append(observations, observation)
	}
	return observations, nil
}

// SNS invokes asynchronously; SQS mappings invoke synchronously and acknowledge
// only successful batches. See the native event and delivery contracts at:
// https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html
// https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html
func TestMessagingProductionCompositionSNSAndSQS(t *testing.T) {
	directory := t.TempDir()
	observationsPath := filepath.Join(directory, "observations")
	require.NoError(t, os.Mkdir(observationsPath, 0700))
	executable, err := os.Executable()
	require.NoError(t, err)
	function := lambdaservice.Function{
		Runtime: "provided", Command: []string{executable, "-test.run=^TestMessagingCompositionProvidedProcess$"}, Timeout: 10 * time.Second,
		Environment: map[string]string{
			"EVENTBUS_MESSAGING_COMPOSITION_PROCESS": "1", "MESSAGING_OBSERVATIONS": observationsPath, "MESSAGING_HANDLER": "base",
		},
	}
	alias := function
	alias.Environment = map[string]string{
		"EVENTBUS_MESSAGING_COMPOSITION_PROCESS": "1", "MESSAGING_OBSERVATIONS": observationsPath, "MESSAGING_HANDLER": "alias",
	}
	recipe, err := yaml.Marshal(lambdaservice.Config{
		Functions: map[string]lambdaservice.Function{"delivery": function, "delivery:live": alias},
		DevAsync:  &lambdaservice.DevAsyncConfig{Workers: 1, Capacity: 4, RetryDelays: []time.Duration{0, 0}, LogPath: filepath.Join(directory, "async.jsonl")},
	})
	require.NoError(t, err)
	recipePath := filepath.Join(directory, "functions.yaml")
	require.NoError(t, os.WriteFile(recipePath, recipe, 0600))
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := reservation.Addr().(*net.TCPAddr).Port
	require.NoError(t, reservation.Close())
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	cfg, err := readConfig(flag.NewFlagSet("messaging-composition", flag.ContinueOnError), nil)
	require.NoError(t, err)
	cfg.port, cfg.workDir, cfg.lambdaFunctions = port, directory, recipePath
	cfg.cognitoDB = filepath.Join(directory, "cognito.db")
	cfg.cognitoLog, cfg.snsLog, cfg.sesLog = filepath.Join(directory, "cognito.jsonl"), filepath.Join(directory, "sns.jsonl"), filepath.Join(directory, "ses.jsonl")
	cfg.issuerBase = endpoint
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- run(ctx, cfg) }()
	t.Cleanup(func() {
		// Always release the handler before joining the app, including on failed
		// assertions. The app remains the sole owner of every child and listener.
		_ = os.WriteFile(filepath.Join(observationsPath, "sns-release"), nil, 0600)
		cancel()
		select {
		case err := <-runDone:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Error("production application did not join its owned work")
		}
	})
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	require.Eventually(t, func() bool {
		response, err := client.Get(endpoint + "/health")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond, "production run must bind its actual router")
	awsConfig := aws.Config{Region: cfg.region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client}
	snsClient := sns.NewFromConfig(awsConfig, func(options *sns.Options) { options.BaseEndpoint = aws.String(endpoint) })
	sqsClient := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(endpoint) })
	requestCtx, stopRequests := context.WithTimeout(t.Context(), 25*time.Second)
	defer stopRequests()
	functionARN := "arn:aws:lambda:" + cfg.region + ":" + cfg.accountID + ":function:delivery:live"
	topic, err := snsClient.CreateTopic(requestCtx, &sns.CreateTopicInput{Name: aws.String("composition")})
	require.NoError(t, err)
	subscription, err := snsClient.Subscribe(requestCtx, &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("lambda"), Endpoint: aws.String(functionARN)})
	require.NoError(t, err)
	published, err := snsClient.Publish(requestCtx, &sns.PublishInput{
		TopicArn: topic.TopicArn, Subject: aws.String("native subject"), Message: aws.String(`{"kind":"sns","owned":true}`),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"text":   {DataType: aws.String("String"), StringValue: aws.String("native value")},
			"binary": {DataType: aws.String("Binary"), BinaryValue: []byte{0, 1, 255}},
		},
	})
	require.NoError(t, err, "Publish must return while the admitted handler is still blocked")
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(observationsPath, "sns-started")); return err == nil }, 5*time.Second, 10*time.Millisecond)
	observations, err := messagingCompositionObservations(observationsPath)
	require.NoError(t, err)
	require.Empty(t, observations, "successful Publish proves admission before handler completion")
	require.NoError(t, os.WriteFile(filepath.Join(observationsPath, "sns-release"), nil, 0600))
	require.Eventually(t, func() bool {
		observations, err = messagingCompositionObservations(observationsPath)
		return err == nil && len(observations) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, functionARN, observations[0].FunctionARN)
	require.Equal(t, "alias", observations[0].Handler, "the exact qualified registry entry must execute")
	var snsEvent struct {
		Records []struct {
			EventSource     string `json:"EventSource"`
			EventVersion    string `json:"EventVersion"`
			SubscriptionARN string `json:"EventSubscriptionArn"`
			SNS             struct {
				MessageID  string                                  `json:"MessageId"`
				TopicARN   string                                  `json:"TopicArn"`
				Message    string                                  `json:"Message"`
				Subject    string                                  `json:"Subject"`
				Attributes map[string]struct{ Type, Value string } `json:"MessageAttributes"`
			} `json:"Sns"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(observations[0].Event, &snsEvent))
	require.Len(t, snsEvent.Records, 1)
	notification := snsEvent.Records[0]
	require.Equal(t, "aws:sns", notification.EventSource)
	require.Equal(t, "1.0", notification.EventVersion)
	require.Equal(t, aws.ToString(subscription.SubscriptionArn), notification.SubscriptionARN)
	require.Equal(t, aws.ToString(published.MessageId), notification.SNS.MessageID)
	require.Equal(t, aws.ToString(topic.TopicArn), notification.SNS.TopicARN)
	require.Equal(t, `{"kind":"sns","owned":true}`, notification.SNS.Message)
	require.Equal(t, "native subject", notification.SNS.Subject)
	require.Equal(t, "native value", notification.SNS.Attributes["text"].Value)
	require.Equal(t, "Binary", notification.SNS.Attributes["binary"].Type)
	require.Equal(t, "AAH/", notification.SNS.Attributes["binary"].Value)

	queue, err := sqsClient.CreateQueue(requestCtx, &sqs.CreateQueueInput{QueueName: aws.String("composition")})
	require.NoError(t, err)
	queueARN := "arn:aws:sqs:" + cfg.region + ":" + cfg.accountID + ":composition"
	mapping := messagingCompositionMapping(t, requestCtx, client, http.MethodPost, endpoint, "", http.StatusAccepted, map[string]any{
		"EventSourceArn": queueARN, "FunctionName": functionARN, "BatchSize": 1,
	})
	require.Equal(t, functionARN, mapping.FunctionARN)
	require.Equal(t, queueARN, mapping.EventSourceARN)
	require.Equal(t, "Enabled", mapping.State)
	trace := "Root=1-65b785e4-1234567890abcdef12345678;Parent=1234567890abcdef;Sampled=1"
	sent, err := sqsClient.SendMessage(requestCtx, &sqs.SendMessageInput{
		QueueUrl: queue.QueueUrl, MessageBody: aws.String(`{"kind":"sqs","owned":true}`),
		MessageAttributes: map[string]sqstypes.MessageAttributeValue{
			"text":   {DataType: aws.String("String"), StringValue: aws.String("queue value")},
			"binary": {DataType: aws.String("Binary"), BinaryValue: []byte{255, 1, 0}},
		},
		MessageSystemAttributes: map[string]sqstypes.MessageSystemAttributeValue{
			"AWSTraceHeader": {DataType: aws.String("String"), StringValue: aws.String(trace)},
		},
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		observations, err = messagingCompositionObservations(observationsPath)
		return err == nil && len(observations) == 2
	}, 5*time.Second, 10*time.Millisecond)
	var sqsObservation messagingCompositionObservation
	for _, observation := range observations {
		if bytes.Contains(observation.Event, []byte(`"aws:sqs"`)) {
			sqsObservation = observation
		}
	}
	require.Equal(t, functionARN, sqsObservation.FunctionARN)
	require.Equal(t, "alias", sqsObservation.Handler)
	var sqsEvent sqsevent.Event
	require.NoError(t, json.Unmarshal(sqsObservation.Event, &sqsEvent))
	require.Len(t, sqsEvent.Records, 1)
	record := sqsEvent.Records[0]
	require.Equal(t, "aws:sqs", record.EventSource)
	require.Equal(t, queueARN, record.EventSourceARN)
	require.Equal(t, cfg.region, record.AWSRegion)
	require.Equal(t, aws.ToString(sent.MessageId), record.MessageID)
	require.Equal(t, aws.ToString(sent.MD5OfMessageBody), record.MD5OfBody)
	require.NotEmpty(t, record.ReceiptHandle)
	require.Equal(t, `{"kind":"sqs","owned":true}`, record.Body)
	require.Equal(t, "1", record.Attributes["ApproximateReceiveCount"])
	require.Equal(t, trace, record.Attributes["AWSTraceHeader"])
	require.Equal(t, "queue value", aws.ToString(record.MessageAttributes["text"].StringValue))
	require.Equal(t, []byte{255, 1, 0}, record.MessageAttributes["binary"].BinaryValue)
	require.Eventually(t, func() bool {
		mapping = messagingCompositionMapping(t, requestCtx, client, http.MethodGet, endpoint, mapping.UUID, http.StatusOK, nil)
		return mapping.LastProcessingResult == "OK"
	}, 5*time.Second, 10*time.Millisecond, "mapping must await successful execution and acknowledge the original receipt")
	attributes, err := sqsClient.GetQueueAttributes(requestCtx, &sqs.GetQueueAttributesInput{
		QueueUrl: queue.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages, sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible},
	})
	require.NoError(t, err)
	require.Equal(t, "0", attributes.Attributes["ApproximateNumberOfMessages"])
	require.Equal(t, "0", attributes.Attributes["ApproximateNumberOfMessagesNotVisible"])
	_, err = sqsClient.DeleteQueue(requestCtx, &sqs.DeleteQueueInput{QueueUrl: queue.QueueUrl})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		mapping = messagingCompositionMapping(t, requestCtx, client, http.MethodGet, endpoint, mapping.UUID, http.StatusOK, nil)
		return mapping.State == "Disabled" && mapping.LastProcessingResult == "Source receive failed"
	}, 5*time.Second, 10*time.Millisecond, "deleting the bound source must stop its mapping")
	_, err = sqsClient.CreateQueue(requestCtx, &sqs.CreateQueueInput{QueueName: aws.String("composition")})
	var native smithy.APIError
	require.ErrorAs(t, err, &native)
	require.Equal(t, "QueueDeletedRecently", native.ErrorCode(), "composition must preserve the native queue-recreation cooldown")
	deleted := messagingCompositionMapping(t, requestCtx, client, http.MethodDelete, endpoint, mapping.UUID, http.StatusAccepted, nil)
	require.Equal(t, "Deleting", deleted.State)
}

func messagingCompositionMapping(t *testing.T, ctx context.Context, client *http.Client, method, endpoint, id string, wantStatus int, input any) eventsource.Mapping {
	t.Helper()
	var payload []byte
	var err error
	if input != nil {
		payload, err = json.Marshal(input)
		require.NoError(t, err)
	}
	path := endpoint + "/2015-03-31/event-source-mappings"
	if id != "" {
		path += "/" + id
	}
	request, err := http.NewRequestWithContext(ctx, method, path, bytes.NewReader(payload))
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, wantStatus, response.StatusCode, "%s %s: %s", method, path, body)
	require.NotEmpty(t, response.Header.Get("X-Amzn-Requestid"))
	var mapping eventsource.Mapping
	require.NoError(t, json.Unmarshal(body, &mapping))
	require.NotEmpty(t, mapping.UUID)
	return mapping
}

func TestMessagingQueueAdapterRetainsOriginalIdentity(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	original := broker.CreateQueue("bound", 30*time.Second, time.Hour)
	bound, err := (sqsMappingSource{broker: broker}).ResolveQueue(t.Context(), original.ARN)
	require.NoError(t, err)
	require.True(t, broker.DeleteQueue("bound"))
	// The direct fixture constructor skips only the native 60-second cooldown.
	// The identical name/ARN makes an accidental adapter re-resolution observable.
	replacement := broker.CreateQueue("bound", 30*time.Second, time.Hour)
	require.Equal(t, original.ARN, replacement.ARN)
	require.NotSame(t, original, replacement)
	sent, err := broker.SendQueueMessage(replacement, messaging.QueueMessageInput{Body: "replacement remains owned"})
	require.NoError(t, err)
	_, err = bound.Receive(t.Context())
	require.ErrorIs(t, err, messaging.ErrQueueUnavailable)
	deleted, err := bound.Delete(t.Context(), "old-receipt")
	require.False(t, deleted)
	require.ErrorIs(t, err, messaging.ErrQueueUnavailable)
	messages, err := broker.ReceiveMessagesContext(t.Context(), replacement, 1, 0)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, sent.MessageID, messages[0].ID)
}

type messagingCompositionLambdaOwner struct {
	drain func(context.Context) error
	close func(context.Context) error
}

func (owner messagingCompositionLambdaOwner) DrainAsync(ctx context.Context) error {
	return owner.drain(ctx)
}
func (owner messagingCompositionLambdaOwner) Close(ctx context.Context) error {
	return owner.close(ctx)
}

type messagingCompositionRotationOwner struct {
	drain func(context.Context) error
	close func(context.Context) error
}

func (owner messagingCompositionRotationOwner) Drain(ctx context.Context) error {
	return owner.drain(ctx)
}
func (owner messagingCompositionRotationOwner) Close(ctx context.Context) error {
	return owner.close(ctx)
}

func TestMessagingLifecycleJoinsMappingsBeforeLambdaDrain(t *testing.T) {
	entered, release, lambdaDrain := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var order []string
	record := func(name string) { mu.Lock(); order = append(order, name); mu.Unlock() }
	owned := &eventBusLifecycle{
		mappings: contextCloseFunc(func(ctx context.Context) error {
			close(entered)
			select {
			case <-release:
				record("mappings joined")
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}),
		scheduler: contextCloseFunc(func(context.Context) error { record("scheduler joined"); return nil }),
		rotation: messagingCompositionRotationOwner{
			drain: func(context.Context) error { record("rotation drained"); return nil },
			close: func(context.Context) error { record("rotation closed"); return nil },
		},
		functions: messagingCompositionLambdaOwner{
			drain: func(context.Context) error { record("Lambda drained"); close(lambdaDrain); return nil },
			close: func(context.Context) error { record("Lambda closed"); return nil },
		},
		sns: closeFunc(func() error { record("capture closed"); return nil }),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- owned.Close(ctx) }()
	<-entered
	select {
	case <-lambdaDrain:
		t.Fatal("Lambda drain started while a mapping could still invoke it")
	default:
	}
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, []string{"mappings joined", "scheduler joined", "rotation drained", "Lambda drained", "rotation closed", "Lambda closed", "capture closed"}, order)
}

func TestMessagingFailedMappingBarrierDrainsPeersAndRetainsResources(t *testing.T) {
	store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	expected := errors.New("mapping join failed")
	var order []string
	owned := &eventBusLifecycle{
		store:     store,
		mappings:  contextCloseFunc(func(context.Context) error { order = append(order, "mapping failed"); return expected }),
		scheduler: contextCloseFunc(func(context.Context) error { order = append(order, "scheduler joined"); return nil }),
		rotation: messagingCompositionRotationOwner{
			drain: func(context.Context) error { order = append(order, "rotation drained"); return nil },
			close: func(context.Context) error { t.Error("rotation released after failed barrier"); return nil },
		},
		functions: messagingCompositionLambdaOwner{
			drain: func(context.Context) error { order = append(order, "Lambda drained"); return nil },
			close: func(context.Context) error { t.Error("Lambda released after failed barrier"); return nil },
		},
		triggers: contextCloseFunc(func(context.Context) error { t.Error("triggers released after failed barrier"); return nil }),
		sns:      closeFunc(func() error { t.Error("capture released after failed barrier"); return nil }),
	}
	require.ErrorIs(t, owned.Close(t.Context()), expected)
	require.Equal(t, []string{"mapping failed", "scheduler joined", "rotation drained", "Lambda drained"}, order)
	require.NoError(t, readCognitoStore(t.Context(), store), "failed mapping join must retain the live store")
	require.ErrorIs(t, owned.Close(t.Context()), expected, "failed release barrier remains terminal")
	require.Len(t, order, 4, "a second close must not rerun owners or release retained resources")
}
