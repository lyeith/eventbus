package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/devquiescence"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// The executable is an application handler, not an emulator replacement. It
// receives native events through the real private Runtime API and produces its
// cleanup descendant through native SQS HTTP on the trusted callback listener.
func TestRetainedFullStackProvidedProcess(t *testing.T) {
	if os.Getenv("EVENTBUS_FULL_STACK_CHILD") != "1" {
		return
	}
	if err := retainedFullStackInvocation(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	os.Exit(0)
}

func retainedFullStackInvocation() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 7 * time.Second}
	defer client.CloseIdleConnections()
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
		return errors.New("invalid native Runtime API invocation")
	}
	role, directory := os.Getenv("FULL_STACK_ROLE"), os.Getenv("FULL_STACK_OBSERVATIONS")
	observation := messagingCompositionObservation{FunctionARN: response.Header.Get("Lambda-Runtime-Invoked-Function-Arn"), Handler: role, RequestID: id, Event: payload}
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
	operation, result, gate, started := "response", `{"processed":true}`, "", ""
	switch role {
	case "worker":
		var event sqsevent.Event
		if err := json.Unmarshal(payload, &event); err != nil || len(event.Records) != 1 || event.Records[0].EventSource != "aws:sqs" {
			return errors.New("worker expected one native SQS record")
		}
		record := event.Records[0]
		switch record.Body {
		case "retry-once":
			if record.Attributes["ApproximateReceiveCount"] == "1" {
				operation, result = "error", `{"errorType":"FixtureRetry","errorMessage":"retry once"}`
			} else {
				gate, started = "retry-release", "retry-started"
			}
		case "cleanup-descendant":
			gate, started = "descendant-release", "descendant-started"
		case "resumed-suite":
		default:
			return fmt.Errorf("unexpected SQS body %q", record.Body)
		}
	case "cleanup":
		var input struct {
			QueueURL string `json:"queue_url"`
		}
		if err := json.Unmarshal(payload, &input); err != nil || input.QueueURL == "" {
			return errors.New("cleanup expected its exact queue URL")
		}
		body, _ := json.Marshal(map[string]string{"QueueUrl": input.QueueURL, "MessageBody": "cleanup-descendant"})
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("FULL_STACK_CALLBACK")+"/", bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/x-amz-json-1.0")
		request.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
		accepted, err := client.Do(request)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(accepted.Body)
		_ = accepted.Body.Close()
		if readErr != nil || accepted.StatusCode != http.StatusOK {
			return fmt.Errorf("cleanup SQS send returned %d: %s (%v)", accepted.StatusCode, body, readErr)
		}
	case "scheduled":
		if string(payload) != `{"suite":"parked-schedule"}` {
			return fmt.Errorf("unexpected native Scheduler input: %s", payload)
		}
	default:
		return fmt.Errorf("unexpected fixture role %q", role)
	}
	if gate != "" {
		if err := os.WriteFile(filepath.Join(directory, started), []byte(id), 0600); err != nil {
			return err
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(filepath.Join(directory, gate)); err == nil {
				break
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	request, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint+id+"/"+operation, strings.NewReader(result))
	if err != nil {
		return err
	}
	ack, err := client.Do(request)
	if err != nil {
		return err
	}
	_ = ack.Body.Close()
	if ack.StatusCode != http.StatusAccepted {
		return fmt.Errorf("Runtime API acknowledgment returned %d", ack.StatusCode)
	}
	return nil
}

func retainedFullStackJSON(client *http.Client, endpoint, target string, input any) retainedResponse {
	body, err := json.Marshal(input)
	if err != nil {
		return retainedResponse{err: err}
	}
	request, err := http.NewRequest(http.MethodPost, endpoint+"/", bytes.NewReader(body))
	if err != nil {
		return retainedResponse{err: err}
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", target)
	response, err := client.Do(request)
	if err != nil {
		return retainedResponse{err: err}
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	return retainedResponse{status: response.StatusCode, body: raw, err: err}
}

func retainedFullStackBarrier(t *testing.T, pending <-chan retainedResponse) devquiescence.Snapshot {
	t.Helper()
	select {
	case result := <-pending:
		require.NoError(t, result.err)
		require.Equal(t, http.StatusOK, result.status, string(result.body))
		var held devquiescence.Snapshot
		require.NoError(t, json.Unmarshal(result.body, &held))
		require.Equal(t, devquiescence.Held, held.State)
		require.True(t, held.FixtureSafe)
		require.Zero(t, held.WorkCount)
		return held
	case <-time.After(15 * time.Second):
		t.Fatal("production retained barrier did not join accepted native work")
		return devquiescence.Snapshot{}
	}
}

// Production composition proves that the same resources survive three suite
// barriers. Native services own retry, delivery and triggering; only handler
// completion and the S3 HTTP response are gated by this test. Real RustFS and
// full SRP challenge completion are covered by their separate acceptance lanes.
func TestProductionRetainedFullStackCleanupAndResume(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.port, cfg.retainedCallbackPort = retainedTestPort(t), retainedTestPort(t)
	require.NotEqual(t, cfg.port, cfg.retainedCallbackPort)
	source, callback := fmt.Sprintf("http://127.0.0.1:%d", cfg.port), fmt.Sprintf("http://127.0.0.1:%d", cfg.retainedCallbackPort)
	cfg.issuerBase, cfg.schedulerExactSeconds = source, true
	cfg.retainedCleanupFunctions = "stack-cleanup:live"
	cfg.snsLog, cfg.cognitoLog = filepath.Join(cfg.workDir, "sns.jsonl"), filepath.Join(cfg.workDir, "cognito.jsonl")
	observations := filepath.Join(cfg.workDir, "observations")
	require.NoError(t, os.Mkdir(observations, 0700))
	executable, err := os.Executable()
	require.NoError(t, err)
	functions := map[string]lambdaservice.Function{}
	for name, role := range map[string]string{"stack-worker:live": "worker", "stack-cleanup:live": "cleanup", "stack-scheduled:live": "scheduled"} {
		functions[name] = lambdaservice.Function{Runtime: "provided", Command: []string{executable, "-test.run=^TestRetainedFullStackProvidedProcess$"}, Timeout: 5 * time.Second, Environment: map[string]string{
			"EVENTBUS_FULL_STACK_CHILD": "1", "FULL_STACK_ROLE": role, "FULL_STACK_OBSERVATIONS": observations, "FULL_STACK_CALLBACK": callback,
		}}
	}
	recipe, err := yaml.Marshal(lambdaservice.Config{Functions: functions, DevAsync: &lambdaservice.DevAsyncConfig{LogPath: filepath.Join(cfg.workDir, "lambda.jsonl")}})
	require.NoError(t, err)
	cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
	require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))

	const pool, appClient, user = "us-east-1_RetainedStack", "retained-stack-client", "retained-stack-user"
	seed, err := yaml.Marshal(cognito.CognitoSeedFile{Pools: []cognito.CognitoSeedPool{{ID: pool, Region: cfg.region,
		Clients: []cognito.CognitoSeedClient{{ID: appClient}}, Users: []cognito.CognitoSeedUser{{Username: user, Email: "retained@example.test", Password: "RetainedPass!42"}},
	}}})
	require.NoError(t, err)
	cfg.cognitoPools = filepath.Join(cfg.workDir, "pools.yaml")
	require.NoError(t, os.WriteFile(cfg.cognitoPools, seed, 0600))
	trigger := `import fs from 'node:fs/promises';
import path from 'node:path';
export async function handler(event) {
  if (event.triggerSource === 'DefineAuthChallenge_Authentication') {
    await fs.writeFile(path.join(process.env.OBSERVATIONS, 'trigger-started'), JSON.stringify(event));
    for (;;) {
      try { await fs.stat(path.join(process.env.OBSERVATIONS, 'trigger-release')); break; }
      catch { await new Promise(resolve => setTimeout(resolve, 10)); }
    }
    event.response = {challengeName: 'PASSWORD_VERIFIER', issueTokens: false, failAuthentication: false};
  }
  return event;
}
`
	require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "auth.mjs"), []byte(trigger), 0600))
	cfg.cognitoTriggers = filepath.Join(cfg.workDir, "triggers.yaml")
	triggerConfig := fmt.Sprintf("pools:\n  %s:\n    DefineAuthChallenge: {handler: auth.mjs, env: {OBSERVATIONS: %q}}\n    CreateAuthChallenge: {handler: auth.mjs}\n    VerifyAuthChallengeResponse: {handler: auth.mjs}\n", pool, observations)
	require.NoError(t, os.WriteFile(cfg.cognitoTriggers, []byte(triggerConfig), 0600))

	putStarted, putRelease := make(chan []byte, 8), make(chan struct{})
	var releasePut sync.Once
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		putStarted <- body
		<-putRelease
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(sink.Close)
	cfg.s3Endpoint = sink.URL
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg) }()
	t.Cleanup(func() {
		for _, gate := range []string{"retry-release", "descendant-release", "trigger-release"} {
			_ = os.WriteFile(filepath.Join(observations, gate), nil, 0600)
		}
		releasePut.Do(func() { close(putRelease) })
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(35 * time.Second):
			t.Error("production retained full stack failed to join")
		}
	})
	client := &http.Client{Timeout: 20 * time.Second}
	defer client.CloseIdleConnections()
	for _, endpoint := range []string{source, callback} {
		require.Eventually(t, func() bool {
			response, err := client.Get(endpoint + "/health")
			if err != nil {
				return false
			}
			_ = response.Body.Close()
			return response.StatusCode == http.StatusOK
		}, 5*time.Second, 10*time.Millisecond)
	}
	awsConfig := aws.Config{Region: cfg.region, Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1, HTTPClient: client}
	queues := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(source) })
	callbackQueues := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) { options.BaseEndpoint = aws.String(callback) })
	topics := sns.NewFromConfig(awsConfig, func(options *sns.Options) { options.BaseEndpoint = aws.String(source) })
	streams := firehose.NewFromConfig(awsConfig, func(options *firehose.Options) { options.BaseEndpoint = aws.String(source) })
	queue, err := queues.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("stack-owned"), Attributes: map[string]string{"VisibilityTimeout": "6"}})
	require.NoError(t, err)
	sentinel, err := queues.CreateQueue(t.Context(), &sqs.CreateQueueInput{QueueName: aws.String("stack-sentinel")})
	require.NoError(t, err)
	sentinelMessage, err := queues.SendMessage(t.Context(), &sqs.SendMessageInput{QueueUrl: sentinel.QueueUrl, MessageBody: aws.String("preserve-unrelated-fixture")})
	require.NoError(t, err)
	queueARN := fmt.Sprintf("arn:aws:sqs:%s:%s:stack-owned", cfg.region, cfg.accountID)
	workerARN := fmt.Sprintf("arn:aws:lambda:%s:%s:function:stack-worker:live", cfg.region, cfg.accountID)
	mapping := messagingCompositionMapping(t, t.Context(), client, http.MethodPost, source, "", http.StatusAccepted, map[string]any{"EventSourceArn": queueARN, "FunctionName": workerARN, "BatchSize": 1})
	stream, err := streams.CreateDeliveryStream(t.Context(), &firehose.CreateDeliveryStreamInput{DeliveryStreamName: aws.String("stack-stream"), ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
		RoleARN: aws.String("arn:aws:iam::000000000000:role/test"), BucketARN: aws.String("arn:aws:s3:::stack-bucket"), Prefix: aws.String("owned/"), BufferingHints: &firehosetypes.BufferingHints{SizeInMBs: aws.Int32(1), IntervalInSeconds: aws.Int32(60)},
	}})
	require.NoError(t, err)
	topic, err := topics.CreateTopic(t.Context(), &sns.CreateTopicInput{Name: aws.String("stack-topic")})
	require.NoError(t, err)
	_, err = topics.Subscribe(t.Context(), &sns.SubscribeInput{TopicArn: topic.TopicArn, Protocol: aws.String("firehose"), Endpoint: stream.DeliveryStreamARN, Attributes: map[string]string{"SubscriptionRoleArn": "arn:aws:iam::000000000000:role/test", "RawMessageDelivery": "true"}})
	require.NoError(t, err)
	_, err = topics.Publish(t.Context(), &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("accepted-firehose-record")})
	require.NoError(t, err)

	due := time.Now().UTC().Add(3 * time.Second).Truncate(time.Second)
	scheduleInput, err := json.Marshal(map[string]any{"ScheduleExpression": "at(" + due.Format("2006-01-02T15:04:05") + ")", "FlexibleTimeWindow": map[string]string{"Mode": "OFF"}, "Target": map[string]string{
		"Arn": fmt.Sprintf("arn:aws:lambda:%s:%s:function:stack-scheduled:live", cfg.region, cfg.accountID), "RoleArn": "arn:aws:iam::000000000000:role/test", "Input": `{"suite":"parked-schedule"}`,
	}})
	require.NoError(t, err)
	createdSchedule := retainedHTTP(t, client, http.MethodPost, source+"/schedules/stack-future", string(scheduleInput), "application/json", http.StatusOK)
	_, err = queues.SendMessage(t.Context(), &sqs.SendMessageInput{QueueUrl: queue.QueueUrl, MessageBody: aws.String("retry-once")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		got, err := messagingCompositionObservations(observations)
		return err == nil && len(got) == 1 && got[0].Handler == "worker"
	}, 3*time.Second, 10*time.Millisecond)
	quiesced := make(chan retainedResponse, 1)
	go func() {
		quiesced <- retainedRequest(client, http.MethodPost, source+devquiescence.ControlPath+"/quiesce", `{"timeout_ms":15000}`, "application/json")
	}()
	require.Eventually(t, func() bool {
		status := retainedControl(t, client, source, "", "", http.StatusOK)
		custody, executing := false, false
		for _, activity := range status.Activities {
			custody = custody || activity.Kind == "sqs_message"
			executing = executing || activity.Kind == "lambda_invoke"
		}
		return status.State == devquiescence.Draining && custody && !executing && !status.FixtureSafe
	}, 3*time.Second, 10*time.Millisecond, "invisible native retries retain custody even with no executing Lambda")
	attributes, err := callbackQueues.GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{QueueUrl: queue.QueueUrl, AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
	require.NoError(t, err)
	require.Equal(t, "1", attributes.Attributes["ApproximateNumberOfMessagesNotVisible"])
	select {
	case object := <-putStarted:
		require.Equal(t, "accepted-firehose-record", string(object))
	case <-time.After(3 * time.Second):
		t.Fatal("production drain did not flush below-threshold SNS Firehose work")
	}
	select {
	case result := <-quiesced:
		t.Fatalf("barrier certified invisible retry and pending S3 response: %+v", result)
	default:
	}
	releasePut.Do(func() { close(putRelease) })
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(observations, "retry-started")); return err == nil }, 8*time.Second, 10*time.Millisecond)
	require.NoError(t, os.WriteFile(filepath.Join(observations, "retry-release"), nil, 0600))
	held := retainedFullStackBarrier(t, quiesced)
	require.False(t, time.Now().UTC().Before(due), "the schedule must have become due while the source fence was closed")
	got, err := messagingCompositionObservations(observations)
	require.NoError(t, err)
	require.Len(t, got, 2, "future Scheduler dispatch remains parked through the fence")
	var first, retried sqsevent.Event
	require.NoError(t, json.Unmarshal(got[0].Event, &first))
	require.NoError(t, json.Unmarshal(got[1].Event, &retried))
	require.Len(t, first.Records, 1)
	require.Len(t, retried.Records, 1)
	require.Equal(t, workerARN, got[0].FunctionARN)
	require.Equal(t, workerARN, got[1].FunctionARN)
	if first.Records[0].Attributes["ApproximateReceiveCount"] == "2" {
		first, retried = retried, first
	}
	require.Equal(t, first.Records[0].MessageID, retried.Records[0].MessageID)
	require.Equal(t, "1", first.Records[0].Attributes["ApproximateReceiveCount"])
	require.Equal(t, "2", retried.Records[0].Attributes["ApproximateReceiveCount"])

	cleanupBody, err := json.Marshal(map[string]string{"queue_url": *queue.QueueUrl})
	require.NoError(t, err)
	retainedHTTP(t, client, http.MethodPost, callback+"/2015-03-31/functions/stack-worker:live/invocations", string(cleanupBody), "application/json", http.StatusServiceUnavailable)
	result := retainedHTTP(t, client, http.MethodPost, callback+"/2015-03-31/functions/stack-cleanup:live/invocations", string(cleanupBody), "application/json", http.StatusOK)
	require.JSONEq(t, `{"processed":true}`, string(result))
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(observations, "descendant-started")); return err == nil }, 3*time.Second, 10*time.Millisecond)
	pendingCleanup := retainedControl(t, client, source, "", "", http.StatusOK)
	require.Equal(t, devquiescence.Draining, pendingCleanup.State)
	require.False(t, pendingCleanup.FixtureSafe)
	require.Equal(t, held.Generation, pendingCleanup.Generation)
	retainedControl(t, client, source, "/resume", fmt.Sprintf(`{"generation":%d}`, held.Generation), http.StatusConflict)
	rejoined := make(chan retainedResponse, 1)
	go func() {
		rejoined <- retainedRequest(client, http.MethodPost, source+devquiescence.ControlPath+"/quiesce", `{"timeout_ms":8000}`, "application/json")
	}()
	blocked := retainedFullStackJSON(client, source, "AmazonSQS.SendMessage", map[string]string{"QueueUrl": *queue.QueueUrl, "MessageBody": "blocked-root"})
	require.NoError(t, blocked.err)
	require.Equal(t, http.StatusServiceUnavailable, blocked.status, string(blocked.body))
	select {
	case result := <-rejoined:
		t.Fatalf("cleanup barrier returned before its gated SQS descendant joined: %+v", result)
	default:
	}
	require.NoError(t, os.WriteFile(filepath.Join(observations, "descendant-release"), nil, 0600))
	afterCleanup := retainedFullStackBarrier(t, rejoined)
	require.Equal(t, held.Generation, afterCleanup.Generation)
	resumed := retainedControl(t, client, source, "/resume", fmt.Sprintf(`{"generation":%d}`, held.Generation), http.StatusOK)
	require.Equal(t, held.Generation+1, resumed.Generation)
	require.Eventually(t, func() bool {
		got, err := messagingCompositionObservations(observations)
		if err != nil {
			return false
		}
		for _, observation := range got {
			if observation.Handler == "scheduled" {
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond, "same parked schedule dispatches after resume")
	currentMapping := messagingCompositionMapping(t, t.Context(), client, http.MethodGet, source, mapping.UUID, http.StatusOK, nil)
	require.Equal(t, mapping.UUID, currentMapping.UUID)
	require.Equal(t, "Enabled", currentMapping.State)
	require.Equal(t, queueARN, currentMapping.EventSourceARN)
	preserved, err := queues.ReceiveMessage(t.Context(), &sqs.ReceiveMessageInput{QueueUrl: sentinel.QueueUrl, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, preserved.Messages, 1)
	require.Equal(t, *sentinelMessage.MessageId, *preserved.Messages[0].MessageId)
	require.Equal(t, "preserve-unrelated-fixture", *preserved.Messages[0].Body)
	_, err = queues.DeleteMessage(t.Context(), &sqs.DeleteMessageInput{QueueUrl: sentinel.QueueUrl, ReceiptHandle: preserved.Messages[0].ReceiptHandle})
	require.NoError(t, err)
	_, err = queues.SendMessage(t.Context(), &sqs.SendMessageInput{QueueUrl: queue.QueueUrl, MessageBody: aws.String("resumed-suite")})
	require.NoError(t, err)

	auth := make(chan retainedResponse, 1)
	go func() {
		auth <- retainedFullStackJSON(client, source, "AWSCognitoIdentityProviderService.AdminInitiateAuth", map[string]any{"UserPoolId": pool, "ClientId": appClient, "AuthFlow": "CUSTOM_AUTH", "AuthParameters": map[string]string{"USERNAME": user, "CHALLENGE_NAME": "SRP_A", "SRP_A": "2"}})
	}()
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(observations, "trigger-started")); return err == nil }, 3*time.Second, 10*time.Millisecond)
	triggerBarrier := make(chan retainedResponse, 1)
	go func() {
		triggerBarrier <- retainedRequest(client, http.MethodPost, source+devquiescence.ControlPath+"/quiesce", `{"timeout_ms":8000}`, "application/json")
	}()
	require.Eventually(t, func() bool {
		return retainedControl(t, client, source, "", "", http.StatusOK).State == devquiescence.Draining
	}, time.Second, 10*time.Millisecond)
	select {
	case result := <-triggerBarrier:
		t.Fatalf("barrier returned before configured Cognito child joined: %+v", result)
	default:
	}
	require.NoError(t, os.WriteFile(filepath.Join(observations, "trigger-release"), nil, 0600))
	authResult := <-auth
	require.NoError(t, authResult.err)
	require.Equal(t, http.StatusOK, authResult.status, string(authResult.body))
	var authChallenge struct {
		ChallengeName string `json:"ChallengeName"`
	}
	require.NoError(t, json.Unmarshal(authResult.body, &authChallenge))
	require.Equal(t, "PASSWORD_VERIFIER", authChallenge.ChallengeName)
	triggerEvent, err := os.ReadFile(filepath.Join(observations, "trigger-started"))
	require.NoError(t, err)
	var nativeTrigger struct {
		Pool     string `json:"userPoolId"`
		Username string `json:"userName"`
		Source   string `json:"triggerSource"`
		Caller   struct {
			Client string `json:"clientId"`
		} `json:"callerContext"`
	}
	require.NoError(t, json.Unmarshal(triggerEvent, &nativeTrigger))
	require.Equal(t, pool, nativeTrigger.Pool)
	require.Equal(t, user, nativeTrigger.Username)
	require.Equal(t, appClient, nativeTrigger.Caller.Client)
	require.Equal(t, "DefineAuthChallenge_Authentication", nativeTrigger.Source)
	last := retainedFullStackBarrier(t, triggerBarrier)
	require.Equal(t, resumed.Generation, last.Generation)
	retainedControl(t, client, source, "/resume", fmt.Sprintf(`{"generation":%d}`, last.Generation), http.StatusOK)
	preservedSchedule := retainedHTTP(t, client, http.MethodGet, source+"/schedules/stack-future", "", "", http.StatusOK)
	var originalSchedule map[string]string
	var currentSchedule struct {
		Arn string `json:"Arn"`
	}
	require.NoError(t, json.Unmarshal(createdSchedule, &originalSchedule))
	require.NoError(t, json.Unmarshal(preservedSchedule, &currentSchedule))
	require.Equal(t, originalSchedule["ScheduleArn"], currentSchedule.Arn)
	described, err := streams.DescribeDeliveryStream(t.Context(), &firehose.DescribeDeliveryStreamInput{DeliveryStreamName: aws.String("stack-stream")})
	require.NoError(t, err)
	require.Equal(t, *stream.DeliveryStreamARN, *described.DeliveryStreamDescription.DeliveryStreamARN)
	got, err = messagingCompositionObservations(observations)
	require.NoError(t, err)
	require.Len(t, got, 6, "two retry deliveries, cleanup root and descendant, parked Scheduler, and resumed suite")
}
