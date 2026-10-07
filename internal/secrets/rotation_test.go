package secrets

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/require"
)

const rotationARN = "arn:aws:lambda:us-east-1:000000000000:function:rotate"

type rotationInvoker struct {
	validate func(context.Context, string) error
	invoke   func(context.Context, string, RotationEvent) error
}

func (invoker rotationInvoker) ValidateRotationTarget(ctx context.Context, name string) error {
	if invoker.validate != nil {
		return invoker.validate(ctx, name)
	}
	if name != rotationARN {
		return errors.New("unregistered target")
	}
	return nil
}
func (invoker rotationInvoker) InvokeRotation(ctx context.Context, name string, event RotationEvent) error {
	return invoker.invoke(ctx, name, event)
}
func waitOutcome(t *testing.T, outcomes <-chan RotationOutcome) RotationOutcome {
	t.Helper()
	select {
	case result := <-outcomes:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("rotation did not join/report")
		return RotationOutcome{}
	}
}
func closeRotation(t *testing.T, service *RotationService) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, service.Close(ctx))
}

// This is application-owned rotation policy expressed through the real Go AWS
// SDK, including handler reentry into Secrets over HTTP. It cannot deadlock on
// the store lock or substitute direct fixture state changes for AWS operations.
func rotateThroughSDK(ctx context.Context, client *secretsmanager.Client, event RotationEvent) error {
	metadata, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(event.SecretID)})
	if err != nil {
		return err
	}
	stages, exists := metadata.VersionIdsToStages[event.ClientRequestToken]
	if !exists {
		return errors.New("rotation token not reserved")
	}
	for _, stage := range stages {
		if stage == "AWSCURRENT" {
			return nil
		}
	}
	switch event.Step {
	case "createSecret":
		_, err = client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(event.SecretID), VersionId: aws.String(event.ClientRequestToken), VersionStage: aws.String("AWSPENDING")})
		if err == nil {
			return nil
		}
		var absent *smtypes.ResourceNotFoundException
		if !errors.As(err, &absent) {
			return err
		}
		password, err := client.GetRandomPassword(ctx, &secretsmanager.GetRandomPasswordInput{PasswordLength: aws.Int64(24), ExcludePunctuation: aws.Bool(true)})
		if err != nil {
			return err
		}
		_, err = client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{SecretId: aws.String(event.SecretID), ClientRequestToken: aws.String(event.ClientRequestToken), SecretString: password.RandomPassword, VersionStages: []string{"AWSPENDING"}})
		return err
	case "setSecret", "testSecret":
		value, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(event.SecretID), VersionId: aws.String(event.ClientRequestToken), VersionStage: aws.String("AWSPENDING")})
		if err == nil && len(aws.ToString(value.SecretString)) != 24 {
			return errors.New("unexpected pending data")
		}
		return err
	case "finishSecret":
		current := ""
		for id, labels := range metadata.VersionIdsToStages {
			for _, label := range labels {
				if label == "AWSCURRENT" {
					current = id
				}
			}
		}
		_, err = client.UpdateSecretVersionStage(ctx, &secretsmanager.UpdateSecretVersionStageInput{SecretId: aws.String(event.SecretID), VersionStage: aws.String("AWSCURRENT"), MoveToVersionId: aws.String(event.ClientRequestToken), RemoveFromVersionId: aws.String(current)})
		return err
	default:
		return errors.New("invalid rotation step")
	}
}

func TestRotateSDKAsyncFourStepsReadbackAndReplay(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	outcomes := make(chan RotationOutcome, 8)
	enteredTest, releaseTest := make(chan struct{}), make(chan struct{})
	var client *secretsmanager.Client
	var mutex sync.Mutex
	steps := []string{}
	invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, event RotationEvent) error {
		mutex.Lock()
		steps = append(steps, event.Step)
		mutex.Unlock()
		if event.Step == "testSecret" {
			close(enteredTest)
			select {
			case <-releaseTest:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return rotateThroughSDK(ctx, client, event)
	}}
	service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	serving := httptest.NewServer(NewHandler(store, service))
	defer serving.Close()
	defer closeRotation(t, service)
	client = secretsSDK(serving.URL)
	created, err := client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{Name: aws.String("rotation-sdk"), SecretString: aws.String("original"), ClientRequestToken: aws.String(strings.Repeat("1", 32))})
	require.NoError(t, err)
	token := strings.Repeat("2", 32)
	rotated, err := client.RotateSecret(t.Context(), &secretsmanager.RotateSecretInput{SecretId: created.ARN, RotationLambdaARN: aws.String(rotationARN), ClientRequestToken: aws.String(token)})
	require.NoError(t, err)
	require.Equal(t, token, aws.ToString(rotated.VersionId))
	select {
	case <-enteredTest:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach asynchronous test step")
	}
	current, err := client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: created.ARN})
	require.NoError(t, err)
	require.Equal(t, "original", aws.ToString(current.SecretString))
	pending, err := client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: created.ARN, VersionStage: aws.String("AWSPENDING")})
	require.NoError(t, err)
	require.Equal(t, token, aws.ToString(pending.VersionId))
	require.Len(t, aws.ToString(pending.SecretString), 24)
	replay, err := client.RotateSecret(t.Context(), &secretsmanager.RotateSecretInput{SecretId: created.ARN, ClientRequestToken: aws.String(token)})
	require.NoError(t, err)
	require.Equal(t, token, aws.ToString(replay.VersionId))
	_, err = client.RotateSecret(t.Context(), &secretsmanager.RotateSecretInput{SecretId: created.ARN, ClientRequestToken: aws.String(strings.Repeat("3", 32))})
	var inProgress *smtypes.InvalidRequestException
	require.ErrorAs(t, err, &inProgress)
	close(releaseTest)
	outcome := waitOutcome(t, outcomes)
	require.Equal(t, "succeeded", outcome.Status)
	require.Equal(t, "finishSecret", outcome.Step)
	require.Equal(t, token, outcome.VersionID)
	current, err = client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: created.ARN})
	require.NoError(t, err)
	require.Equal(t, token, aws.ToString(current.VersionId))
	require.Equal(t, aws.ToString(pending.SecretString), aws.ToString(current.SecretString))
	previous, err := client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: created.ARN, VersionStage: aws.String("AWSPREVIOUS")})
	require.NoError(t, err)
	require.Equal(t, "original", aws.ToString(previous.SecretString))
	metadata, err := client.DescribeSecret(t.Context(), &secretsmanager.DescribeSecretInput{SecretId: created.ARN})
	require.NoError(t, err)
	require.True(t, aws.ToBool(metadata.RotationEnabled))
	require.Equal(t, rotationARN, aws.ToString(metadata.RotationLambdaARN))
	require.NotNil(t, metadata.LastRotatedDate)
	_, err = client.RotateSecret(t.Context(), &secretsmanager.RotateSecretInput{SecretId: created.ARN, ClientRequestToken: aws.String(token)})
	require.NoError(t, err)
	mutex.Lock()
	require.Equal(t, []string{"createSecret", "setSecret", "testSecret", "finishSecret"}, steps)
	mutex.Unlock()
}

func TestRotationFailureRetainsPendingAndSameTokenRetry(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	outcomes := make(chan RotationOutcome, 8)
	var client *secretsmanager.Client
	var fail atomic.Bool
	fail.Store(true)
	invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, event RotationEvent) error {
		if event.Step == "setSecret" && fail.Load() {
			return errors.New("private password must never enter outcome")
		}
		return rotateThroughSDK(ctx, client, event)
	}}
	service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	serving := httptest.NewServer(NewHandler(store, service))
	defer serving.Close()
	defer closeRotation(t, service)
	client = secretsSDK(serving.URL)
	_, err := client.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{Name: aws.String("retry"), SecretString: aws.String("original")})
	require.NoError(t, err)
	token := strings.Repeat("2", 32)
	input := &secretsmanager.RotateSecretInput{SecretId: aws.String("retry"), RotationLambdaARN: aws.String(rotationARN), ClientRequestToken: aws.String(token)}
	_, err = client.RotateSecret(t.Context(), input)
	require.NoError(t, err)
	outcome := waitOutcome(t, outcomes)
	require.Equal(t, "handler_failure", outcome.Status)
	require.Equal(t, "setSecret", outcome.Step)
	require.NotContains(t, fmt.Sprintf("%+v", outcome), "private password")
	firstPending, err := client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: input.SecretId, VersionStage: aws.String("AWSPENDING")})
	require.NoError(t, err)
	current, err := client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: input.SecretId})
	require.NoError(t, err)
	require.Equal(t, "original", aws.ToString(current.SecretString))
	fail.Store(false)
	_, err = client.RotateSecret(t.Context(), input)
	require.NoError(t, err)
	require.Equal(t, "succeeded", waitOutcome(t, outcomes).Status)
	current, err = client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: input.SecretId})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(firstPending.SecretString), aws.ToString(current.SecretString), "retry must reuse immutable pending data")
}

func TestRotationAdmissionRefusalAndEmptyPending(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	original, err := store.Create(CreateInput{Name: "admission", Value: stringValue("original")})
	require.NoError(t, err)
	entered := make(chan RotationEvent, 1)
	invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, event RotationEvent) error {
		entered <- event
		<-ctx.Done()
		return ctx.Err()
	}}
	service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, MaxPending: 1, Observer: func(RotationOutcome) {}})
	defer func() { _, err := service.Cancel("admission"); require.NoError(t, err); closeRotation(t, service) }()
	for _, input := range []RotateInput{
		{SecretID: "admission", RotationLambdaARN: "invalid"},
		{SecretID: "admission", RotationLambdaARN: "arn:aws:lambda:us-east-1:000000000000:function:missing"},
		{SecretID: "admission", RotationLambdaARN: rotationARN, RotationRules: &RotationRules{AutomaticallyAfterDays: integer(30)}},
		{SecretID: "admission", RotationLambdaARN: rotationARN, RotationRules: &RotationRules{ScheduleExpression: "rate(30 days)"}},
		{SecretID: "admission", RotationLambdaARN: rotationARN, ClientRequestToken: "short"},
	} {
		_, err := service.Rotate(t.Context(), input)
		requireAPICode(t, err, "InvalidParameterException")
	}
	before, err := store.Describe("admission")
	require.NoError(t, err)
	require.Nil(t, before.RotationEnabled)
	require.Len(t, before.VersionIdsToStages, 1)
	token := strings.Repeat("2", 32)
	_, err = service.Rotate(t.Context(), RotateInput{SecretID: "admission", RotationLambdaARN: rotationARN, ClientRequestToken: token})
	require.NoError(t, err)
	event := <-entered
	require.Equal(t, "createSecret", event.Step)
	require.Equal(t, original.ARN, event.SecretID)
	_, err = store.GetValue("admission", token, "AWSPENDING")
	requireAPICode(t, err, "ResourceNotFoundException")
	after, err := store.Describe("admission")
	require.NoError(t, err)
	require.Equal(t, []string{"AWSPENDING"}, after.VersionIdsToStages[token])
	_, err = store.UpdateVersionStage(StageInput{SecretID: "admission", VersionStage: "AWSCURRENT", MoveToVersionID: token, RemoveFromVersionID: original.VersionID})
	requireAPICode(t, err, "InvalidRequestException")
	_, err = store.Create(CreateInput{Name: "another", Value: stringValue("original")})
	require.NoError(t, err)
	_, err = service.Rotate(t.Context(), RotateInput{SecretID: "another", RotationLambdaARN: rotationARN})
	requireAPICode(t, err, "LimitExceededException")
	another, err := store.Describe("another")
	require.NoError(t, err)
	require.Nil(t, another.RotationEnabled)
}

func TestRotationTestOnlyCancelTimeoutAndClose(t *testing.T) {
	t.Run("test-only", func(t *testing.T) {
		for _, fail := range []bool{false, true} {
			t.Run(fmt.Sprintf("failure-%t", fail), func(t *testing.T) {
				store := NewSecretsStore("us-east-1", "000000000000")
				_, err := store.Create(CreateInput{Name: "test-only", Value: stringValue("original")})
				require.NoError(t, err)
				outcomes := make(chan RotationOutcome, 1)
				invoker := rotationInvoker{invoke: func(_ context.Context, _ string, event RotationEvent) error {
					if event.Step != "testSecret" {
						return errors.New("must only test configuration")
					}
					pending, err := store.Describe(event.SecretID)
					if err != nil {
						return err
					}
					if len(pending.VersionIdsToStages[event.ClientRequestToken]) != 1 {
						return errors.New("pending not reserved")
					}
					value, err := store.GetValue(event.SecretID, event.ClientRequestToken, "AWSPENDING")
					if err != nil {
						return err
					}
					if value.SecretString != "original" {
						return errors.New("test-only pending must use existing credentials")
					}
					if fail {
						return errors.New("private test failure")
					}
					return nil
				}}
				service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
				defer closeRotation(t, service)
				result, err := service.Rotate(t.Context(), RotateInput{SecretID: "test-only", RotationLambdaARN: rotationARN, RotateImmediately: boolean(false)})
				require.NoError(t, err)
				expected := "succeeded"
				if fail {
					expected = "handler_failure"
				}
				require.Equal(t, expected, waitOutcome(t, outcomes).Status)
				metadata, err := store.Describe("test-only")
				require.NoError(t, err)
				require.Len(t, metadata.VersionIdsToStages, 1)
				require.Zero(t, metadata.LastRotatedDate)
				_, err = store.GetValue("test-only", result.VersionID, "")
				requireAPICode(t, err, "ResourceNotFoundException")
				current, err := store.GetValue("test-only", "", "")
				require.NoError(t, err)
				require.Equal(t, "original", current.SecretString)
			})
		}
	})
	t.Run("timeout", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		_, err := store.Create(CreateInput{Name: "timeout", Value: stringValue("original")})
		require.NoError(t, err)
		outcomes := make(chan RotationOutcome, 1)
		invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, _ RotationEvent) error { <-ctx.Done(); return ctx.Err() }}
		service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 2, AttemptTimeout: 10 * time.Millisecond, RetryDelay: time.Millisecond, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
		defer closeRotation(t, service)
		_, err = service.Rotate(t.Context(), RotateInput{SecretID: "timeout", RotationLambdaARN: rotationARN})
		require.NoError(t, err)
		outcome := waitOutcome(t, outcomes)
		require.Equal(t, "timed_out", outcome.Status)
		require.Equal(t, 2, outcome.Attempts)
		current, err := store.GetValue("timeout", "", "")
		require.NoError(t, err)
		require.Equal(t, "original", current.SecretString)
	})
	t.Run("cancel-and-graceful-close", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		_, err := store.Create(CreateInput{Name: "cancel", Value: stringValue("original")})
		require.NoError(t, err)
		entered, release := make(chan struct{}), make(chan struct{})
		outcomes := make(chan RotationOutcome, 1)
		invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, _ RotationEvent) error {
			close(entered)
			<-ctx.Done()
			<-release
			return ctx.Err()
		}}
		service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
		defer func() {
			select {
			case <-release:
			default:
				close(release)
			}
			closeRotation(t, service)
		}()
		token := strings.Repeat("2", 32)
		_, err = service.Rotate(t.Context(), RotateInput{SecretID: "cancel", RotationLambdaARN: rotationARN, ClientRequestToken: token})
		require.NoError(t, err)
		<-entered
		canceled, err := service.Cancel("cancel")
		require.NoError(t, err)
		require.Equal(t, token, canceled.VersionID)
		require.False(t, *canceled.RotationEnabled)
		close(release)
		closeRotation(t, service)
		require.Equal(t, "canceled", waitOutcome(t, outcomes).Status)
		_, err = service.Rotate(t.Context(), RotateInput{SecretID: "cancel", RotationLambdaARN: rotationARN, ClientRequestToken: token})
		requireAPICode(t, err, "InternalServiceError")
		current, err := store.GetValue("cancel", "", "")
		require.NoError(t, err)
		require.Equal(t, "original", current.SecretString)
		metadata, err := store.Describe("cancel")
		require.NoError(t, err)
		require.Equal(t, []string{"AWSPENDING"}, metadata.VersionIdsToStages[token], "cancel leaves labels for native caller cleanup")
	})
	t.Run("invalid-finish-is-not-success", func(t *testing.T) {
		store := NewSecretsStore("us-east-1", "000000000000")
		_, err := store.Create(CreateInput{Name: "unfinished", Value: stringValue("original")})
		require.NoError(t, err)
		outcomes := make(chan RotationOutcome, 1)
		service := NewRotationService(store, rotationInvoker{invoke: func(context.Context, string, RotationEvent) error { return nil }}, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
		defer closeRotation(t, service)
		_, err = service.Rotate(t.Context(), RotateInput{SecretID: "unfinished", RotationLambdaARN: rotationARN})
		require.NoError(t, err)
		require.Equal(t, "invalid_finish", waitOutcome(t, outcomes).Status)
		current, err := store.GetValue("unfinished", "", "")
		require.NoError(t, err)
		require.Equal(t, "original", current.SecretString)
	})
}

func TestRotationDrainDeadlineCancelsQueuedTestAndCleansItsPending(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	for _, name := range []string{"running", "queued-test"} {
		_, err := store.Create(CreateInput{Name: name, Value: stringValue("original")})
		require.NoError(t, err)
	}
	started := make(chan struct{})
	outcomes := make(chan RotationOutcome, 2)
	var calls atomic.Int32
	invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, _ RotationEvent) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		return ctx.Err()
	}}
	service := NewRotationService(store, invoker, RotationOptions{MaxConcurrent: 1, MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	defer func() { require.ErrorIs(t, service.Close(context.Background()), context.DeadlineExceeded) }()
	_, err := service.Rotate(t.Context(), RotateInput{SecretID: "running", RotationLambdaARN: rotationARN})
	require.NoError(t, err)
	<-started
	queued, err := service.Rotate(t.Context(), RotateInput{SecretID: "queued-test", RotationLambdaARN: rotationARN, RotateImmediately: boolean(false)})
	require.NoError(t, err)
	pending, err := store.GetValue("queued-test", queued.VersionID, "AWSPENDING")
	require.NoError(t, err)
	require.Equal(t, "original", pending.SecretString)
	drainCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, service.Drain(drainCtx), context.DeadlineExceeded)
	for index := 0; index < 2; index++ {
		require.Equal(t, "canceled", waitOutcome(t, outcomes).Status)
	}
	require.Equal(t, int32(1), calls.Load(), "closed queued work must never enter a handler")
	_, err = store.GetValue("queued-test", queued.VersionID, "")
	requireAPICode(t, err, "ResourceNotFoundException")
	current, err := store.GetValue("queued-test", "", "")
	require.NoError(t, err)
	require.Equal(t, "original", current.SecretString)
}

func TestRotationDrainPreservesSDKCallbacksAndRefusesLateAdmission(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	_, err := store.Create(CreateInput{Name: "drain", Value: stringValue("original")})
	require.NoError(t, err)
	outcomes := make(chan RotationOutcome, 1)
	entered, release := make(chan struct{}), make(chan struct{})
	var client *secretsmanager.Client
	invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, event RotationEvent) error {
		if event.Step == "testSecret" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return rotateThroughSDK(ctx, client, event)
	}}
	service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	serving := httptest.NewServer(NewHandler(store, service))
	defer serving.Close()
	defer closeRotation(t, service)
	client = secretsSDK(serving.URL)
	token := strings.Repeat("2", 32)
	_, err = client.RotateSecret(t.Context(), &secretsmanager.RotateSecretInput{SecretId: aws.String("drain"), ClientRequestToken: aws.String(token), RotationLambdaARN: aws.String(rotationARN)})
	require.NoError(t, err)
	<-entered
	drainDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		drainDone <- service.Drain(ctx)
	}()
	require.Eventually(t, func() bool { service.mu.Lock(); defer service.mu.Unlock(); return service.closed }, time.Second, time.Millisecond)
	_, err = service.Rotate(t.Context(), RotateInput{SecretID: "drain", ClientRequestToken: strings.Repeat("3", 32)})
	requireAPICode(t, err, "InternalServiceError")
	select {
	case err := <-drainDone:
		t.Fatalf("drain returned before accepted job completed: %v", err)
	default:
	}
	close(release)
	require.Equal(t, "succeeded", waitOutcome(t, outcomes).Status)
	require.NoError(t, <-drainDone)
	current, err := client.GetSecretValue(t.Context(), &secretsmanager.GetSecretValueInput{SecretId: aws.String("drain")})
	require.NoError(t, err)
	require.Equal(t, token, aws.ToString(current.VersionId))
	require.NoError(t, service.Drain(context.Background()))
}

func TestRotationDrainDeadlineCancelsAndJoinsBeforeReturningRetainedError(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	_, err := store.Create(CreateInput{Name: "drain-abort", Value: stringValue("original")})
	require.NoError(t, err)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	outcomes := make(chan RotationOutcome, 1)
	invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, _ RotationEvent) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	}}
	service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	_, err = service.Rotate(t.Context(), RotateInput{SecretID: "drain-abort", RotationLambdaARN: rotationARN})
	require.NoError(t, err)
	<-entered
	drainDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		drainDone <- service.Drain(ctx)
	}()
	<-canceled
	select {
	case err := <-drainDone:
		t.Fatalf("deadline returned while owned callback remained unjoined: %v", err)
	default:
	}
	close(release)
	require.ErrorIs(t, <-drainDone, context.DeadlineExceeded)
	require.Equal(t, "canceled", waitOutcome(t, outcomes).Status)
	require.ErrorIs(t, service.Drain(context.Background()), context.DeadlineExceeded)
	require.ErrorIs(t, service.Close(context.Background()), context.DeadlineExceeded)
	current, err := store.GetValue("drain-abort", "", "")
	require.NoError(t, err)
	require.Equal(t, "original", current.SecretString)
}

func TestCompletedRotationDrainWinsOverCanceledCaller(t *testing.T) {
	service := NewRotationService(NewSecretsStore("us-east-1", "000000000000"), rotationInvoker{}, RotationOptions{})
	require.NoError(t, service.Drain(context.Background()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for index := 0; index < 100; index++ {
		require.NoError(t, service.Drain(ctx))
	}
	require.NoError(t, service.Close(ctx))
}

func TestRotationRuntimeTimeoutSentinelIsRedactedTimedOut(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	_, err := store.Create(CreateInput{Name: "runtime-timeout", Value: stringValue("original")})
	require.NoError(t, err)
	outcomes := make(chan RotationOutcome, 1)
	service := NewRotationService(store, rotationInvoker{invoke: func(context.Context, string, RotationEvent) error {
		return fmt.Errorf("redacted runtime timeout: %w", context.DeadlineExceeded)
	}}, RotationOptions{MaxAttempts: 1, AttemptTimeout: time.Minute, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	defer closeRotation(t, service)
	_, err = service.Rotate(t.Context(), RotateInput{SecretID: "runtime-timeout", RotationLambdaARN: rotationARN})
	require.NoError(t, err)
	outcome := waitOutcome(t, outcomes)
	require.Equal(t, "timed_out", outcome.Status)
	require.Equal(t, 1, outcome.Attempts)
	require.NotContains(t, fmt.Sprintf("%+v", outcome), "redacted runtime timeout")
}

func TestRotationCancellationDuringAdmissionHasNoStateSideEffects(t *testing.T) {
	for _, holdLock := range []bool{false, true} {
		t.Run(fmt.Sprintf("waiting-lock-%t", holdLock), func(t *testing.T) {
			store := NewSecretsStore("us-east-1", "000000000000")
			_, err := store.Create(CreateInput{Name: "canceled-admission", Value: stringValue("original")})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			validated := make(chan struct{})
			invoker := rotationInvoker{validate: func(context.Context, string) error {
				if !holdLock {
					cancel()
				}
				close(validated)
				return nil
			}, invoke: func(context.Context, string, RotationEvent) error {
				return errors.New("canceled admission reached execution")
			}}
			service := NewRotationService(store, invoker, RotationOptions{})
			defer closeRotation(t, service)
			if holdLock {
				service.mu.Lock()
			}
			result := make(chan error, 1)
			go func() {
				_, err := service.Rotate(ctx, RotateInput{SecretID: "canceled-admission", RotationLambdaARN: rotationARN})
				result <- err
			}()
			<-validated
			if holdLock {
				cancel()
				service.mu.Unlock()
			}
			requireAPICode(t, <-result, "InvalidRequestException")
			metadata, err := store.Describe("canceled-admission")
			require.NoError(t, err)
			require.Nil(t, metadata.RotationEnabled)
			require.Len(t, metadata.VersionIdsToStages, 1)
		})
	}
}
