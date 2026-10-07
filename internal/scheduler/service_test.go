package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTargetARN = "arn:aws:lambda:us-east-1:000000000000:function:scheduled:live"

var testNow = time.Date(2030, 1, 2, 15, 4, 20, 0, time.UTC)

type testInvoker struct {
	validate func(context.Context, string) error
	admit    func(context.Context, string, []byte) error
}

func (invoker testInvoker) ValidateTarget(ctx context.Context, arn string) error {
	if invoker.validate != nil {
		return invoker.validate(ctx, arn)
	}
	return ctx.Err()
}
func (invoker testInvoker) AdmitTarget(ctx context.Context, arn string, payload []byte) error {
	if invoker.admit != nil {
		return invoker.admit(ctx, arn, payload)
	}
	return ctx.Err()
}

type testAdmissionError struct{ retry bool }

func (err testAdmissionError) Error() string   { return "fixture admission failure with private payload" }
func (err testAdmissionError) Retryable() bool { return err.retry }

func testService(t *testing.T, invoker TargetInvoker, dev DevOptions) *Service {
	t.Helper()
	return testServiceWithCloseError(t, invoker, dev, nil)
}

func testServiceWithCloseError(t *testing.T, invoker TargetInvoker, dev DevOptions, wantCloseError error) *Service {
	t.Helper()
	if dev.Clock == nil {
		dev.Clock = func() time.Time { return testNow }
	}
	service, err := New(Options{Region: "us-east-1", AccountID: "000000000000", Dev: dev}, invoker)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if wantCloseError == nil {
			require.NoError(t, service.Close(ctx))
		} else {
			require.ErrorIs(t, service.Close(ctx), wantCloseError)
		}
	})
	return service
}

func testCreate(name string) CreateInput {
	payload := "{\"job\":\"owned\",\"nested\":{\"value\":1}}"
	return CreateInput{
		Name: name, ClientToken: "token-" + name, State: "DISABLED",
		ScheduleExpression: "at(" + testNow.Add(time.Hour).Format("2006-01-02T15:04:05") + ")",
		FlexibleTimeWindow: &FlexibleTimeWindow{Mode: "OFF"},
		Target:             &Target{Arn: testTargetARN, RoleArn: "arn:aws:iam::000000000000:role/local/scheduler", Input: &payload},
	}
}

func requireAPIError(t *testing.T, err error, code string, status int) {
	t.Helper()
	var failure *APIError
	require.ErrorAs(t, err, &failure)
	assert.Equal(t, code, failure.Code)
	assert.Equal(t, status, failure.Status)
}

func awaitOutcome(t *testing.T, outcomes <-chan Outcome) Outcome {
	t.Helper()
	select {
	case outcome := <-outcomes:
		return outcome
	case <-time.After(3 * time.Second):
		t.Fatal("owned schedule did not complete")
		return Outcome{}
	}
}

func TestSchedulerReadbackOwnsNestedSnapshotsAndInstances(t *testing.T) {
	service := testService(t, testInvoker{}, DevOptions{Groups: []string{"owned"}})
	other := testService(t, testInvoker{}, DevOptions{Groups: []string{"owned"}})
	input := testCreate("snapshot")
	input.GroupName = "owned"
	attempts, age := 2, 120
	start, end := float64(testNow.Add(2*time.Hour).Unix()), float64(testNow.Add(-time.Hour).Unix())
	input.Target.RetryPolicy = &RetryPolicy{MaximumRetryAttempts: &attempts, MaximumEventAgeInSeconds: &age}
	input.Target.DeadLetterConfig = json.RawMessage("{}")
	input.StartDate, input.EndDate = &start, &end
	originalPayload := *input.Target.Input
	arn, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:scheduler:us-east-1:000000000000:schedule/owned/snapshot", arn)
	*input.Target.Input = "{}"
	attempts, age, start, end = 0, 60, 0, 0
	input.Target.Arn = "changed"
	input.Target.DeadLetterConfig[0] = '['
	input.FlexibleTimeWindow.Mode = "FLEXIBLE"
	first, err := service.Get("owned", "snapshot")
	require.NoError(t, err)
	assert.Equal(t, testTargetARN, first.Target.Arn)
	assert.Equal(t, originalPayload, *first.Target.Input)
	assert.Equal(t, 2, *first.Target.RetryPolicy.MaximumRetryAttempts)
	assert.Equal(t, 120, *first.Target.RetryPolicy.MaximumEventAgeInSeconds)
	assert.Equal(t, "{}", string(first.Target.DeadLetterConfig))
	assert.Equal(t, "OFF", first.FlexibleTimeWindow.Mode)
	assert.Equal(t, float64(testNow.Add(2*time.Hour).Unix()), *first.StartDate)
	assert.Equal(t, float64(testNow.Add(-time.Hour).Unix()), *first.EndDate)
	*first.Target.Input = "null"
	*first.Target.RetryPolicy.MaximumRetryAttempts = 185
	first.Target.DeadLetterConfig[0] = '['
	first.FlexibleTimeWindow.Mode = "FLEXIBLE"
	*first.StartDate, *first.EndDate = 0, 0
	second, err := service.Get("owned", "snapshot")
	require.NoError(t, err)
	assert.Equal(t, originalPayload, *second.Target.Input)
	assert.Equal(t, 2, *second.Target.RetryPolicy.MaximumRetryAttempts)
	assert.Equal(t, "{}", string(second.Target.DeadLetterConfig))
	assert.Equal(t, "OFF", second.FlexibleTimeWindow.Mode)
	assert.NotZero(t, *second.StartDate)
	assert.NotZero(t, *second.EndDate)
	_, err = other.Get("owned", "snapshot")
	requireAPIError(t, err, "ResourceNotFoundException", 404)
	_, err = service.Get("default", "snapshot")
	requireAPIError(t, err, "ResourceNotFoundException", 404)
}

func TestSchedulerNativeAtTimeAndHarnessPrecision(t *testing.T) {
	for _, fixture := range []struct {
		name, zone, expression string
		exact                  bool
		want                   time.Time
	}{
		{"native-minute", "", "at(2030-01-02T16:04:55)", false, time.Date(2030, 1, 2, 16, 4, 0, 0, time.UTC)},
		{"explicit-seconds", "UTC", "at(2030-01-02T16:04:55)", true, time.Date(2030, 1, 2, 16, 4, 55, 0, time.UTC)},
		{"iana-timezone", "Asia/Tokyo", "at(2030-01-03T01:04:55)", true, time.Date(2030, 1, 2, 16, 4, 55, 0, time.UTC)},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			service := testService(t, testInvoker{}, DevOptions{ExactSeconds: fixture.exact})
			input := testCreate("time")
			input.ScheduleExpression, input.ScheduleExpressionTimezone = fixture.expression, fixture.zone
			input.State, input.ActionAfterCompletion = "", ""
			_, err := service.Create(t.Context(), input)
			require.NoError(t, err)
			service.mu.Lock()
			due := service.entries["default/time"].due
			service.mu.Unlock()
			assert.True(t, due.Equal(fixture.want), "due=%s want=%s", due, fixture.want)
			readback, err := service.Get("", input.Name)
			require.NoError(t, err)
			assert.Equal(t, "ENABLED", readback.State)
			assert.Equal(t, "NONE", readback.ActionAfterCompletion)
			assert.Equal(t, "default", readback.GroupName)
		})
	}
	service := testService(t, testInvoker{}, DevOptions{})
	input := testCreate("unicode")
	input.Description = strings.Repeat("界", 512)
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err, "AWS description limits count characters")
}

func TestSchedulerRejectsUnsupportedAndInvalidRequestsBeforeMutation(t *testing.T) {
	one, tooMany, tooOld := 1, 186, 59
	for _, fixture := range []struct {
		name   string
		change func(*CreateInput)
		code   string
	}{
		{"name", func(i *CreateInput) { i.Name = "bad/name" }, "ValidationException"},
		{"missing-group", func(i *CreateInput) { i.GroupName = "missing" }, "ResourceNotFoundException"},
		{"token", func(i *CreateInput) { i.ClientToken = "bad token" }, "ValidationException"},
		{"description", func(i *CreateInput) { i.Description = strings.Repeat("x", 513) }, "ValidationException"},
		{"non-finite-start", func(i *CreateInput) { value := math.NaN(); i.StartDate = &value }, "ValidationException"},
		{"non-finite-end", func(i *CreateInput) { value := math.Inf(1); i.EndDate = &value }, "ValidationException"},
		{"rate", func(i *CreateInput) { i.ScheduleExpression = "rate(1 minute)" }, "ValidationException"},
		{"cron", func(i *CreateInput) { i.ScheduleExpression = "cron(0 * * * ? *)" }, "ValidationException"},
		{"invalid-date", func(i *CreateInput) { i.ScheduleExpression = "at(2030-02-30T16:04:20)" }, "ValidationException"},
		{"offset", func(i *CreateInput) { i.ScheduleExpression = "at(2030-01-02T16:04:20Z)" }, "ValidationException"},
		{"dst-gap", func(i *CreateInput) {
			i.ScheduleExpressionTimezone = "America/New_York"
			i.ScheduleExpression = "at(2030-03-10T02:30:00)"
		}, "ValidationException"},
		{"timezone", func(i *CreateInput) { i.ScheduleExpressionTimezone = "Missing/Zone" }, "ValidationException"},
		{"window", func(i *CreateInput) { i.FlexibleTimeWindow.Mode = "FLEXIBLE" }, "ValidationException"},
		{"window-option", func(i *CreateInput) { i.FlexibleTimeWindow.MaximumWindowInMinutes = &one }, "ValidationException"},
		{"window-required", func(i *CreateInput) { i.FlexibleTimeWindow = nil }, "ValidationException"},
		{"state", func(i *CreateInput) { i.State = "PAUSED" }, "ValidationException"},
		{"completion", func(i *CreateInput) { i.ActionAfterCompletion = "ARCHIVE" }, "ValidationException"},
		{"kms", func(i *CreateInput) { i.KmsKeyArn = "arn:aws:kms:us-east-1:000000000000:key/owned" }, "ValidationException"},
		{"missing-target", func(i *CreateInput) { i.Target = nil }, "ValidationException"},
		{"foreign-account", func(i *CreateInput) { i.Target.Arn = strings.Replace(testTargetARN, "000000000000", "111111111111", 1) }, "ValidationException"},
		{"foreign-region", func(i *CreateInput) { i.Target.Arn = strings.Replace(testTargetARN, "us-east-1", "eu-west-1", 1) }, "ValidationException"},
		{"sqs-target", func(i *CreateInput) { i.Target.Arn = "arn:aws:sqs:us-east-1:000000000000:queue" }, "ValidationException"},
		{"role", func(i *CreateInput) { i.Target.RoleArn = "local-role" }, "ValidationException"},
		{"input-json", func(i *CreateInput) { value := "invalid-json"; i.Target.Input = &value }, "ValidationException"},
		{"input-empty", func(i *CreateInput) { value := ""; i.Target.Input = &value }, "ValidationException"},
		{"input-omitted", func(i *CreateInput) { i.Target.Input = nil }, "ValidationException"},
		{"input-limit", func(i *CreateInput) { value := "\"" + strings.Repeat("x", 256<<10) + "\""; i.Target.Input = &value }, "ValidationException"},
		{"retry-count", func(i *CreateInput) { i.Target.RetryPolicy = &RetryPolicy{MaximumRetryAttempts: &tooMany} }, "ValidationException"},
		{"retry-age", func(i *CreateInput) { i.Target.RetryPolicy = &RetryPolicy{MaximumEventAgeInSeconds: &tooOld} }, "ValidationException"},
		{"dead-letter", func(i *CreateInput) { i.Target.DeadLetterConfig = json.RawMessage("{\"Arn\":\"local\"}") }, "ValidationException"},
		{"ecs", func(i *CreateInput) { i.Target.EcsParameters = json.RawMessage("{\"TaskCount\":1}") }, "ValidationException"},
		{"eventbridge", func(i *CreateInput) { i.Target.EventBridgeParameters = json.RawMessage("{\"Source\":\"test\"}") }, "ValidationException"},
		{"kinesis", func(i *CreateInput) { i.Target.KinesisParameters = json.RawMessage("{\"PartitionKey\":\"test\"}") }, "ValidationException"},
		{"sagemaker", func(i *CreateInput) {
			i.Target.SageMakerPipelineParameters = json.RawMessage("{\"PipelineParameterList\":[]}")
		}, "ValidationException"},
		{"sqs", func(i *CreateInput) { i.Target.SqsParameters = json.RawMessage("{\"MessageGroupId\":\"test\"}") }, "ValidationException"},
		{"past", func(i *CreateInput) {
			i.ScheduleExpression = "at(" + testNow.Add(-time.Minute).Format("2006-01-02T15:04:05") + ")"
		}, "ValidationException"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			var validations atomic.Int32
			service := testService(t, testInvoker{validate: func(context.Context, string) error { validations.Add(1); return nil }}, DevOptions{ExactSeconds: true})
			input := testCreate("invalid")
			fixture.change(&input)
			_, err := service.Create(t.Context(), input)
			status := 400
			if fixture.code == "ResourceNotFoundException" {
				status = 404
			}
			requireAPIError(t, err, fixture.code, status)
			assert.Zero(t, validations.Load(), "unsupported requests must not enter the target port")
			assert.Empty(t, service.entries)
			assert.Empty(t, service.creates)
		})
	}
}

func TestSchedulerRequestTokensSurviveCompletionDeleteAndNameReuse(t *testing.T) {
	outcomes := make(chan Outcome, 4)
	var now atomic.Int64
	now.Store(testNow.UnixNano())
	var calls atomic.Int32
	service := testService(t, testInvoker{admit: func(context.Context, string, []byte) error { calls.Add(1); return nil }}, DevOptions{ExactSeconds: true, Clock: func() time.Time { return time.Unix(0, now.Load()) }, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := testCreate("complete")
	input.State, input.ActionAfterCompletion = "ENABLED", "DELETE"
	input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
	arn, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, "accepted", awaitOutcome(t, outcomes).Status)
	_, err = service.Get("", input.Name)
	requireAPIError(t, err, "ResourceNotFoundException", 404)
	now.Store(testNow.Add(2 * time.Hour).UnixNano())
	retryARN, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, arn, retryARN)
	assert.EqualValues(t, 1, calls.Load())
	conflict := input
	conflict.Description = "different request"
	_, err = service.Create(t.Context(), conflict)
	requireAPIError(t, err, "ConflictException", 409)

	pending := testCreate("reused")
	pending.ScheduleExpression = "at(" + testNow.Add(3*time.Hour).Format("2006-01-02T15:04:05") + ")"
	_, err = service.Create(t.Context(), pending)
	require.NoError(t, err)
	require.NoError(t, service.Delete(t.Context(), "", pending.Name, "delete-token"))
	_, err = service.Create(t.Context(), pending)
	require.NoError(t, err, "accepted Create token cannot resurrect a deleted schedule")
	_, err = service.Get("", pending.Name)
	requireAPIError(t, err, "ResourceNotFoundException", 404)
	pending.ClientToken = "new-generation"
	pending.Description = "replacement"
	_, err = service.Create(t.Context(), pending)
	require.NoError(t, err)
	require.NoError(t, service.Delete(t.Context(), "", pending.Name, "delete-token"))
	readback, err := service.Get("", pending.Name)
	require.NoError(t, err, "retrying old deletion cannot delete a new generation")
	assert.Equal(t, "replacement", readback.Description)
	err = service.Delete(t.Context(), "", "other-name", "delete-token")
	requireAPIError(t, err, "ConflictException", 409)
}

func TestSchedulerConcurrentCreateTokensAndResourceConflict(t *testing.T) {
	for _, sameToken := range []bool{true, false} {
		t.Run(fmt.Sprintf("same-token-%t", sameToken), func(t *testing.T) {
			service := testService(t, testInvoker{}, DevOptions{})
			start := make(chan struct{})
			var workers sync.WaitGroup
			var accepted, conflicts atomic.Int32
			for index := range 24 {
				workers.Add(1)
				go func() {
					defer workers.Done()
					<-start
					input := testCreate("concurrent")
					if !sameToken {
						input.ClientToken = fmt.Sprintf("token-%d", index)
					}
					_, err := service.Create(t.Context(), input)
					if err == nil {
						accepted.Add(1)
						return
					}
					var failure *APIError
					if errors.As(err, &failure) && failure.Code == "ConflictException" {
						conflicts.Add(1)
						return
					}
					t.Errorf("unexpected create error: %v", err)
				}()
			}
			close(start)
			workers.Wait()
			if sameToken {
				assert.EqualValues(t, 24, accepted.Load())
				assert.Zero(t, conflicts.Load())
			} else {
				assert.EqualValues(t, 1, accepted.Load())
				assert.EqualValues(t, 23, conflicts.Load())
			}
			assert.Len(t, service.entries, 1)
			assert.Len(t, service.creates, 1)
		})
	}
}

func TestSchedulerAdmissionRetriesOwnPayloadAndPolicy(t *testing.T) {
	outcomes := make(chan Outcome, 1)
	var attempts atomic.Int32
	input := testCreate("retry")
	input.State, input.ActionAfterCompletion = "ENABLED", "DELETE"
	input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
	count := 2
	input.Target.RetryPolicy = &RetryPolicy{MaximumRetryAttempts: &count}
	payload := *input.Target.Input
	service := testService(t, testInvoker{admit: func(ctx context.Context, arn string, received []byte) error {
		assert.Equal(t, testTargetARN, arn)
		assert.Equal(t, payload, string(received), "each admission receives an owned payload copy")
		received[0] = '['
		if attempts.Add(1) < 3 {
			return testAdmissionError{retry: true}
		}
		return nil
	}}, DevOptions{ExactSeconds: true, RetryDelay: time.Millisecond, Observe: func(outcome Outcome) { outcomes <- outcome }})
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 3, outcome.Attempts)
	assert.Empty(t, outcome.Code, "successful retry evidence must not retain a failure code")
	_, err = service.Get("", input.Name)
	requireAPIError(t, err, "ResourceNotFoundException", 404)
	encoded, err := json.Marshal(outcome)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), "private payload")
	assert.NotContains(t, string(encoded), "nested")
}

func TestSchedulerTerminalAdmissionFailureAndEventAge(t *testing.T) {
	for _, fixture := range []struct {
		name          string
		retries, want int
		failure       error
		expire        bool
	}{
		{"zero-retries", 0, 1, errors.New("admission unavailable"), false},
		{"bounded-retries", 2, 3, testAdmissionError{retry: true}, false},
		{"permanent-failure", 5, 1, testAdmissionError{retry: false}, false},
		{"event-age", 5, 1, testAdmissionError{retry: true}, true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			outcomes := make(chan Outcome, 1)
			var now atomic.Int64
			now.Store(testNow.UnixNano())
			service := testService(t, testInvoker{admit: func(context.Context, string, []byte) error {
				if fixture.expire {
					now.Store(testNow.Add(time.Minute).UnixNano())
				}
				return fixture.failure
			}}, DevOptions{ExactSeconds: true, RetryDelay: time.Millisecond, Clock: func() time.Time { return time.Unix(0, now.Load()) }, Observe: func(outcome Outcome) { outcomes <- outcome }})
			input := testCreate("failed")
			input.State, input.ActionAfterCompletion = "ENABLED", "DELETE"
			input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
			age := 60
			input.Target.RetryPolicy = &RetryPolicy{MaximumRetryAttempts: &fixture.retries, MaximumEventAgeInSeconds: &age}
			_, err := service.Create(t.Context(), input)
			require.NoError(t, err)
			outcome := awaitOutcome(t, outcomes)
			assert.Equal(t, "failed", outcome.Status)
			assert.Equal(t, fixture.want, outcome.Attempts)
			code := "TargetAdmissionFailed"
			if fixture.expire {
				code = "EventAgeExceeded"
			}
			assert.Equal(t, code, outcome.Code)
			_, err = service.Get("", input.Name)
			requireAPIError(t, err, "ResourceNotFoundException", 404)
		})
	}
}

func TestSchedulerCapacityAndFailedTargetValidationDoNotPublish(t *testing.T) {
	var registered atomic.Bool
	service := testService(t, testInvoker{validate: func(context.Context, string) error {
		if !registered.Load() {
			return errors.New("not registered")
		}
		return nil
	}}, DevOptions{MaxSchedules: 1})
	first := testCreate("first")
	_, err := service.Create(t.Context(), first)
	requireAPIError(t, err, "ResourceNotFoundException", 404)
	registered.Store(true)
	_, err = service.Create(t.Context(), first)
	require.NoError(t, err, "failed validation must not consume its request token or capacity")
	_, err = service.Create(t.Context(), testCreate("second"))
	requireAPIError(t, err, "ServiceQuotaExceededException", 402)
	_, err = service.Create(t.Context(), first)
	require.NoError(t, err, "accepted token retries remain available under pressure")
	require.NoError(t, service.Delete(t.Context(), "", "first", "delete-first"))
	_, err = service.Create(t.Context(), testCreate("second"))
	requireAPIError(t, err, "ServiceQuotaExceededException", 402)
	require.NoError(t, service.Delete(t.Context(), "", "first", "delete-first"))
}

func TestSchedulerPendingCancellationNeverAdmitsTarget(t *testing.T) {
	payloads := make(chan []byte, 1)
	outcomes := make(chan Outcome, 2)
	service := testService(t, testInvoker{admit: func(_ context.Context, _ string, payload []byte) error { payloads <- payload; return nil }}, DevOptions{ExactSeconds: true, Observe: func(outcome Outcome) { outcomes <- outcome }})
	pending := testCreate("pending")
	pending.State = "ENABLED"
	_, err := service.Create(t.Context(), pending)
	require.NoError(t, err)
	require.NoError(t, service.Delete(t.Context(), "", pending.Name, "cancel-pending"))
	assert.Equal(t, "canceled", awaitOutcome(t, outcomes).Status)
	assert.Empty(t, payloads, "deleted pending work cannot enter the target")
}

func TestSchedulerCloseAndDeleteJoinCanceledTargetCallbacks(t *testing.T) {
	for _, operation := range []string{"delete", "close-deadline"} {
		t.Run(operation, func(t *testing.T) {
			entered, canceled, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(cleanup) }) }
			defer release()
			var wantCloseError error
			if operation == "close-deadline" {
				wantCloseError = context.DeadlineExceeded
			}
			service := testServiceWithCloseError(t, testInvoker{admit: func(ctx context.Context, _ string, _ []byte) error {
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-cleanup
				return ctx.Err()
			}}, DevOptions{ExactSeconds: true}, wantCloseError)
			input := testCreate("joining")
			input.State = "ENABLED"
			input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
			_, err := service.Create(t.Context(), input)
			require.NoError(t, err)
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("target did not start")
			}
			finished := make(chan error, 1)
			if operation == "delete" {
				go func() { finished <- service.Delete(t.Context(), "", input.Name, "join-delete") }()
			} else {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
				defer cancel()
				go func() { finished <- service.Close(ctx) }()
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("target was not canceled")
			}
			select {
			case err := <-finished:
				t.Fatalf("%s returned before callback cleanup joined: %v", operation, err)
			case <-time.After(30 * time.Millisecond):
			}
			release()
			select {
			case err = <-finished:
				if operation == "delete" {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			case <-time.After(time.Second):
				t.Fatal("operation did not join callback")
			}
			if operation == "close-deadline" {
				_, err = service.Create(t.Context(), testCreate("closed"))
				requireAPIError(t, err, "InternalServerException", 503)
			}
		})
	}
}

func TestSchedulerContextAttributesAreResolvedPerAdmissionAttempt(t *testing.T) {
	outcomes := make(chan Outcome, 1)
	payloads := make(chan map[string]string, 2)
	var attempts atomic.Int32
	service := testService(t, testInvoker{admit: func(_ context.Context, _ string, payload []byte) error {
		var decoded map[string]string
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return err
		}
		payloads <- decoded
		if attempts.Add(1) == 1 {
			return testAdmissionError{retry: true}
		}
		return nil
	}}, DevOptions{RetryDelay: time.Millisecond, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := testCreate("context")
	input.State = "ENABLED"
	input.ScheduleExpression = "at(2030-01-02T15:04:55)"
	payload := "{\"arn\":\"<aws.scheduler.schedule-arn>\",\"time\":\"<aws.scheduler.scheduled-time>\",\"execution\":\"<aws.scheduler.execution-id>\",\"attempt\":\"<aws.scheduler.attempt-number>\",\"literal\":\"unchanged\"}"
	input.Target.Input = &payload
	retries := 1
	input.Target.RetryPolicy = &RetryPolicy{MaximumRetryAttempts: &retries}
	arn, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 2, outcome.Attempts)
	first, second := <-payloads, <-payloads
	for _, received := range []map[string]string{first, second} {
		assert.Equal(t, arn, received["arn"])
		assert.Equal(t, "2030-01-02T15:04:55Z", received["time"], "context preserves the expression time despite native minute precision")
		assert.Equal(t, "unchanged", received["literal"])
		assert.NotEmpty(t, received["execution"])
	}
	assert.NotEqual(t, first["execution"], second["execution"])
	assert.Equal(t, "1", first["attempt"])
	assert.Equal(t, "2", second["attempt"])
}

func TestSchedulerCanceledCreateAndCloseAreInstanceLocal(t *testing.T) {
	entered := make(chan struct{})
	service := testService(t, testInvoker{validate: func(ctx context.Context, _ string) error {
		close(entered)
		<-ctx.Done()
		return nil
	}}, DevOptions{})
	ctx, cancel := context.WithCancel(t.Context())
	finished := make(chan error, 1)
	go func() { _, err := service.Create(ctx, testCreate("canceled-create")); finished <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("target validation did not start")
	}
	cancel()
	require.ErrorIs(t, <-finished, context.Canceled)
	assert.Empty(t, service.entries)
	assert.Empty(t, service.creates)

	outcomes := make(chan Outcome, 1)
	var admitted atomic.Int32
	pending := testService(t, testInvoker{admit: func(context.Context, string, []byte) error { admitted.Add(1); return nil }}, DevOptions{ExactSeconds: true, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := testCreate("future")
	input.State = "ENABLED"
	_, err := pending.Create(t.Context(), input)
	require.NoError(t, err)
	require.NoError(t, pending.Close(t.Context()))
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "canceled", outcome.Status)
	assert.Zero(t, outcome.Attempts)
	assert.Zero(t, admitted.Load())

	otherOutcomes := make(chan Outcome, 1)
	other := testService(t, testInvoker{}, DevOptions{ExactSeconds: true, Observe: func(outcome Outcome) { otherOutcomes <- outcome }})
	input = testCreate("future")
	input.State = "ENABLED"
	input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
	_, err = other.Create(t.Context(), input)
	require.NoError(t, err)
	assert.Equal(t, "accepted", awaitOutcome(t, otherOutcomes).Status)
}

func TestSchedulerCancellationDuringRetryIsNotReportedAsAdmissionFailure(t *testing.T) {
	entered := make(chan struct{})
	outcomes := make(chan Outcome, 1)
	var attempts atomic.Int32
	service := testService(t, testInvoker{admit: func(ctx context.Context, _ string, _ []byte) error {
		if attempts.Add(1) == 1 {
			return testAdmissionError{retry: true}
		}
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}}, DevOptions{ExactSeconds: true, RetryDelay: time.Millisecond, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := testCreate("cancel-retry")
	input.State = "ENABLED"
	input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("retry admission did not start")
	}
	require.NoError(t, service.Close(t.Context()))
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "canceled", outcome.Status)
	assert.Equal(t, 2, outcome.Attempts)
}

func TestSchedulerCachedDeleteAlsoJoinsCanceledAdmission(t *testing.T) {
	entered, canceled, cleanup := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(cleanup) }) }
	defer release()
	service := testService(t, testInvoker{admit: func(ctx context.Context, _ string, _ []byte) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-cleanup
		return ctx.Err()
	}}, DevOptions{ExactSeconds: true})
	input := testCreate("delete-join-retry")
	input.State = "ENABLED"
	input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("admission did not start")
	}
	first, retry := make(chan error, 1), make(chan error, 1)
	go func() { first <- service.Delete(t.Context(), "", input.Name, "same-delete-token") }()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("admission did not cancel")
	}
	go func() { retry <- service.Delete(t.Context(), "", input.Name, "same-delete-token") }()
	select {
	case err := <-first:
		t.Fatalf("first deletion returned before admission cleanup: %v", err)
	case err := <-retry:
		t.Fatalf("cached deletion returned before admission cleanup: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	for _, result := range []<-chan error{first, retry} {
		select {
		case err := <-result:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("deletion did not join")
		}
	}
}

func TestSchedulerCompletedCloseIgnoresLateCallerCancellation(t *testing.T) {
	service := testService(t, testInvoker{}, DevOptions{})
	require.NoError(t, service.Close(t.Context()))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	results := make(chan error, 16)
	for range cap(results) {
		go func() { results <- service.Close(ctx) }()
	}
	for range cap(results) {
		select {
		case err := <-results:
			require.NoError(t, err, "a canceled late caller cannot change a successfully completed shutdown")
		case <-time.After(time.Second):
			t.Fatal("completed Close did not return")
		}
	}
}

// Signal after taking the initial cancellation snapshot, so the test can
// deterministically cancel a Delete while its final publication lock is held.
type initialSchedulerContextCheck struct {
	context.Context
	checked chan struct{}
	first   sync.Once
}

func (ctx *initialSchedulerContextCheck) Err() error {
	err := ctx.Context.Err()
	ctx.first.Do(func() { close(ctx.checked) })
	return err
}

func TestSchedulerDeleteCanceledBeforeFinalLockPreservesScheduleAndToken(t *testing.T) {
	service := testService(t, testInvoker{}, DevOptions{})
	input := testCreate("delete-canceled-on-lock")
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	ctx := &initialSchedulerContextCheck{Context: parent, checked: make(chan struct{})}
	finished := make(chan error, 1)
	service.mu.Lock()
	go func() { finished <- service.Delete(ctx, "", input.Name, "not-published-delete") }()
	select {
	case <-ctx.checked:
		cancel()
	case <-time.After(time.Second):
		service.mu.Unlock()
		t.Fatal("Delete did not take its initial cancellation snapshot")
	}
	service.mu.Unlock()
	select {
	case err = <-finished:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled Delete did not return")
	}
	readback, err := service.Get("", input.Name)
	require.NoError(t, err, "cancellation before mutation cannot remove a schedule")
	assert.Equal(t, input.ScheduleExpression, readback.ScheduleExpression)
	assert.Empty(t, service.deletes, "canceled Delete cannot consume the request token")
}

func TestSchedulerSuccessfulAdmissionWinsOverLateCancellation(t *testing.T) {
	entered := make(chan struct{})
	outcomes := make(chan Outcome, 1)
	acceptedPayloads := make(chan []byte, 1)
	service := testService(t, testInvoker{admit: func(ctx context.Context, _ string, payload []byte) error {
		// Ownership has transferred successfully; caller cancellation during
		// the adapter's return cannot undo the target's admitted work.
		acceptedPayloads <- append([]byte(nil), payload...)
		close(entered)
		<-ctx.Done()
		return nil
	}}, DevOptions{ExactSeconds: true, Observe: func(outcome Outcome) { outcomes <- outcome }})
	input := testCreate("accepted-before-cancel")
	input.State, input.ActionAfterCompletion = "ENABLED", "DELETE"
	input.ScheduleExpression = "at(" + testNow.Format("2006-01-02T15:04:05") + ")"
	_, err := service.Create(t.Context(), input)
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("target admission did not start")
	}
	require.NoError(t, service.Close(t.Context()))
	outcome := awaitOutcome(t, outcomes)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 1, outcome.Attempts)
	assert.Empty(t, outcome.Code)
	assert.Equal(t, *input.Target.Input, string(<-acceptedPayloads))
	_, err = service.Get("", input.Name)
	requireAPIError(t, err, "ResourceNotFoundException", 404)
}
