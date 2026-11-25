package lambda

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type asyncDestinationFixture struct {
	validate func(string, bool) error
	deliver  func(context.Context, string, AsyncInvocationRecord) error
}

func (fixture asyncDestinationFixture) ValidateAsyncDestination(arn string, onFailure bool) error {
	if fixture.validate != nil {
		return fixture.validate(arn, onFailure)
	}
	return nil
}

func (fixture asyncDestinationFixture) DeliverAsyncDestination(ctx context.Context, arn string, record AsyncInvocationRecord) error {
	return fixture.deliver(ctx, arn, record)
}

func TestAsyncPolicyExpiresBeforeDelayedRetryWithoutNewAttempt(t *testing.T) {
	now := time.Now()
	service, task := shutdownOwner(t, "retrying", 1, now.Add(5*time.Minute), nil)
	service.asyncContext = t.Context()
	task.input = invocation{name: "worker:live", payload: []byte(`{"fixture":true}`)}
	task.record.QueuedAt = now.Add(-time.Minute + 10*time.Second)
	task.policy = asyncInvokePolicy{configured: true, age: time.Minute, retries: 1, destinations: DestinationConfig{OnFailure: Destination{Destination: "arn"}}}
	var delivered []AsyncInvocationRecord
	service.asyncDestinations = asyncDestinationFixture{deliver: func(_ context.Context, arn string, record AsyncInvocationRecord) error {
		require.Equal(t, "arn", arn)
		delivered = append(delivered, record)
		return nil
	}}
	selected, wake, next, stopped := service.pollAsync()
	require.Nil(t, selected)
	require.NotNil(t, wake)
	require.Equal(t, task.record.QueuedAt.Add(time.Minute), next)
	require.False(t, stopped)
	task.record.QueuedAt = now.Add(-time.Minute - time.Second)
	selected, wake, _, stopped = service.pollAsync()
	require.Nil(t, selected)
	require.Nil(t, wake)
	require.False(t, stopped)
	require.Len(t, delivered, 1)
	require.Equal(t, "EventAgeExceeded", delivered[0].RequestContext.Condition)
	require.Equal(t, 1, delivered[0].RequestContext.ApproximateInvokeCount)
	require.Nil(t, delivered[0].ResponseContext)
	require.JSONEq(t, `{"fixture":true}`, string(delivered[0].RequestPayload))
	history := service.AsyncSnapshot()
	require.Len(t, history, 1)
	require.Equal(t, "failed", history[0].State)
	require.Equal(t, "delivered", history[0].DestinationStatus)
	require.True(t, service.AsyncIdle())
}

func TestAsyncDestinationJoinsDeliveryAndCanAdmitAnotherEvent(t *testing.T) {
	directory := t.TempDir()
	functions := map[string]Function{
		"first": asyncCommand(t, filepath.Join(directory, "first.jsonl"), "", "", "success"),
		"next":  asyncCommand(t, filepath.Join(directory, "next.jsonl"), "", "", "success"),
	}
	const target = "arn:aws:lambda:us-east-1:000000000000:function:next"
	var service *Service
	started, proceed := make(chan struct{}), make(chan struct{})
	var err error
	service, err = NewService(&Config{Functions: functions, DevAsync: &DevAsyncConfig{Workers: 1, Capacity: 3, LogWriter: io.Discard},
		AsyncDestinations: asyncDestinationFixture{deliver: func(ctx context.Context, arn string, record AsyncInvocationRecord) error {
			if arn != target || record.RequestContext.Condition != "Success" {
				return errors.New("wrong destination record")
			}
			if _, err := service.Admit(ctx, InvokeInput{FunctionName: arn, Payload: record.RequestPayload}); err != nil {
				return err
			}
			close(started)
			select {
			case <-proceed:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}},
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, service.Close(ctx))
	})
	input := eventInvokeConfigInput{DestinationConfig: &eventInvokeDestinationInput{OnSuccess: &Destination{Destination: target}}}
	_, err = service.writeEventInvokeConfig("first", input, false)
	require.NoError(t, err)
	first, err := service.Admit(t.Context(), InvokeInput{FunctionName: "first", Payload: []byte(`{"id":"fixture"}`)})
	require.NoError(t, err)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("destination could not admit its downstream event")
	}
	require.False(t, service.AsyncIdle())
	short, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	require.ErrorIs(t, service.WaitAsyncIdle(short), context.DeadlineExceeded)
	cancel()
	close(proceed)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	require.NoError(t, service.WaitAsyncIdle(ctx))
	require.True(t, service.AsyncIdle())
	history := service.AsyncSnapshot()
	require.Len(t, history, 2)
	require.Equal(t, first.RequestID, history[0].RequestID)
	require.Equal(t, "delivered", history[0].DestinationStatus)
	require.Equal(t, "next", history[1].FunctionName)
	require.Equal(t, "succeeded", history[1].State)
}

func TestAsyncConfigOverwriteValidationAndAdmissionSnapshot(t *testing.T) {
	directory := t.TempDir()
	output := filepath.Join(directory, "attempts.jsonl")
	gate := filepath.Join(directory, "gate")
	function := asyncCommand(t, output, gate, filepath.Join(directory, "started"), "fail")
	delivered := make(chan string, 2)
	service, err := NewService(&Config{Functions: map[string]Function{"worker": function, "worker:live": function},
		DevAsync: &DevAsyncConfig{Workers: 1, LogWriter: io.Discard, RetryDelays: []time.Duration{0, 0}},
		AsyncDestinations: asyncDestinationFixture{deliver: func(_ context.Context, destination string, _ AsyncInvocationRecord) error {
			delivered <- destination
			return nil
		}},
	}, directory)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, service.Close(ctx))
	})
	const destination = "arn:aws:sqs:us-east-1:000000000000:failures"
	require.Error(t, service.validateAsyncDestination("worker", "arn:aws:lambda:us-east-1:000000000000:function:worker:$LATEST", false))
	require.Error(t, service.validateAsyncDestination("worker", "arn:aws:s3:::local-bucket", false))
	require.Error(t, service.validateAsyncDestination("worker", "arn:aws:sqs:us-east-1:000000000000:test.fifo", true))
	require.Error(t, service.validateAsyncDestination("worker", "arn:aws:sqs:us-east-1:111111111111:cross-account", true))
	retries := 0
	input := eventInvokeConfigInput{MaximumRetryAttempts: &retries, DestinationConfig: &eventInvokeDestinationInput{OnFailure: &Destination{Destination: destination}}}
	alias, err := service.writeEventInvokeConfig("worker:live", input, false)
	require.NoError(t, err)
	require.Equal(t, 21600, alias.MaximumEventAgeInSeconds)
	require.Equal(t, 0, alias.MaximumRetryAttempts)
	require.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:worker:live", alias.FunctionARN)
	invalidAge := 59
	_, err = service.writeEventInvokeConfig("worker:live", eventInvokeConfigInput{MaximumEventAgeInSeconds: &invalidAge}, true)
	require.Error(t, err)
	readback, err := service.getEventInvokeConfig("worker:live", false)
	require.NoError(t, err)
	require.Equal(t, alias, readback)
	accepted, err := service.Admit(t.Context(), InvokeInput{FunctionName: "worker", Qualifier: "live", Payload: []byte(`{"id":"original-policy"}`)})
	require.NoError(t, err)
	overwritten, err := service.writeEventInvokeConfig("worker:live", eventInvokeConfigInput{}, false)
	require.NoError(t, err)
	require.Equal(t, 2, overwritten.MaximumRetryAttempts)
	require.Empty(t, overwritten.DestinationConfig.OnFailure.Destination)
	require.NoError(t, os.WriteFile(gate, nil, 0600))
	terminal := waitAsyncState(t, service, accepted.RequestID, "failed")
	require.Equal(t, 1, terminal.Attempts)
	require.Equal(t, "delivered", terminal.DestinationStatus)
	require.Equal(t, destination, <-delivered)
}

func TestAsyncDestinationFailureDoesNotRetryExecution(t *testing.T) {
	service, task := shutdownOwner(t, "running", 1, time.Time{}, nil)
	service.asyncContext = t.Context()
	task.input = invocation{name: "worker", payload: []byte(`{"private_input":"fixture"}`)}
	task.policy.destinations.OnSuccess = Destination{Destination: "local-arn"}
	service.asyncDestinations = asyncDestinationFixture{deliver: func(_ context.Context, _ string, _ AsyncInvocationRecord) error {
		return errors.New("private destination error")
	}}
	service.completeAsync(task, invocationResult{state: InvocationSucceeded, payload: []byte(`{"private_output":"fixture"}`)}, nil)
	records := service.AsyncSnapshot()
	require.Len(t, records, 1)
	require.Equal(t, "succeeded", records[0].State)
	require.Equal(t, 1, records[0].Attempts)
	require.Equal(t, "failed", records[0].DestinationStatus)
	require.Empty(t, records[0].ErrorType)
	require.Empty(t, service.asyncQueue)
	require.NoError(t, service.WaitAsyncIdle(t.Context()))
}
