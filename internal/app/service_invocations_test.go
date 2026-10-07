package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/secrets"
)

type invocationBackend struct {
	validate func(string, string) error
	execute  func(context.Context, lambdaservice.InvokeInput) (lambdaservice.InvokeOutput, error)
	admit    func(context.Context, lambdaservice.InvokeInput) (lambdaservice.Admission, error)
}

func (backend invocationBackend) ValidateTarget(name, qualifier string) error {
	if backend.validate == nil {
		return nil
	}
	return backend.validate(name, qualifier)
}
func (backend invocationBackend) Execute(ctx context.Context, input lambdaservice.InvokeInput) (lambdaservice.InvokeOutput, error) {
	return backend.execute(ctx, input)
}
func (backend invocationBackend) Admit(ctx context.Context, input lambdaservice.InvokeInput) (lambdaservice.Admission, error) {
	return backend.admit(ctx, input)
}

func TestServiceLambdaRotationAdapterNativeEventAndRedaction(t *testing.T) {
	const target = "arn:aws:lambda:eu-west-1:123456789012:function:rotate:live"
	const sensitive = "secret-value-must-not-escape"
	event := secrets.RotationEvent{SecretID: "arn:aws:secretsmanager:eu-west-1:123456789012:secret:owned", ClientRequestToken: "owned-token", Step: "createSecret"}
	var executed bool
	backend := invocationBackend{validate: func(name, qualifier string) error {
		if name != target || qualifier != "" {
			t.Fatalf("target changed %s %s", name, qualifier)
		}
		return nil
	}, execute: func(ctx context.Context, input lambdaservice.InvokeInput) (lambdaservice.InvokeOutput, error) {
		executed = true
		if input.FunctionName != target || input.Qualifier != "" || input.ClientContext != "" || input.TraceID != "" {
			t.Fatalf("unexpected execution %v", input)
		}
		var got secrets.RotationEvent
		if err := json.Unmarshal(input.Payload, &got); err != nil || got != event {
			t.Fatalf("rotation event changed %s %v", input.Payload, err)
		}
		return lambdaservice.InvokeOutput{Payload: []byte(`{"unused":"` + sensitive + `"}`)}, nil
	}}
	invoker := rotationLambdaInvoker{runtime: backend}
	if err := invoker.ValidateRotationTarget(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	if err := invoker.InvokeRotation(t.Context(), target, event); err != nil || !executed {
		t.Fatalf("execution did not complete: %v", err)
	}
	for _, test := range []struct {
		name     string
		output   lambdaservice.InvokeOutput
		failure  error
		deadline bool
		canceled bool
	}{
		{name: "function failure", output: lambdaservice.InvokeOutput{FunctionError: true, Payload: []byte(`{"errorType":"Exception","errorMessage":"` + sensitive + `"}`)}},
		{name: "malformed failure", output: lambdaservice.InvokeOutput{FunctionError: true, Payload: []byte(sensitive)}},
		{name: "safe native timeout", output: lambdaservice.InvokeOutput{FunctionError: true, Payload: []byte(`{"errorType":"Sandbox.Timedout","errorMessage":"` + sensitive + `"}`)}, deadline: true},
		{name: "safe timeout name", output: lambdaservice.InvokeOutput{FunctionError: true, Payload: []byte(`{"errorType":"TimeoutError","errorMessage":"` + sensitive + `"}`)}, deadline: true},
		{name: "message cannot claim timeout", output: lambdaservice.InvokeOutput{FunctionError: true, Payload: []byte(`{"errorType":"Exception","errorMessage":"Sandbox.Timedout ` + sensitive + `"}`)}},
		{name: "invocation failure", failure: &lambdaservice.InvokeError{Status: 500, Message: sensitive, Code: "ServiceException"}},
		{name: "wrapped deadline", failure: fmt.Errorf("%s: %w", sensitive, context.DeadlineExceeded), deadline: true},
		{name: "wrapped cancellation", failure: fmt.Errorf("%s: %w", sensitive, context.Canceled), canceled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := rotationLambdaInvoker{runtime: invocationBackend{execute: func(context.Context, lambdaservice.InvokeInput) (lambdaservice.InvokeOutput, error) {
				return test.output, test.failure
			}}}
			err := adapter.InvokeRotation(t.Context(), target, event)
			if err == nil || strings.Contains(err.Error(), sensitive) {
				t.Fatalf("function failure escaped or claimed success: %v", err)
			}
			if errors.Is(err, context.DeadlineExceeded) != test.deadline || errors.Is(err, context.Canceled) != test.canceled {
				t.Fatalf("wrong safe classification: %v", err)
			}
		})
	}
}

func TestServiceLambdaSchedulerAdapterAdmitsAndClassifiesRetries(t *testing.T) {
	const target = "arn:aws:lambda:eu-west-1:123456789012:function:target:live"
	const sensitive = "private-payload-or-function-error"
	payload := []byte(`{"owned":"` + sensitive + `"}`)
	var admitted bool
	invoker := schedulerLambdaInvoker{runtime: invocationBackend{admit: func(ctx context.Context, input lambdaservice.InvokeInput) (lambdaservice.Admission, error) {
		admitted = true
		if input.FunctionName != target || input.Qualifier != "" || string(input.Payload) != string(payload) {
			t.Fatal("target admission changed event")
		}
		return lambdaservice.Admission{RequestID: "accepted"}, nil
	}}}
	if err := invoker.AdmitTarget(t.Context(), target, payload); err != nil || !admitted {
		t.Fatalf("accepted admission lost: %v", err)
	}
	for _, status := range []int{400, 401, 403, 404, 413, 429, 500, 503, 599, 600} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			adapter := schedulerLambdaInvoker{runtime: invocationBackend{admit: func(context.Context, lambdaservice.InvokeInput) (lambdaservice.Admission, error) {
				return lambdaservice.Admission{}, fmt.Errorf("%s: %w", sensitive, &lambdaservice.InvokeError{Status: status, Message: sensitive, Code: "NativeFailure"})
			}}}
			err := adapter.AdmitTarget(t.Context(), target, payload)
			if err == nil || strings.Contains(err.Error(), sensitive) {
				t.Fatalf("private admission detail escaped: %v", err)
			}
			var classified interface{ Retryable() bool }
			if !errors.As(err, &classified) || classified.Retryable() != (status == http.StatusTooManyRequests || status >= 500 && status <= 599) {
				t.Fatalf("status %d wrong retry classification: %v", status, err)
			}
		})
	}
}

func TestServiceLambdaAdaptersCancellationMissingRuntimeAndValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	backend := invocationBackend{validate: func(string, string) error { t.Fatal("canceled validation ran"); return nil }, execute: func(context.Context, lambdaservice.InvokeInput) (lambdaservice.InvokeOutput, error) {
		t.Fatal("canceled execution ran")
		return lambdaservice.InvokeOutput{}, nil
	}, admit: func(context.Context, lambdaservice.InvokeInput) (lambdaservice.Admission, error) {
		t.Fatal("canceled admission ran")
		return lambdaservice.Admission{}, nil
	}}
	rotation := rotationLambdaInvoker{runtime: backend}
	scheduler := schedulerLambdaInvoker{runtime: backend}
	for _, err := range []error{rotation.ValidateRotationTarget(ctx, "owned"), rotation.InvokeRotation(ctx, "owned", secrets.RotationEvent{}), scheduler.ValidateTarget(ctx, "owned"), scheduler.AdmitTarget(ctx, "owned", []byte(`{}`))} {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation lost %v", err)
		}
	}
	for _, err := range []error{(rotationLambdaInvoker{}).ValidateRotationTarget(t.Context(), "owned"), (rotationLambdaInvoker{}).InvokeRotation(t.Context(), "owned", secrets.RotationEvent{}), (schedulerLambdaInvoker{}).ValidateTarget(t.Context(), "owned"), (schedulerLambdaInvoker{}).AdmitTarget(t.Context(), "owned", []byte(`{}`))} {
		if err == nil {
			t.Fatal("missing runtime was successful")
		}
	}
	const sensitive = "secret-handler-detail"
	failed := invocationBackend{validate: func(string, string) error { return errors.New(sensitive) }}
	for _, err := range []error{(rotationLambdaInvoker{runtime: failed}).ValidateRotationTarget(t.Context(), "owned"), (schedulerLambdaInvoker{runtime: failed}).ValidateTarget(t.Context(), "owned")} {
		if err == nil || strings.Contains(err.Error(), sensitive) {
			t.Fatalf("validation detail escaped: %v", err)
		}
	}
}
