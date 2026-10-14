package app

import (
	"context"
	"errors"
	"github.com/lyeith/eventbus/internal/devquiescence"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type orderedLambdaLifecycle struct{ order *[]string }

func (owner orderedLambdaLifecycle) DrainAsync(context.Context) error {
	*owner.order = append(*owner.order, "lambda.drain")
	return nil
}
func (owner orderedLambdaLifecycle) Close(context.Context) error {
	*owner.order = append(*owner.order, "lambda.close")
	return nil
}

func TestEventBridgeSourcesJoinBeforeAsyncDrainAndResourceRelease(t *testing.T) {
	var order []string
	owned := &eventBusLifecycle{
		events:    contextCloseFunc(func(context.Context) error { order = append(order, "events.close"); return nil }),
		functions: orderedLambdaLifecycle{&order},
		ses:       closeFunc(func() error { order = append(order, "capture.close"); return nil }),
	}
	require.NoError(t, owned.Close(t.Context()))
	require.Equal(t, []string{"events.close", "lambda.drain", "lambda.close", "capture.close"}, order)
	require.NoError(t, owned.Close(t.Context()))
	require.Len(t, order, 4, "each owned resource joins once")
}

func TestFailedEventBridgeJoinRetainsDependentResources(t *testing.T) {
	failure := errors.New("admission ownership uncertain")
	eventsClosed := 0
	var order []string
	owned := &eventBusLifecycle{
		events:    contextCloseFunc(func(context.Context) error { eventsClosed++; return failure }),
		functions: orderedLambdaLifecycle{&order},
		ses:       closeFunc(func() error { order = append(order, "capture.close"); return nil }),
	}
	require.ErrorIs(t, owned.Close(t.Context()), failure)
	require.Equal(t, []string{"lambda.drain"}, order, "independent quiescence proceeds; release is withheld")
	require.ErrorIs(t, owned.Close(t.Context()), failure)
	require.Equal(t, 1, eventsClosed)
}

func TestRetainedAbortIncludesEventBridgeBeforeLambda(t *testing.T) {
	var order []string
	record := func(name string) contextCloser {
		return contextCloseFunc(func(context.Context) error { order = append(order, name); return nil })
	}
	listener := &eventBusListener{owned: &eventBusLifecycle{events: record("events"), functions: record("lambda")}}
	require.NoError(t, listener.abortRetainedOwners(t.Context()))
	require.Equal(t, []string{"events", "events", "lambda", "lambda"}, order)
}

func TestRetainedEventBridgeCleanupOnlyAllowsIntakeReducingOperations(t *testing.T) {
	for _, scenario := range []struct {
		target, path string
		allowed      bool
	}{
		{"AWSEvents.DisableRule", "/", true},
		{"AWSEvents.RemoveTargets", "/", true},
		{"AWSEvents.DeleteRule", "/", true},
		{"AWSEvents.PutRule", "/", false},
		{"AWSEvents.PutTargets", "/", false},
		{"AWSEvents.EnableRule", "/", false},
		{"AWSEvents.DescribeRule", "/", false},
		{"AWSEventss.DeleteRule", "/", false},
		{"AWSEvents.DeleteRule", "/unrelated", false},
		{"AWSEvents.DeleteRule", "/queue/unrelated", false},
	} {
		t.Run(scenario.target+scenario.path, func(t *testing.T) {
			owner := devquiescence.New()
			held, err := owner.Quiesce(t.Context())
			require.NoError(t, err)
			called := false
			payload := `{ "Name":"owned", "signature":"application-owned" }`
			router := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called = true
				require.Equal(t, devquiescence.CleanupOnly, devquiescence.RequestMode(r))
				require.Equal(t, scenario.target, r.Header.Get("X-Amz-Target"))
				data, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Equal(t, payload, string(data))
				w.WriteHeader(http.StatusNoContent)
			})
			request := httptest.NewRequest("POST", scenario.path, strings.NewReader(payload))
			request.Header.Set("X-Amz-Target", scenario.target)
			response := httptest.NewRecorder()
			owner.Wrap(devquiescence.Callback, router, retainedCleanupHandler(router)).ServeHTTP(response, request)
			require.Equal(t, scenario.allowed, called)
			if scenario.allowed {
				require.Equal(t, http.StatusNoContent, response.Code)
				rejoined, err := owner.Quiesce(t.Context())
				require.NoError(t, err)
				require.Equal(t, held.Generation, rejoined.Generation)
				require.True(t, rejoined.FixtureSafe)
			} else {
				require.Equal(t, http.StatusServiceUnavailable, response.Code)
				require.True(t, owner.Snapshot().FixtureSafe)
			}
		})
	}
}
