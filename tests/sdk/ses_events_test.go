//go:build sdksmoke

package sdk

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/stretchr/testify/require"
)

// This test adapter only connects SES's port to the real SNS owner. Native
// destination validation, event selection, envelopes and retry remain in core.
type sdkSESEventPublisher struct{ broker *messaging.Broker }

func (publisher sdkSESEventPublisher) ValidateTopic(arn string) error {
	if publisher.broker.GetTopic(arn) == nil {
		return errors.New("SNS topic does not exist")
	}
	return nil
}

func (publisher sdkSESEventPublisher) PublishEvent(ctx context.Context, arn, message, requestID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := publisher.broker.PublishSNS(messaging.SNSPublishInput{
		Operation: "Publish", RequestID: requestID, TopicARN: arn, Message: message,
	})
	return err
}

func TestSESConfigurationEventsPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	directory := t.TempDir()
	output, gate := filepath.Join(directory, "effects.jsonl"), filepath.Join(directory, "retry-gate")
	asyncPath := filepath.Join(directory, "lambda.jsonl")
	sesPath, snsPath := filepath.Join(directory, "ses.jsonl"), filepath.Join(directory, "sns.jsonl")
	functions, err := lambda.NewService(&lambda.Config{
		Functions: map[string]lambda.Function{
			"ses-tracking:live": {
				Runtime: "python", Command: []string{python, "-E", "-s"},
				Handler:     fixturePath("python", "ses_events_smoke.py") + "#handler",
				Timeout:     5 * time.Second,
				Environment: map[string]string{"SES_EVENTS_OUTPUT": output, "SES_EVENTS_GATE": gate},
			},
		},
		DevAsync: &lambda.DevAsyncConfig{Workers: 1, Capacity: 16, RetryDelays: []time.Duration{0, 0}, LogPath: asyncPath},
	}, directory)
	require.NoError(t, err)
	snsCapture, err := messaging.OpenSNSCapture(snsPath)
	require.NoError(t, err)
	sesCapture, err := ses.OpenSESCapture(sesPath)
	require.NoError(t, err)
	serving := httptest.NewUnstartedServer(nil)
	broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
	broker.SetSNSCapture(snsCapture)
	broker.SetLambdaDelivery(sdkSNSLambdaDelivery{functions: functions})
	manager := ses.NewSESManager(ses.SESFixtures{}, sesCapture,
		ses.WithEventPublisher(sdkSESEventPublisher{broker}, "us-east-1", "000000000000"))
	handler := ses.NewHandler(manager)
	native := server.New(server.Services{SES: handler, Messaging: messaging.NewHandler(broker), Lambda: functions, QueryBodyLimit: ses.QueryBodyLimit})
	serving.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == ses.DevOutcomePath {
			handler.ServeDevOutcome(w, r)
			return
		}
		native.ServeHTTP(w, r)
	})
	serving.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		drainErr := functions.DrainAsync(ctx)
		serving.Close()
		closeErr := functions.Close(ctx)
		sesErr, snsErr := manager.Close(), snsCapture.Close()
		require.NoError(t, errors.Join(drainErr, closeErr, sesErr, snsErr))
	})
	env := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"SES_EVENTS_ENDPOINT="+serving.URL,
		"SES_EVENTS_OUTPUT="+output,
		"SES_EVENTS_GATE="+gate,
		"SES_EVENTS_ASYNC="+asyncPath,
		"SES_EVENTS_CAPTURE="+sesPath,
		"SES_EVENTS_FUNCTION_ARN=arn:aws:lambda:us-east-1:000000000000:function:ses-tracking:live",
	)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	logs, err := runSDKProcess(ctx, python, fixturePath("python", "ses_events_smoke.py"), env)
	t.Logf("real SES SDK send/configuration + SNS/native Lambda proof:\n%s", logs)
	require.NoError(t, err)
}
