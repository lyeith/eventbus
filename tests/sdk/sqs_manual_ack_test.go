//go:build sdksmoke

package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/eventsource"
	"github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// This fixture composes the same queue and execution ports as app. Its only
// control closes the real mapping owner; the Python handler uses native SDKs.
type sqsManualAckSDKFixture struct {
	serving  *httptest.Server
	root     string
	database string
}

func newSQSManualAckSDKFixture(t *testing.T, python string) *sqsManualAckSDKFixture {
	t.Helper()
	directory := t.TempDir()
	root := filepath.Join(directory, "evidence")
	require.NoError(t, os.Mkdir(root, 0700))
	fixture := &sqsManualAckSDKFixture{root: root, database: filepath.Join(directory, "business.db"), serving: httptest.NewUnstartedServer(nil)}
	endpoint := "http://" + fixture.serving.Listener.Addr().String()
	function := lambda.Function{
		Runtime: "python", Command: []string{python, "-E", "-s"},
		Handler: fixturePath("python", "sqs_manual_ack_smoke.py") + "#handler", Timeout: 4 * time.Second,
		Environment: map[string]string{"SQS_MANUAL_ACK_ENDPOINT": endpoint, "SQS_MANUAL_ACK_ROOT": root,
			"SQS_MANUAL_ACK_DATABASE": fixture.database, "SQS_MANUAL_ACK_VARIANT": "live"},
	}
	short := function
	short.Timeout = 1500 * time.Millisecond
	short.Environment = map[string]string{"SQS_MANUAL_ACK_ENDPOINT": endpoint, "SQS_MANUAL_ACK_ROOT": root,
		"SQS_MANUAL_ACK_DATABASE": fixture.database, "SQS_MANUAL_ACK_VARIANT": "short"}
	// Native same-name recreation requires 60 seconds. A separate alias keeps
	// the original admitted invocation live across that real AWS cooldown.
	long := function
	long.Timeout = 75 * time.Second
	long.Environment = map[string]string{"SQS_MANUAL_ACK_ENDPOINT": endpoint, "SQS_MANUAL_ACK_ROOT": root,
		"SQS_MANUAL_ACK_DATABASE": fixture.database, "SQS_MANUAL_ACK_VARIANT": "long"}
	var functions *lambda.Service
	var mappings *eventsource.Service
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var failures []error
		if mappings != nil {
			failures = append(failures, mappings.Close(ctx))
		}
		fixture.serving.Close()
		if functions != nil {
			failures = append(failures, functions.Close(ctx))
		}
		if err := errors.Join(failures...); err != nil {
			t.Errorf("manual ACK SDK owned cleanup: %v", err)
		}
	})
	var err error
	functions, err = lambda.NewService(&lambda.Config{Functions: map[string]lambda.Function{
		"manual-handler:live": function, "manual-handler:short": short, "manual-handler:long": long,
	}}, directory)
	require.NoError(t, err)
	broker := messaging.NewBroker("us-east-1", "000000000000", fixture.serving.Listener.Addr().(*net.TCPAddr).Port)
	mappings, err = eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000"}, sdkMappingSource{broker}, sdkMappingInvoker{functions})
	require.NoError(t, err)
	aws := server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions, EventSources: eventsource.NewHandler(mappings)})
	fixture.serving.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/__sdk/close-mappings" {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := mappings.Close(ctx); err != nil {
				http.Error(w, "owned mappings did not join", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]bool{"closed": true})
			return
		}
		aws.ServeHTTP(w, r)
	})
	fixture.serving.Start()
	return fixture
}

func TestSQSLambdaManualAcknowledgePythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	fixture := newSQSManualAckSDKFixture(t, python)
	environment := append(sdkEnvironment(t.TempDir(), "", "", ""), "SQS_MANUAL_ACK_ENDPOINT="+fixture.serving.URL,
		"SQS_MANUAL_ACK_ROOT="+fixture.root, "SQS_MANUAL_ACK_DATABASE="+fixture.database)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "sqs_manual_ack_smoke.py"), environment)
	t.Logf("actual boto3 manual/mapping ACK, durable business, FIFO and ownership proof:\n%s", output)
	require.NoError(t, err)
}
