//go:build sdksmoke

package sdk

import (
	"context"
	"encoding/json"
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

type sqsBatchSDKFixture struct {
	serving *httptest.Server
	root    string
}

func newSQSBatchSDKFixture(t *testing.T, python, owner string) *sqsBatchSDKFixture {
	t.Helper()
	directory := t.TempDir()
	root := filepath.Join(directory, "evidence")
	require.NoError(t, os.Mkdir(root, 0700))
	function := lambda.Function{
		Runtime: "python", Command: []string{python, "-E", "-s"}, Handler: fixturePath("python", "sqs_batch_smoke.py") + "#handler", Timeout: 6 * time.Second,
		Environment: map[string]string{"SQS_BATCH_ROOT": root, "SQS_BATCH_OWNER": owner, "SQS_BATCH_VARIANT": "base"},
	}
	alias, short := function, function
	alias.Environment = map[string]string{"SQS_BATCH_ROOT": root, "SQS_BATCH_OWNER": owner, "SQS_BATCH_VARIANT": "alias"}
	short.Environment = map[string]string{"SQS_BATCH_ROOT": root, "SQS_BATCH_OWNER": owner, "SQS_BATCH_VARIANT": "short"}
	short.Timeout = 750 * time.Millisecond
	functions, err := lambda.NewService(&lambda.Config{
		Functions: map[string]lambda.Function{"batch-handler": function, "batch-handler:live": alias, "batch-handler:short": short},
		DevAsync:  &lambda.DevAsyncConfig{LogPath: filepath.Join(directory, "async.jsonl")},
	}, directory)
	require.NoError(t, err)
	serving := httptest.NewUnstartedServer(nil)
	broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
	mappings, err := eventsource.New(eventsource.Options{Region: "us-east-1", AccountID: "000000000000"}, sdkMappingSource{broker: broker}, sdkMappingInvoker{functions: functions})
	require.NoError(t, err)
	aws := server.New(server.Services{Messaging: messaging.NewHandler(broker), Lambda: functions, EventSources: eventsource.NewHandler(mappings)})
	serving.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/__sdk/close-mappings" && r.Method == http.MethodPost {
			// Fixture control closes the actual owner; event generation, process
			// execution, lease and settlement policies remain in the core.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := mappings.Close(ctx); err != nil {
				http.Error(w, "owned mappings did not close", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]bool{"closed": true})
			return
		}
		aws.ServeHTTP(w, r)
	})
	serving.Start()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		mappingErr := mappings.Close(ctx)
		serving.Close()
		functionErr := functions.Close(ctx)
		if mappingErr != nil || functionErr != nil {
			t.Errorf("batch SDK owner cleanup: mappings=%v Lambda=%v", mappingErr, functionErr)
		}
	})
	return &sqsBatchSDKFixture{serving: serving, root: root}
}

func TestSQSBatchPythonSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	first, second := newSQSBatchSDKFixture(t, python, "A"), newSQSBatchSDKFixture(t, python, "B")
	env := append(sdkEnvironment(t.TempDir(), "", "", ""),
		"SQS_BATCH_ENDPOINT_A="+first.serving.URL, "SQS_BATCH_ROOT_A="+first.root,
		"SQS_BATCH_ENDPOINT_B="+second.serving.URL, "SQS_BATCH_ROOT_B="+second.root)
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	output, err := runSDKProcess(ctx, python, fixturePath("python", "sqs_batch_smoke.py"), env)
	t.Logf("real boto3 batch/concurrency/FIFO/settlement/child-join proof:\n%s", output)
	require.NoError(t, err)
}
