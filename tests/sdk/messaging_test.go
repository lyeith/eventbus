//go:build sdksmoke

package sdk

import (
	"context"
	"net"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestMessagingPythonSDKSmoke(t *testing.T) {
	capturePath := filepath.Join(t.TempDir(), "sns.jsonl")
	capture, err := messaging.OpenSNSCapture(capturePath)
	require.NoError(t, err)
	serving := httptest.NewUnstartedServer(nil)
	broker := messaging.NewBroker("us-east-1", "000000000000", serving.Listener.Addr().(*net.TCPAddr).Port)
	broker.SetSNSCapture(capture)
	serving.Config.Handler = server.New(server.Services{Messaging: messaging.NewHandler(broker)})
	serving.Start()
	t.Cleanup(func() { serving.Close(); require.NoError(t, capture.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	env := append(sdkEnvironment(t.TempDir(), "", "", ""), "MESSAGING_ENDPOINT_URL="+serving.URL, "SNS_CAPTURE_LOG="+capturePath)
	output, err := runSDKProcess(ctx, sdkPython(t), fixturePath("python", "smoke_messaging.py"), env)
	t.Logf("real SQS/SNS Python SDK:\n%s", output)
	require.NoError(t, err)
}
