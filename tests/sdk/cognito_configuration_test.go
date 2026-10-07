//go:build sdksmoke

package sdk

import (
	"context"
	"net"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// Real SDK provisioning starts with no seeded domain rows, then reopens the
// same owned address and SQLite file to verify persistence and token admission.
func TestCognitoNativeConfigurationSDK(t *testing.T) {
	python, node := sdkPython(t), sdkNode(t)
	directory := t.TempDir()
	var clock atomic.Int64
	clock.Store(time.Now().Unix())
	serving := httptest.NewUnstartedServer(nil)
	address, endpoint := serving.Listener.Addr().String(), "http://"+serving.Listener.Addr().String()
	var store *cognito.CognitoStore
	var capture *cognito.NotificationCapture
	start := func() {
		var err error
		if serving == nil {
			serving = httptest.NewUnstartedServer(nil)
			require.NoError(t, serving.Listener.Close())
			serving.Listener, err = net.Listen("tcp", address)
			require.NoError(t, err)
		}
		store, err = cognito.OpenCognitoStore(filepath.Join(directory, "cognito.db"))
		require.NoError(t, err)
		capture, err = cognito.OpenNotificationCapture(filepath.Join(directory, "notifications.jsonl"))
		require.NoError(t, err)
		handler := cognito.NewHandler(store, cognito.Options{IssuerBase: endpoint, Region: "eu-west-1", AccountID: "123456789012", Notifications: capture, Clock: func() time.Time { return time.Unix(clock.Load(), 0) }})
		serving.Config.Handler = server.New(server.Services{Cognito: handler, CognitoURLs: &server.CognitoURLs{Issuer: endpoint, JWKS: endpoint}})
		serving.Start()
		require.Equal(t, endpoint, serving.URL)
	}
	closeOwned := func() {
		if serving != nil {
			serving.Close()
			serving = nil
		}
		if capture != nil {
			require.NoError(t, capture.Close())
			capture = nil
		}
		if store != nil {
			require.NoError(t, store.Close())
			store = nil
		}
	}
	t.Cleanup(closeOwned)
	start()
	isolatedStore, err := cognito.OpenCognitoStore(filepath.Join(directory, "isolated.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, isolatedStore.Close()) })
	isolated := httptest.NewServer(server.New(server.Services{Cognito: cognito.NewHandler(isolatedStore, cognito.Options{Region: "eu-west-1"})}))
	t.Cleanup(isolated.Close)
	environment := append(sdkEnvironment(directory, endpoint, "", ""), "SMOKE_STATE_PATH="+filepath.Join(directory, "state.json"), "COGNITO_CAPTURE_PATH="+filepath.Join(directory, "notifications.jsonl"), "COGNITO_ISOLATED_ENDPOINT="+isolated.URL)
	runPython := func(phase string) {
		ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
		defer cancel()
		output, err := runSDKProcess(ctx, python, fixturePath("python", "smoke_cognito_configuration.py"), append(environment, "SMOKE_PHASE="+phase))
		t.Logf("native Python SDK %s:\n%s", phase, output)
		require.NoError(t, err)
	}
	runPython("create")
	closeOwned()
	start()
	runPython("restart")
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	output, err := runJavascriptSDKProcess(ctx, node, fixturePath("javascript", "native_configuration.mjs"), nil, environment)
	cancel()
	t.Logf("native Node SDK:\n%s", output)
	require.NoError(t, err)
	clock.Add(3600)
	runPython("expiry-cleanup")
}
