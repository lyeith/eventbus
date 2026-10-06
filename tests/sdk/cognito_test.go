//go:build sdksmoke

package sdk

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

func TestCognitoSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	for _, script := range []string{"smoke_admin_ops.py", "smoke_initiate_auth.py", "smoke_tier_c.py"} {
		t.Run(script, func(t *testing.T) {
			store, err := cognito.OpenCognitoStore(filepath.Join(t.TempDir(), "cognito.db"))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			serving := httptest.NewUnstartedServer(nil)
			endpoint := "http://" + serving.Listener.Addr().String()
			serving.Config.Handler = server.New(server.Services{
				Cognito:     cognito.NewHandler(store, cognito.Options{IssuerBase: endpoint}),
				CognitoURLs: &server.CognitoURLs{Issuer: endpoint, JWKS: endpoint},
			})
			t.Cleanup(serving.Close)
			pool := "sdk-pool-" + uuid.NewString()
			client := "sdk-client-" + uuid.NewString()
			require.NoError(t, store.UpsertPool(t.Context(), pool, "us-east-1"))
			require.NoError(t, store.UpsertClient(t.Context(), client, pool, ""))
			serving.Start()

			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			scriptPath := fixturePath("python", script)
			output, err := runSDKProcess(ctx, python, scriptPath, sdkEnvironment(t.TempDir(), endpoint, pool, client))
			t.Logf("real SDK %s:\n%s", script, output)
			require.NoError(t, err)
		})
	}
}
