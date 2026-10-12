//go:build sdksmoke

package sdk

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/stretchr/testify/require"
)

// Each language owns two SQLite stores and OS-assigned loopback listeners.
// Provisioning, inventories, validation, persistence and cleanup use native SDK
// requests only; Go supplies resource lifetime and no private inventory reads.
func TestCognitoInventorySDKSmoke(t *testing.T) {
	for _, language := range []string{"javascript", "python"} {
		t.Run(language, func(t *testing.T) {
			directory := t.TempDir()
			fixtures := []*cognitoInventorySDKFixture{
				{path: filepath.Join(directory, "first.db")},
				{path: filepath.Join(directory, "second.db")},
			}
			for _, fixture := range fixtures {
				fixture.start(t)
				t.Cleanup(func() { fixture.close(t) })
			}
			run := func(phase string) {
				t.Helper()
				ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
				defer cancel()
				environment := append(sdkEnvironment(directory, fixtures[0].serving.URL, "", ""),
					"COGNITO_SECOND_ENDPOINT_URL="+fixtures[1].serving.URL,
					"SMOKE_STATE_PATH="+filepath.Join(directory, "inventory.json"),
					"SMOKE_PHASE="+phase,
				)
				var output []byte
				var err error
				if language == "javascript" {
					output, err = runJavascriptSDKProcess(ctx, sdkNode(t), fixturePath("javascript", "inventory.mjs"), []string{phase}, environment)
				} else {
					output, err = runSDKProcess(ctx, sdkPython(t), fixturePath("python", "smoke_cognito_inventory.py"), environment)
				}
				t.Logf("real %s inventory SDK %s:\n%s", language, phase, output)
				require.NoError(t, err)
			}
			run("exercise")
			for _, fixture := range fixtures {
				fixture.close(t)
				fixture.start(t)
			}
			run("restart")
		})
	}
}

type cognitoInventorySDKFixture struct {
	path    string
	store   *cognito.CognitoStore
	serving *httptest.Server
}

func (f *cognitoInventorySDKFixture) start(t *testing.T) {
	t.Helper()
	store, err := cognito.OpenCognitoStore(f.path)
	require.NoError(t, err)
	f.store = store
	f.serving = httptest.NewServer(server.New(server.Services{
		Cognito: cognito.NewHandler(store, cognito.Options{Region: "eu-west-1", AccountID: "123456789012"}),
	}))
}

func (f *cognitoInventorySDKFixture) close(t *testing.T) {
	t.Helper()
	if f.serving != nil {
		f.serving.Close()
		f.serving = nil
	}
	if f.store != nil {
		require.NoError(t, f.store.Close())
		f.store = nil
	}
}
