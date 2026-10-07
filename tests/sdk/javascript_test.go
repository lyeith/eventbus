//go:build sdksmoke

package sdk

import (
	"context"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/stretchr/testify/require"
)

func sdkNode(t *testing.T) string {
	t.Helper()
	node := os.Getenv("EVENTBUS_SMOKE_NODE")
	if node == "" {
		var err error
		node, err = exec.LookPath("node")
		require.NoError(t, err, "Node >=20 is required for the JavaScript SDK lane")
	}
	absolute, err := filepath.Abs(node)
	require.NoError(t, err)
	info, err := os.Stat(absolute)
	require.NoError(t, err)
	require.False(t, info.IsDir())
	return absolute
}

func runJavascriptSDKProcess(ctx context.Context, node, script string, arguments, environment []string) ([]byte, error) {
	command := exec.CommandContext(ctx, node, append([]string{script}, arguments...)...)
	command.Env = environment
	command.WaitDelay = time.Second
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		return output, fmt.Errorf("JavaScript SDK smoke deadline: %w", ctx.Err())
	}
	if err != nil {
		return output, fmt.Errorf("JavaScript SDK smoke child: %w", err)
	}
	passes := 0
	for _, line := range strings.Split(string(output), "\n") {
		switch strings.TrimSpace(line) {
		case "PASS":
			passes++
		case "FAIL":
			return output, fmt.Errorf("JavaScript SDK smoke reported FAIL")
		}
	}
	if passes != 1 {
		return output, fmt.Errorf("JavaScript SDK smoke requires exactly one PASS marker, got %d", passes)
	}
	return output, nil
}

type javascriptFixture struct {
	node, directory, address, endpoint, pool, runID string
	serving                                         *httptest.Server
	store                                           *cognito.CognitoStore
	triggers                                        *cognitotrigger.Runner
	capture                                         *ses.SESManager
	clockOffset                                     atomic.Int64
}

func newJavascriptFixture(t *testing.T, node string) *javascriptFixture {
	t.Helper()
	serving := httptest.NewUnstartedServer(nil)
	fixture := &javascriptFixture{
		node: node, directory: t.TempDir(), serving: serving,
		address: serving.Listener.Addr().String(), endpoint: "http://" + serving.Listener.Addr().String(),
		pool:  "us-east-1_" + strings.ReplaceAll(uuid.NewString(), "-", ""),
		runID: "js-" + uuid.NewString(),
	}
	t.Cleanup(func() { fixture.close(t) })
	fixture.start(t, "define.mjs")
	return fixture
}

func (fixture *javascriptFixture) start(t *testing.T, defineModule string) {
	t.Helper()
	if fixture.serving == nil {
		fixture.serving = httptest.NewUnstartedServer(nil)
		require.NoError(t, fixture.serving.Listener.Close())
		listener, err := net.Listen("tcp", fixture.address)
		require.NoError(t, err, "rebind only the SDK fixture's owned loopback address")
		fixture.serving.Listener = listener
	}
	store, err := cognito.OpenCognitoStore(filepath.Join(fixture.directory, "cognito.db"))
	require.NoError(t, err)
	fixture.store = store
	require.NoError(t, store.UpsertPool(t.Context(), fixture.pool, "us-east-1"))
	capture, err := ses.OpenSESCapture(filepath.Join(fixture.directory, "emails.jsonl"))
	require.NoError(t, err)
	fixture.capture = ses.NewSESManager(ses.SESFixtures{}, capture)
	entry := func(name string) *cognitotrigger.Entry {
		return &cognitotrigger.Entry{Handler: filepath.Join("triggers", name) + "#handler", TimeoutSeconds: 1}
	}
	create := entry("create.mjs")
	create.TimeoutSeconds = 5
	create.Env = map[string]string{"SES_ENDPOINT_URL": fixture.endpoint}
	triggers, err := cognitotrigger.New(&cognitotrigger.Config{
		Node: fixture.node,
		Pools: map[string]cognitotrigger.Pool{fixture.pool: {
			DefineAuthChallenge: entry(defineModule), CreateAuthChallenge: create,
			VerifyAuthChallengeResponse: entry("verify.mjs"),
		}},
	}, fixturePath("javascript"))
	require.NoError(t, err)
	fixture.triggers = triggers
	fixture.serving.Config.Handler = server.New(server.Services{
		Cognito:     cognito.NewHandler(store, cognito.Options{DevProfile: cognito.DevProfileLegacyFixtures, IssuerBase: fixture.endpoint, Triggers: triggers, Clock: func() time.Time { return time.Now().Add(time.Duration(fixture.clockOffset.Load()) * time.Second) }}),
		CognitoURLs: &server.CognitoURLs{Issuer: fixture.endpoint, JWKS: fixture.endpoint},
		SES:         ses.NewHandler(fixture.capture),
	})
	fixture.serving.Start()
	require.Equal(t, fixture.endpoint, fixture.serving.URL)
}

func (fixture *javascriptFixture) close(t *testing.T) {
	t.Helper()
	if fixture.serving != nil {
		fixture.serving.Close()
		fixture.serving = nil
	}
	if fixture.triggers != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		require.NoError(t, fixture.triggers.Close(ctx))
		cancel()
		fixture.triggers = nil
	}
	if fixture.capture != nil {
		require.NoError(t, fixture.capture.Close())
		fixture.capture = nil
	}
	if fixture.store != nil {
		require.NoError(t, fixture.store.Close())
		fixture.store = nil
	}
}

func (fixture *javascriptFixture) run(t *testing.T, script, phase string, extra ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	environment := append(sdkEnvironment(fixture.directory, fixture.endpoint, fixture.pool, ""),
		"SMOKE_RUN_ID="+fixture.runID,
		"SMOKE_STATE_PATH="+filepath.Join(fixture.directory, "state.json"),
		"SES_CAPTURE_PATH="+filepath.Join(fixture.directory, "emails.jsonl"),
	)
	environment = append(environment, extra...)
	output, err := runJavascriptSDKProcess(ctx, fixture.node, fixturePath("javascript", script), []string{phase}, environment)
	t.Logf("real JavaScript SDK %s %s:\n%s", script, phase, output)
	require.NoError(t, err)
}

// Each run owns its users, database, capture and loopback listener. A restart
// closes every owned resource and reopens the same store and address.
func TestCognitoJavascriptSDKSmoke(t *testing.T) {
	node := sdkNode(t)
	for run := range 2 {
		t.Run(fmt.Sprintf("isolated-%d", run+1), func(t *testing.T) {
			fixture := newJavascriptFixture(t, node)
			fixture.run(t, "lifecycle.mjs", "exercise")
			fixture.run(t, "custom_auth.mjs", "exercise")
			fixture.run(t, "custom_auth.mjs", "prepare-expiry")
			// Advance the owned service clock beyond the native/legacy challenge
			// lifetime; the SDK still submits the real pending signed session.
			fixture.clockOffset.Store(6 * 60)
			fixture.run(t, "custom_auth.mjs", "assert-expiry")
			fixture.clockOffset.Store(0)
			fixture.run(t, "custom_auth.mjs", "prepare-restart")
			fixture.close(t)
			fixture.start(t, "define.mjs")
			fixture.run(t, "lifecycle.mjs", "restart")
			fixture.run(t, "custom_auth.mjs", "resume-restart")

			for _, failure := range []struct{ module, code string }{
				{"failure.mjs", "UnexpectedLambdaException"},
				{"invalid.mjs", "InvalidLambdaResponseException"},
				{"timeout.mjs", "UnexpectedLambdaException"},
			} {
				fixture.close(t)
				fixture.start(t, failure.module)
				fixture.run(t, "custom_auth.mjs", "trigger-error", "EXPECT_TRIGGER_ERROR="+failure.code)
			}
		})
	}
}

func TestJavascriptSDKProcessControls(t *testing.T) {
	node := sdkNode(t)
	for _, scenario := range []struct {
		name, body string
		pass       bool
	}{
		{"success", "console.log('PASS');", true},
		{"empty", "", false},
		{"false-marker", "console.log('NOT PASS');", false},
		{"duplicate", "console.log('PASS\\nPASS');", false},
		{"nonzero", "console.log('PASS'); process.exitCode=7;", false},
		{"assertion", "console.log('PASS'); throw new Error('assertion failed');", false},
		{"conflicting-markers", "console.log('FAIL\\nPASS');", false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			script := filepath.Join(t.TempDir(), "probe.mjs")
			require.NoError(t, os.WriteFile(script, []byte(scenario.body), 0o600))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, err := runJavascriptSDKProcess(ctx, node, script, nil, sdkEnvironment(t.TempDir(), "", "", ""))
			if scenario.pass {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
	t.Run("deadline", func(t *testing.T) {
		script := filepath.Join(t.TempDir(), "probe.mjs")
		require.NoError(t, os.WriteFile(script, []byte("setTimeout(()=>console.log('PASS'), 30000);"), 0o600))
		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()
		_, err := runJavascriptSDKProcess(ctx, node, script, nil, sdkEnvironment(t.TempDir(), "", "", ""))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("missing-script", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := runJavascriptSDKProcess(ctx, node, filepath.Join(t.TempDir(), "absent.mjs"), nil, sdkEnvironment(t.TempDir(), "", "", ""))
		require.Error(t, err)
	})
	t.Run("environment-isolation", func(t *testing.T) {
		t.Setenv("AWS_PROFILE", "inherited-profile")
		t.Setenv("HTTPS_PROXY", "inherited-proxy")
		t.Setenv("NODE_OPTIONS", "--require=inherited-preload")
		script := filepath.Join(t.TempDir(), "probe.mjs")
		require.NoError(t, os.WriteFile(script, []byte("import assert from 'node:assert/strict'; for(const key of ['AWS_PROFILE','HTTPS_PROXY','NODE_OPTIONS']) assert.equal(process.env[key],undefined); console.log('PASS');"), 0o600))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := runJavascriptSDKProcess(ctx, node, script, nil, sdkEnvironment(t.TempDir(), "", "", ""))
		require.NoError(t, err)
	})
}
