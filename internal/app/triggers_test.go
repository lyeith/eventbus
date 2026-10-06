package app

import (
	"context"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestCognitoTriggersDisabledDoesNotRequireNodeOrWorkDir(t *testing.T) {
	t.Setenv("PATH", "")
	runner, err := loadCognitoTriggers("", filepath.Join(t.TempDir(), "missing-root"))
	require.NoError(t, err)
	require.Nil(t, runner)
}

func TestCognitoTriggerConfigAndHandlersResolveAgainstApplicationRoot(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "settings"), 0700))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "handlers"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "handlers", "auth.mjs"), []byte("export async function handler(event){event.response.application=process.env.APP_SETTING;return event;}"), 0600))
	config := "pools:\n  owned-pool:\n    DefineAuthChallenge: {handler: handlers/auth.mjs, env: {APP_SETTING: app-owned}}\n    CreateAuthChallenge: {handler: handlers/auth.mjs}\n    VerifyAuthChallengeResponse: {handler: handlers/auth.mjs}\n"
	path := filepath.Join(root, "settings", "triggers.yaml")
	require.NoError(t, os.WriteFile(path, []byte(config), 0600))
	for _, configuredPath := range []string{"settings/triggers.yaml", path} {
		runner, err := loadCognitoTriggers(configuredPath, root)
		require.NoError(t, err)
		require.True(t, runner.Supports("owned-pool"))
		result, err := runner.Invoke(t.Context(), "owned-pool", cognitotrigger.DefineAuthChallenge, map[string]any{"response": map[string]any{}})
		require.NoError(t, err)
		require.Equal(t, "app-owned", result["response"].(map[string]any)["application"])
		require.NoError(t, runner.Close(t.Context()))
	}
}

func TestRunWithoutTriggersStartsAndClosesWithNoNode(t *testing.T) {
	t.Setenv("PATH", "")
	cfg := runTestConfig(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, run(ctx, cfg))
}

func TestRunRejectsConfiguredTriggerFailureBeforeListening(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.cognitoTriggers = "settings/missing-triggers.yaml"
	err := run(t.Context(), cfg)
	require.ErrorContains(t, err, "failed to configure Cognito triggers")
}

func TestRunConfiguredTriggersClosesWithoutStartingChildren(t *testing.T) {
	cfg := runTestConfig(t)
	require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "auth.mjs"), []byte("throw new Error('must not execute during construction');"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(cfg.workDir, "triggers.yaml"), []byte("pools:\n  owned-pool:\n    DefineAuthChallenge: {handler: auth.mjs}\n    CreateAuthChallenge: {handler: auth.mjs}\n    VerifyAuthChallengeResponse: {handler: auth.mjs}\n"), 0600))
	cfg.cognitoTriggers = "triggers.yaml"
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.NoError(t, run(ctx, cfg))
}

func runTestConfig(t *testing.T) config {
	t.Helper()
	flags := flag.NewFlagSet("eventbus-test", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	cfg, err := readConfig(flags, nil)
	require.NoError(t, err)
	dir := t.TempDir()
	cfg.port = 0
	cfg.cognitoDB = filepath.Join(dir, "cognito.db")
	cfg.sesLog = filepath.Join(dir, "ses.jsonl")
	cfg.workDir = dir
	level, logger, timeFormat := zerolog.GlobalLevel(), log.Logger, zerolog.TimeFieldFormat
	t.Cleanup(func() { zerolog.SetGlobalLevel(level); log.Logger = logger; zerolog.TimeFieldFormat = timeFormat })
	return cfg
}
