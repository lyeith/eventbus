package cognitotrigger

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadStrictCompleteConfiguration(t *testing.T) {
	valid := "pools:\n  local-pool:\n    DefineAuthChallenge: {handler: define.mjs#handler}\n    CreateAuthChallenge: {handler: create.cjs, timeout_seconds: 2, env: {AWS_REGION: us-east-1}}\n    VerifyAuthChallengeResponse: {handler: verify.js#custom}\n"
	for _, scenario := range []struct {
		name, body string
		success    bool
	}{
		{"valid", valid, true},
		{"unknown-top-level", "unknown: true\n" + valid, false},
		{"unknown-trigger", strings.Replace(valid, "CreateAuthChallenge:", "OtherTrigger:", 1), false},
		{"unknown-entry-field", strings.Replace(valid, "timeout_seconds:", "timeout:", 1), false},
		{"missing-trigger", "pools: {local-pool: {DefineAuthChallenge: {handler: define.mjs}}}", false},
		{"empty-pools", "pools: {}", false},
		{"extra-document", valid + "---\npools: {}", false},
		{"invalid-export", strings.Replace(valid, "define.mjs#handler", "define.mjs#handler.bad", 1), false},
		{"negative-timeout", strings.Replace(valid, "timeout_seconds: 2", "timeout_seconds: -1", 1), false},
		{"excess-timeout", strings.Replace(valid, "timeout_seconds: 2", "timeout_seconds: 6", 1), false},
		{"invalid-env-name", strings.Replace(valid, "AWS_REGION", "INVALID=KEY", 1), false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "triggers.yaml")
			require.NoError(t, os.WriteFile(path, []byte(scenario.body), 0600))
			config, err := Load(path)
			if !scenario.success {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "define.mjs#handler", config.Pools["local-pool"].DefineAuthChallenge.Handler)
			require.Equal(t, 2, config.Pools["local-pool"].CreateAuthChallenge.TimeoutSeconds)
		})
	}
}

func TestNewRejectsMissingRuntimeAndModule(t *testing.T) {
	dir := t.TempDir()
	config := fixtureConfig("missing.mjs", nil)
	_, err := New(config, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "module")
	config.Node = filepath.Join(dir, "absent-node")
	_, err = New(config, dir)
	require.Error(t, err)
	require.Contains(t, err.Error(), "executable")
}

func TestProcessEnvironmentRequiresExplicitAWSAndNodeOptions(t *testing.T) {
	for name, value := range map[string]string{
		"AWS_PROFILE": "ambient-profile", "AWS_ACCESS_KEY_ID": "ambient-key",
		"HTTPS_PROXY": "ambient-proxy", "NODE_OPTIONS": "ambient-options",
		"NODE_PATH": "ambient-module-path", "HOME": "ambient-home",
	} {
		t.Setenv(name, value)
	}
	values := processEnvironment(map[string]string{
		"AWS_REGION": "ap-southeast-1", "AWS_ACCESS_KEY_ID": "declared-key",
		"AWS_SECRET_ACCESS_KEY": "declared-secret", "APP_SETTING": "value",
	})
	environment := strings.Join(values, "\n")
	require.NotContains(t, environment, "ambient")
	require.Contains(t, environment, "AWS_ACCESS_KEY_ID=declared-key")
	require.Contains(t, environment, "AWS_EC2_METADATA_DISABLED=true")
	require.Contains(t, environment, "AWS_SHARED_CREDENTIALS_FILE="+os.DevNull)
	require.Contains(t, environment, "APP_SETTING=value")
}
