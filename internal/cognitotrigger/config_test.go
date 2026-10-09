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
		{"warm", "dev_warm: {max_workers: 3}\n" + valid, true},
		{"warm-excess-cap", "dev_warm: {max_workers: 33}\n" + valid, false},
		{"warm-negative-cap", "dev_warm: {max_workers: -1}\n" + valid, false},
		{"warm-unknown", "dev_warm: {capacity: 3}\n" + valid, false},
		{"unknown-top-level", "unknown: true\n" + valid, false},
		{"observer-is-not-yaml", "dev_activity: {}\n" + valid, false},
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
