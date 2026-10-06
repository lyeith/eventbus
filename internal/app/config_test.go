package app

import (
	"flag"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCLIConfigDefaultsAndOverrides(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		check func(*testing.T, config)
	}{
		{"defaults", nil, func(t *testing.T, c config) {
			require.Equal(t, 4100, c.port)
			require.Equal(t, "us-east-1", c.region)
			require.Equal(t, "000000000000", c.accountID)
			require.Equal(t, "http://localhost:4100", c.issuerBase)
			require.Empty(t, c.jwksBase)
			require.Equal(t, "/tmp/cognito-dev.db", c.cognitoDB)
			require.Equal(t, time.Hour, c.accessTokenTTL)
			require.Equal(t, 24*time.Hour, c.refreshTokenTTL)
			require.Equal(t, "-", c.sesLog)
		}},
		{"overrides", []string{"--port", "14100", "--region", "local-1", "--account-id", "123", "--s3-endpoint", "http://127.0.0.1:9001", "--consumers", "app/consumers.yaml", "--work-dir", "app", "--issuer-base", "http://issuer", "--jwks-base", "http://keys", "--cognito-pools", "pools.yaml", "--cognito-db", "identities.db", "--access-token-ttl", "30m", "--refresh-token-ttl", "48h", "--ses-log", "emails.jsonl", "--ses-config", "ses.yaml", "--debug"}, func(t *testing.T, c config) {
			require.Equal(t, 14100, c.port)
			require.Equal(t, "local-1", c.region)
			require.Equal(t, "123", c.accountID)
			require.Equal(t, "http://127.0.0.1:9001", c.s3Endpoint)
			require.Equal(t, "app/consumers.yaml", c.consumersFile)
			require.Equal(t, "app", c.workDir)
			require.Equal(t, "http://issuer", c.issuerBase)
			require.Equal(t, "http://keys", c.jwksBase)
			require.Equal(t, "pools.yaml", c.cognitoPools)
			require.Equal(t, "identities.db", c.cognitoDB)
			require.Equal(t, 30*time.Minute, c.accessTokenTTL)
			require.Equal(t, 48*time.Hour, c.refreshTokenTTL)
			require.Equal(t, "emails.jsonl", c.sesLog)
			require.Equal(t, "ses.yaml", c.sesConfig)
			require.True(t, c.debug)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags := flag.NewFlagSet("eventbus", flag.ContinueOnError)
			flags.SetOutput(io.Discard)
			c, err := readConfig(flags, tc.args)
			require.NoError(t, err)
			tc.check(t, c)
		})
	}
}
