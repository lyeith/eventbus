//go:build sdksmoke

package main

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSESSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	for _, mode := range []string{"full", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			capturePath := filepath.Join(t.TempDir(), "ses.jsonl")
			capture, err := OpenSESCapture(capturePath)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, capture.Close())
			})
			enabled := mode != "disabled"
			fixtures := SESFixtures{
				Templates: map[string]SESTemplate{
					"welcome": {SubjectPart: "Hello {{name}}", TextPart: "Hi {{name}}", HtmlPart: "<p>{{name}}</p>"},
				},
				CustomVerificationTemplates: map[string]SESCustomVerificationTemplate{
					"verify": {
						FromEmailAddress:      "sender@example.test",
						TemplateSubject:       "Verify SDK identity",
						TemplateContent:       "<p>Verify this SDK identity.</p>",
						SuccessRedirectionURL: "https://example.test/success",
						FailureRedirectionURL: "https://example.test/failure",
					},
				},
				ConfigurationSets:         []string{"sdk-config"},
				VerifiedIdentities:        []string{"example.test"},
				RequireVerifiedIdentities: true,
				SendingEnabled:            &enabled,
				ReceivedMessages: map[string]SESReceivedMessage{
					"sdk-inbound": {
						From:       "origin@example.test",
						ReceivedAt: time.Now().UTC().Add(-time.Hour),
						Recipients: []string{"sender@example.test"},
					},
					"sdk-expired": {
						From:       "origin@example.test",
						ReceivedAt: time.Now().UTC().Add(-25 * time.Hour),
						Recipients: []string{"sender@example.test"},
					},
				},
			}
			broker := NewBroker("us-east-1", "000000000000", 0)
			firehose := NewFirehoseManager("us-east-1", "000000000000", "http://127.0.0.1:1", "test", "test")
			t.Cleanup(func() {
				require.NoError(t, firehose.Shutdown())
			})
			server := NewServer(broker, firehose, NewSSMStore(), NewSecretsStore("us-east-1", "000000000000"))
			server.SetSES(NewSESManager(fixtures, capture))
			serving := httptest.NewServer(server)
			t.Cleanup(serving.Close)

			models, err := filepath.Abs(filepath.Join("tests", "aws_models"))
			require.NoError(t, err)
			script, err := filepath.Abs(filepath.Join("tests", "smoke_ses.py"))
			require.NoError(t, err)
			env := append(sdkEnvironment(t.TempDir(), "", "", ""),
				"SES_ENDPOINT_URL="+serving.URL,
				"SES_CAPTURE_LOG="+capturePath,
				"SES_SMOKE_MODE="+mode,
				"AWS_DATA_PATH="+models,
			)
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			output, err := runSDKProcess(ctx, python, script, env)
			t.Logf("real SES SDK mode=%s:\n%s", mode, output)
			require.NoError(t, err)
			info, err := os.Stat(capturePath)
			require.NoError(t, err)
			require.Greater(t, info.Size(), int64(0), "successful SDK probes must have durable capture evidence")
		})
	}
}
