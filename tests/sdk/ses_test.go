//go:build sdksmoke

package sdk

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/stretchr/testify/require"
)

func TestSESSDKSmoke(t *testing.T) {
	python := sdkPython(t)
	for _, mode := range []string{"full", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			capturePath := filepath.Join(t.TempDir(), "ses.jsonl")
			capture, err := ses.OpenSESCapture(capturePath)
			require.NoError(t, err)
			enabled := mode != "disabled"
			fixtures := ses.SESFixtures{
				Templates: map[string]ses.SESTemplate{
					"welcome": {SubjectPart: "Hello {{name}}", TextPart: "Hi {{name}}", HtmlPart: "<p>{{name}}</p>"},
				},
				CustomVerificationTemplates: map[string]ses.SESCustomVerificationTemplate{
					"verify": {
						FromEmailAddress:      "sender@example.test",
						TemplateSubject:       "Verify SDK identity",
						TemplateContent:       "<p>Verify this SDK identity.</p>",
						SuccessRedirectionURL: "https://example.test/success",
						FailureRedirectionURL: "https://example.test/failure",
					},
				},
				ConfigurationSets:         []string{"sdk-config", "sdk-header-config"},
				VerifiedIdentities:        []string{"example.test"},
				RequireVerifiedIdentities: true,
				SendingEnabled:            &enabled,
				ReceivedMessages: map[string]ses.SESReceivedMessage{
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
			manager := ses.NewSESManager(fixtures, capture)
			// Cleanup runs in reverse order: the HTTP listener drains first and
			// the SES owner then joins/syncs its capture. The SDK process is joined
			// before these cleanup callbacks run.
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			router := server.New(server.Services{SES: ses.NewHandler(manager), QueryBodyLimit: ses.QueryBodyLimit})
			serving := httptest.NewServer(router)
			t.Cleanup(serving.Close)

			models := fixturePath("aws_models")
			script := fixturePath("python", "smoke_ses.py")
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
