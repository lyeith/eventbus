package ses

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Exercise the native Query adapter and durable capture together: resolving a
// MIME selector must never insert an API field or rewrite the original bytes.
func TestSESV1RawConfigurationSetSelectionAndOriginalCapture(t *testing.T) {
	cases := []struct {
		name, header, apiSet, effective, errorCode, rawOverride string
		apiProvided, invalidAPIShape                            bool
	}{
		{name: "header only", header: "X-SES-CONFIGURATION-SET: header-set\r\n", effective: "header-set"},
		{name: "header casing", header: "x-sEs-cOnFiGuRaTiOn-SeT: header-set\r\n", effective: "header-set"},
		{name: "folded header", header: "X-SES-CONFIGURATION-SET:\r\n\theader-set\r\n", effective: "header-set"},
		{name: "header whitespace", header: "X-SES-CONFIGURATION-SET: \theader-set \t\r\n", effective: "header-set"},
		{name: "absent selectors"},
		{name: "API only", apiProvided: true, apiSet: "api-set", effective: "api-set"},
		{name: "API wins conflicting owned header", header: "X-SES-CONFIGURATION-SET: header-set\r\n", apiProvided: true, apiSet: "api-set", effective: "api-set"},
		{name: "API ignores nonexistent header", header: "X-SES-CONFIGURATION-SET: absent-header\r\n", apiProvided: true, apiSet: "api-set", effective: "api-set"},
		{name: "nonexistent header", header: "X-SES-CONFIGURATION-SET: absent-header\r\n", errorCode: "ConfigurationSetDoesNotExist"},
		{name: "set names remain case sensitive", header: "X-SES-CONFIGURATION-SET: HEADER-SET\r\n", errorCode: "ConfigurationSetDoesNotExist"},
		{name: "invalid API cannot fall back to valid header", header: "X-SES-CONFIGURATION-SET: header-set\r\n", apiProvided: true, apiSet: "absent-api", errorCode: "ConfigurationSetDoesNotExist"},
		{name: "API shape validation remains first", header: "X-SES-CONFIGURATION-SET: header-set\r\n", invalidAPIShape: true, errorCode: "InvalidParameterValue"},
		{name: "API existence validation remains before raw validation", apiProvided: true, apiSet: "absent-api", rawOverride: "%%%", errorCode: "ConfigurationSetDoesNotExist"},
		// An explicitly empty API selector already means no selection. Preserve
		// that behavior, including API-over-header priority, rather than infer a
		// different AWS validation contract for the empty optional field.
		{name: "explicit empty API preserves existing selection", header: "X-SES-CONFIGURATION-SET: header-set\r\n", apiProvided: true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "ses.jsonl")
			capture, err := OpenSESCapture(path)
			require.NoError(t, err)
			manager := NewSESManager(SESFixtures{ConfigurationSets: []string{"api-set", "header-set"}}, capture)
			t.Cleanup(func() { require.NoError(t, manager.Close()) })
			handler := NewHandler(manager)
			original := []byte(test.header + "From: Sender <sender@example.test>\r\nTo: to@example.test\r\n" +
				"Subject: Original bytes\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=owned-boundary\r\n\r\n" +
				"--owned-boundary\r\nContent-Type: text/plain\r\n\r\nX-SES-CONFIGURATION-SET: body-must-not-select\r\n" +
				"--owned-boundary\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=original.bin\r\n" +
				"Content-Transfer-Encoding: base64\r\n\r\nAAH+/w==\r\n--owned-boundary--\r\n")
			encoded := base64.StdEncoding.EncodeToString(original)
			if test.rawOverride != "" {
				encoded = test.rawOverride
			}
			form := url.Values{"Action": {"SendRawEmail"}, "Version": {"2010-12-01"}, "RawMessage.Data": {encoded}}
			if test.apiProvided {
				form.Set("ConfigurationSetName", test.apiSet)
			}
			if test.invalidAPIShape {
				form.Set("ConfigurationSetName.member.1", "api-set")
			}
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			response := httptest.NewRecorder()
			handler.ServeQuery(response, request, "SendRawEmail", nil)
			// ServeQuery is synchronous. Close joins and syncs the owned capture;
			// then inspect the original submitted evidence from disk.
			require.NoError(t, manager.Close())
			captured, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, 1, strings.Count(string(captured), "\n"))
			var record map[string]any
			require.NoError(t, json.Unmarshal(captured, &record))
			require.EqualValues(t, 1, record["schema_version"])
			require.Equal(t, "ses", record["api"])
			require.Equal(t, "SendRawEmail", record["operation"])
			require.Equal(t, response.Header().Get("x-amzn-requestid"), record["request_id"])
			submitted := sesObject(record["request"])
			require.Equal(t, encoded, sesObject(submitted["RawMessage"])["Data"])
			if test.rawOverride == "" {
				decoded, err := base64.StdEncoding.DecodeString(sesString(sesObject(submitted["RawMessage"])["Data"]))
				require.NoError(t, err)
				require.Equal(t, original, decoded)
			}
			_, capturedAPI := submitted["ConfigurationSetName"]
			require.Equal(t, test.apiProvided || test.invalidAPIShape, capturedAPI, "MIME selection must not mutate API input")
			outcome := sesObject(record["outcome"])
			if test.errorCode != "" {
				require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
				var wire struct{ Error struct{ Code string } }
				require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &wire))
				require.Equal(t, test.errorCode, wire.Error.Code)
				require.Equal(t, test.errorCode, sesObject(outcome["error"])["code"])
				require.EqualValues(t, http.StatusBadRequest, outcome["http_status"])
				require.Empty(t, record["emails"])
				require.NotContains(t, response.Body.String(), "MessageId")
				return
			}
			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			emails := record["emails"].([]any)
			require.Len(t, emails, 1)
			email := sesObject(emails[0])
			require.Equal(t, test.effective, email["configuration_set"])
			require.Equal(t, "RawMessage.Data", email["request_content_path"])
			require.Equal(t, email["message_id"], sesObject(outcome["response"])["MessageId"])
			if test.header != "" {
				require.Equal(t, []string{strings.TrimSpace(strings.ReplaceAll(strings.SplitN(test.header, ":", 2)[1], "\r\n", ""))},
					sesStrings(sesObject(email["headers"])["X-Ses-Configuration-Set"]))
			}
		})
	}
}
