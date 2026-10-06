package cognito

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsCognitoTarget(t *testing.T) {
	cases := []struct {
		target string
		want   bool
	}{
		{"AWSCognitoIdentityProviderService.InitiateAuth", true},
		{"AWSCognitoIdentityProviderService.AdminCreateUser", true},
		{"Firehose_20150804.PutRecord", false},
		{"AmazonSQS.SendMessage", false},
		{"", false},
		{"AWSCognitoIdentityProviderService", false},
	}
	for _, c := range cases {
		t.Run(c.target, func(t *testing.T) {
			assert.Equal(t, c.want, IsTarget(c.target))
		})
	}
}

func TestExtractCognitoAction(t *testing.T) {
	assert.Equal(t, "InitiateAuth", Action("AWSCognitoIdentityProviderService.InitiateAuth"))
	assert.Equal(t, "", Action("Firehose_20150804.PutRecord"))
	assert.Equal(t, "", Action(""))
}

func TestCognitoDispatch_UnknownAction(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/", bytes.NewBufferString(`{}`))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService.MadeUpOp")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	var env map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	assert.Equal(t, "InvalidAction", env["__type"])
}

// TestCognitoErrorEnvelopeShape pins down the exact wire shape to prevent
// regressions: lowercase `message`, content type application/x-amz-json-1.1.
// The provider-side error mappers in cognito.py / cognito.go grep on these
// exact characters.
func TestCognitoErrorEnvelopeShape(t *testing.T) {
	rec := httptest.NewRecorder()
	cognitoJSONError(rec, http.StatusUnauthorized, "NotAuthorizedException", "bad creds")

	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, "application/x-amz-json-1.1", resp.Header.Get("Content-Type"))

	body, _ := io.ReadAll(resp.Body)
	assert.JSONEq(t, `{"__type":"NotAuthorizedException","message":"bad creds"}`, string(body))
}
