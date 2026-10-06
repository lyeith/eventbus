// User creation, deletion and introspection through the Cognito wire protocol.
package cognito

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// postCognito builds an X-Amz-Target Cognito request and returns the parsed
// response (status + body). Common across every test in this file so the
// dispatcher path is exercised end-to-end.
func postCognito(t *testing.T, baseURL, action string, payload interface{}) (int, map[string]interface{}) {
	t.Helper()
	body, err := json.Marshal(payload)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, baseURL+"/", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := map[string]interface{}{}
	if len(raw) > 0 {
		require.NoError(t, json.Unmarshal(raw, &out), "body=%q", string(raw))
	}
	return resp.StatusCode, out
}

// assertAttribute pulls a Name/Value attribute out of the wire-shape array
// returned by AdminCreateUser ("Attributes") or GetUser ("UserAttributes").
func assertAttribute(t *testing.T, list interface{}, name, want string) {
	t.Helper()
	arr, ok := list.([]interface{})
	require.True(t, ok, "attribute list is not []interface{}: %T", list)
	for _, raw := range arr {
		m, ok := raw.(map[string]interface{})
		require.True(t, ok)
		if m["Name"] == name {
			assert.Equal(t, want, m["Value"], "attribute %q value", name)
			return
		}
	}
	t.Fatalf("attribute %q not found in %v", name, arr)
}

// seedPool installs a freshly-named pool so each test starts clean.
func seedPool(t *testing.T, store *CognitoStore, poolID string) {
	t.Helper()
	require.NoError(t, store.UpsertPool(context.Background(), poolID, "us-east-1"))
}

// --- AdminCreateUser ----------------------------------------------------

func TestAdminCreateUser_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "pool-create-happy"
	seedPool(t, store, poolID)

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": poolID,
		"Username":   "alice@example.com",
		"UserAttributes": []map[string]string{
			{"Name": "email", "Value": "alice@example.com"},
			{"Name": "email_verified", "Value": "true"},
		},
		"TemporaryPassword": "TempPass1!",
		"MessageAction":     "SUPPRESS",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	user, ok := body["User"].(map[string]interface{})
	require.True(t, ok, "User missing from response: %v", body)
	assert.Equal(t, "alice@example.com", user["Username"])
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", user["UserStatus"])
	assert.Equal(t, true, user["Enabled"])
	assert.IsType(t, float64(0), user["UserCreateDate"])
	assert.IsType(t, float64(0), user["UserLastModifiedDate"])

	assertAttribute(t, user["Attributes"], "email", "alice@example.com")
	assertAttribute(t, user["Attributes"], "email_verified", "true")

	// DB state: user row + attribute rows present, password is bcrypt hash.
	got, err := store.LookupUserByEmail(context.Background(), poolID, "alice@example.com")
	require.NoError(t, err)
	assert.Equal(t, "FORCE_CHANGE_PASSWORD", got.Status)
	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(got.PasswordHash), []byte("TempPass1!")))

	attrs, err := store.LoadUserAttributes(context.Background(), got.Sub)
	require.NoError(t, err)
	assert.Equal(t, "alice@example.com", attrs["email"])
	assert.Equal(t, "true", attrs["email_verified"])
	assert.Equal(t, got.Sub, attrs["sub"])
}

func TestAdminCreateUser_UsernameExists(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "pool-create-collide"
	seedPool(t, store, poolID)

	first, _ := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "dup@example.com",
		"TemporaryPassword": "TempPass1!",
	})
	require.Equal(t, http.StatusOK, first)

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "dup@example.com",
		"TemporaryPassword": "TempPass1!",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "UsernameExistsException", body["__type"])
	assert.Contains(t, body["message"], "dup@example.com")
}

func TestAdminCreateUser_PoolMissing(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": "no-such-pool",
		"Username":   "ghost@example.com",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
	assert.Contains(t, body["message"], "no-such-pool")
}

// TestAdminCreateUser_GeneratesSub asserts the wire shape returns a
// 26-character ULID-shaped sub. Catches accidental UUID/v4 fallbacks or
// hex generation that leaves auth providers with the wrong opaque length.
func TestAdminCreateUser_GeneratesSub(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "pool-sub-shape"
	seedPool(t, store, poolID)

	_, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": poolID,
		"Username":   "subshape@example.com",
	})
	user := body["User"].(map[string]interface{})
	attrs := user["Attributes"].([]interface{})

	var sub string
	for _, raw := range attrs {
		m := raw.(map[string]interface{})
		if m["Name"] == "sub" {
			sub = m["Value"].(string)
			break
		}
	}
	require.NotEmpty(t, sub, "sub attribute missing")
	assert.Len(t, sub, 26, "ULID is 26 Crockford-base32 chars")
	// Crockford base32: 0-9, A-Z minus I, L, O, U.
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	for _, c := range sub {
		assert.Containsf(t, alphabet, string(c), "char %q not in Crockford base32", c)
	}

	// Also: matches what's persisted.
	got, err := store.LookupPoolUser(context.Background(), poolID, "subshape@example.com")
	require.NoError(t, err)
	assert.Equal(t, sub, got.Sub)
}

func TestAdminCreateUser_DoesNotInventEmailOrVerification(t *testing.T) {
	_, server, store := newCognitoTestServer(t)
	seedPool(t, store, "unverified-pool")
	status, body := postCognito(t, server.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": "unverified-pool", "Username": "stable-user", "MessageAction": "SUPPRESS",
		"UserAttributes": []map[string]string{{"Name": "email", "Value": "invitee@example.test"}},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err := store.LookupPoolUser(t.Context(), "unverified-pool", "stable-user")
	require.NoError(t, err)
	attributes, err := store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "invitee@example.test", attributes["email"])
	require.NotContains(t, attributes, "email_verified")
	status, body = postCognito(t, server.URL, "AdminCreateUser", map[string]interface{}{"UserPoolId": "unverified-pool", "Username": "without-email", "MessageAction": "SUPPRESS"})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err = store.LookupPoolUser(t.Context(), "unverified-pool", "without-email")
	require.NoError(t, err)
	require.Empty(t, user.Email)
}

// --- AdminDeleteUser ----------------------------------------------------

func TestAdminDeleteUser_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "pool-delete-happy"
	seedPool(t, store, poolID)

	createStatus, _ := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "doomed@example.com",
		"TemporaryPassword": "TempPass1!",
	})
	require.Equal(t, http.StatusOK, createStatus)

	got, err := store.LookupPoolUser(context.Background(), poolID, "doomed@example.com")
	require.NoError(t, err)
	sub := got.Sub

	status, body := postCognito(t, ts.URL, "AdminDeleteUser", map[string]interface{}{
		"UserPoolId": poolID,
		"Username":   "doomed@example.com",
	})
	assert.Equal(t, http.StatusOK, status)
	assert.Empty(t, body)

	// users row gone.
	_, err = store.LookupPoolUser(context.Background(), poolID, "doomed@example.com")
	assert.Error(t, err, "user row should be gone")

	// user_attributes also gone (FK cascade).
	attrs, err := store.LoadUserAttributes(context.Background(), sub)
	require.NoError(t, err)
	assert.Empty(t, attrs, "attributes should cascade-delete with user row")
}

// TestAdminDeleteUser_IdempotentOnMissingUser verifies that both
// providers swallow UserNotFoundException from this op. The dev service
// MUST therefore return success even if the user is not present, otherwise
// `bootstrap_platform.py` retries / cleanup paths break.
func TestAdminDeleteUser_IdempotentOnMissingUser(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "pool-delete-missing"
	seedPool(t, store, poolID)

	status, body := postCognito(t, ts.URL, "AdminDeleteUser", map[string]interface{}{
		"UserPoolId": poolID,
		"Username":   "ghost@example.com",
	})
	assert.Equal(t, http.StatusOK, status, "missing user must succeed (idempotent)")
	assert.Empty(t, body)
}

func TestAdminDeleteUser_PoolMissing(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "AdminDeleteUser", map[string]interface{}{
		"UserPoolId": "no-such-pool",
		"Username":   "anyone@example.com",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// --- GetUser ------------------------------------------------------------

func TestGetUser_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "local-pool-1"
	seedPool(t, store, poolID)

	createStatus, createBody := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "carol@example.com",
		"UserAttributes":    []map[string]string{{"Name": "email", "Value": "carol@example.com"}, {"Name": "email_verified", "Value": "true"}},
		"TemporaryPassword": "TempPass1!",
	})
	require.Equal(t, http.StatusOK, createStatus)

	user := createBody["User"].(map[string]interface{})
	var sub string
	for _, raw := range user["Attributes"].([]interface{}) {
		if m := raw.(map[string]interface{}); m["Name"] == "sub" {
			sub = m["Value"].(string)
		}
	}
	require.NotEmpty(t, sub)

	// Mint a token whose `iss` matches the SetCognito issuerBase
	// ("http://localhost:4100") wired by newCognitoTestServer. The
	// VerifyAccessToken helper validates `iss` against that exact value
	// regardless of the httptest server URL.
	token := signTestAccessToken(t, store, "http://localhost:4100", poolID, sub, "carol@example.com", time.Hour)

	status, body := postCognito(t, ts.URL, "GetUser", map[string]interface{}{
		"AccessToken": token,
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	assert.Equal(t, "carol@example.com", body["Username"])
	assertAttribute(t, body["UserAttributes"], "sub", sub)
	assertAttribute(t, body["UserAttributes"], "email", "carol@example.com")
	assertAttribute(t, body["UserAttributes"], "email_verified", "true")
}

func TestGetUser_InvalidSignature(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "local-pool-1"
	seedPool(t, store, poolID)

	// Mint a "good" token so it has the right iss + claim shape, then
	// corrupt the signature segment so verification must fail.
	token := signTestAccessToken(t, store, "http://localhost:4100", poolID, "abc", "x@example.com", time.Hour)
	parts := strings.Split(token, ".")
	require.Len(t, parts, 3)
	parts[2] = strings.Repeat("A", len(parts[2])) // syntactically valid base64url, wrong bytes
	bad := strings.Join(parts, ".")

	status, body := postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": bad})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestGetUser_ExpiredToken(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "local-pool-1"
	seedPool(t, store, poolID)

	// Negative TTL — issued and expired in the past.
	token := signTestAccessToken(t, store, "http://localhost:4100", poolID, "abc", "x@example.com", -time.Minute)
	status, body := postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": token})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestGetUser_MalformedToken(t *testing.T) {
	_, ts, _ := newCognitoTestServer(t)

	status, body := postCognito(t, ts.URL, "GetUser", map[string]interface{}{
		"AccessToken": "not-a-jwt-at-all",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

// TestGetUser_UserDeleted confirms a token survives but the user row no
// longer does — the handler must surface UserNotFoundException, NOT a
// silent empty response.
func TestGetUser_UserDeleted(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "local-pool-1"
	seedPool(t, store, poolID)

	createStatus, createBody := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "vanish@example.com",
		"TemporaryPassword": "TempPass1!",
	})
	require.Equal(t, http.StatusOK, createStatus)
	var sub string
	for _, raw := range createBody["User"].(map[string]interface{})["Attributes"].([]interface{}) {
		if m := raw.(map[string]interface{}); m["Name"] == "sub" {
			sub = m["Value"].(string)
		}
	}
	require.NotEmpty(t, sub)

	token := signTestAccessToken(t, store, "http://localhost:4100", poolID, sub, "vanish@example.com", time.Hour)

	delStatus, _ := postCognito(t, ts.URL, "AdminDeleteUser", map[string]interface{}{
		"UserPoolId": poolID,
		"Username":   "vanish@example.com",
	})
	require.Equal(t, http.StatusOK, delStatus)

	status, body := postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": token})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "UserNotFoundException", body["__type"])
}

// TestGetUser_WrongTokenUse pins the §3c contract: only access tokens are
// accepted on GetUser. An id/refresh token must be rejected.
func TestGetUser_WrongTokenUse(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "local-pool-1"
	signing, err := store.EnsureSigningKey(context.Background(), poolID)
	require.NoError(t, err)

	now := time.Now()
	claims := jwt.MapClaims{
		"sub":       "abc",
		"email":     "x@example.com",
		"iss":       "http://localhost:4100/" + poolID,
		"aud":       "test-client",
		"iat":       now.Unix(),
		"exp":       now.Add(time.Hour).Unix(),
		"token_use": "id", // wrong — must be "access"
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = signing.Kid
	idToken, err := tok.SignedString(signing.Private)
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "GetUser", map[string]interface{}{"AccessToken": idToken})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}
