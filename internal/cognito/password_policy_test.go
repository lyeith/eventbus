package cognito

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedPoolWithPolicy sets up a pool with the given password policy applied.
func seedPoolWithPolicy(t *testing.T, store *CognitoStore, poolID string, policy *PasswordPolicy) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, store.UpsertPool(ctx, poolID, "us-east-1"))
	if policy != nil {
		raw, err := json.Marshal(policy)
		require.NoError(t, err)
		require.NoError(t, store.SetPoolPasswordPolicy(ctx, poolID, string(raw)))
	}
}

func TestAdminCreateUser_PolicyViolation_TooShort(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-short"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{MinLength: 12})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "Short1!",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	assert.Equal(t, changePasswordPolicyPrefix+"Password not long enough", body["message"])
}

func TestAdminCreateUser_PolicyViolation_NoDigit(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-nodigit"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:     8,
		RequireDigits: true,
	})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "NoDigitsHere!",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	assert.Equal(t, changePasswordPolicyPrefix+"Password must have numeric characters", body["message"])
}

func TestAdminCreateUser_PolicyViolation_NoUppercase(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-noupper"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:        8,
		RequireUppercase: true,
	})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "no-upper-1!",
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	assert.Contains(t, body["message"], "uppercase")
}

func TestAdminCreateUser_PolicyHonored_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-ok"
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:        8,
		RequireUppercase: true,
		RequireLowercase: true,
		RequireDigits:    true,
		RequireSymbols:   true,
	})

	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "Strong1Pass!",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestRespondToAuthChallenge_NewPasswordRequired_PolicyViolation(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-newpw-policy"
		clientID = "c-newpw-policy"
		email    = "newpw@example.com"
	)
	seedPoolWithPolicy(t, store, poolID, &PasswordPolicy{
		MinLength:     12,
		RequireDigits: true,
	})
	ctx := context.Background()
	require.NoError(t, store.UpsertClient(ctx, clientID, poolID, ""))
	sub, err := store.UpsertUser(ctx, poolID, email, "", false)
	require.NoError(t, err)
	session := seedSessionRow(t, store, sub, poolID, clientID, "NEW_PASSWORD_REQUIRED", time.Minute)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "NEW_PASSWORD_REQUIRED",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":     email,
			"NEW_PASSWORD": "weak",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidPasswordException", body["__type"])
	// Should mention BOTH violations (length + digit) in one message.
	msg, _ := body["message"].(string)
	assert.Contains(t, msg, "12 characters")
	assert.Contains(t, msg, "digit")
}

func TestPasswordPolicy_NoPolicyMeansAccept(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const poolID = "p-policy-none"
	require.NoError(t, store.UpsertPool(context.Background(), poolID, "us-east-1"))

	// Even a single-character password should be accepted when no
	// policy is configured. (Empty password is still rejected, matching
	// PasswordPolicy.Validate's "non-nil-policy" baseline.)
	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId":        poolID,
		"Username":          "alice@example.com",
		"TemporaryPassword": "x",
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

// PasswordPolicy.Validate unit tests catch corner cases the integration
// tests miss (e.g. multiple-failure messages, nil receiver, empty input).

func TestPasswordPolicy_Validate_NilReceiver(t *testing.T) {
	var p *PasswordPolicy
	assert.NoError(t, p.Validate("anything"))
	assert.Error(t, p.Validate(""), "empty password is rejected even with nil policy")
}

func TestPasswordPolicy_Validate_MultipleFailures(t *testing.T) {
	p := &PasswordPolicy{
		MinLength:        12,
		RequireUppercase: true,
		RequireDigits:    true,
		RequireSymbols:   true,
	}
	// "lower" is 5 chars (under 12), all lowercase, no digit, no symbol —
	// fails every rule, so the message should list each.
	err := p.Validate("lower")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "12 characters")
	assert.Contains(t, err.Error(), "uppercase")
	assert.Contains(t, err.Error(), "digit")
	assert.Contains(t, err.Error(), "symbol")
}

func TestPasswordPolicyAWSJSONAndLegacyFixtures(t *testing.T) {
	var policy PasswordPolicy
	require.NoError(t, json.Unmarshal([]byte(`{"MinimumLength":12,"RequireNumbers":true,"RequireUppercase":true,"TemporaryPasswordValidityDays":2,"require_digits":false}`), &policy))
	require.Equal(t, 12, policy.MinLength)
	require.True(t, policy.RequireDigits)
	require.True(t, policy.RequireUppercase)
	require.Equal(t, 2, policy.TemporaryPasswordValidityDays)
	encoded, err := json.Marshal(policy)
	require.NoError(t, err)
	var wire map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &wire))
	require.Equal(t, float64(12), wire["MinimumLength"])
	require.Equal(t, true, wire["RequireNumbers"])
	require.NotContains(t, wire, "min_length")
	require.NotContains(t, wire, "RequireDigits")
	require.NoError(t, json.Unmarshal([]byte(`{"min_length":9,"require_digits":true,"require_symbols":true}`), &policy))
	require.Equal(t, 9, policy.MinLength)
	require.True(t, policy.RequireDigits)
	require.True(t, policy.RequireSymbols)
}

func TestPasswordPolicyCountsCharactersAndFailsClosed(t *testing.T) {
	require.Error(t, (&PasswordPolicy{MinLength: 6}).Validate("界界"))
	require.NoError(t, (&PasswordPolicy{MinLength: 6}).Validate("界界界界界界"))
	_, ts, store := newCognitoTestServer(t)
	require.NoError(t, store.UpsertPool(t.Context(), "policy-corrupt", "us-east-1"))
	require.NoError(t, store.SetPoolPasswordPolicy(t.Context(), "policy-corrupt", "{"))
	_, err := loadPoolPasswordPolicy(t.Context(), store, "policy-corrupt")
	require.Error(t, err)
	status, body := postCognito(t, ts.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": "policy-corrupt", "Username": "test", "TemporaryPassword": "Password1!",
	})
	require.Equal(t, http.StatusInternalServerError, status)
	require.Equal(t, "InternalErrorException", body["__type"])
}
