package cognito

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedMFAUserWithTOTPSecret registers an MFA-enabled user with the given
// base32 TOTP secret. Returns the user's sub.
func seedMFAUserWithTOTPSecret(t *testing.T, store *CognitoStore, poolID, clientID, email, password, totpSecret string) string {
	t.Helper()
	sub := seedMFAUser(t, store, poolID, clientID, email, password)
	require.NoError(t, store.SetUserTOTPSecret(context.Background(), sub, totpSecret))
	return sub
}

const testTOTPSecret = "JBSWY3DPEHPK3PXP" // base32 of "Hello!\xde"

func TestRespondToAuthChallenge_SoftwareTokenMFA_TOTP_HappyPath(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-happy"
		clientID = "c-totp-happy"
		email    = "totp@example.com"
		password = "TempPass1!"
	)
	seedMFAUserWithTOTPSecret(t, store, poolID, clientID, email, password, testTOTPSecret)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	code, err := totp.GenerateCode(testTOTPSecret, time.Now())
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": code,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	auth, ok := body["AuthenticationResult"].(map[string]interface{})
	require.True(t, ok)
	assert.NotEmpty(t, auth["AccessToken"])
}

func TestRespondToAuthChallenge_SoftwareTokenMFA_TOTP_WrongCode(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-wrong"
		clientID = "c-totp-wrong"
		email    = "totp@example.com"
		password = "TempPass1!"
	)
	seedMFAUserWithTOTPSecret(t, store, poolID, clientID, email, password, testTOTPSecret)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	// "000000" is overwhelmingly unlikely to match a real TOTP window.
	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "000000",
		},
	})
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "CodeMismatchException", body["__type"])
}

func TestRespondToAuthChallenge_SoftwareTokenMFA_TOTP_PrevWindowAccepted(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-skew"
		clientID = "c-totp-skew"
		email    = "totp@example.com"
		password = "TempPass1!"
	)
	seedMFAUserWithTOTPSecret(t, store, poolID, clientID, email, password, testTOTPSecret)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	// Code generated for the previous 30s window should still be accepted
	// thanks to skew=1 in validateTOTPCode.
	code, err := totp.GenerateCode(testTOTPSecret, time.Now().Add(-30*time.Second))
	require.NoError(t, err)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": code,
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}

func TestRespondToAuthChallenge_SoftwareTokenMFA_FallbackTo6Digits_WhenNoTotpSecret(t *testing.T) {
	_, ts, store := newCognitoTestServer(t)
	const (
		poolID   = "p-totp-fallback"
		clientID = "c-totp-fallback"
		email    = "mfa@example.com"
		password = "TempPass1!"
	)
	// No totp_secret seeded — should accept any 6 digits.
	seedMFAUser(t, store, poolID, clientID, email, password)
	session := initiateMFAChallenge(t, ts.URL, clientID, email, password)

	status, body := postCognito(t, ts.URL, "RespondToAuthChallenge", map[string]interface{}{
		"ChallengeName": "SOFTWARE_TOKEN_MFA",
		"ClientId":      clientID,
		"Session":       session,
		"ChallengeResponses": map[string]string{
			"USERNAME":                email,
			"SOFTWARE_TOKEN_MFA_CODE": "123456",
		},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}
