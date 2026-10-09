package cognito

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/require"
)

const replacementTOTPSecret = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"

type mfaStoreFixture struct {
	store   *CognitoStore
	handler *Handler
	user    *CognitoUser
	auth    mfaAuthorization
	now     time.Time
}

func newMFAStoreFixture(t *testing.T) mfaStoreFixture {
	t.Helper()
	store, _ := newCognitoTestStore(t)
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	handler := NewHandler(store, Options{IssuerBase: "http://mfa.test", Clock: func() time.Time { return now }})
	require.NoError(t, store.UpsertPool(t.Context(), "mfa-pool", "us-east-1"))
	// These transition tests never authenticate with a password. Hash-only
	// fixture creation keeps their cost separate from bcrypt/SRP provisioning.
	require.NoError(t, store.CreateUser(t.Context(), "mfa-sub", "mfa-pool", "mfa@example.test", "fixture-password-hash", false))
	user, err := store.LookupUserBySub(t.Context(), "mfa-sub")
	require.NoError(t, err)
	return mfaStoreFixture{store, handler, user, mfaAuthorization{AuthVersion: user.AuthVersion, OriginJTI: "mfa-device-grant", SelfService: true}, now}
}

func (f mfaStoreFixture) current(t *testing.T) *CognitoUser {
	t.Helper()
	user, err := f.store.LookupUserBySub(t.Context(), f.user.Sub)
	require.NoError(t, err)
	return user
}

func (f mfaStoreFixture) associate(t *testing.T, secret string) *CognitoUser {
	t.Helper()
	require.NoError(t, f.store.setPendingTOTPSecret(t.Context(), f.user.Sub, secret, f.auth))
	return f.current(t)
}

func TestMFAPromotionUsesOnlyVerifiedSnapshot(t *testing.T) {
	f := newMFAStoreFixture(t)
	first := f.associate(t, testTOTPSecret)
	require.Empty(t, first.TOTPSecret)
	require.False(t, first.SoftwareTokenVerified)
	require.NoError(t, f.store.confirmPendingTOTPSecret(t.Context(), first.Sub, first.PendingTOTPSecret, f.auth))
	verified := f.current(t)
	require.Equal(t, testTOTPSecret, verified.TOTPSecret)
	require.Empty(t, verified.PendingTOTPSecret)
	require.True(t, verified.SoftwareTokenVerified)
	require.False(t, verified.MFAEnabled, "verification does not change the explicit MFA preference")
	require.NoError(t, f.store.setMFAPreference(t.Context(), first.Sub, true, f.auth))

	// Associate a replacement, then replace it again before the earlier code's
	// proof is admitted. The previous verified factor stays active throughout.
	stale := f.associate(t, testTOTPSecret)
	current := f.associate(t, replacementTOTPSecret)
	require.Equal(t, verified.TOTPSecret, current.TOTPSecret)
	require.True(t, current.SoftwareTokenVerified)
	require.True(t, current.MFAEnabled)
	code, err := totp.GenerateCode(stale.PendingTOTPSecret, f.now)
	require.NoError(t, err)
	require.NoError(t, validateTOTPCodeAt(stale.PendingTOTPSecret, code, f.now), "the obsolete secret's proof is valid")
	require.ErrorIs(t, f.store.confirmPendingTOTPSecret(t.Context(), stale.Sub, stale.PendingTOTPSecret, f.auth), errTOTPEnrollmentChanged)
	require.Equal(t, current, f.current(t), "the obsolete proof must neither promote nor consume the newer association")
	require.NoError(t, f.store.confirmPendingTOTPSecret(t.Context(), current.Sub, current.PendingTOTPSecret, f.auth))
	completed := f.current(t)
	require.Equal(t, replacementTOTPSecret, completed.TOTPSecret)
	require.Empty(t, completed.PendingTOTPSecret)
	require.True(t, completed.SoftwareTokenVerified)
	require.True(t, completed.MFAEnabled)
	require.ErrorIs(t, f.store.confirmPendingTOTPSecret(t.Context(), current.Sub, current.PendingTOTPSecret, f.auth), errTOTPEnrollmentChanged)
	require.Equal(t, completed, f.current(t), "an already consumed proof cannot claim a second success")
	require.ErrorIs(t, f.store.confirmPendingTOTPSecret(t.Context(), current.Sub, "", f.auth), errTOTPEnrollmentChanged)
}

func TestMFATransitionsRejectStaleAccountOrGrant(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		mutate func(*testing.T, mfaStoreFixture)
		gone   bool
	}{
		{"global signout", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.RevokeUserTokens(t.Context(), f.user.Sub, f.now.Unix()))
		}, false},
		{"admin password reset", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.SetUserPassword(t.Context(), f.user.Sub, "NewPassword1!", "CONFIRMED"))
		}, false},
		{"disabled", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, false))
		}, false},
		{"disabled then reenabled", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, false))
			require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, true))
		}, false},
		{"revoked device grant", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.RevokeRefreshToken(t.Context(), f.auth.OriginJTI))
		}, false},
		{"deleted", func(t *testing.T, f mfaStoreFixture) {
			deleted, err := f.store.DeletePoolUser(t.Context(), f.user.PoolID, f.user.Sub)
			require.NoError(t, err)
			require.True(t, deleted)
		}, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newMFAStoreFixture(t)
			require.NoError(t, f.store.SetUserTOTPSecret(t.Context(), f.user.Sub, testTOTPSecret))
			snapshot := f.associate(t, replacementTOTPSecret)
			scenario.mutate(t, f)
			var before *CognitoUser
			if !scenario.gone {
				before = f.current(t)
			}
			require.ErrorIs(t, f.store.confirmPendingTOTPSecret(t.Context(), snapshot.Sub, snapshot.PendingTOTPSecret, f.auth), errTokenRevoked)
			require.ErrorIs(t, f.store.setPendingTOTPSecret(t.Context(), snapshot.Sub, testTOTPSecret, f.auth), errTokenRevoked)
			require.ErrorIs(t, f.store.setMFAPreference(t.Context(), snapshot.Sub, true, f.auth), errTokenRevoked)
			require.ErrorIs(t, f.store.setMFAPreference(t.Context(), snapshot.Sub, false, f.auth), errTokenRevoked)
			if scenario.gone {
				_, err := f.store.LookupUserBySub(t.Context(), snapshot.Sub)
				require.ErrorIs(t, err, sql.ErrNoRows)
			} else {
				require.Equal(t, before, f.current(t), "rejected transitions must preserve current account and factor state")
			}
		})
	}
}

func TestMFAEnrollmentRefusesDisabledCurrentRevision(t *testing.T) {
	f := newMFAStoreFixture(t)
	f.associate(t, testTOTPSecret)
	require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, false))
	before := f.current(t)
	// Enrollment must enforce Enabled itself even if passed the current revision
	// without the caller's self-service flag. Only admin preferences may differ.
	authorization := mfaAuthorization{AuthVersion: before.AuthVersion}
	require.ErrorIs(t, f.store.setPendingTOTPSecret(t.Context(), before.Sub, replacementTOTPSecret, authorization), errTokenRevoked)
	require.ErrorIs(t, f.store.confirmPendingTOTPSecret(t.Context(), before.Sub, before.PendingTOTPSecret, authorization), errTokenRevoked)
	authorization.SelfService = true
	require.ErrorIs(t, f.store.setMFAPreference(t.Context(), before.Sub, false, authorization), errTokenRevoked)
	require.Equal(t, before, f.current(t))
}

func TestMFAPromotionConcurrentProofAdmitsOnce(t *testing.T) {
	f := newMFAStoreFixture(t)
	snapshot := f.associate(t, testTOTPSecret)
	start := make(chan struct{})
	results := make(chan error, 2)
	ctx := t.Context()
	for range 2 {
		go func() {
			<-start
			results <- f.store.confirmPendingTOTPSecret(ctx, snapshot.Sub, snapshot.PendingTOTPSecret, f.auth)
		}()
	}
	close(start)
	first, second := <-results, <-results // Join both callers before assertions.
	if first != nil {
		first, second = second, first
	}
	require.NoError(t, first)
	require.ErrorIs(t, second, errTOTPEnrollmentChanged)
	user := f.current(t)
	require.Equal(t, testTOTPSecret, user.TOTPSecret)
	require.Empty(t, user.PendingTOTPSecret)
	require.True(t, user.SoftwareTokenVerified)
}

func TestMFAPreferenceRechecksFactorAndKeepsAdminSemantics(t *testing.T) {
	f := newMFAStoreFixture(t)
	require.ErrorIs(t, f.store.setMFAPreference(t.Context(), f.user.Sub, true, f.auth), errSoftwareTokenNotVerified)
	require.NoError(t, f.store.SetUserTOTPSecret(t.Context(), f.user.Sub, testTOTPSecret))
	verifiedSnapshot := f.current(t)
	require.True(t, verifiedSnapshot.SoftwareTokenVerified)
	require.NoError(t, f.store.SetUserTOTPSecret(t.Context(), f.user.Sub, ""))
	require.ErrorIs(t, f.store.setMFAPreference(t.Context(), f.user.Sub, true, f.auth), errSoftwareTokenNotVerified)
	require.False(t, f.current(t).MFAEnabled)

	require.NoError(t, f.store.SetUserTOTPSecret(t.Context(), f.user.Sub, testTOTPSecret))
	require.NoError(t, f.store.RevokeRefreshToken(t.Context(), f.auth.OriginJTI))
	otherDevice := f.auth
	otherDevice.OriginJTI = "other-device-grant"
	require.NoError(t, f.store.setMFAPreference(t.Context(), f.user.Sub, true, otherDevice), "device revocation does not affect another authorized device")
	require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, false))
	disabled := f.current(t)
	admin := mfaAuthorization{AuthVersion: disabled.AuthVersion}
	require.NoError(t, f.store.setMFAPreference(t.Context(), f.user.Sub, false, admin), "admin can reset a disabled user's preference")
	require.NoError(t, f.store.setMFAPreference(t.Context(), f.user.Sub, true, admin), "admin enablement still requires a currently verified factor")
	require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, true))
	require.NoError(t, f.store.RevokeUserTokens(t.Context(), f.user.Sub, f.now.Unix()))
	require.ErrorIs(t, f.store.setMFAPreference(t.Context(), f.user.Sub, false, admin), errTokenRevoked)
	deleted, err := f.store.DeletePoolUser(t.Context(), f.user.PoolID, f.user.Sub)
	require.NoError(t, err)
	require.True(t, deleted)
	require.ErrorIs(t, f.store.setMFAPreference(t.Context(), f.user.Sub, false, admin), sql.ErrNoRows)
}

func TestVerifySoftwareTokenSnapshotRefusalsUseNativeErrors(t *testing.T) {
	for _, scenario := range []struct {
		name, expected string
		mutate         func(*testing.T, mfaStoreFixture)
	}{
		{"replacement", "EnableSoftwareTokenMFAException", func(t *testing.T, f mfaStoreFixture) {
			f.associate(t, replacementTOTPSecret)
		}},
		{"revision", "NotAuthorizedException", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.RevokeUserTokens(t.Context(), f.user.Sub, f.now.Unix()))
		}},
		{"disabled", "NotAuthorizedException", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, false))
		}},
		{"deleted", "NotAuthorizedException", func(t *testing.T, f mfaStoreFixture) {
			deleted, err := f.store.DeletePoolUser(t.Context(), f.user.PoolID, f.user.Sub)
			require.NoError(t, err)
			require.True(t, deleted)
		}},
		{"device revoked", "NotAuthorizedException", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.RevokeRefreshToken(t.Context(), f.auth.OriginJTI))
		}},
		{"storage failure", "InternalErrorException", func(t *testing.T, f mfaStoreFixture) {
			require.NoError(t, f.store.Close())
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			f := newMFAStoreFixture(t)
			snapshot := f.associate(t, testTOTPSecret)
			code, err := totp.GenerateCode(snapshot.PendingTOTPSecret, f.now)
			require.NoError(t, err)
			scenario.mutate(t, f)
			// Exercise the real post-authorization handler step with the snapshot
			// a mutation overtook. No test hook or competing authentication path.
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(t.Context())
			f.handler.verifySoftwareTokenForUser(recorder, request, snapshot, f.auth, code)
			expectedStatus := http.StatusBadRequest
			if scenario.expected == "InternalErrorException" {
				expectedStatus = http.StatusInternalServerError
			}
			require.Equal(t, expectedStatus, recorder.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &body))
			require.Equal(t, scenario.expected, body["__type"])
			require.NotContains(t, recorder.Body.String(), snapshot.PendingTOTPSecret)
		})
	}
}

func TestMFAEnrollmentNativeAPIWithRealJWT(t *testing.T) {
	f := newMFAStoreFixture(t)
	const clientID = "mfa-native-client"
	require.NoError(t, f.store.UpsertClient(t.Context(), clientID, f.user.PoolID, ""))
	client, err := f.store.LookupClient(t.Context(), clientID)
	require.NoError(t, err)
	client.Native = true
	require.NoError(t, f.store.SaveClient(t.Context(), client))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.handler.ServeAction(w, r, Action(r.Header.Get("X-Amz-Target")))
	}))
	t.Cleanup(server.Close)
	grant := tokenGrant{AuthTime: f.now, OriginJTI: f.auth.OriginJTI, AuthVersion: f.auth.AuthVersion}
	access, err := SignAccessToken(t.Context(), f.store, f.handler.issuerBase, f.user.PoolID, clientID, f.user.Sub, f.user.Email, grant, time.Hour)
	require.NoError(t, err)
	status, body := postCognito(t, server.URL, "AssociateSoftwareToken", map[string]any{"AccessToken": access})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	firstSecret := body["SecretCode"].(string)
	require.NotEmpty(t, firstSecret)
	code, err := totp.GenerateCode(firstSecret, f.now)
	require.NoError(t, err)
	status, body = postCognito(t, server.URL, "VerifySoftwareToken", map[string]any{"AccessToken": access, "UserCode": code})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Equal(t, "SUCCESS", body["Status"])
	require.False(t, f.current(t).MFAEnabled)
	status, body = postCognito(t, server.URL, "SetUserMFAPreference", map[string]any{
		"AccessToken": access, "SoftwareTokenMfaSettings": map[string]bool{"Enabled": true, "PreferredMfa": true},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)

	status, body = postCognito(t, server.URL, "AssociateSoftwareToken", map[string]any{"AccessToken": access})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	secondSecret := body["SecretCode"].(string)
	require.NotEqual(t, firstSecret, secondSecret)
	associated := f.current(t)
	require.Equal(t, firstSecret, associated.TOTPSecret, "association preserves the current verified factor until replacement verification")
	require.Equal(t, secondSecret, associated.PendingTOTPSecret)
	require.True(t, associated.MFAEnabled)
	code, err = totp.GenerateCode(secondSecret, f.now)
	require.NoError(t, err)
	status, body = postCognito(t, server.URL, "VerifySoftwareToken", map[string]any{"AccessToken": access, "UserCode": code})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Equal(t, "SUCCESS", body["Status"])
	verified := f.current(t)
	require.Equal(t, secondSecret, verified.TOTPSecret)
	require.Empty(t, verified.PendingTOTPSecret)
	require.True(t, verified.SoftwareTokenVerified)
	require.True(t, verified.MFAEnabled)

	require.NoError(t, f.store.RevokeRefreshToken(t.Context(), grant.OriginJTI))
	for _, request := range []struct {
		action string
		body   map[string]any
	}{
		{"AssociateSoftwareToken", map[string]any{"AccessToken": access}},
		{"VerifySoftwareToken", map[string]any{"AccessToken": access, "UserCode": code}},
		{"SetUserMFAPreference", map[string]any{"AccessToken": access, "SoftwareTokenMfaSettings": map[string]bool{"Enabled": false}}},
	} {
		status, body = postCognito(t, server.URL, request.action, request.body)
		require.Equal(t, http.StatusBadRequest, status, "action=%s body=%v", request.action, body)
		require.Equal(t, "NotAuthorizedException", body["__type"])
		require.Equal(t, verified, f.current(t))
	}
	grant.OriginJTI = "other-native-device-grant"
	otherAccess, err := SignAccessToken(t.Context(), f.store, f.handler.issuerBase, f.user.PoolID, clientID, f.user.Sub, f.user.Email, grant, time.Hour)
	require.NoError(t, err)
	status, body = postCognito(t, server.URL, "AssociateSoftwareToken", map[string]any{"AccessToken": otherAccess})
	require.Equal(t, http.StatusOK, status, "another device's grant remains usable: body=%v", body)

	require.NoError(t, f.store.SetUserEnabled(t.Context(), f.user.Sub, false))
	status, body = postCognito(t, server.URL, "AdminSetUserMFAPreference", map[string]any{
		"UserPoolId": f.user.PoolID, "Username": f.user.Sub, "SoftwareTokenMfaSettings": map[string]bool{"Enabled": false},
	})
	require.Equal(t, http.StatusOK, status, "admin can reset a disabled account: body=%v", body)
	require.False(t, f.current(t).MFAEnabled)
}
