package cognito

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLegacyIdentityMigrationPreservesUsersAndSigningKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	legacy, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	for _, statement := range []string{
		`CREATE TABLE pools(id TEXT PRIMARY KEY,region TEXT NOT NULL,created_at INTEGER NOT NULL)`,
		`CREATE TABLE users(sub TEXT PRIMARY KEY,pool_id TEXT NOT NULL,email TEXT NOT NULL,password_hash TEXT NOT NULL DEFAULT '',mfa_enabled INTEGER NOT NULL DEFAULT 0,mfa_secret TEXT NOT NULL DEFAULT '',status TEXT NOT NULL DEFAULT 'CONFIRMED',created_at INTEGER NOT NULL)`,
		`CREATE TABLE signing_keys(pool_id TEXT PRIMARY KEY,kid TEXT NOT NULL,private_pem TEXT NOT NULL,public_pem TEXT NOT NULL,created_at INTEGER NOT NULL)`,
		`INSERT INTO pools VALUES('legacy-pool','us-east-1',1600000000)`,
	} {
		_, err = legacy.Exec(statement)
		require.NoError(t, err)
	}
	hash, err := hashUserPassword("LegacyPass1!")
	require.NoError(t, err)
	_, err = legacy.Exec(`INSERT INTO users(sub,pool_id,email,password_hash,created_at) VALUES('durable-sub','legacy-pool','legacy@example.test',?,1600000001)`, hash)
	require.NoError(t, err)
	private, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	privatePEM, err := encodePrivateKeyPEM(private)
	require.NoError(t, err)
	publicPEM, err := encodePublicKeyPEM(&private.PublicKey)
	require.NoError(t, err)
	_, err = legacy.Exec(`INSERT INTO signing_keys VALUES('legacy-pool','durable-kid',?,?,1600000000)`, privatePEM, publicPEM)
	require.NoError(t, err)
	require.NoError(t, legacy.Close())
	for attempt := 0; attempt < 2; attempt++ {
		store, err := OpenCognitoStore(path)
		require.NoError(t, err)
		user, err := store.LookupPoolUser(t.Context(), "legacy-pool", "legacy@example.test")
		require.NoError(t, err)
		require.Equal(t, "durable-sub", user.Sub)
		require.Equal(t, "legacy@example.test", user.Username)
		require.True(t, user.Enabled)
		require.Equal(t, int64(1600000001), user.CreatedAt)
		require.Equal(t, user.CreatedAt, user.UpdatedAt)
		require.Equal(t, hash, user.PasswordHash)
		require.Zero(t, user.AuthVersion)
		key, err := store.LoadSigningKey(t.Context(), "legacy-pool")
		require.NoError(t, err)
		require.Equal(t, "durable-kid", key.Kid)
		require.Equal(t, private.N, key.Private.N)
		body, err := store.BuildJWKS(t.Context(), "legacy-pool")
		require.NoError(t, err)
		require.Contains(t, string(body), "durable-kid")
		require.NoError(t, store.Close())
	}
}

func TestAdminLifecyclePreservesIdentityThroughDisableAndResend(t *testing.T) {
	_, server, store := newCognitoTestServer(t)
	require.NoError(t, store.UpsertPool(t.Context(), "lifecycle-pool", "us-east-1"))
	status, body := postCognito(t, server.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": "lifecycle-pool", "Username": "integration-user-001", "TemporaryPassword": "TempPass1!", "MessageAction": "SUPPRESS",
		"UserAttributes": []map[string]string{{"Name": "email", "Value": "invitee@example.test"}, {"Name": "email_verified", "Value": "false"}, {"Name": "custom:owner", "Value": "original"}},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	profile := body["User"].(map[string]interface{})
	require.Equal(t, "integration-user-001", profile["Username"])
	require.Equal(t, "FORCE_CHANGE_PASSWORD", profile["UserStatus"])
	user, err := store.LookupPoolUser(t.Context(), "lifecycle-pool", "integration-user-001")
	require.NoError(t, err)
	originalSub, created := user.Sub, user.CreatedAt
	require.NotEmpty(t, user.SRPSalt)
	require.NotEmpty(t, user.SRPVerifier)
	require.NoError(t, store.CreateChallengeSession(t.Context(), "stale-challenge", user.Sub, user.PoolID, "client", "NEW_PASSWORD_REQUIRED", time.Minute))
	_, err = store.DB().Exec(`UPDATE users SET updated_at=1 WHERE sub=?`, user.Sub)
	require.NoError(t, err)
	status, body = postCognito(t, server.URL, "AdminSetUserPassword", map[string]interface{}{"UserPoolId": user.PoolID, "Username": user.Username, "Password": "PermanentPass2!", "Permanent": true})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	_, err = store.LookupChallengeSession(t.Context(), "stale-challenge")
	require.ErrorIs(t, err, sql.ErrNoRows)
	user, err = store.LookupUserBySub(t.Context(), originalSub)
	require.NoError(t, err)
	require.Equal(t, "CONFIRMED", user.Status)
	require.Greater(t, user.UpdatedAt, int64(1))
	for iteration := 0; iteration < 2; iteration++ {
		status, body = postCognito(t, server.URL, "AdminDisableUser", map[string]interface{}{"UserPoolId": user.PoolID, "Username": user.Sub})
		require.Equal(t, http.StatusOK, status, "body=%v", body)
	}
	user, err = store.LookupUserBySub(t.Context(), originalSub)
	require.NoError(t, err)
	require.False(t, user.Enabled)
	require.Equal(t, int64(2), user.AuthVersion)
	status, body = postCognito(t, server.URL, "AdminCreateUser", map[string]interface{}{
		"UserPoolId": user.PoolID, "Username": user.Username, "TemporaryPassword": "ReinvitePass3!", "MessageAction": "RESEND",
		"UserAttributes": []map[string]string{{"Name": "email", "Value": "replacement@example.test"}, {"Name": "custom:owner", "Value": "replacement"}},
	})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err = store.LookupUserBySub(t.Context(), originalSub)
	require.NoError(t, err)
	require.False(t, user.Enabled)
	require.Equal(t, created, user.CreatedAt)
	require.Equal(t, "FORCE_CHANGE_PASSWORD", user.Status)
	require.Equal(t, int64(3), user.AuthVersion)
	require.Equal(t, "invitee@example.test", user.Email)
	require.NoError(t, compareUserPasswordHash(user.PasswordHash, "ReinvitePass3!"))
	status, body = postCognito(t, server.URL, "AdminGetUser", map[string]interface{}{"UserPoolId": user.PoolID, "Username": originalSub})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Equal(t, false, body["Enabled"])
	require.Equal(t, float64(created), body["UserCreateDate"])
	assertAttribute(t, body["UserAttributes"], "email", "invitee@example.test")
	assertAttribute(t, body["UserAttributes"], "email_verified", "false")
	assertAttribute(t, body["UserAttributes"], "custom:owner", "original")
	status, body = postCognito(t, server.URL, "AdminEnableUser", map[string]interface{}{"UserPoolId": user.PoolID, "Username": user.Username})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err = store.LookupUserBySub(t.Context(), originalSub)
	require.NoError(t, err)
	require.True(t, user.Enabled)
	require.Equal(t, int64(3), user.AuthVersion)
}

func TestUserIdentityAndCredentialsPersistAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	store, err := OpenCognitoStore(path)
	require.NoError(t, err)
	require.NoError(t, store.UpsertPool(t.Context(), "restart-pool", "us-east-1"))
	user, err := store.CreateUserIdentity(t.Context(), "restart-pool", "stable-id", "mail@example.test", "InitialPass1!", "FORCE_CHANGE_PASSWORD", map[string]string{"email": "mail@example.test"})
	require.NoError(t, err)
	key, err := store.EnsureSigningKey(t.Context(), user.PoolID)
	require.NoError(t, err)
	require.NoError(t, store.SetUserPassword(t.Context(), user.Sub, "PersistentPass2!", "CONFIRMED"))
	require.NoError(t, store.SetUserEnabled(t.Context(), user.Sub, false))
	before, err := store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.NoError(t, store.Close())
	store, err = OpenCognitoStore(path)
	require.NoError(t, err)
	defer store.Close()
	after, err := store.LookupPoolUser(t.Context(), "restart-pool", "stable-id")
	require.NoError(t, err)
	require.Equal(t, before, after)
	loaded, err := store.LoadSigningKey(t.Context(), user.PoolID)
	require.NoError(t, err)
	require.Equal(t, key.Kid, loaded.Kid)
	require.NoError(t, compareUserPasswordHash(after.PasswordHash, "PersistentPass2!"))
}

func TestPoolSignInRulesKeepCanonicalUsernameSeparateFromEmail(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	for _, scenario := range []struct {
		name   string
		config PoolSignInConfig
	}{
		{"username", PoolSignInConfig{CaseSensitive: true}},
		{"alias", PoolSignInConfig{CaseSensitive: true, EmailAlias: true}},
		{"email", PoolSignInConfig{CaseSensitive: true, EmailAsUsername: true}},
		{"insensitive-alias", PoolSignInConfig{EmailAlias: true}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			require.NoError(t, store.UpsertPool(t.Context(), scenario.name, "us-east-1"))
			require.NoError(t, store.SetPoolSignInConfig(t.Context(), scenario.name, scenario.config))
			user, err := store.CreateUserIdentity(t.Context(), scenario.name, "StableUser", "Invitee@example.test", "Password1!", "CONFIRMED", map[string]string{"email": "Invitee@example.test", "email_verified": "false"})
			require.NoError(t, err)
			resolved, err := store.LookupPoolUser(t.Context(), scenario.name, user.Sub)
			require.NoError(t, err)
			require.Equal(t, user.Username, resolved.Username)
			_, err = store.ResolveSignInUser(t.Context(), scenario.name, "Invitee@example.test")
			if scenario.config.EmailAsUsername {
				require.NoError(t, err)
				require.Equal(t, user.Sub, user.Username)
			} else {
				require.ErrorIs(t, err, sql.ErrNoRows)
			}
			require.NoError(t, store.SetUserAttribute(t.Context(), user.Sub, "email_verified", "true"))
			resolved, err = store.ResolveSignInUser(t.Context(), scenario.name, "Invitee@example.test")
			if scenario.config.EmailAlias || scenario.config.EmailAsUsername {
				require.NoError(t, err)
				require.Equal(t, user.Sub, resolved.Sub)
			} else {
				require.ErrorIs(t, err, sql.ErrNoRows)
			}
			if !scenario.config.CaseSensitive {
				resolved, err = store.ResolveSignInUser(t.Context(), scenario.name, "INVITEE@EXAMPLE.TEST")
				require.NoError(t, err)
				require.Equal(t, "stableuser", resolved.Username)
			}
			if !scenario.config.EmailAsUsername {
				resolved, err = store.ResolveSignInUser(t.Context(), scenario.name, user.Username)
				require.NoError(t, err)
				require.Equal(t, user.Sub, resolved.Sub)
			}
		})
	}
}

func TestConcurrentCreationHasExactlyOneCanonicalIdentity(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "race-pool", "us-east-1"))
	results := make(chan error, 8)
	var wait sync.WaitGroup
	for iteration := 0; iteration < 8; iteration++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := store.CreateUserIdentity(context.Background(), "race-pool", "same-user", "same@example.test", "Password1!", "CONFIRMED", nil)
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else {
			require.ErrorIs(t, err, errUsernameExists)
		}
	}
	require.Equal(t, 1, successes)
	var count int
	require.NoError(t, store.DB().QueryRow(`SELECT COUNT(*) FROM users WHERE pool_id='race-pool'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestSessionConsumeAndPasswordTransitionsGuardCurrentRevision(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "session-pool", "us-east-1"))
	user, err := store.CreateUserIdentity(t.Context(), "session-pool", "username", "mail@example.test", "InitialPass1!", "CONFIRMED", nil)
	require.NoError(t, err)
	require.NoError(t, store.CreateChallengeSessionWithState(t.Context(), "single-use", user.Sub, user.PoolID, "client", "CUSTOM_CHALLENGE", time.Minute, `{"history":["PASSWORD_VERIFIER"]}`))
	row, err := store.LookupChallengeSession(t.Context(), "single-use")
	require.NoError(t, err)
	require.Equal(t, user.AuthVersion, row.AuthVersion)
	require.JSONEq(t, `{"history":["PASSWORD_VERIFIER"]}`, row.StateJSON)
	consumed := make(chan bool, 8)
	failures := make(chan error, 8)
	for iteration := 0; iteration < 8; iteration++ {
		go func() {
			ok, err := store.ConsumeChallengeSession(context.Background(), "single-use")
			consumed <- ok
			failures <- err
		}()
	}
	winners := 0
	for iteration := 0; iteration < 8; iteration++ {
		if <-consumed {
			winners++
		}
		require.NoError(t, <-failures)
	}
	require.Equal(t, 1, winners)
	require.NoError(t, store.CreateChallengeSession(t.Context(), "expired", user.Sub, user.PoolID, "client", "CUSTOM_CHALLENGE", -time.Minute))
	ok, err := store.ConsumeChallengeSession(t.Context(), "expired")
	require.NoError(t, err)
	require.False(t, ok)
	require.NoError(t, store.CreateChallengeSession(t.Context(), "expires-now", user.Sub, user.PoolID, "client", "CUSTOM_CHALLENGE", 0))
	ok, err = store.ConsumeChallengeSession(t.Context(), "expires-now")
	require.NoError(t, err)
	require.False(t, ok, "a session at its expiration second is already expired")
	require.NoError(t, store.SetUserPassword(t.Context(), user.Sub, "AdminReset2!", "CONFIRMED"))
	require.ErrorIs(t, store.ChangeUserPasswordAtVersion(t.Context(), user.Sub, "StaleSelfChange3!", user.AuthVersion), errTokenRevoked)
	require.ErrorIs(t, store.SetUserPasswordAtVersion(t.Context(), user.Sub, "StaleChallenge4!", "CONFIRMED", user.AuthVersion), errTokenRevoked)
	current, err := store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.NoError(t, compareUserPasswordHash(current.PasswordHash, "AdminReset2!"))
	require.NoError(t, store.ChangeUserPasswordAtVersion(t.Context(), current.Sub, "CurrentSelfChange5!", current.AuthVersion))
	updated, err := store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, current.AuthVersion, updated.AuthVersion)
	require.NoError(t, store.SetUserEnabled(t.Context(), user.Sub, false))
	require.ErrorIs(t, store.ChangeUserPasswordAtVersion(t.Context(), user.Sub, "DisabledChange6!", updated.AuthVersion), errTokenRevoked)
	require.NoError(t, store.SetUserEnabled(t.Context(), user.Sub, true))
	require.ErrorIs(t, store.SetUserPasswordAtVersion(t.Context(), user.Sub, "ReenabledOldProof7!", "CONFIRMED", updated.AuthVersion), errTokenRevoked)
}

func TestListUsersFiltersPaginationAndDisabledVisibility(t *testing.T) {
	_, server, store := newCognitoTestServer(t)
	require.NoError(t, store.UpsertPool(t.Context(), "list-pool", "us-east-1"))
	require.NoError(t, store.UpsertPool(t.Context(), "other-pool", "us-east-1"))
	for _, email := range []string{"Alpha@example.test", "beta@example.test", "beta2@example.test", "literal_value@example.test"} {
		user, err := store.CreateUserIdentity(t.Context(), "list-pool", email, email, "Password1!", "CONFIRMED", map[string]string{"email": email})
		require.NoError(t, err)
		if email == "beta@example.test" {
			require.NoError(t, store.SetUserEnabled(t.Context(), user.Sub, false))
		}
	}
	status, body := postCognito(t, server.URL, "ListUsers", map[string]interface{}{"UserPoolId": "list-pool", "Filter": `email ^= "BETA"`, "Limit": 1})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	first := body["Users"].([]interface{})
	require.Len(t, first, 1)
	token := body["PaginationToken"].(string)
	status, second := postCognito(t, server.URL, "ListUsers", map[string]interface{}{"UserPoolId": "list-pool", "Filter": `email ^= "BETA"`, "Limit": 1, "PaginationToken": token})
	require.Equal(t, http.StatusOK, status, "body=%v", second)
	next := second["Users"].([]interface{})
	require.Len(t, next, 1)
	require.NotEqual(t, first[0].(map[string]interface{})["Username"], next[0].(map[string]interface{})["Username"])
	require.NotContains(t, second, "PaginationToken")
	enabled := []interface{}{first[0].(map[string]interface{})["Enabled"], next[0].(map[string]interface{})["Enabled"]}
	require.Contains(t, enabled, false)
	for _, query := range []map[string]interface{}{
		{"UserPoolId": "other-pool", "Filter": `email ^= "BETA"`, "Limit": 1, "PaginationToken": token},
		{"UserPoolId": "list-pool", "Filter": `email = "beta@example.test"`, "Limit": 1, "PaginationToken": token},
		{"UserPoolId": "list-pool", "Filter": `email ^= "BETA"`, "Limit": 1, "PaginationToken": "malformed"},
		{"UserPoolId": "list-pool", "Filter": `email ^= "BETA"`, "Limit": 1, "PaginationToken": token, "AttributesToGet": []string{"sub"}},
	} {
		status, body = postCognito(t, server.URL, "ListUsers", query)
		require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
		require.Equal(t, "InvalidParameterException", body["__type"])
	}
	status, body = postCognito(t, server.URL, "ListUsers", map[string]interface{}{"UserPoolId": "list-pool", "Filter": `email ^= "literal_"`})
	require.Equal(t, http.StatusOK, status)
	require.Len(t, body["Users"], 1)
	status, body = postCognito(t, server.URL, "ListUsers", map[string]interface{}{"UserPoolId": "list-pool", "Filter": `email = "alpha@EXAMPLE.TEST"`})
	require.Equal(t, http.StatusOK, status)
	require.Len(t, body["Users"], 1)
	status, body = postCognito(t, server.URL, "ListUsers", map[string]interface{}{"UserPoolId": "list-pool", "Filter": `email = "x\" OR 1=1 --"`})
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body["Users"])
	status, body = postCognito(t, server.URL, "ListUsers", map[string]interface{}{"UserPoolId": "list-pool", "Limit": 0})
	require.Equal(t, http.StatusOK, status)
	require.Empty(t, body["Users"])
	data, err := base64.RawURLEncoding.DecodeString(token)
	require.NoError(t, err)
	var cursor userListCursor
	require.NoError(t, json.Unmarshal(data, &cursor))
	cursor.Expires = time.Now().Add(-time.Minute).Unix()
	data, err = json.Marshal(cursor)
	require.NoError(t, err)
	_, _, err = store.ListPoolUsers(t.Context(), "list-pool", UserListOptions{Filter: `email ^= "BETA"`, Limit: 1, PaginationToken: base64.RawURLEncoding.EncodeToString(data)})
	var invalid *invalidUserListParameter
	require.True(t, errors.As(err, &invalid))
}

func TestGlobalSignOutAdvancesRevisionAndInvalidatesChallenges(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "global-revoke", "us-east-1"))
	user, err := store.CreateUserIdentity(t.Context(), "global-revoke", "username", "", "Password1!", "CONFIRMED", nil)
	require.NoError(t, err)
	for iteration, at := range []int64{500, 500, 499} {
		sessionID := fmt.Sprintf("pending-%d", iteration)
		require.NoError(t, store.CreateChallengeSession(t.Context(), sessionID, user.Sub, user.PoolID, "client", "CUSTOM_CHALLENGE", time.Minute))
		require.NoError(t, store.RevokeUserTokens(t.Context(), user.Sub, at))
		updated, err := store.LookupUserBySub(t.Context(), user.Sub)
		require.NoError(t, err)
		require.Equal(t, int64(iteration+1), updated.AuthVersion)
		require.Equal(t, int64(500), updated.TokensRevokedBefore)
		require.True(t, updated.Enabled)
		require.Equal(t, user.CreatedAt, updated.CreatedAt)
		require.Equal(t, user.UpdatedAt, updated.UpdatedAt)
		_, err = store.LookupChallengeSession(t.Context(), sessionID)
		require.ErrorIs(t, err, sql.ErrNoRows)
	}
	require.NoError(t, store.CreateChallengeSession(t.Context(), "fresh-after-signout", user.Sub, user.PoolID, "client", "CUSTOM_CHALLENGE", time.Minute))
	session, err := store.LookupChallengeSession(t.Context(), "fresh-after-signout")
	require.NoError(t, err)
	require.Equal(t, int64(3), session.AuthVersion)
	consumed, err := store.ConsumeChallengeSession(t.Context(), session.Session)
	require.NoError(t, err)
	require.True(t, consumed)
}
