package cognito

import (
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestWorkflowVerificationPersistsAcrossStoreRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.db")
	store, err := OpenCognitoStore(path)
	require.NoError(t, err)
	require.NoError(t, store.UpsertPool(t.Context(), "persisted-pool", "us-east-1"))
	user, err := store.CreateUserIdentity(t.Context(), "persisted-pool", "stable-user", "mail@example.test", "InitialPass1!", "UNCONFIRMED", map[string]string{"email": "mail@example.test", "email_verified": "false"})
	require.NoError(t, err)
	require.NoError(t, store.putVerification(t.Context(), user, verificationSpec{"signup", "email", user.Email, "012345", store.now().Add(24 * time.Hour)}))
	require.NoError(t, store.Close())
	store, err = OpenCognitoStore(path)
	require.NoError(t, err)
	defer store.Close()
	require.NoError(t, store.confirmVerification(t.Context(), user, "signup", "012345", true))
	confirmed, err := store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "CONFIRMED", confirmed.Status)
	require.Equal(t, user.Sub, confirmed.Sub)
	require.Equal(t, user.Username, confirmed.Username)
	attrs, err := store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "true", attrs["email_verified"])
}

func TestWorkflowCodeAttemptLimitDoesNotConfirmUser(t *testing.T) {
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), "attempts-pool", "us-east-1"))
	user, err := store.CreateUserIdentity(t.Context(), "attempts-pool", "attempt-user", "mail@example.test", "InitialPass1!", "UNCONFIRMED", map[string]string{"email": "mail@example.test"})
	require.NoError(t, err)
	require.NoError(t, store.putVerification(t.Context(), user, verificationSpec{"signup", "email", user.Email, "012345", store.now().Add(24 * time.Hour)}))
	for range 5 {
		var workflow *workflowError
		require.ErrorAs(t, store.confirmVerification(t.Context(), user, "signup", "999999", true), &workflow)
		require.Equal(t, "CodeMismatchException", workflow.Code)
	}
	var workflow *workflowError
	require.ErrorAs(t, store.confirmVerification(t.Context(), user, "signup", "012345", true), &workflow)
	require.Equal(t, "LimitExceededException", workflow.Code)
	unchanged, err := store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "UNCONFIRMED", unchanged.Status)
	require.NoError(t, store.putVerification(t.Context(), user, verificationSpec{"signup", "email", user.Email, "123456", store.now().Add(24 * time.Hour)}))
	require.NoError(t, store.confirmVerification(t.Context(), user, "signup", "123456", true))
}
