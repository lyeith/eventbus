package ssm

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSMStoreSnapshotsAreIndependentAcrossConcurrentReads(t *testing.T) {
	store := NewSSMStore()
	written, err := store.PutParameterValue(SSMParameter{Name: "/owned/value", Value: "original", Type: "String", Description: "retained"}, false)
	require.NoError(t, err)
	written.Value = "changed returned put result"
	read, err := store.GetParameter("/owned/value")
	require.NoError(t, err)
	read.Name, read.Value, read.Type = "/other", "changed returned get result", "SecureString"
	list := store.GetParametersByPath("/owned")
	list[0].Value = "changed returned list result"
	again, err := store.GetParameter("/owned/value")
	require.NoError(t, err)
	require.Equal(t, "original", again.Value)
	require.Equal(t, "String", again.Type)
	require.Equal(t, "retained", again.Description)

	var group sync.WaitGroup
	failures := make(chan error, 16)
	for worker := 0; worker < 16; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for i := 0; i < 20; i++ {
				snapshot, err := store.GetParameter("/owned/value")
				if err != nil {
					failures <- err
					return
				}
				snapshot.Value = "owned mutation"
				if err := store.PutParameter("/owned/value", "stored", "", true); err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	group.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	latest, err := store.GetParameter("/owned/value")
	require.NoError(t, err)
	require.Equal(t, int64(321), latest.Version)
	require.Equal(t, "stored", latest.Value)
	require.Equal(t, "retained", latest.Description)
}

func TestSSMStoreHierarchyUsesBoundariesAndSnapshots(t *testing.T) {
	store := NewSSMStore()
	for _, name := range []string{"/app", "/app/a", "/app/b", "/app/nested/deep", "/application/leak"} {
		require.NoError(t, store.PutParameter(name, name, "String", false))
	}
	all := store.GetParametersByPath("/app")
	require.Len(t, all, 3)
	direct, token, err := store.ListParametersByPath("/app/", false, false, 10, "")
	require.NoError(t, err)
	require.Empty(t, token)
	require.Len(t, direct, 2)
	require.Equal(t, "/app/a", direct[0].Name)
	require.Equal(t, "/app/b", direct[1].Name)
}

func TestSSMStoreVersionsRetentionAndSealedSnapshots(t *testing.T) {
	store := NewSSMStore()
	for i := 1; i <= 105; i++ {
		written, err := store.PutParameterValue(SSMParameter{Name: "/versions", Value: fmt.Sprintf("value-%d", i), Type: "SecureString"}, i != 1)
		require.NoError(t, err)
		require.Equal(t, int64(i), written.Version)
	}
	_, err := store.GetParameterValue("/versions", 5, true)
	require.ErrorIs(t, err, errParameterVersionNotFound)
	old, err := store.GetParameterValue("/versions", 6, true)
	require.NoError(t, err)
	require.Equal(t, "value-6", old.Value)
	latest, err := store.GetParameter("/versions")
	require.NoError(t, err)
	require.Equal(t, "value-105", latest.Value)
	ciphertext, err := store.GetParameterValue("/versions", 0, false)
	require.NoError(t, err)
	require.NotEqual(t, latest.Value, ciphertext.Value)
	ciphertext.Value = "owned ciphertext snapshot mutation"
	latest, err = store.GetParameter("/versions")
	require.NoError(t, err)
	require.Equal(t, "value-105", latest.Value)
	for _, stored := range store.parameters["/versions"] {
		require.Empty(t, stored.parameter.Value, "SecureString plaintext must not be retained in stored records")
		require.NotEmpty(t, stored.sealedValue)
	}
	require.True(t, store.DeleteParameterIfExists("/versions"))
	require.False(t, store.DeleteParameterIfExists("/versions"))
	_, err = store.GetParameterValue("/versions", 6, true)
	require.ErrorIs(t, err, errParameterNotFound)
}
