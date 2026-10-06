package cognito

import (
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"strings"
	"testing"
)

func TestCredentialHashSupportsAWSLengthAndLegacy(t *testing.T) {
	legacy, err := bcrypt.GenerateFromPassword([]byte("Legacy1!"), bcrypt.MinCost)
	require.NoError(t, err)
	require.NoError(t, compareUserPasswordHash(string(legacy), "Legacy1!"))
	require.Error(t, compareUserPasswordHash(string(legacy), "Wrong1!"))
	for _, password := range []string{"Short1!", strings.Repeat("a", 255) + "!", strings.Repeat("界", 100) + "1!"} {
		hash, err := hashUserPassword(password)
		require.NoError(t, err)
		require.NoError(t, compareUserPasswordHash(hash, password))
		require.Error(t, compareUserPasswordHash(hash, password+"x"))
		require.Equal(t, len(password) > 72, strings.HasPrefix(hash, digestPasswordPrefix))
	}
}
