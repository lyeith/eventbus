package secrets

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/require"
)

func boolean(value bool) *bool { return &value }
func integer(value int) *int   { return &value }
func TestRandomPasswordControls(t *testing.T) {
	password, err := GenerateRandomPassword(RandomPasswordInput{})
	require.NoError(t, err)
	require.Len(t, password, 32)
	require.True(t, strings.ContainsFunc(password, unicode.IsLower))
	require.True(t, strings.ContainsFunc(password, unicode.IsUpper))
	require.True(t, strings.ContainsFunc(password, unicode.IsDigit))
	require.True(t, strings.ContainsAny(password, "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"))
	require.NotContains(t, password, " ")
	password, err = GenerateRandomPassword(RandomPasswordInput{PasswordLength: integer(4096), ExcludeLowercase: boolean(true), ExcludePunctuation: boolean(true), ExcludeUppercase: boolean(true), ExcludeCharacters: "012345678", IncludeSpace: boolean(true)})
	require.NoError(t, err)
	require.Len(t, password, 4096)
	for _, character := range password {
		require.Contains(t, "9 ", string(character))
	}
	require.Contains(t, password, "9")
	password, err = GenerateRandomPassword(RandomPasswordInput{PasswordLength: integer(1), ExcludeNumbers: boolean(true), ExcludePunctuation: boolean(true), ExcludeLowercase: boolean(true), ExcludeUppercase: boolean(true), IncludeSpace: boolean(true)})
	require.NoError(t, err)
	require.Equal(t, " ", password)
	for _, input := range []RandomPasswordInput{
		{PasswordLength: integer(0)}, {PasswordLength: integer(4097)}, {PasswordLength: integer(3)},
		{ExcludeCharacters: strings.Repeat("x", 4097)},
		{ExcludeNumbers: boolean(true), ExcludePunctuation: boolean(true), ExcludeLowercase: boolean(true), ExcludeUppercase: boolean(true)},
		{ExcludeCharacters: "0123456789"},
	} {
		_, err := GenerateRandomPassword(input)
		requireAPICode(t, err, "InvalidParameterException")
	}
	password, err = GenerateRandomPassword(RandomPasswordInput{PasswordLength: integer(1), RequireEachIncludedType: boolean(false), ExcludeCharacters: "0123456789"})
	require.NoError(t, err)
	require.Len(t, password, 1)
	require.False(t, strings.ContainsFunc(password, unicode.IsDigit))
}
