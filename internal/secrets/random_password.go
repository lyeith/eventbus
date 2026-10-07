package secrets

import (
	"crypto/rand"
	"math/big"
	"strings"
	"unicode/utf8"
)

type RandomPasswordInput struct {
	PasswordLength                                                                                                *int   `json:"PasswordLength"`
	ExcludeCharacters                                                                                             string `json:"ExcludeCharacters"`
	ExcludeNumbers, ExcludePunctuation, ExcludeUppercase, ExcludeLowercase, IncludeSpace, RequireEachIncludedType *bool
}

func enabled(value *bool) bool { return value != nil && *value }
func GenerateRandomPassword(input RandomPasswordInput) (string, error) {
	length := 32
	if input.PasswordLength != nil {
		length = *input.PasswordLength
	}
	if length < 1 || length > 4096 || utf8.RuneCountInString(input.ExcludeCharacters) > 4096 {
		return "", invalidParameter("Invalid password length or excluded character list")
	}
	types := []struct {
		characters string
		excluded   bool
	}{{"abcdefghijklmnopqrstuvwxyz", enabled(input.ExcludeLowercase)}, {"ABCDEFGHIJKLMNOPQRSTUVWXYZ", enabled(input.ExcludeUppercase)}, {"0123456789", enabled(input.ExcludeNumbers)}, {"!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", enabled(input.ExcludePunctuation)}}

	requireEach := input.RequireEachIncludedType == nil || *input.RequireEachIncludedType
	groups := []string{}
	alphabet := ""
	for _, kind := range types {
		if kind.excluded {
			continue
		}
		remaining := ""
		for _, character := range kind.characters {
			if !strings.ContainsRune(input.ExcludeCharacters, character) {
				remaining += string(character)
			}
		}
		if remaining == "" {
			if requireEach {
				return "", invalidParameter("An included character type is entirely excluded")
			}
			continue
		}
		groups = append(groups, remaining)
		alphabet += remaining
	}
	if enabled(input.IncludeSpace) && !strings.ContainsRune(input.ExcludeCharacters, ' ') {
		alphabet += " "
	}
	if alphabet == "" || requireEach && length < len(groups) {
		return "", invalidParameter("The selected password controls cannot satisfy the requested length")
	}
	choose := func(limit int) (int, error) {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(limit)))
		if err != nil {
			return 0, &APIError{"InternalServiceError", "Unable to generate a random password"}
		}
		return int(n.Int64()), nil
	}
	password := make([]byte, length)
	position := 0
	if requireEach {
		for _, group := range groups {
			index, err := choose(len(group))
			if err != nil {
				return "", err
			}
			password[position] = group[index]
			position++
		}
	}
	for position < length {
		index, err := choose(len(alphabet))
		if err != nil {
			return "", err
		}
		password[position] = alphabet[index]
		position++
	}
	for index := length - 1; index > 0; index-- {
		other, err := choose(index + 1)
		if err != nil {
			return "", err
		}
		password[index], password[other] = password[other], password[index]
	}
	return string(password), nil
}
