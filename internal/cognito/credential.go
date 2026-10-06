package cognito

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Legacy short-password hashes keep their existing bcrypt representation.
// Cognito permits passwords beyond bcrypt's 72-byte input limit; an explicit
// prefix distinguishes the SHA-256 prehash used for those credentials.
const digestPasswordPrefix = "eventbus-sha256:"

func hashUserPassword(password string) (string, error) {
	input := password
	prefix := ""
	if len(password) > 72 {
		input = passwordDigest(password)
		prefix = digestPasswordPrefix
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(input), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return prefix + string(hash), nil
}

func compareUserPasswordHash(hash, password string) error {
	if strings.HasPrefix(hash, digestPasswordPrefix) {
		hash = strings.TrimPrefix(hash, digestPasswordPrefix)
		password = passwordDigest(password)
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}

func passwordDigest(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}
