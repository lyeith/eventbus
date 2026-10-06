package cognito

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"
)

// Cognito's group and positive-integer padding follow the AWS Amplify SRP
// client at revision 88f2d79878df66754caf85b7c409df912739f53d:
// packages/auth/src/providers/cognito/utils/srp/{constants,getPaddedHex}.ts.
// The 3072-bit modulus is the RFC 3526 group; Cognito uses generator 2.
const srpModulusHex = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1" +
	"29024E088A67CC74020BBEA63B139B22514A08798E3404DD" +
	"EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245" +
	"E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7ED" +
	"EE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3D" +
	"C2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F" +
	"83655D23DCA3AD961C62F356208552BB9ED529077096966D" +
	"670C354E4ABC9804F1746C08CA18217C32905E462E36CE3B" +
	"E39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9" +
	"DE2BCBF6955817183995497CEA956AE515D2261898FA0510" +
	"15728E5A8AAAC42DAD33170D04507A33A85521ABDF1CBA64" +
	"ECFB850458DBEF0A8AEA71575D060C7DB3970F85A6E1E4C7" +
	"ABF5AE8CDB0933D71E8C94E04A25619DCEE3D2261AD2EE6B" +
	"F12FFA06D98A0864D87602733EC86A64521F2B18177B200C" +
	"BBE117577A615D6C770988C0BAD946E208E24FA074E5AB31" +
	"43DB5BFCE0FD108E4B82D120A93AD2CAFFFFFFFFFFFFFFFF"

const (
	srpSaltBytes       = 16
	srpPrivateBytes    = 128
	srpSecretBlockSize = 32
	srpKeySize         = 16
	srpChallengeTTL    = 5 * time.Minute
	// Amplify emits a UTC timestamp with an unpadded day of month.
	srpTimestampLayout = "Mon Jan 2 15:04:05 UTC 2006"
)

var (
	srpN = func() *big.Int {
		value, ok := new(big.Int).SetString(srpModulusHex, 16)
		if !ok {
			panic("invalid Cognito SRP modulus")
		}
		return value
	}()
	srpG               = big.NewInt(2)
	srpK               = srpHashInteger(srpPad(srpN), srpPad(srpG))
	errSRPVerification = errors.New("invalid SRP password claim")
)

// SRPState contains the public challenge and only the ephemeral key needed to
// authenticate its response. Marshal it into the private challenge row:
// neither the password, password verifier, private b, nor shared S survives.
// ChallengeParameters is the only representation to expose to the client.
type SRPState struct {
	PoolSuffix  string    `json:"pool_suffix"`
	Username    string    `json:"username"`
	SRPB        string    `json:"srp_b"`
	Salt        string    `json:"salt"`
	SecretBlock string    `json:"secret_block"`
	DerivedKey  []byte    `json:"derived_key"`
	IssuedAt    time.Time `json:"issued_at"`
}

func (state SRPState) ChallengeParameters() map[string]string {
	return map[string]string{
		"SRP_B":           state.SRPB,
		"SALT":            state.Salt,
		"SECRET_BLOCK":    state.SecretBlock,
		"USER_ID_FOR_SRP": state.Username,
	}
}

// makeSRPCredentials runs only while the password is already available for
// provisioning/replacement. Persist this salt/verifier atomically with bcrypt;
// SRP cannot reconstruct its verifier from a bcrypt password hash.
func makeSRPCredentials(poolID, username, password string) (string, string, error) {
	return makeSRPCredentialsWithEntropy(poolID, username, password, rand.Reader)
}

func makeSRPCredentialsWithEntropy(poolID, username, password string, entropy io.Reader) (string, string, error) {
	poolSuffix, err := srpPoolSuffix(poolID)
	if err != nil {
		return "", "", err
	}
	if username == "" || password == "" {
		return "", "", errors.New("SRP credentials require username and password")
	}
	saltBytes := make([]byte, srpSaltBytes)
	if _, err := io.ReadFull(entropy, saltBytes); err != nil {
		return "", "", fmt.Errorf("generate SRP salt: %w", err)
	}
	salt := new(big.Int).SetBytes(saltBytes)
	identityHash := sha256.Sum256([]byte(poolSuffix + username + ":" + password))
	x := srpHashInteger(srpPad(salt), identityHash[:])
	verifier := new(big.Int).Exp(srpG, x, srpN)
	return salt.Text(16), verifier.Text(16), nil
}

func newSRPChallenge(poolID, username, saltHex, verifierHex, aHex string) (SRPState, error) {
	return newSRPChallengeWithEntropy(poolID, username, saltHex, verifierHex, aHex, rand.Reader, time.Now().UTC())
}

func newSRPChallengeWithEntropy(poolID, username, saltHex, verifierHex, aHex string, entropy io.Reader, issuedAt time.Time) (SRPState, error) {
	poolSuffix, err := srpPoolSuffix(poolID)
	if err != nil {
		return SRPState{}, err
	}
	if username == "" || issuedAt.IsZero() {
		return SRPState{}, errors.New("SRP challenge requires username and issue time")
	}
	salt, err := srpHexInteger("salt", saltHex, 2*(srpSaltBytes+1))
	if err != nil {
		return SRPState{}, err
	}
	verifier, err := srpHexInteger("verifier", verifierHex, len(srpModulusHex)+2)
	if err != nil || verifier.Sign() <= 0 || verifier.Cmp(srpN) >= 0 {
		return SRPState{}, errors.New("invalid SRP verifier")
	}
	A, err := srpHexInteger("SRP_A", aHex, len(srpModulusHex)+2)
	if err != nil || A.Sign() <= 0 || A.Cmp(srpN) >= 0 {
		return SRPState{}, errors.New("invalid SRP_A")
	}

	// A uniformly random 1024-bit exponent exceeds the group's required
	// security strength. Adding one excludes zero without a rejection loop.
	private := make([]byte, srpPrivateBytes)
	if _, err := io.ReadFull(entropy, private); err != nil {
		return SRPState{}, fmt.Errorf("generate SRP exponent: %w", err)
	}
	b := new(big.Int).SetBytes(private)
	b.Add(b, big.NewInt(1))
	B := new(big.Int).Mul(srpK, verifier)
	B.Add(B, new(big.Int).Exp(srpG, b, srpN))
	B.Mod(B, srpN)
	if B.Sign() == 0 {
		return SRPState{}, errors.New("invalid SRP_B")
	}
	u := srpHashInteger(srpPad(A), srpPad(B))
	if u.Sign() == 0 {
		return SRPState{}, errors.New("invalid SRP scrambling parameter")
	}
	base := new(big.Int).Exp(verifier, u, srpN)
	base.Mul(base, A).Mod(base, srpN)
	shared := new(big.Int).Exp(base, b, srpN)
	if shared.Sign() == 0 {
		return SRPState{}, errors.New("invalid SRP shared secret")
	}
	key := srpDerivedKey(shared, u)
	block := make([]byte, srpSecretBlockSize)
	if _, err := io.ReadFull(entropy, block); err != nil {
		return SRPState{}, fmt.Errorf("generate SRP secret block: %w", err)
	}
	return SRPState{
		PoolSuffix: poolSuffix, Username: username,
		SRPB: B.Text(16), Salt: salt.Text(16),
		SecretBlock: base64.StdEncoding.EncodeToString(block),
		DerivedKey:  key, IssuedAt: issuedAt.UTC(),
	}, nil
}

// Verify authenticates the exact Cognito password claim. The enclosing
// controller must bind the row to pool/user/client and consume its session
// atomically; cryptographic verification alone cannot prevent session replay.
// The local flow admits the Amplify timestamp form within five minutes of the
// verification clock, while the challenge itself expires after five minutes.
func (state SRPState) Verify(signatureBase64, secretBlockBase64, timestamp string, now time.Time) error {
	if len(state.DerivedKey) != srpKeySize || state.Username == "" || state.PoolSuffix == "" ||
		state.IssuedAt.IsZero() || now.Before(state.IssuedAt) || !now.Before(state.IssuedAt.Add(srpChallengeTTL)) {
		return errSRPVerification
	}
	claimedAt, err := time.Parse(srpTimestampLayout, timestamp)
	if err != nil || claimedAt.Format(srpTimestampLayout) != timestamp ||
		claimedAt.Before(now.Add(-srpChallengeTTL)) || claimedAt.After(now.Add(srpChallengeTTL)) {
		return errSRPVerification
	}
	block, err := srpBase64(secretBlockBase64, srpSecretBlockSize)
	if err != nil {
		return errSRPVerification
	}
	bound, err := srpBase64(state.SecretBlock, srpSecretBlockSize)
	if err != nil || !hmac.Equal(bound, block) {
		return errSRPVerification
	}
	signature, err := srpBase64(signatureBase64, sha256.Size)
	if err != nil {
		return errSRPVerification
	}
	mac := hmac.New(sha256.New, state.DerivedKey)
	mac.Write([]byte(state.PoolSuffix))
	mac.Write([]byte(state.Username))
	mac.Write(block)
	mac.Write([]byte(timestamp))
	if !hmac.Equal(mac.Sum(nil), signature) {
		return errSRPVerification
	}
	return nil
}

func srpPoolSuffix(poolID string) (string, error) {
	if poolID == "" {
		return "", errors.New("SRP requires a user pool")
	}
	if region, suffix, found := strings.Cut(poolID, "_"); found {
		if region == "" || suffix == "" || strings.Contains(suffix, "_") {
			return "", errors.New("invalid SRP user pool identifier")
		}
		return suffix, nil
	}
	// Existing local fixtures predate AWS-shaped pool identifiers. Their full
	// identifier is the deterministic pool name; real region_suffix IDs use
	// exactly the suffix expected by Cognito clients.
	return poolID, nil
}

func srpHexInteger(field, text string, maxLength int) (*big.Int, error) {
	if len(text) == 0 || len(text) > maxLength {
		return nil, fmt.Errorf("invalid SRP %s", field)
	}
	for _, character := range text {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f' || character >= 'A' && character <= 'F') {
			return nil, fmt.Errorf("invalid SRP %s", field)
		}
	}
	value, ok := new(big.Int).SetString(text, 16)
	if !ok {
		return nil, fmt.Errorf("invalid SRP %s", field)
	}
	return value, nil
}

// srpPad is positive BigInteger.toByteArray(), not modulus-width padding.
func srpPad(value *big.Int) []byte {
	bytes := value.Bytes()
	if len(bytes) == 0 {
		return []byte{0}
	}
	if bytes[0]&0x80 != 0 {
		return append([]byte{0}, bytes...)
	}
	return bytes
}

func srpHashInteger(parts ...[]byte) *big.Int {
	hash := sha256.New()
	for _, part := range parts {
		hash.Write(part)
	}
	return new(big.Int).SetBytes(hash.Sum(nil))
}

func srpDerivedKey(shared, u *big.Int) []byte {
	extract := hmac.New(sha256.New, srpPad(u))
	extract.Write(srpPad(shared))
	expand := hmac.New(sha256.New, extract.Sum(nil))
	expand.Write([]byte("Caldera Derived Key"))
	expand.Write([]byte{1})
	return expand.Sum(nil)[:srpKeySize]
}

func srpBase64(text string, size int) ([]byte, error) {
	if len(text) != base64.StdEncoding.EncodedLen(size) {
		return nil, errSRPVerification
	}
	raw, err := base64.StdEncoding.Strict().DecodeString(text)
	if err != nil || len(raw) != size || base64.StdEncoding.EncodeToString(raw) != text {
		return nil, errSRPVerification
	}
	return raw, nil
}
