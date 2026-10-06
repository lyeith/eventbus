package cognito

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var srpTestTime = time.Date(2026, time.October, 6, 10, 20, 30, 0, time.UTC)

func TestSRPPositiveIntegerPadding(t *testing.T) {
	for _, test := range []struct {
		value string
		want  string
	}{
		{"0", "00"}, {"1", "01"}, {"7f", "7f"},
		{"80", "0080"}, {"ff", "00ff"}, {"100", "0100"},
		{"8000", "008000"}, {"000001", "01"},
	} {
		t.Run(test.value, func(t *testing.T) {
			value, ok := new(big.Int).SetString(test.value, 16)
			require.True(t, ok)
			require.Equal(t, test.want, hex.EncodeToString(srpPad(value)))
		})
	}
	require.Equal(t, 3072, srpN.BitLen())
	require.Equal(t, 2, int(srpG.Int64()))
}

func TestSRPIndependentClientProofAndPersistence(t *testing.T) {
	for _, test := range []struct {
		name, poolID, username, password string
	}{
		{"canonical username", "us-east-1_ExamplePool", "canonical-user-42", "TestPassword!42"},
		{"UTF8 credentials", "ap-southeast-1_Pool42", "ユーザー-42", "ÉmailPass!42"},
		{"legacy local pool", "local-pool-1", "canonical-user", "Password!42"},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, a := srpTestState(t, test.poolID, test.username, test.password)
			poolName := test.poolID
			if _, suffix, found := strings.Cut(poolName, "_"); found {
				poolName = suffix
			}
			key, signature := srpIndependentClientProof(t, poolName, test.username, test.password, a, state, srpTestTime.Format(srpTimestampLayout))
			require.Equal(t, key, state.DerivedKey, "server and independently calculated client shared keys")
			require.NoError(t, state.Verify(signature, state.SecretBlock, srpTestTime.Format(srpTimestampLayout), srpTestTime))

			parameters := state.ChallengeParameters()
			require.Equal(t, map[string]string{
				"SRP_B": state.SRPB, "SALT": state.Salt,
				"SECRET_BLOCK": state.SecretBlock, "USER_ID_FOR_SRP": test.username,
			}, parameters)
			require.NotContains(t, parameters, "derived_key")

			encoded, err := json.Marshal(state)
			require.NoError(t, err)
			var persisted map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(encoded, &persisted))
			require.Len(t, persisted, 7)
			require.NotContains(t, persisted, "password")
			require.NotContains(t, persisted, "verifier")
			require.NotContains(t, persisted, "private_b")
			require.NotContains(t, persisted, "shared_s")
			require.NotContains(t, string(encoded), test.password)

			var restored SRPState
			require.NoError(t, json.Unmarshal(encoded, &restored))
			require.NoError(t, restored.Verify(signature, state.SecretBlock, srpTestTime.Format(srpTimestampLayout), srpTestTime))

			_, incorrectPasswordSignature := srpIndependentClientProof(t, poolName, test.username, "IncorrectPassword!42", a, state, srpTestTime.Format(srpTimestampLayout))
			require.ErrorIs(t, state.Verify(incorrectPasswordSignature, state.SecretBlock, srpTestTime.Format(srpTimestampLayout), srpTestTime), errSRPVerification)
		})
	}
}

func TestSRPCredentialsBindPoolUsernameAndPassword(t *testing.T) {
	entropy := bytes.Repeat([]byte{0x80}, srpSaltBytes)
	makeCredentials := func(poolID, username, password string) (string, string) {
		t.Helper()
		salt, verifier, err := makeSRPCredentialsWithEntropy(poolID, username, password, bytes.NewReader(entropy))
		require.NoError(t, err)
		return salt, verifier
	}
	salt, verifier := makeCredentials("us-east-1_PoolA", "canonical-user", "Password!42")
	require.Equal(t, strings.Repeat("80", 16), salt)
	for _, identity := range []struct{ poolID, username, password string }{
		{"us-east-1_PoolB", "canonical-user", "Password!42"},
		{"us-east-1_PoolA", "email@example.test", "Password!42"},
		{"us-east-1_PoolA", "canonical-user", "ChangedPassword!42"},
	} {
		otherSalt, otherVerifier := makeCredentials(identity.poolID, identity.username, identity.password)
		require.Equal(t, salt, otherSalt)
		require.NotEqual(t, verifier, otherVerifier)
	}
	// Cognito hashes the pool suffix; the region is outside the SRP identity.
	_, sameSuffixVerifier := makeCredentials("eu-west-1_PoolA", "canonical-user", "Password!42")
	require.Equal(t, verifier, sameSuffixVerifier)

	firstSalt, firstVerifier, err := makeSRPCredentials("us-east-1_PoolA", "canonical-user", "Password!42")
	require.NoError(t, err)
	secondSalt, secondVerifier, err := makeSRPCredentials("us-east-1_PoolA", "canonical-user", "Password!42")
	require.NoError(t, err)
	require.NotEqual(t, firstSalt, secondSalt)
	require.NotEqual(t, firstVerifier, secondVerifier)

	for _, identity := range []struct{ poolID, username, password string }{
		{"", "user", "password"}, {"_Pool", "user", "password"},
		{"us-east-1_", "user", "password"}, {"us-east-1_Pool_Extra", "user", "password"},
		{"us-east-1_Pool", "", "password"}, {"us-east-1_Pool", "user", ""},
	} {
		salt, verifier, err := makeSRPCredentialsWithEntropy(identity.poolID, identity.username, identity.password, bytes.NewReader(entropy))
		require.Error(t, err)
		require.Empty(t, salt)
		require.Empty(t, verifier)
	}
	salt, verifier, err = makeSRPCredentialsWithEntropy("us-east-1_Pool", "user", "password", bytes.NewReader(entropy[:15]))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Empty(t, salt)
	require.Empty(t, verifier)
}

func TestSRPChallengeRejectsInvalidInputsAndEntropyFailure(t *testing.T) {
	salt, verifier, err := makeSRPCredentialsWithEntropy("us-east-1_Pool", "canonical-user", "Password!42", bytes.NewReader(bytes.Repeat([]byte{1}, 16)))
	require.NoError(t, err)
	validA := new(big.Int).Exp(big.NewInt(2), big.NewInt(12345), srpN).Text(16)
	for _, publicA := range []string{
		"", "0", "00", "-1", "+1", "0x01", " 01", "01\n", "zz", "１",
		srpModulusHex, new(big.Int).Add(srpN, big.NewInt(1)).Text(16),
		new(big.Int).Mul(srpN, big.NewInt(2)).Text(16), strings.Repeat("f", 771),
	} {
		t.Run("SRP_A_"+publicA[:min(len(publicA), 20)], func(t *testing.T) {
			_, err := newSRPChallengeWithEntropy("us-east-1_Pool", "canonical-user", salt, verifier, publicA, bytes.NewReader(make([]byte, 160)), srpTestTime)
			require.Error(t, err)
		})
	}
	for _, invalidVerifier := range []string{"", "0", "-1", "xx", srpModulusHex, strings.Repeat("f", 771)} {
		_, err := newSRPChallengeWithEntropy("us-east-1_Pool", "canonical-user", salt, invalidVerifier, validA, bytes.NewReader(make([]byte, 160)), srpTestTime)
		require.Error(t, err)
	}
	for _, invalidSalt := range []string{"", "-1", "xx", strings.Repeat("f", 35)} {
		_, err := newSRPChallengeWithEntropy("us-east-1_Pool", "canonical-user", invalidSalt, verifier, validA, bytes.NewReader(make([]byte, 160)), srpTestTime)
		require.Error(t, err)
	}
	_, err = newSRPChallengeWithEntropy("us-east-1_Pool", "", salt, verifier, validA, bytes.NewReader(make([]byte, 160)), srpTestTime)
	require.Error(t, err)
	_, err = newSRPChallengeWithEntropy("us-east-1_Pool", "canonical-user", salt, verifier, validA, bytes.NewReader(make([]byte, 160)), time.Time{})
	require.Error(t, err)
	for _, length := range []int{0, 127, 128, 159} {
		state, err := newSRPChallengeWithEntropy("us-east-1_Pool", "canonical-user", salt, verifier, validA, bytes.NewReader(make([]byte, length)), srpTestTime)
		require.Error(t, err)
		require.Equal(t, SRPState{}, state)
	}
}

func TestSRPChallengeUsesFreshServerEntropy(t *testing.T) {
	salt, verifier, err := makeSRPCredentials("us-east-1_Pool", "canonical-user", "Password!42")
	require.NoError(t, err)
	a := big.NewInt(123456789)
	publicA := new(big.Int).Exp(big.NewInt(2), a, srpN).Text(16)
	first, err := newSRPChallenge("us-east-1_Pool", "canonical-user", salt, verifier, publicA)
	require.NoError(t, err)
	second, err := newSRPChallenge("us-east-1_Pool", "canonical-user", salt, verifier, publicA)
	require.NoError(t, err)
	require.NotEqual(t, first.SRPB, second.SRPB)
	require.NotEqual(t, first.SecretBlock, second.SecretBlock)
	require.NotEqual(t, first.DerivedKey, second.DerivedKey)
}

func TestSRPVerifyRejectsChangedClaims(t *testing.T) {
	state, a := srpTestState(t, "us-east-1_ExamplePool", "canonical-user-42", "TestPassword!42")
	timestamp := srpTestTime.Format(srpTimestampLayout)
	_, signature := srpIndependentClientProof(t, "ExamplePool", state.Username, "TestPassword!42", a, state, timestamp)
	changedBlock := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32))
	for _, test := range []struct {
		name                        string
		mutate                      func(*SRPState)
		signature, block, timestamp string
		now                         time.Time
	}{
		{name: "changed signature", signature: base64.StdEncoding.EncodeToString(make([]byte, 32))},
		{name: "missing signature", signature: "missing"},
		{name: "signature without padding", signature: strings.TrimRight(signature, "=")},
		{name: "oversized signature", signature: signature + signature},
		{name: "newline in signature", signature: signature[:20] + "\n" + signature[20:]},
		{name: "changed block", block: changedBlock},
		{name: "missing block", block: "missing"},
		{name: "block without padding", block: strings.TrimRight(state.SecretBlock, "=")},
		{name: "oversized block", block: state.SecretBlock + state.SecretBlock},
		{name: "changed timestamp", timestamp: srpTestTime.Add(time.Second).Format(srpTimestampLayout)},
		{name: "changed canonical username", mutate: func(state *SRPState) { state.Username = "email@example.test" }},
		{name: "changed pool", mutate: func(state *SRPState) { state.PoolSuffix = "OtherPool" }},
		{name: "changed bound block", mutate: func(state *SRPState) { state.SecretBlock = changedBlock }},
		{name: "missing key", mutate: func(state *SRPState) { state.DerivedKey = nil }},
		{name: "oversized key", mutate: func(state *SRPState) { state.DerivedKey = make([]byte, 17) }},
		{name: "missing issue time", mutate: func(state *SRPState) { state.IssuedAt = time.Time{} }},
		{name: "before issue time", now: srpTestTime.Add(-time.Nanosecond)},
		{name: "expiry boundary", now: srpTestTime.Add(5 * time.Minute)},
		{name: "after expiry", now: srpTestTime.Add(6 * time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := state
			if test.mutate != nil {
				test.mutate(&current)
			}
			proof, block, claimTime, now := test.signature, test.block, test.timestamp, test.now
			if proof == "" {
				proof = signature
			}
			if block == "" {
				block = state.SecretBlock
			}
			if claimTime == "" {
				claimTime = timestamp
			}
			if now.IsZero() {
				now = srpTestTime
			}
			require.ErrorIs(t, current.Verify(proof, block, claimTime, now), errSRPVerification)
		})
	}
	// Even a correct MAC over a different secret block cannot replace the block
	// bound to the challenge. This is distinct from merely rejecting a bad MAC.
	changedProof := srpIndependentMAC(state.DerivedKey, state.PoolSuffix, state.Username, changedBlock, timestamp)
	require.ErrorIs(t, state.Verify(changedProof, changedBlock, timestamp, srpTestTime), errSRPVerification)
}

func TestSRPVerifyChecksTimestampSyntaxAndWindow(t *testing.T) {
	state, _ := srpTestState(t, "us-east-1_ExamplePool", "canonical-user-42", "TestPassword!42")
	for _, timestamp := range []string{
		"", "2026-10-06T10:20:30Z", "Tue Oct 06 10:20:30 UTC 2026",
		"Mon Oct 6 10:20:30 UTC 2026", "Tue Oct 6 10:20:30 GMT 2026",
		"Tue Oct 6 10:20:30 UTC 2026\n", "Tue Oct 32 10:20:30 UTC 2026",
		srpTestTime.Add(-5*time.Minute - time.Second).Format(srpTimestampLayout),
		srpTestTime.Add(5*time.Minute + time.Second).Format(srpTimestampLayout),
	} {
		signature := srpIndependentMAC(state.DerivedKey, state.PoolSuffix, state.Username, state.SecretBlock, timestamp)
		require.ErrorIs(t, state.Verify(signature, state.SecretBlock, timestamp, srpTestTime), errSRPVerification, "timestamp=%q", timestamp)
	}
	for _, timestamp := range []string{
		srpTestTime.Format(srpTimestampLayout),
		srpTestTime.Add(-5 * time.Minute).Format(srpTimestampLayout),
		srpTestTime.Add(5 * time.Minute).Format(srpTimestampLayout),
	} {
		signature := srpIndependentMAC(state.DerivedKey, state.PoolSuffix, state.Username, state.SecretBlock, timestamp)
		require.NoError(t, state.Verify(signature, state.SecretBlock, timestamp, srpTestTime))
	}
	// A still-live session may be checked just before its precise expiry.
	timestamp := srpTestTime.Format(srpTimestampLayout)
	signature := srpIndependentMAC(state.DerivedKey, state.PoolSuffix, state.Username, state.SecretBlock, timestamp)
	require.NoError(t, state.Verify(signature, state.SecretBlock, timestamp, srpTestTime.Add(5*time.Minute-time.Nanosecond)))
}

func srpTestState(t *testing.T, poolID, username, password string) (SRPState, *big.Int) {
	t.Helper()
	saltEntropy := make([]byte, 16)
	for i := range saltEntropy {
		saltEntropy[i] = byte(0x80 + i)
	}
	salt, verifier, err := makeSRPCredentialsWithEntropy(poolID, username, password, bytes.NewReader(saltEntropy))
	require.NoError(t, err)
	a, ok := new(big.Int).SetString("123456789abcdef123456789abcdef123456789abcdef", 16)
	require.True(t, ok)
	A := new(big.Int).Exp(big.NewInt(2), a, srpN)
	serverEntropy := make([]byte, 160)
	for i := 0; i < 128; i++ {
		serverEntropy[i] = byte(i + 1)
	}
	for i := 0; i < 32; i++ {
		serverEntropy[128+i] = byte(i)
	}
	state, err := newSRPChallengeWithEntropy(poolID, username, salt, verifier, A.Text(16), bytes.NewReader(serverEntropy), srpTestTime)
	require.NoError(t, err)
	return state, a
}

// srpIndependentClientProof implements the other side of the exchange from
// public parameters. It never calls the server's padding/hash/HKDF helpers.
// A fixed Node-crypto vector below also pins the group and shared derivation.
func srpIndependentClientProof(t *testing.T, poolSuffix, username, password string, a *big.Int, state SRPState, timestamp string) ([]byte, string) {
	t.Helper()
	N, ok := new(big.Int).SetString(srpModulusHex, 16)
	require.True(t, ok)
	g := big.NewInt(2)
	B, ok := new(big.Int).SetString(state.SRPB, 16)
	require.True(t, ok)
	salt, ok := new(big.Int).SetString(state.Salt, 16)
	require.True(t, ok)
	A := new(big.Int).Exp(g, a, N)
	hashInt := func(parts ...[]byte) *big.Int {
		hash := sha256.New()
		for _, part := range parts {
			_, _ = hash.Write(part)
		}
		return new(big.Int).SetBytes(hash.Sum(nil))
	}
	pad := func(value *big.Int) []byte {
		text := value.Text(16)
		if len(text)%2 != 0 {
			text = "0" + text
		}
		if strings.ContainsRune("89abcdef", rune(text[0])) {
			text = "00" + text
		}
		valueBytes, err := hex.DecodeString(text)
		require.NoError(t, err)
		return valueBytes
	}
	k := hashInt(pad(N), pad(g))
	u := hashInt(pad(A), pad(B))
	identity := sha256.Sum256([]byte(poolSuffix + username + ":" + password))
	x := hashInt(pad(salt), identity[:])
	gx := new(big.Int).Exp(g, x, N)
	base := new(big.Int).Sub(B, new(big.Int).Mul(k, gx))
	base.Mod(base, N)
	exponent := new(big.Int).Add(a, new(big.Int).Mul(u, x))
	shared := new(big.Int).Exp(base, exponent, N)
	extract := hmac.New(sha256.New, pad(u))
	_, _ = extract.Write(pad(shared))
	expand := hmac.New(sha256.New, extract.Sum(nil))
	_, _ = expand.Write(append([]byte("Caldera Derived Key"), 1))
	key := expand.Sum(nil)[:16]
	return key, srpIndependentMAC(key, poolSuffix, username, state.SecretBlock, timestamp)
}

func srpIndependentMAC(key []byte, poolSuffix, username, blockBase64, timestamp string) string {
	block, err := base64.StdEncoding.DecodeString(blockBase64)
	if err != nil {
		panic(errors.New("invalid test block"))
	}
	message := append([]byte(poolSuffix+username), block...)
	message = append(message, []byte(timestamp)...)
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(message)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func TestSRPKnownIndependentNodeVector(t *testing.T) {
	// Generated by the independently implemented Node-crypto SRP client in
	// tests/sdk/javascript/srp.mjs. Inputs and the full fixture are recorded in
	// tests/sdk/javascript/srp-vector.json; these fixed outputs are deliberately
	// not loaded from that file, so changes in either implementation are visible.
	state, a := srpTestState(t, "us-east-1_ExamplePool", "canonical-user-42", "TestPassword!42")
	saltEntropy := make([]byte, 16)
	for i := range saltEntropy {
		saltEntropy[i] = byte(0x80 + i)
	}
	salt, verifier, err := makeSRPCredentialsWithEntropy("us-east-1_ExamplePool", "canonical-user-42", "TestPassword!42", bytes.NewReader(saltEntropy))
	require.NoError(t, err)
	require.Equal(t, "808182838485868788898a8b8c8d8e8f", salt)
	require.Equal(t, "538282c4354742d7cbbde2359fcf67f9f5b3a6b08791e5011b43b8a5b66d9ee6", srpK.Text(16))
	hexHash := func(text string) string {
		sum := sha256.Sum256([]byte(text))
		return hex.EncodeToString(sum[:])
	}
	A := new(big.Int).Exp(big.NewInt(2), a, srpN)
	require.Equal(t, "d4206c4e86e6e471e57a5a6e45a9429ba7321842a3af5c2326e8c2dd956b3211", hexHash(A.Text(16)))
	require.Equal(t, "9c9227c68e301fcd154637da7f140fbbbe2544b0e0c9f2bd4c348dd0b3d2cdb3", hexHash(verifier))
	require.Equal(t, "107bd6d76abfa4441195c2755bbcd21be6cc4cb6a1b0ba97c91da26a8094b3e9", hexHash(state.SRPB))
	require.Equal(t, "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=", state.SecretBlock)
	require.Equal(t, "6fc2ae9dc9e67599702398b330838bf2", hex.EncodeToString(state.DerivedKey))
	key, signature := srpIndependentClientProof(t, "ExamplePool", state.Username, "TestPassword!42", a, state, "Tue Oct 6 10:20:30 UTC 2026")
	require.Equal(t, "6fc2ae9dc9e67599702398b330838bf2", hex.EncodeToString(key))
	require.Equal(t, "0BQz1o8DIhnnWOjTlnWpNlwD/hPvOQmUW2et+lAF86E=", signature)
	require.NoError(t, state.Verify(signature, state.SecretBlock, "Tue Oct 6 10:20:30 UTC 2026", srpTestTime))
}
