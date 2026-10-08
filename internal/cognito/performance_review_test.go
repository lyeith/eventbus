//go:build performance && (linux || darwin)

package cognito

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/lyeith/eventbus/internal/testperf"
)

const cognitoPerformanceStartupSamples = 5
const cognitoPerformanceWarmSamples = 20
const cognitoPerformancePool = "us-east-1_performance"
const cognitoPerformanceClient = "performance-client"
const cognitoPerformancePassword = "PerformancePass1!"

const cognitoPerformanceTinySeed = `pools:
  - id: us-east-1_performance
    region: us-east-1
    clients:
      - id: performance-client
    users:
      - username: performance-user
        email: performance@example.test
        password: PerformancePass1!
        attributes:
          name: Performance User
`

// Every measurement owns a private directory and DB. No default DB, service,
// listener, external seed, password-cost reduction or simulated key is used.
func cognitoPerformancePrivateDB(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
}

func cognitoPerformanceOpen(t *testing.T, path string) *CognitoStore {
	t.Helper()
	store, err := OpenCognitoStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func cognitoPerformanceElapsed(t *testing.T, operation func() error) float64 {
	t.Helper()
	started := time.Now()
	err := operation()
	elapsed := float64(time.Since(started)) / float64(time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	return elapsed
}

func TestPerformanceReviewCognitoStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	phases := []string{"bootstrap_empty", "reopen_empty", "load_tiny_yaml", "apply_tiny_seed_first_with_crypto", "reopen_seeded", "reapply_tiny_seed_unchanged"}
	samples := make(map[string][]float64)
	for range cognitoPerformanceStartupSamples {
		directory := t.TempDir()
		path := filepath.Join(directory, "private.db")
		seedPath := filepath.Join(directory, "tiny-seed.yaml")
		cognitoPerformancePrivateDB(t, path)
		if err := os.WriteFile(seedPath, []byte(cognitoPerformanceTinySeed), 0600); err != nil {
			t.Fatal(err)
		}
		var store *CognitoStore
		open := func() error {
			var err error
			store, err = OpenCognitoStore(path)
			return err
		}
		samples[phases[0]] = append(samples[phases[0]], cognitoPerformanceElapsed(t, open))
		// Register the concrete handle before any assertion can fail. Explicit
		// closes below isolate reopen timing; SQL Close is idempotent.
		firstStore := store
		t.Cleanup(func() {
			if err := firstStore.Close(); err != nil {
				t.Error(err)
			}
		})
		for _, table := range []string{"pools", "users", "signing_keys"} {
			var count int
			if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 0 {
				t.Fatalf("unseeded bootstrap provisioned %s: %d %v", table, count, err)
			}
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		samples[phases[1]] = append(samples[phases[1]], cognitoPerformanceElapsed(t, open))
		secondStore := store
		t.Cleanup(func() {
			if err := secondStore.Close(); err != nil {
				t.Error(err)
			}
		})
		var seed *CognitoSeedFile
		samples[phases[2]] = append(samples[phases[2]], cognitoPerformanceElapsed(t, func() error {
			var err error
			seed, err = LoadCognitoSeed(seedPath)
			return err
		}))
		samples[phases[3]] = append(samples[phases[3]], cognitoPerformanceElapsed(t, func() error { return ApplyCognitoSeed(ctx, store, seed) }))
		user, err := store.LookupPoolUser(ctx, cognitoPerformancePool, "performance-user")
		if err != nil {
			t.Fatal(err)
		}
		key, err := store.LoadSigningKey(ctx, cognitoPerformancePool)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		samples[phases[4]] = append(samples[phases[4]], cognitoPerformanceElapsed(t, open))
		thirdStore := store
		t.Cleanup(func() {
			if err := thirdStore.Close(); err != nil {
				t.Error(err)
			}
		})
		samples[phases[5]] = append(samples[phases[5]], cognitoPerformanceElapsed(t, func() error { return ApplyCognitoSeed(ctx, store, seed) }))
		reopenedUser, err := store.LookupPoolUser(ctx, cognitoPerformancePool, "performance-user")
		if err != nil {
			t.Fatal(err)
		}
		reopenedKey, err := store.LoadSigningKey(ctx, cognitoPerformancePool)
		if err != nil {
			t.Fatal(err)
		}
		if reopenedUser.Sub != user.Sub || reopenedUser.PasswordHash != user.PasswordHash || reopenedUser.CreatedAt != user.CreatedAt || reopenedUser.UpdatedAt != user.UpdatedAt || reopenedUser.AuthVersion != user.AuthVersion || reopenedKey.Kid != key.Kid || reopenedKey.Private.N.Cmp(key.Private.N) != 0 {
			t.Fatal("reopen/reapply changed durable identity, credentials or signing key")
		}
		for _, table := range []string{"pools", "clients", "users", "signing_keys"} {
			var count int
			if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count); err != nil || count != 1 {
				t.Fatalf("tiny seed cardinality %s: %d %v", table, count, err)
			}
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range phases {
		testperf.Report(t, "cognito/startup/"+phase, "wall_ms", samples[phase])
	}
}

func cognitoPerformanceNativeFixture(t *testing.T) (*CognitoStore, *CognitoUser) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "private.db")
	cognitoPerformancePrivateDB(t, path)
	store := cognitoPerformanceOpen(t, path)
	schema, err := NormalizeSchema(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SavePool(t.Context(), &CognitoPool{ID: cognitoPerformancePool, Name: "Performance pool", Region: "us-east-1", AccountID: "000000000000", Native: true, SignIn: PoolSignInConfig{CaseSensitive: true}, SchemaAttributes: schema}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveClient(t.Context(), &CognitoClient{ID: cognitoPerformanceClient, PoolID: cognitoPerformancePool, Name: "Performance client", Native: true, AuthSessionValidity: 3}); err != nil {
		t.Fatal(err)
	}
	user, err := store.CreateUserIdentity(t.Context(), cognitoPerformancePool, "performance-user", "performance@example.test", cognitoPerformancePassword, "CONFIRMED", map[string]string{"email": "performance@example.test", "email_verified": "true", "name": "Performance User"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureSigningKey(t.Context(), cognitoPerformancePool); err != nil {
		t.Fatal(err)
	}
	return store, user
}

func TestPerformanceReviewCognitoKeysAndJWT(t *testing.T) {
	store, user := cognitoPerformanceNativeFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	key, err := store.LoadSigningKey(ctx, cognitoPerformancePool)
	if err != nil {
		t.Fatal(err)
	}
	privatePEM, err := encodePrivateKeyPEM(key.Private)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM, err := encodePublicKeyPEM(key.Public)
	if err != nil {
		t.Fatal(err)
	}
	grant := tokenGrant{AuthTime: store.now(), OriginJTI: newJTI(), AuthVersion: user.AuthVersion}
	const issuer = "http://performance.invalid"
	access, err := SignAccessToken(ctx, store, issuer, user.PoolID, cognitoPerformanceClient, user.Sub, user.Email, grant, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(store, Options{IssuerBase: issuer})
	operations := []struct {
		name  string
		count int
		run   func() error
	}{
		{"rsa2048_generate_only", cognitoPerformanceStartupSamples, func() error { _, err := rsa.GenerateKey(rand.Reader, rsaKeySize); return err }},
		{"bcrypt_hash_only", cognitoPerformanceStartupSamples, func() error { _, err := hashUserPassword(cognitoPerformancePassword); return err }},
		{"bcrypt_compare_only", cognitoPerformanceStartupSamples, func() error { return compareUserPasswordHash(user.PasswordHash, cognitoPerformancePassword) }},
		{"srp_verifier_only", cognitoPerformanceStartupSamples, func() error {
			_, _, err := makeSRPCredentials(user.PoolID, user.Username, cognitoPerformancePassword)
			return err
		}},
		{"parse_private_pem_only", cognitoPerformanceWarmSamples, func() error { _, err := decodePrivateKeyPEM(privatePEM); return err }},
		{"parse_public_pem_only", cognitoPerformanceWarmSamples, func() error { _, err := decodePublicKeyPEM(publicPEM); return err }},
		{"db_select_key_pem_only", cognitoPerformanceWarmSamples, func() error {
			var kid, priv, pub string
			var created int64
			return store.db.QueryRowContext(ctx, `SELECT kid,private_pem,public_pem,created_at FROM signing_keys WHERE pool_id=?`, user.PoolID).Scan(&kid, &priv, &pub, &created)
		}},
		{"load_persisted_key", cognitoPerformanceWarmSamples, func() error { _, err := store.LoadSigningKey(ctx, user.PoolID); return err }},
		{"ensure_existing_key", cognitoPerformanceWarmSamples, func() error { _, err := store.EnsureSigningKey(ctx, user.PoolID); return err }},
		// Comparisons reuse this test's immutable real key only. Production still
		// loads persisted keys; these rows isolate encoding and signing CPU cost.
		{"jwks_encode_only", cognitoPerformanceWarmSamples, func() error {
			_, err := json.Marshal(JWKS{Keys: []JWK{PublicKeyToJWK(key.Public, key.Kid)}})
			return err
		}},
		{"rsa_sign_cached_test_key", cognitoPerformanceWarmSamples, func() error {
			token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"sub": user.Sub, "iat": grant.AuthTime.Unix(), "exp": grant.AuthTime.Add(time.Hour).Unix()})
			token.Header["kid"] = key.Kid
			_, err := token.SignedString(key.Private)
			return err
		}},
		{"build_jwks", cognitoPerformanceWarmSamples, func() error {
			body, err := store.BuildJWKS(ctx, user.PoolID)
			if err == nil && len(body) == 0 {
				return fmt.Errorf("empty JWKS")
			}
			return err
		}},
		{"serve_jwks", cognitoPerformanceWarmSamples, func() error {
			response := httptest.NewRecorder()
			handler.ServeJWKS(response, httptest.NewRequest(http.MethodGet, "/"+user.PoolID+"/.well-known/jwks.json", nil).WithContext(ctx))
			if response.Code != http.StatusOK {
				return fmt.Errorf("JWKS status %d", response.Code)
			}
			return nil
		}},
		{"sign_native_access_token", cognitoPerformanceWarmSamples, func() error {
			_, err := SignAccessToken(ctx, store, issuer, user.PoolID, cognitoPerformanceClient, user.Sub, user.Email, grant, time.Hour)
			return err
		}},
		{"sign_native_id_token", cognitoPerformanceWarmSamples, func() error {
			_, err := SignIDToken(ctx, store, issuer, user.PoolID, cognitoPerformanceClient, user, grant, time.Hour)
			return err
		}},
		{"verify_native_access_token", cognitoPerformanceWarmSamples, func() error { _, err := VerifyAccessToken(ctx, store, issuer, access); return err }},
	}
	for _, operation := range operations {
		samples := make([]float64, 0, operation.count)
		for range operation.count {
			samples = append(samples, cognitoPerformanceElapsed(t, operation.run))
		}
		testperf.Report(t, "cognito/"+operation.name, "wall_ms", samples)
	}
	var cold []float64
	for index := range cognitoPerformanceStartupSamples {
		pool := fmt.Sprintf("us-east-1_cold%d", index)
		if err := store.UpsertPool(ctx, pool, "us-east-1"); err != nil {
			t.Fatal(err)
		}
		cold = append(cold, cognitoPerformanceElapsed(t, func() error { _, err := store.EnsureSigningKey(ctx, pool); return err }))
	}
	testperf.Report(t, "cognito/ensure_new_persisted_key", "wall_ms", cold)
	body, err := store.BuildJWKS(ctx, user.PoolID)
	if err != nil {
		t.Fatal(err)
	}
	var document JWKS
	if err := json.Unmarshal(body, &document); err != nil || len(document.Keys) != 1 || document.Keys[0].Kid != key.Kid {
		t.Fatalf("stable persisted JWKS: %s %v", body, err)
	}
	if claims, err := VerifyAccessToken(ctx, store, issuer, access); err != nil || claims["sub"] != user.Sub {
		t.Fatalf("real native access token validation: %v %v", claims, err)
	}
}

func TestPerformanceReviewCognitoConcurrentStore(t *testing.T) {
	store, user := cognitoPerformanceNativeFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	grant := tokenGrant{AuthTime: store.now(), OriginJTI: newJTI(), AuthVersion: user.AuthVersion}
	operations := []struct {
		name string
		run  func() error
	}{
		{"lookup_user", func() error { _, err := store.LookupUserBySub(ctx, user.Sub); return err }},
		{"load_key", func() error { _, err := store.LoadSigningKey(ctx, user.PoolID); return err }},
		{"build_jwks", func() error { _, err := store.BuildJWKS(ctx, user.PoolID); return err }},
		{"sign_native_access", func() error {
			_, err := SignAccessToken(ctx, store, "http://performance.invalid", user.PoolID, cognitoPerformanceClient, user.Sub, user.Email, grant, time.Hour)
			return err
		}},
	}
	const count = 40 // Fixed total work makes worker1/worker4 comparable.
	for _, operation := range operations {
		for _, workers := range []int{1, 4} {
			before := store.db.Stats()
			samples := make([]float64, count)
			errors := make([]error, count)
			start := make(chan struct{})
			var joined sync.WaitGroup
			for worker := range workers {
				joined.Add(1)
				go func() {
					defer joined.Done()
					<-start
					for index := worker; index < count; index += workers {
						started := time.Now()
						errors[index] = operation.run()
						samples[index] = float64(time.Since(started)) / float64(time.Millisecond)
					}
				}()
			}
			started := time.Now()
			close(start)
			joined.Wait()
			wallMS := float64(time.Since(started)) / float64(time.Millisecond)
			after := store.db.Stats()
			for index, err := range errors {
				if err != nil {
					t.Fatalf("joined operation %s/%d sample%d: %v", operation.name, workers, index, err)
				}
			}
			name := fmt.Sprintf("cognito/concurrent/%s/workers%d", operation.name, workers)
			testperf.Report(t, name, "operation_wall_ms", samples)
			testperf.Report(t, name, "joined_total_wall_ms", []float64{wallMS})
			t.Logf("PERFORMANCE_COGNITO_DB case=%s operations=%d max_open=%d waits=%d wait_ms=%.6f", name, count, after.MaxOpenConnections, after.WaitCount-before.WaitCount, float64(after.WaitDuration-before.WaitDuration)/float64(time.Millisecond))
		}
	}
}
