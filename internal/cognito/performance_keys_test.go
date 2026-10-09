//go:build performance && (linux || darwin)

package cognito

import (
	"fmt"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/testperf"
)

func TestPerformanceSigningKeyReuse(t *testing.T) {
	store, user := cognitoPerformanceNativeFixture(t)
	grant := tokenGrant{AuthTime: store.now(), OriginJTI: newJTI(), AuthVersion: user.AuthVersion}
	const issuer = "http://performance.invalid"
	token, err := SignAccessToken(t.Context(), store, issuer, user.PoolID, cognitoPerformanceClient, user.Sub, user.Email, grant, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		run  func() error
	}{
		{"load_persisted_key", func() error { _, err := store.LoadSigningKey(t.Context(), user.PoolID); return err }},
		{"ensure_existing_key", func() error { _, err := store.EnsureSigningKey(t.Context(), user.PoolID); return err }},
		{"borrow_cached_key", func() error { _, err := store.loadSigningKey(t.Context(), user.PoolID); return err }},
		{"ensure_cached_key", func() error { _, err := store.ensureSigningKey(t.Context(), user.PoolID); return err }},
		{"build_jwks", func() error {
			encoded, err := store.BuildJWKS(t.Context(), user.PoolID)
			if err == nil && len(encoded) == 0 {
				return fmt.Errorf("empty JWKS")
			}
			return err
		}},
		{"verify_native_access_token", func() error { _, err := VerifyAccessToken(t.Context(), store, issuer, token); return err }},
		{"sign_native_access_token", func() error {
			_, err := SignAccessToken(t.Context(), store, issuer, user.PoolID, cognitoPerformanceClient, user.Sub, user.Email, grant, time.Hour)
			return err
		}},
	}
	for _, operation := range operations {
		samples := make([]float64, cognitoPerformanceWarmSamples)
		for index := range samples {
			samples[index] = cognitoPerformanceElapsed(t, operation.run)
		}
		testperf.Report(t, "cognito/"+operation.name, "wall_ms", samples)
	}
	claims, err := VerifyAccessToken(t.Context(), store, issuer, token)
	if err != nil || claims["sub"] != user.Sub {
		t.Fatalf("real native token verification: %v %v", claims, err)
	}
}
