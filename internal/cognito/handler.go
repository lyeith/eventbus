// Package cognito owns the local Cognito IDP emulator, SQLite fixtures and signed JWTs.
package cognito

import (
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultAccessTokenTTL  = time.Hour
	defaultRefreshTokenTTL = 24 * time.Hour
)

// Options configures token issuance. Non-positive TTLs use the service defaults.
// Issuers are the trimmed base URL followed by the pool ID.
type Options struct {
	IssuerBase      string
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}

// Handler serves Cognito operations using an externally owned SQLite store.
// The application drains HTTP and joins cleanup workers before closing the store.
type Handler struct {
	cognito         *CognitoStore
	issuerBase      string
	accessTokenTTL  time.Duration
	refreshTokenTTL time.Duration
}

// NewHandler wires the store and immutable token configuration. It does not
// open or close the store, bind a listener, or start background workers.
func NewHandler(store *CognitoStore, options Options) *Handler {
	if options.AccessTokenTTL <= 0 {
		options.AccessTokenTTL = defaultAccessTokenTTL
	}
	if options.RefreshTokenTTL <= 0 {
		options.RefreshTokenTTL = defaultRefreshTokenTTL
	}
	return &Handler{
		cognito:         store,
		issuerBase:      strings.TrimRight(options.IssuerBase, "/"),
		accessTokenTTL:  options.AccessTokenTTL,
		refreshTokenTTL: options.RefreshTokenTTL,
	}
}

// ServeJWKS serves /<pool-id>/.well-known/jwks.json. It publishes a persisted
// signing key on demand, including for a pool without preloaded fixtures.
func (s *Handler) ServeJWKS(w http.ResponseWriter, r *http.Request) {
	if s.cognito == nil {
		http.Error(w, "cognito store not configured", http.StatusServiceUnavailable)
		return
	}
	const suffix = "/.well-known/jwks.json"
	path := strings.TrimPrefix(r.URL.Path, "/")
	if !strings.HasSuffix(path, suffix) {
		http.NotFound(w, r)
		return
	}
	poolID := strings.TrimSuffix(path, suffix)
	if poolID == "" || strings.Contains(poolID, "/") {
		http.Error(w, "pool id required in path", http.StatusBadRequest)
		return
	}
	body, err := s.cognito.BuildJWKS(r.Context(), poolID)
	if err != nil {
		log.Error().Err(err).Str("pool", poolID).Msg("Failed to build JWKS")
		http.Error(w, "failed to build JWKS", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
