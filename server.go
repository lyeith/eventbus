package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// Default token TTLs (overridable via SetCognito). The 24h refresh default
// matches MockProvider (mock.py:285) for local-dev convenience; the 1h
// access default matches what AWS Cognito itself emits by default.
const (
	defaultAccessTokenTTL  = time.Hour
	defaultRefreshTokenTTL = 24 * time.Hour
)

type Server struct {
	broker          *Broker
	firehose        *FirehoseManager
	ssm             *SSMStore
	secrets         *SecretsStore
	ses             *SESManager
	cognito         *CognitoStore // optional; nil disables the JWKS route + Cognito dispatch
	issuerBase      string        // e.g. "http://localhost:4100" — published in /health
	jwksBase        string        // defaults to issuerBase
	accessTokenTTL  time.Duration // defaults to defaultAccessTokenTTL
	refreshTokenTTL time.Duration // defaults to defaultRefreshTokenTTL
	mux             *http.ServeMux
}

func NewServer(broker *Broker, firehose *FirehoseManager, ssm *SSMStore, secrets *SecretsStore) *Server {
	s := &Server{broker: broker, firehose: firehose, ssm: ssm, secrets: secrets}
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/", s.handleAWSAction)
	s.mux.HandleFunc("/health", s.handleHealth)
	// SQS queue URLs use /queue/{name} path but the Action is still in the form body
	s.mux.HandleFunc("/queue/", s.handleAWSAction)
	return s
}

// SetCognito wires the Cognito store and issuer/JWKS base URLs. If cognito is
// nil the dispatch and JWKS endpoint are disabled (Cognito requests will fall
// through to the default branch and return InvalidAction).
//
// Pass zero (or negative) durations for accessTokenTTL / refreshTokenTTL to
// fall back to the package defaults. Both TTLs are honored by the
// InitiateAuth handler when minting tokens; per design §3f the refresh token
// is NOT rotated on REFRESH_TOKEN_AUTH, so the refresh TTL is only consulted
// at first issuance.
func (s *Server) SetCognito(store *CognitoStore, issuerBase, jwksBase string, accessTokenTTL, refreshTokenTTL time.Duration) {
	s.cognito = store
	s.issuerBase = strings.TrimRight(issuerBase, "/")
	if jwksBase == "" {
		jwksBase = issuerBase
	}
	s.jwksBase = strings.TrimRight(jwksBase, "/")
	if accessTokenTTL <= 0 {
		accessTokenTTL = defaultAccessTokenTTL
	}
	if refreshTokenTTL <= 0 {
		refreshTokenTTL = defaultRefreshTokenTTL
	}
	s.accessTokenTTL = accessTokenTTL
	s.refreshTokenTTL = refreshTokenTTL
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.ses != nil && strings.HasPrefix(r.URL.Path, "/v2/email/") {
		action, matched := sesV2Action(r)
		limit := sesWireLimit
		if action == "SendBulkEmail" {
			limit = sesBulkWireLimit
		}
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		var apiErr *sesAPIError
		if !matched {
			apiErr = &sesAPIError{Code: "NotFoundException", Message: "SES operation is not implemented", Status: http.StatusNotFound}
		}
		s.handleSES(w, r, "v2", action, apiErr)
		return
	}
	// Per-pool JWKS route lives outside the AWS-action dispatcher.
	// Match on path suffix so any /<pool>/.well-known/jwks.json shape works
	// regardless of how the operator nests the issuer base. We do NOT force
	// a registered pool — the JWKS publisher creates one on demand to keep
	// the local dev path zero-config.
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/.well-known/jwks.json") {
		s.handleJWKS(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	body := map[string]interface{}{
		"status":  "healthy",
		"service": "eventbus",
	}
	if s.cognito != nil {
		// Echo the operator-configured base URLs so consuming services know
		// which COGNITO_ISSUER / COGNITO_JWKS_URL to set. The per-pool URL is
		// just `<base>/<pool>/...`; clients append the pool id themselves.
		body["cognito_issuer"] = s.issuerBase
		body["cognito_jwks_url"] = s.jwksBase
	}
	_ = json.NewEncoder(w).Encode(body)
}

// handleJWKS serves GET /<pool-id>/.well-known/jwks.json. Pool id is whatever
// path component sits between the leading slash and `/.well-known/`.
func (s *Server) handleJWKS(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) handleAWSAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		xmlError(w, http.StatusMethodNotAllowed, "InvalidMethod", "Only POST is supported")
		return
	}

	// JSON protocol dispatch: SQS, Firehose, SSM, Secrets Manager, Cognito IDP
	// all use the X-Amz-Target header. Branch on the target prefix.
	if target := r.Header.Get("X-Amz-Target"); target != "" {
		if isFirehoseTarget(target) {
			s.handleFirehoseJSON(w, r, extractFirehoseAction(target))
			return
		}
		if isSSMTarget(target) {
			s.handleSSMJSON(w, r, extractSSMAction(target))
			return
		}
		if isSecretsManagerTarget(target) {
			s.handleSecretsJSON(w, r, extractSecretsAction(target))
			return
		}
		if isCognitoTarget(target) {
			s.handleCognitoJSON(w, r, extractCognitoAction(target))
			return
		}
		// Default: SQS
		s.handleSQSJSON(w, r, extractSQSJSONAction(target))
		return
	}

	// An explicit wire limit replaces ParseForm's 10 MB default. SES raw
	// payloads grow under transport base64/form encoding; message quotas are
	// enforced separately by SES against the decoded MIME content.
	r.Body = http.MaxBytesReader(w, r.Body, sesWireLimit)
	parseErr := r.ParseForm()
	action := r.FormValue("Action")
	if s.ses != nil && sesQueryRequest(r, action) {
		var apiErr *sesAPIError
		if parseErr != nil {
			apiErr = sesInvalid("v1", "Malformed Query request: "+parseErr.Error())
		}
		s.handleSES(w, r, "v1", action, apiErr)
		return
	}
	if parseErr != nil {
		xmlError(w, http.StatusBadRequest, "InvalidParameter", "Malformed Query request")
		return
	}

	log.Debug().Str("action", action).Str("path", r.URL.Path).Msg("AWS API request")

	switch action {
	// SNS
	case "CreateTopic":
		s.handleCreateTopic(w, r)
	case "Subscribe":
		s.handleSubscribe(w, r)
	case "Publish":
		s.handlePublish(w, r)
	case "ListTopics":
		s.handleListTopics(w, r)
	case "ListSubscriptionsByTopic":
		s.handleListSubscriptionsByTopic(w, r)
	case "DeleteTopic":
		s.handleDeleteTopic(w, r)

	// SQS
	case "CreateQueue":
		s.handleCreateQueue(w, r)
	case "GetQueueAttributes":
		s.handleGetQueueAttributes(w, r)
	case "GetQueueUrl":
		s.handleGetQueueUrl(w, r)
	case "ReceiveMessage":
		s.handleReceiveMessage(w, r)
	case "DeleteMessage":
		s.handleDeleteMessage(w, r)
	case "PurgeQueue":
		s.handlePurgeQueue(w, r)
	case "DeleteQueue":
		s.handleDeleteQueue(w, r)
	case "ListQueues":
		s.handleListQueues(w, r)

	default:
		msg := fmt.Sprintf("Unknown action: %s", action)
		log.Warn().Str("action", action).Msg("Unknown AWS API action")
		xmlError(w, http.StatusBadRequest, "InvalidAction", msg)
	}
}
