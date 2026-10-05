package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	if err := run(); err != nil {
		log.Error().Err(err).Msg("EventBus failed")
		os.Exit(1)
	}
}

func run() (resultErr error) {
	port := flag.Int("port", 4100, "Port to listen on")
	region := flag.String("region", "us-east-1", "AWS region")
	accountID := flag.String("account-id", "000000000000", "AWS account ID")
	s3Endpoint := flag.String("s3-endpoint", "http://localhost:9000", "S3 endpoint for Firehose flush (RustFS)")
	consumersFile := flag.String("consumers", "", "Path to consumers.yaml (enables Lambda pollers)")
	workDir := flag.String("work-dir", "", "Project root for uv run (auto-detected if empty)")
	// Cognito dev service flags. The `iss` claim emitted in tokens is
	// `<issuer-base>/<pool-id>`; JWKS is served at
	// `<jwks-base>/<pool-id>/.well-known/jwks.json`. Both default to the
	// same value so plain local works without flags. See
	// docs/workplans/COGNITO-DEV-SERVICE-DESIGN.md §3a-bis.
	issuerBase := flag.String("issuer-base", "http://localhost:4100", "Base URL for Cognito 'iss' claim and JWKS path")
	jwksBase := flag.String("jwks-base", "", "Base URL for JWKS endpoints (defaults to --issuer-base)")
	cognitoPools := flag.String("cognito-pools", "", "Path to cognito_pools.yaml (optional seed file)")
	cognitoDB := flag.String("cognito-db", "/tmp/cognito-dev.db", "SQLite path for the local Cognito dev store")
	// Token TTLs — design §3f. Access defaults to 1h (real Cognito default).
	// Refresh defaults to 24h (matches MockProvider mock.py:285); refresh
	// tokens are NOT rotated on REFRESH_TOKEN_AUTH per §3f.
	accessTokenTTL := flag.Duration("access-token-ttl", time.Hour, "TTL for issued access tokens (e.g. 1h, 30m)")
	refreshTokenTTL := flag.Duration("refresh-token-ttl", 24*time.Hour, "TTL for issued refresh tokens (e.g. 24h, 7d)")
	sesLog := flag.String("ses-log", "-", "SES JSON Lines capture path ('-' for stdout; no email delivery)")
	sesConfig := flag.String("ses-config", "", "Optional SES sending fixtures (templates, identities, configuration sets, received messages)")
	debug := flag.Bool("debug", false, "Enable debug logging")
	flag.Parse()

	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"})

	broker := NewBroker(*region, *accountID, *port)
	firehose := NewFirehoseManager(*region, *accountID, *s3Endpoint, "test", "testtest123")
	ssm := NewSSMStore()
	secrets := NewSecretsStore(*region, *accountID)
	server := NewServer(broker, firehose, ssm, secrets)

	// Cognito IDP-compatible local dev service. Open the SQLite store, wire
	// the issuer/JWKS bases into the server, and (optionally) apply a YAML
	// seed file. The store is always opened — even without a seed file —
	// because the dispatch surface depends on it (skeleton handlers and the
	// JWKS endpoint both need the DB handle).
	cognitoStore, err := OpenCognitoStore(*cognitoDB)
	if err != nil {
		return fmt.Errorf("failed to open cognito store: %w", err)
	}
	owned := &eventBusLifecycle{store: cognitoStore, firehose: firehose}
	listenerOwns := false
	defer func() {
		if !listenerOwns {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, owned.Close(ctx))
		}
	}()
	var sesFixtures SESFixtures
	if *sesConfig != "" {
		sesFixtures, err = LoadSESFixtures(*sesConfig)
		if err != nil {
			return fmt.Errorf("failed to load SES fixtures: %w", err)
		}
	}
	capture, err := OpenSESCapture(*sesLog)
	if err != nil {
		return fmt.Errorf("failed to open SES capture: %w", err)
	}
	owned.ses = NewSESManager(sesFixtures, capture)
	server.SetSES(owned.ses)
	server.SetCognito(cognitoStore, *issuerBase, *jwksBase, *accessTokenTTL, *refreshTokenTTL)
	if *cognitoPools != "" {
		seed, err := LoadCognitoSeed(*cognitoPools)
		if err != nil {
			return fmt.Errorf("failed to load cognito seed: %w", err)
		}
		if err := ApplyCognitoSeed(context.Background(), cognitoStore, seed); err != nil {
			return fmt.Errorf("failed to apply cognito seed: %w", err)
		}
		log.Info().Int("pools", len(seed.Pools)).Str("file", *cognitoPools).Msg("Cognito seed applied")
	}

	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	owned.cancel = cancelWorkers
	defer cancelWorkers() // On unsuccessful HTTP drain, stop producers but do not close their resources.
	owned.requeueDone = broker.StartRequeueLoop(workerCtx)
	owned.sessionsDone = startChallengeCleanup(workerCtx, cognitoStore, 60*time.Second)

	// Start Lambda consumer pollers if configured.
	var consumerManager *ConsumerManager
	if *consumersFile != "" {
		cfg, err := LoadConsumerConfig(*consumersFile)
		if err != nil {
			return fmt.Errorf("failed to load consumer config: %w", err)
		}

		projectRoot := *workDir
		if projectRoot == "" {
			projectRoot = FindProjectRoot(".")
		}

		consumerManager = NewConsumerManager(broker, projectRoot)
		owned.consumers = consumerManager
		consumerManager.Start(workerCtx, cfg.Consumers)
		log.Info().Int("consumers", len(cfg.Consumers)).Str("workDir", projectRoot).Msg("Lambda pollers started")
	}

	httpServer := &http.Server{
		Addr:           fmt.Sprintf(":%d", *port),
		Handler:        server,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 16,
	}

	shutdown := newEventBusListener(httpServer, owned, 30*time.Second)
	listenerOwns = true
	return shutdown.Run(context.Background())
}
