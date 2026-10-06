package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/consumer"
	"github.com/lyeith/eventbus/internal/firehose"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/secrets"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/lyeith/eventbus/internal/ssm"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Run() (resultErr error) {
	cfg, err := readConfig(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:])
	if err != nil {
		return err
	}

	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	if cfg.debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	} else {
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	}
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: "15:04:05"})

	broker := messaging.NewBroker(cfg.region, cfg.accountID, cfg.port)
	firehoseManager := firehose.NewFirehoseManager(cfg.region, cfg.accountID, cfg.s3Endpoint, "test", "testtest123")
	ssmStore := ssm.NewSSMStore()
	secretsStore := secrets.NewSecretsStore(cfg.region, cfg.accountID)

	// Cognito IDP-compatible local dev service. Open the SQLite store, wire
	// the issuer/JWKS bases into the server, and (optionally) apply a YAML
	// seed file. The store is always opened — even without a seed file —
	// because both authentication handlers and JWKS need the DB handle.
	cognitoStore, err := cognito.OpenCognitoStore(cfg.cognitoDB)
	if err != nil {
		return fmt.Errorf("failed to open cognito store: %w", err)
	}
	owned := &eventBusLifecycle{store: cognitoStore, firehose: firehoseManager}
	listenerOwns := false
	defer func() {
		if !listenerOwns {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, owned.Close(ctx))
		}
	}()
	var sesFixtures ses.SESFixtures
	if cfg.sesConfig != "" {
		sesFixtures, err = ses.LoadSESFixtures(cfg.sesConfig)
		if err != nil {
			return fmt.Errorf("failed to load SES fixtures: %w", err)
		}
	}
	capture, err := ses.OpenSESCapture(cfg.sesLog)
	if err != nil {
		return fmt.Errorf("failed to open SES capture: %w", err)
	}
	sesManager := ses.NewSESManager(sesFixtures, capture)
	owned.ses = sesManager
	jwksURL := cfg.jwksBase
	if jwksURL == "" {
		jwksURL = cfg.issuerBase
	}
	router := server.New(server.Services{
		Messaging:      messaging.NewHandler(broker),
		Firehose:       firehose.NewHandler(firehoseManager),
		SSM:            ssm.NewHandler(ssmStore),
		Secrets:        secrets.NewHandler(secretsStore),
		Cognito:        cognito.NewHandler(cognitoStore, cognito.Options{IssuerBase: cfg.issuerBase, AccessTokenTTL: cfg.accessTokenTTL, RefreshTokenTTL: cfg.refreshTokenTTL}),
		SES:            ses.NewHandler(sesManager),
		CognitoURLs:    &server.CognitoURLs{Issuer: strings.TrimRight(cfg.issuerBase, "/"), JWKS: strings.TrimRight(jwksURL, "/")},
		QueryBodyLimit: ses.QueryBodyLimit,
	})
	if cfg.cognitoPools != "" {
		seed, err := cognito.LoadCognitoSeed(cfg.cognitoPools)
		if err != nil {
			return fmt.Errorf("failed to load cognito seed: %w", err)
		}
		if err := cognito.ApplyCognitoSeed(context.Background(), cognitoStore, seed); err != nil {
			return fmt.Errorf("failed to apply cognito seed: %w", err)
		}
		log.Info().Int("pools", len(seed.Pools)).Str("file", cfg.cognitoPools).Msg("Cognito seed applied")
	}

	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	owned.cancel = cancelWorkers
	defer cancelWorkers() // On unsuccessful HTTP drain, stop producers but do not close their resources.
	owned.requeueDone = broker.StartRequeueLoop(workerCtx)
	owned.sessionsDone = cognito.StartChallengeCleanup(workerCtx, cognitoStore, 60*time.Second)

	// Start Lambda consumer pollers if configured.
	var consumerManager *consumer.ConsumerManager
	if cfg.consumersFile != "" {
		consumersConfig, err := consumer.LoadConsumerConfig(cfg.consumersFile)
		if err != nil {
			return fmt.Errorf("failed to load consumer config: %w", err)
		}

		projectRoot := cfg.workDir
		if projectRoot == "" {
			projectRoot = consumer.FindProjectRoot(".")
		}

		consumerManager = consumer.NewConsumerManager(broker, projectRoot)
		owned.consumers = consumerManager
		consumerManager.Start(workerCtx, consumersConfig.Consumers)
		log.Info().Int("consumers", len(consumersConfig.Consumers)).Str("workDir", projectRoot).Msg("Lambda pollers started")
	}

	httpServer := &http.Server{
		Addr:           fmt.Sprintf(":%d", cfg.port),
		Handler:        router,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 16,
	}

	shutdown := newEventBusListener(httpServer, owned, 30*time.Second)
	listenerOwns = true
	return shutdown.Run(context.Background())
}
