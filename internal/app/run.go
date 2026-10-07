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
	"github.com/lyeith/eventbus/internal/devquiescence"
	"github.com/lyeith/eventbus/internal/eventsource"
	"github.com/lyeith/eventbus/internal/firehose"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/scheduler"
	"github.com/lyeith/eventbus/internal/secrets"
	"github.com/lyeith/eventbus/internal/server"
	"github.com/lyeith/eventbus/internal/ses"
	"github.com/lyeith/eventbus/internal/ssm"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Run() error {
	cfg, err := readConfig(flag.NewFlagSet(os.Args[0], flag.ExitOnError), os.Args[1:])
	if err != nil {
		return err
	}
	return run(context.Background(), cfg)
}

func run(ctx context.Context, cfg config) (resultErr error) {
	if err := validateRetainedConfig(cfg); err != nil {
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
	if err := firehoseManager.SetMetadataExtractor(firehose.NewGoJQMetadataExtractor()); err != nil {
		return fmt.Errorf("failed to configure Firehose metadata extraction: %w", err)
	}
	if cfg.retainedCallbackPort == 0 {
		broker.SetFirehoseDelivery(firehoseManager)
	}
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
	snsCapture, err := messaging.OpenSNSCapture(cfg.snsLog)
	if err != nil {
		return fmt.Errorf("failed to open SNS capture: %w", err)
	}
	owned.sns = snsCapture
	broker.SetSNSCapture(snsCapture)
	var retained *devquiescence.Coordinator
	if cfg.retainedCallbackPort != 0 {
		retained = devquiescence.New(snsCapture.Err)
	}
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
	projectRoot := cfg.workDir
	if projectRoot == "" {
		projectRoot = findProjectRoot(".")
	}
	var functions *lambdaservice.Service
	var functionHandler http.Handler
	if cfg.lambdaFunctions != "" {
		runner, configureErr := loadRetainedFunctions(cfg.lambdaFunctions, projectRoot, retained)
		functions, err = runner, configureErr
		if err != nil {
			return fmt.Errorf("failed to configure Lambda functions: %w", err)
		}
		owned.functions = runner
		functionHandler = runner
	}
	var mappingFunctions eventsource.FunctionInvoker
	if functions != nil {
		broker.SetLambdaDelivery(snsLambdaInvoker{runtime: functions})
		mappingFunctions = eventSourceLambdaInvoker{runtime: functions}
	}
	mappings, err := eventsource.New(eventsource.Options{Region: cfg.region, AccountID: cfg.accountID}, sqsMappingSource{broker: broker}, mappingFunctions)
	if err != nil {
		return fmt.Errorf("failed to configure Lambda event-source mappings: %w", err)
	}
	owned.mappings = mappings
	triggers, err := loadCognitoTriggers(cfg.cognitoTriggers, projectRoot)
	if err != nil {
		return fmt.Errorf("failed to configure Cognito triggers: %w", err)
	}
	if triggers != nil {
		owned.triggers = triggers
	}
	cognitoLog := cfg.cognitoLog
	if cognitoLog == "" {
		cognitoLog = "-"
	}
	notifications, err := cognito.OpenNotificationCapture(cognitoLog)
	if err != nil {
		return fmt.Errorf("failed to open Cognito notification capture: %w", err)
	}
	owned.notifications = notifications
	cognitoOptions := cognito.Options{Region: cfg.region, AccountID: cfg.accountID, DevProfile: cfg.cognitoProfile, Notifications: notifications, IssuerBase: cfg.issuerBase, AccessTokenTTL: cfg.accessTokenTTL, RefreshTokenTTL: cfg.refreshTokenTTL}
	if triggers != nil {
		cognitoOptions.Triggers = triggers
	}
	jwksURL := cfg.jwksBase
	if jwksURL == "" {
		jwksURL = cfg.issuerBase
	}
	var rotationInvoker secrets.RotationInvoker
	var schedulerInvoker scheduler.TargetInvoker
	if functions != nil {
		rotationInvoker = rotationLambdaInvoker{runtime: functions}
		schedulerInvoker = schedulerLambdaInvoker{runtime: functions}
	}
	rotation := secrets.NewRotationService(secretsStore, rotationInvoker, secrets.RotationOptions{})
	owned.rotation = rotation
	var groups []string
	if cfg.schedulerGroups != "" {
		groups = strings.Split(cfg.schedulerGroups, ",")
		for i := range groups {
			groups[i] = strings.TrimSpace(groups[i])
		}
	}
	schedules, err := scheduler.New(scheduler.Options{Region: cfg.region, AccountID: cfg.accountID, Dev: scheduler.DevOptions{
		Groups: groups, ExactSeconds: cfg.schedulerExactSeconds,
		Observe: func(outcome scheduler.Outcome) {
			log.Info().Str("schedule_arn", outcome.ScheduleARN).Str("status", outcome.Status).Str("code", outcome.Code).Int("attempts", outcome.Attempts).Msg("Scheduler target admission completed")
		},
	}}, schedulerInvoker)
	if err != nil {
		return fmt.Errorf("failed to configure Scheduler: %w", err)
	}
	owned.scheduler = schedules
	services := server.Services{
		Messaging:      messaging.NewHandler(broker),
		Lambda:         functionHandler,
		EventSources:   eventsource.NewHandler(mappings),
		Scheduler:      scheduler.NewHandler(schedules),
		Firehose:       firehose.NewHandler(firehoseManager),
		SSM:            ssm.NewHandler(ssmStore),
		Secrets:        secrets.NewHandler(secretsStore, rotation),
		Cognito:        cognito.NewHandler(cognitoStore, cognitoOptions),
		SES:            ses.NewHandler(sesManager),
		CognitoURLs:    &server.CognitoURLs{Issuer: strings.TrimRight(cfg.issuerBase, "/"), JWKS: strings.TrimRight(jwksURL, "/")},
		QueryBodyLimit: ses.QueryBodyLimit,
	}
	if retained != nil {
		services = retainedServices(services)
	}
	var router http.Handler = server.New(services)
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

	var devHTTP *devRetainedHTTP
	if retained != nil {
		httpServer.Addr = fmt.Sprintf("127.0.0.1:%d", cfg.port)
		httpServer.WriteTimeout = 0 // Controls use their explicit bounded deadline, up to five minutes.
		httpServer.Handler, devHTTP = newRetainedHTTP(retained, router, cfg.retainedCallbackPort)
	}
	shutdown := newEventBusListener(httpServer, owned, 30*time.Second)
	shutdown.devRetained = devHTTP
	listenerOwns = true
	return shutdown.Run(ctx)
}
