package app

import (
	"flag"
	"fmt"
	"time"
)

type config struct {
	port                     int
	retainedCallbackPort     int
	retainedCleanupFunctions string
	region                   string
	accountID                string
	s3Endpoint               string
	consumersFile            string
	lambdaFunctions          string
	workDir                  string
	issuerBase               string
	jwksBase                 string
	cognitoPools             string
	cognitoTriggers          string
	cognitoDB                string
	cognitoProfile           string
	cognitoLog               string
	schedulerGroups          string
	schedulerExactSeconds    bool
	accessTokenTTL           time.Duration
	refreshTokenTTL          time.Duration
	snsLog                   string
	sqsDeliveryLog           string
	sesLog                   string
	sesConfig                string
	debug                    bool
}

// readConfig accepts an owned FlagSet so CLI parsing does not mutate Go's
// process-global flags. The CLI uses ExitOnError to preserve exit behavior.
func readConfig(flags *flag.FlagSet, args []string) (config, error) {
	port := flags.Int("port", 4100, "Port to listen on")
	retainedCallbackPort := flags.Int("retained-owner-callback-port", 0, "Opt-in exclusive retained-owner mode: trusted callback loopback port (0 disables)")
	retainedCleanupFunctions := flags.String("retained-owner-cleanup-functions", "", "Comma-separated exact registered Lambda cleanup targets (RequestResponse while retained sources remain fenced)")
	region := flags.String("region", "us-east-1", "AWS region")
	accountID := flags.String("account-id", "000000000000", "AWS account ID")
	s3Endpoint := flags.String("s3-endpoint", "http://localhost:9000", "S3 endpoint for Firehose flush (RustFS)")
	lambdaFunctions := flags.String("lambda-functions", "", "Application-owned Lambda functions YAML (Go/provided, Python, Node or command handlers)")
	consumersFile := flags.String("consumers", "", "Path to consumers.yaml (enables Lambda pollers)")
	workDir := flags.String("work-dir", "", "Application root for consumer and trigger handlers (auto-detected if empty)")
	// Cognito dev service flags. The `iss` claim emitted in tokens is
	// `<issuer-base>/<pool-id>`; JWKS is served at
	// `<jwks-base>/<pool-id>/.well-known/jwks.json`. Both default to the
	// same value so plain local works without flags.
	issuerBase := flags.String("issuer-base", "http://localhost:4100", "Base URL for Cognito 'iss' claim and JWKS path")
	jwksBase := flags.String("jwks-base", "", "Base URL for JWKS endpoints (defaults to --issuer-base)")
	cognitoPools := flags.String("cognito-pools", "", "Path to cognito_pools.yaml (optional seed file)")
	cognitoTriggers := flags.String("cognito-triggers", "", "Application-owned Cognito custom trigger YAML (relative to --work-dir)")
	cognitoDB := flags.String("cognito-db", "/tmp/cognito-dev.db", "SQLite path for the local Cognito dev store")
	cognitoProfile := flags.String("cognito-profile", "", "Cognito fixture profile (empty for native; legacy-fixtures for legacy seeded MFA)")
	cognitoLog := flags.String("cognito-log", "-", "Cognito notification JSON Lines capture path (no email/SMS delivery)")
	schedulerGroups := flags.String("scheduler-groups", "", "Comma-separated local Scheduler group fixtures (default group always exists)")
	schedulerExactSeconds := flags.Bool("scheduler-exact-seconds", false, "Development override: execute one-time schedules at exact seconds instead of native minute precision")
	// Token TTLs default to 1h access / 24h refresh; refresh authentication
	// preserves the original refresh token.
	accessTokenTTL := flags.Duration("access-token-ttl", time.Hour, "Legacy seeded-client access/ID token TTL (native app clients own token validity)")
	refreshTokenTTL := flags.Duration("refresh-token-ttl", 24*time.Hour, "Legacy seeded-client refresh TTL (native app clients own token validity)")
	snsLog := flags.String("sns-log", "-", "SNS JSON Lines capture path (external deliveries stay local)")
	sqsDeliveryLog := flags.String("sqs-delivery-log", "", "Optional private JSON Lines evidence path for native SQS delivery and joined completion (requires --lambda-functions)")
	sesLog := flags.String("ses-log", "-", "SES JSON Lines capture path ('-' for stdout; no email delivery)")
	sesConfig := flags.String("ses-config", "", "Optional SES sending fixtures (templates, identities, configuration sets, received messages)")
	debug := flags.Bool("debug", false, "Enable debug logging")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if *cognitoProfile != "" && *cognitoProfile != "legacy-fixtures" {
		return config{}, fmt.Errorf("unknown Cognito fixture profile %q", *cognitoProfile)
	}
	cfg := config{
		retainedCallbackPort:     *retainedCallbackPort,
		retainedCleanupFunctions: *retainedCleanupFunctions,
		port:                     *port,
		region:                   *region,
		accountID:                *accountID,
		s3Endpoint:               *s3Endpoint,
		consumersFile:            *consumersFile,
		lambdaFunctions:          *lambdaFunctions,
		workDir:                  *workDir,
		issuerBase:               *issuerBase,
		jwksBase:                 *jwksBase,
		cognitoPools:             *cognitoPools,
		cognitoTriggers:          *cognitoTriggers,
		cognitoDB:                *cognitoDB,
		cognitoProfile:           *cognitoProfile,
		cognitoLog:               *cognitoLog,
		schedulerGroups:          *schedulerGroups,
		schedulerExactSeconds:    *schedulerExactSeconds,
		accessTokenTTL:           *accessTokenTTL,
		refreshTokenTTL:          *refreshTokenTTL,
		snsLog:                   *snsLog,
		sqsDeliveryLog:           *sqsDeliveryLog,
		sesLog:                   *sesLog,
		sesConfig:                *sesConfig,
		debug:                    *debug,
	}
	if err := validateDevEvidenceConfig(cfg); err != nil {
		return config{}, err
	}
	return cfg, validateRetainedConfig(cfg)
}
