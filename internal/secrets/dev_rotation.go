package secrets

import (
	"time"

	"github.com/rs/zerolog/log"
)

// RotationOptions bounds local execution and admits development outcome observers.
// These controls are not additional fields on the AWS RotateSecret request.
type RotationOptions struct {
	MaxConcurrent, MaxPending, MaxAttempts int
	AttemptTimeout, RetryDelay             time.Duration
	Observer                               func(RotationOutcome)
}

func normalizeRotationOptions(options RotationOptions) RotationOptions {
	if options.MaxConcurrent <= 0 {
		options.MaxConcurrent = 2
	}
	if options.MaxPending <= 0 {
		options.MaxPending = 128
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}
	if options.AttemptTimeout <= 0 {
		options.AttemptTimeout = 15 * time.Minute
	}
	if options.RetryDelay <= 0 {
		options.RetryDelay = 100 * time.Millisecond
	}
	return options
}

// RotationOutcome is redacted harness evidence, not an AWS DescribeSecret field.
// Handler error text/results are intentionally excluded because they may contain values.
type RotationOutcome struct {
	SecretARN   string    `json:"secret_arn"`
	VersionID   string    `json:"version_id"`
	Step        string    `json:"step"`
	Status      string    `json:"status"`
	Attempts    int       `json:"attempts"`
	CompletedAt time.Time `json:"completed_at"`
}

func (service *RotationService) report(outcome RotationOutcome) {
	outcome.CompletedAt = time.Now().UTC()
	if service.options.Observer != nil {
		service.options.Observer(outcome)
		return
	}
	log.Info().Str("secret_arn", outcome.SecretARN).Str("version_id", outcome.VersionID).Str("step", outcome.Step).Str("status", outcome.Status).Int("attempts", outcome.Attempts).Msg("Secrets rotation completed")
}
