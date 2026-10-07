package app

import (
	"errors"
	"fmt"

	"github.com/lyeith/eventbus/internal/devcapture"
	"github.com/lyeith/eventbus/internal/eventsource"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
)

// Delivery evidence needs the actual native runtime to admit and correlate
// executions. Refuse an unusable opt-in before opening any service resources.
func validateDevEvidenceConfig(cfg config) error {
	if cfg.sqsDeliveryLog != "" && cfg.lambdaFunctions == "" {
		return errors.New("sqs-delivery-log requires lambda-functions")
	}
	return nil
}

// Diagnostics may contain application credentials. Keep their private file
// separate from redacted delivery and ordinary service captures. Each service
// still owns path validation, opening, append, health and final closure.
func validateDevEvidencePaths(diagnostics, delivery, sns, ses, cognito string) error {
	for _, capture := range []struct{ name, path string }{
		{"SQS delivery", delivery}, {"SNS", sns}, {"SES", ses}, {"Cognito", cognito},
	} {
		conflict, err := devcapture.PathsConflict(diagnostics, capture.path)
		if err != nil {
			return fmt.Errorf("compare Lambda diagnostics and %s capture: %w", capture.name, err)
		}
		if conflict {
			return fmt.Errorf("Lambda diagnostics must use a separate private file from %s capture", capture.name)
		}
	}
	return nil
}

// Concrete optional owners are assigned before listeners start. Their terminal
// health is checked outside the coordinator lock without typed-nil interfaces.
func devInvocationEvidence(functions *lambdaservice.Service, mappings *eventsource.Service) error {
	var functionErr, mappingErr error
	if functions != nil {
		functionErr = functions.DevEvidence()
	}
	if mappings != nil {
		mappingErr = mappings.DevEvidence()
	}
	return errors.Join(functionErr, mappingErr)
}
