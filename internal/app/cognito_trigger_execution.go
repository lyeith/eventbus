package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/lyeith/eventbus/internal/cognitotrigger"
	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
)

// Cognito owns this execution port and its private response policy. App maps
// registered handlers to a hidden instance of the single managed runtime
// implementation. It is never registered on the public Lambda HTTP surface.
type cognitoTriggerExecution struct {
	runtime *lambdaservice.Service
	targets map[string]map[string]string
}

var _ cognitotrigger.Execution = (*cognitoTriggerExecution)(nil)

func newCognitoTriggerExecution(config *cognitotrigger.Config, workDir string) (*cognitoTriggerExecution, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	functions := make(map[string]lambdaservice.Function)
	targets := make(map[string]map[string]string)
	// Identical entries within a pool intentionally represent the same local
	// Lambda function. Distinct pools, envs, modules, exports and timeouts retain
	// separate imported state. Sort only for stable, nonsecret target names.
	var poolIDs []string
	for poolID := range config.Pools {
		poolIDs = append(poolIDs, poolID)
	}
	sort.Strings(poolIDs)
	identities := make(map[string]string)
	node := config.Node
	if node == "" {
		node = "node"
	}
	for _, poolID := range poolIDs {
		pool := config.Pools[poolID]
		entries := map[string]*cognitotrigger.Entry{
			cognitotrigger.DefineAuthChallenge:         pool.DefineAuthChallenge,
			cognitotrigger.CreateAuthChallenge:         pool.CreateAuthChallenge,
			cognitotrigger.VerifyAuthChallengeResponse: pool.VerifyAuthChallengeResponse,
		}
		targets[poolID] = make(map[string]string)
		for _, name := range []string{cognitotrigger.DefineAuthChallenge, cognitotrigger.CreateAuthChallenge, cognitotrigger.VerifyAuthChallengeResponse} {
			entry := entries[name]
			module, exported, err := cognitotrigger.HandlerReference(entry.Handler)
			if err != nil {
				return nil, err
			}
			timeout := entry.TimeoutSeconds
			if timeout == 0 {
				timeout = 5
			}
			identity, _ := json.Marshal(struct {
				Pool, Module, Export string
				Timeout              int
				Env                  map[string]string
			}{poolID, module, exported, timeout, entry.Env})
			target := identities[string(identity)]
			if target == "" {
				target = fmt.Sprintf("cognito_trigger_%d", len(functions)+1)
				identities[string(identity)] = target
				functions[target] = lambdaservice.Function{Runtime: "node", Command: []string{node}, Handler: module + "#" + exported,
					Environment: maps.Clone(entry.Env), Timeout: time.Duration(timeout) * time.Second, ContextFunctionName: exported}
			}
			targets[poolID][name] = target
		}
	}
	recipe := &lambdaservice.Config{Functions: functions, DevActivity: config.DevActivity,
		ExecutionPolicy: &lambdaservice.ExecutionPolicy{MaxResponseBytes: cognitotrigger.MaxResultBytes,
			MaxLogBytes: cognitotrigger.MaxDiagnosticBytes, PrivateErrors: true,
			ValidateResponse: func(payload []byte) error { _, err := cognitotrigger.DecodeResponse(payload); return err }}}
	if config.DevWarm != nil {
		recipe.DevWarm = &lambdaservice.DevWarmConfig{MaxWorkers: config.DevWarm.MaxWorkers}
	}
	runtime, err := lambdaservice.NewService(recipe, workDir)
	if err != nil {
		return nil, err
	}
	return &cognitoTriggerExecution{runtime: runtime, targets: targets}, nil
}
func (execution *cognitoTriggerExecution) Execute(ctx context.Context, poolID, name string, payload []byte) (cognitotrigger.ExecutionResult, error) {
	target := execution.targets[poolID][name]
	if target == "" {
		return cognitotrigger.ExecutionResult{Failure: cognitotrigger.NotConfigured}, nil
	}
	outcome, err := execution.runtime.ExecuteObserved(ctx, lambdaservice.InvokeInput{FunctionName: target, Payload: payload}, nil)
	result := cognitotrigger.ExecutionResult{Payload: outcome.Output.Payload, OwnershipErr: outcome.OwnershipErr}
	if err != nil {
		// SDK/native errors can contain configuration references. Keep those
		// private; only an actual caller cancellation remains an unwrap cause.
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		return result, errors.New("Cognito trigger execution unavailable")
	}
	if outcome.Output.FunctionError {
		var detail struct {
			Type string `json:"errorType"`
		}
		_ = json.Unmarshal(outcome.Output.Payload, &detail)
		result.Payload = nil
		result.Failure = cognitotrigger.HandlerFailure
		if detail.Type == "EventBus.InvalidResponse" {
			result.Failure = cognitotrigger.InvalidResponse
		}
		if strings.HasPrefix(detail.Type, "Sandbox.") {
			result.Failure = cognitotrigger.Timeout
		}
	}
	return result, nil
}
func (execution *cognitoTriggerExecution) Close(ctx context.Context) error {
	return execution.runtime.Close(ctx)
}
func (execution *cognitoTriggerExecution) DevBeginWarmDrain() error {
	return execution.runtime.DevBeginWarmDrain()
}
func (execution *cognitoTriggerExecution) DevResumeWarm() error {
	return execution.runtime.DevResumeWarm()
}
func (execution *cognitoTriggerExecution) DevEvidence() error { return execution.runtime.DevEvidence() }
