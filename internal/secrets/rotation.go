package secrets

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"time"
)

// RotationInvoker is owned by Secrets. App composition supplies the registered
// Lambda runtime; the Secrets workflow never invokes a handler under a store lock.
type RotationInvoker interface {
	ValidateRotationTarget(context.Context, string) error
	InvokeRotation(context.Context, string, RotationEvent) error
}
type RotationEvent struct {
	SecretID           string `json:"SecretId"`
	ClientRequestToken string `json:"ClientRequestToken"`
	Step               string `json:"Step"`
}
type RotateInput struct {
	SecretID, ClientRequestToken, RotationLambdaARN string
	RotationRules                                   *RotationRules
	RotateImmediately                               *bool
}

type rotationJob struct {
	arn, token, function string
	testOnly             bool
	cancel               context.CancelFunc
}
type RotationService struct {
	store               *SecretsStore
	invoker             RotationInvoker
	options             RotationOptions
	context             context.Context
	cancel              context.CancelFunc
	mu                  sync.Mutex
	closed              bool
	drainErr            error
	active              map[string]*rotationJob
	pending, concurrent chan struct{}
	inflight            sync.WaitGroup
	done                chan struct{}
}

func NewRotationService(store *SecretsStore, invoker RotationInvoker, options RotationOptions) *RotationService {
	options = normalizeRotationOptions(options)
	ctx, cancel := context.WithCancel(context.Background())
	return &RotationService{store: store, invoker: invoker, options: options, context: ctx, cancel: cancel, active: map[string]*rotationJob{}, pending: make(chan struct{}, options.MaxPending), concurrent: make(chan struct{}, options.MaxConcurrent), done: make(chan struct{})}
}

var rotationFunctionARN = regexp.MustCompile(`^arn:(aws|aws-cn|aws-us-gov):lambda:[A-Za-z0-9-]+:[0-9]{12}:function:[A-Za-z0-9_-]{1,64}(:[A-Za-z0-9_$-]{1,128})?$`)

func validateRotationRules(rules *RotationRules) error {
	if rules != nil && (rules.AutomaticallyAfterDays != nil || rules.Duration != "" || rules.ScheduleExpression != "") {
		return invalidParameter("Scheduled rotation rules are not supported; use on-demand rotation")
	}
	return nil
}
func (service *RotationService) Rotate(ctx context.Context, input RotateInput) (*Secret, error) {
	if ctx.Err() != nil {
		return nil, invalidRequest("Rotation request was canceled before admission")
	}
	if err := validateSecretID(input.SecretID); err != nil {
		return nil, err
	}
	if err := validateToken(input.ClientRequestToken); err != nil {
		return nil, err
	}
	if err := validateRotationRules(input.RotationRules); err != nil {
		return nil, err
	}
	existing, err := service.store.Describe(input.SecretID)
	if err != nil {
		return nil, err
	}
	function := input.RotationLambdaARN
	if function == "" {
		function = existing.RotationLambdaARN
	}
	if function == "" {
		return nil, invalidRequest("A rotation Lambda ARN must be configured")
	}
	if !rotationFunctionARN.MatchString(function) {
		return nil, invalidParameter("Invalid RotationLambdaARN")
	}
	if service.invoker == nil {
		return nil, invalidRequest("No rotation function invoker is configured")
	}
	if err := service.invoker.ValidateRotationTarget(ctx, function); err != nil {
		return nil, invalidParameter("The rotation function is not available")
	}
	token := requestedOrGeneratedVersionID(input.ClientRequestToken)
	key := existing.ARN + "\x00" + token
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed {
		return nil, &APIError{"InternalServiceError", "Rotation service is closing"}
	}
	if ctx.Err() != nil {
		return nil, invalidRequest("Rotation request was canceled before admission")
	}
	if job := service.active[key]; job != nil {
		if job.function != function {
			return nil, invalidParameter("An admitted rotation token cannot select another function")
		}
		return service.store.rotationSnapshot(input.SecretID, token)
	}
	select {
	case service.pending <- struct{}{}:
	default:
		return nil, &APIError{"LimitExceededException", "Local rotation admission limit exceeded"}
	}
	result, alreadyCurrent, err := service.store.beginRotation(input, function, token)
	if err != nil || alreadyCurrent {
		<-service.pending
		return result, err
	}
	execution, cancel := context.WithCancel(service.context)
	job := &rotationJob{arn: result.ARN, token: token, function: function, cancel: cancel, testOnly: input.RotateImmediately != nil && !*input.RotateImmediately}
	service.active[key] = job
	service.inflight.Add(1)
	go service.run(execution, key, job)
	return result, nil
}
func (ss *SecretsStore) rotationSnapshot(id, token string) (*Secret, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	state := ss.findLocked(id)
	if state == nil {
		return nil, notFound()
	}
	return state.snapshot(token), nil
}
func (ss *SecretsStore) beginRotation(input RotateInput, function, token string) (*Secret, bool, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(input.SecretID)
	if state == nil {
		return nil, false, notFound()
	}
	current := state.labels["AWSCURRENT"]
	if version := state.versions[current]; version == nil || !version.hasValue {
		return nil, false, invalidRequest("Rotation requires a stored current secret value")
	}
	if pending := state.labels["AWSPENDING"]; pending != "" && pending != current && pending != token {
		return nil, false, invalidRequest("Another rotation is still pending")
	}
	if state.versions[token] != nil && token != current && state.labels["AWSPENDING"] != token {
		return nil, false, invalidRequest("The rotation token identifies an unrelated version")
	}
	if token != current && state.labels["AWSPENDING"] == "" && len(state.labels) >= 20 {
		return nil, false, &APIError{"LimitExceededException", "A secret supports at most 20 staging labels"}
	}
	enabled := true
	state.rotationEnabled = &enabled
	state.rotationLambda = function
	if input.RotationRules != nil {
		state.rotationRules = cloneRules(input.RotationRules)
	}
	state.changed = ss.now()
	if token == current {
		return state.snapshot(token), true, nil
	}
	if state.versions[token] == nil {
		version := &secretVersion{created: ss.now()}
		// Configuration-only testing skips create/set, so the pending credentials
		// must already identify the current target for testSecret to read and check.
		if input.RotateImmediately != nil && !*input.RotateImmediately {
			version.value = copyValue(state.versions[current].value)
			version.hasValue = true
		}
		state.versions[token] = version
	}
	state.labels["AWSPENDING"] = token
	return state.snapshot(token), false, nil
}
func (service *RotationService) run(ctx context.Context, key string, job *rotationJob) {
	outcome := RotationOutcome{SecretARN: job.arn, VersionID: job.token, Status: "succeeded"}
	defer func() {
		job.cancel()
		if job.testOnly {
			service.store.removeTestPending(job.arn, job.token)
		}
		service.mu.Lock()
		delete(service.active, key)
		service.mu.Unlock()
		<-service.pending
		service.report(outcome)
		service.inflight.Done()
	}()
	select {
	case service.concurrent <- struct{}{}:
		defer func() { <-service.concurrent }()
	case <-ctx.Done():
		outcome.Status = "canceled"
		return
	}
	if ctx.Err() != nil {
		outcome.Status = "canceled"
		return
	}
	steps := []string{"createSecret", "setSecret", "testSecret", "finishSecret"}
	if job.testOnly {
		steps = []string{"testSecret"}
	}
	for _, step := range steps {
		outcome.Step = step
		var failure error
		for attempt := 1; attempt <= service.options.MaxAttempts; attempt++ {
			outcome.Attempts = attempt
			execution, cancel := context.WithTimeout(ctx, service.options.AttemptTimeout)
			failure = service.invoker.InvokeRotation(execution, job.function, RotationEvent{SecretID: job.arn, ClientRequestToken: job.token, Step: step})
			executionErr := execution.Err()
			timedOut := errors.Is(executionErr, context.DeadlineExceeded) || errors.Is(failure, context.DeadlineExceeded)
			cancel()
			if failure == nil && executionErr == nil {
				break
			}
			if failure == nil {
				failure = context.Canceled
			}
			if ctx.Err() != nil {
				outcome.Status = "canceled"
				break
			}
			if timedOut {
				outcome.Status = "timed_out"
			} else {
				outcome.Status = "handler_failure"
			}
			if attempt < service.options.MaxAttempts {
				timer := time.NewTimer(service.options.RetryDelay)
				select {
				case <-timer.C:
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					outcome.Status = "canceled"
				}
				if ctx.Err() != nil {
					break
				}
			}
		}
		if failure != nil {
			return
		}
		outcome.Status = "succeeded"
	}
	if !job.testOnly {
		if err := service.store.finishRotation(job.arn, job.token); err != nil {
			outcome.Status = "invalid_finish"
		}
	}
}
func (ss *SecretsStore) finishRotation(id, token string) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(id)
	if state == nil {
		return notFound()
	}
	if state.labels["AWSCURRENT"] != token || state.versions[token] == nil || !state.versions[token].hasValue {
		return invalidRequest("The rotation handler did not promote the pending value")
	}
	state.rotated = ss.now()
	return nil
}
func (ss *SecretsStore) removeTestPending(id, token string) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(id)
	if state == nil {
		return
	}
	if state.labels["AWSPENDING"] == token && state.labels["AWSCURRENT"] != token {
		delete(state.labels, "AWSPENDING")
		labeled := false
		for _, versionID := range state.labels {
			if versionID == token {
				labeled = true
				break
			}
		}
		if !labeled {
			delete(state.versions, token)
		}
	}
}
func (service *RotationService) Cancel(id string) (*Secret, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	result, err := service.store.cancelRotation(id)
	if err != nil {
		return nil, err
	}
	for _, job := range service.active {
		if job.arn == result.ARN {
			job.cancel()
		}
	}
	return result, nil
}
func (ss *SecretsStore) cancelRotation(id string) (*Secret, error) {
	if err := validateSecretID(id); err != nil {
		return nil, err
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	state := ss.findLocked(id)
	if state == nil {
		return nil, notFound()
	}
	enabled := false
	state.rotationEnabled = &enabled
	state.changed = ss.now()
	pending := state.labels["AWSPENDING"]
	if pending == state.labels["AWSCURRENT"] {
		pending = ""
	}
	return state.snapshot(pending), nil
}

// Drain stops new rotation admission and joins accepted workflows while their
// Lambda owner and AWS HTTP listener remain available. A deadline aborts and
// joins owned execution before returning; subsequent drains retain that error.
func (service *RotationService) Drain(ctx context.Context) error {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	first := !service.closed
	service.closed = true
	service.mu.Unlock()
	if first {
		go func() { service.inflight.Wait(); close(service.done) }()
	}
	select {
	case <-service.done:
	case <-ctx.Done():
		service.mu.Lock()
		// A previously completed healthy drain wins over an already-canceled
		// caller. Both channels can be ready on repeated Drain/Close calls.
		select {
		case <-service.done:
		default:
			if service.drainErr == nil {
				service.drainErr = ctx.Err()
			}
			service.cancel()
		}
		service.mu.Unlock()
		<-service.done
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.drainErr
}

// Close releases the coordinator context only after graceful completion or a
// fully joined abort. App composition drains this owner before Lambda/HTTP.
func (service *RotationService) Close(ctx context.Context) error {
	if service == nil {
		return nil
	}
	err := service.Drain(ctx)
	service.cancel()
	return err
}
