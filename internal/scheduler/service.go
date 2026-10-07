// Package scheduler owns the bounded one-time EventBridge Scheduler contract.
// Target admission belongs to a consumer-owned port; Lambda owns execution.
package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"
	"unicode/utf8"

	"github.com/google/uuid"
)

// TargetInvoker validates local registration and admits an asynchronous target.
// Implementations must honor context cancellation and must not wait for business
// completion. A successful admission is the schedule's completed API invocation.
type TargetInvoker interface {
	ValidateTarget(context.Context, string) error
	AdmitTarget(context.Context, string, []byte) error
}

type RetryPolicy struct {
	MaximumEventAgeInSeconds *int `json:"MaximumEventAgeInSeconds,omitempty"`
	MaximumRetryAttempts     *int `json:"MaximumRetryAttempts,omitempty"`
}

type Target struct {
	Arn                         string          `json:"Arn"`
	RoleArn                     string          `json:"RoleArn"`
	Input                       *string         `json:"Input,omitempty"`
	RetryPolicy                 *RetryPolicy    `json:"RetryPolicy,omitempty"`
	DeadLetterConfig            json.RawMessage `json:"DeadLetterConfig,omitempty"`
	EcsParameters               json.RawMessage `json:"EcsParameters,omitempty"`
	EventBridgeParameters       json.RawMessage `json:"EventBridgeParameters,omitempty"`
	KinesisParameters           json.RawMessage `json:"KinesisParameters,omitempty"`
	SageMakerPipelineParameters json.RawMessage `json:"SageMakerPipelineParameters,omitempty"`
	SqsParameters               json.RawMessage `json:"SqsParameters,omitempty"`
}

type FlexibleTimeWindow struct {
	Mode                   string `json:"Mode"`
	MaximumWindowInMinutes *int   `json:"MaximumWindowInMinutes,omitempty"`
}

type CreateInput struct {
	Name                       string              `json:"-"`
	GroupName                  string              `json:"GroupName,omitempty"`
	ClientToken                string              `json:"ClientToken,omitempty"`
	Description                string              `json:"Description,omitempty"`
	ScheduleExpression         string              `json:"ScheduleExpression"`
	ScheduleExpressionTimezone string              `json:"ScheduleExpressionTimezone,omitempty"`
	FlexibleTimeWindow         *FlexibleTimeWindow `json:"FlexibleTimeWindow"`
	Target                     *Target             `json:"Target"`
	State                      string              `json:"State,omitempty"`
	ActionAfterCompletion      string              `json:"ActionAfterCompletion,omitempty"`
	KmsKeyArn                  string              `json:"KmsKeyArn,omitempty"`
	StartDate                  *float64            `json:"StartDate,omitempty"`
	EndDate                    *float64            `json:"EndDate,omitempty"`
}

type Schedule struct {
	Arn                        string              `json:"Arn"`
	Name                       string              `json:"Name"`
	GroupName                  string              `json:"GroupName"`
	Description                string              `json:"Description,omitempty"`
	ScheduleExpression         string              `json:"ScheduleExpression"`
	ScheduleExpressionTimezone string              `json:"ScheduleExpressionTimezone"`
	FlexibleTimeWindow         *FlexibleTimeWindow `json:"FlexibleTimeWindow"`
	Target                     *Target             `json:"Target"`
	State                      string              `json:"State"`
	ActionAfterCompletion      string              `json:"ActionAfterCompletion"`
	CreationDate               float64             `json:"CreationDate"`
	LastModificationDate       float64             `json:"LastModificationDate"`
	StartDate                  *float64            `json:"StartDate,omitempty"`
	EndDate                    *float64            `json:"EndDate,omitempty"`
}

// Outcome is redacted harness evidence. Payloads and credentials are excluded.
type Outcome struct {
	ScheduleARN string `json:"schedule_arn"`
	Status      string `json:"status"`
	Attempts    int    `json:"attempts"`
	Code        string `json:"code,omitempty"`
}

type APIError struct {
	Code    string
	Status  int
	Message string
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }
func validation(message string) error {
	return &APIError{"ValidationException", http.StatusBadRequest, message}
}
func absent() error {
	return &APIError{"ResourceNotFoundException", http.StatusNotFound, "Schedule or configured group not found"}
}

type targetFailure interface{ Retryable() bool }
type tokenResult struct {
	fingerprint [32]byte
	arn         string
	done        <-chan struct{}
}
type entry struct {
	schedule    Schedule
	due         time.Time
	scheduledAt time.Time
	cancel      context.CancelFunc
	done        chan struct{}
}

type Options struct {
	Region, AccountID string
	Dev               DevOptions
}
type Service struct {
	mu               sync.Mutex
	region, account  string
	groups           map[string]bool
	entries          map[string]*entry
	creates, deletes map[string]tokenResult
	invoker          TargetInvoker
	dev              DevOptions
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	closing          bool
	done             chan struct{}
	closeErr         error
}

var namePattern = regexp.MustCompile(`^[0-9A-Za-z_.-]{1,64}$`)
var tokenPattern = regexp.MustCompile(`^[0-9A-Za-z_-]{1,64}$`)
var rolePattern = regexp.MustCompile(`^arn:aws(?:-[a-z]+)?:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@/-]+$`)
var functionPattern = regexp.MustCompile(`^arn:aws(?:-[a-z]+)?:lambda:([a-z0-9-]+):([0-9]{12}):function:([A-Za-z0-9_-]{1,64})(?::[A-Za-z0-9_$-]{1,128})?$`)

func New(options Options, invoker TargetInvoker) (*Service, error) {
	if options.Region == "" || !regexp.MustCompile(`^[0-9]{12}$`).MatchString(options.AccountID) {
		return nil, validation("Region and twelve-digit AccountID are required")
	}
	if options.Dev.MaxSchedules == 0 {
		options.Dev.MaxSchedules = 10000
	}
	if options.Dev.MaxSchedules < 1 || options.Dev.RetryDelay < 0 {
		return nil, validation("Invalid development capacity or retry delay")
	}
	if options.Dev.Clock == nil {
		options.Dev.Clock = time.Now
	}
	groups := map[string]bool{"default": true}
	for _, group := range options.Dev.Groups {
		if !namePattern.MatchString(group) {
			return nil, validation("Invalid configured group name")
		}
		groups[group] = true
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Service{region: options.Region, account: options.AccountID, groups: groups, entries: map[string]*entry{}, creates: map[string]tokenResult{}, deletes: map[string]tokenResult{}, invoker: invoker, dev: options.Dev, ctx: ctx, cancel: cancel, done: make(chan struct{})}, nil
}

func selected(data json.RawMessage) bool {
	return len(data) > 0 && string(data) != "null" && string(data) != "{}"
}
func (s *Service) validate(input *CreateInput) (time.Time, error) {
	if !namePattern.MatchString(input.Name) {
		return time.Time{}, validation("Invalid schedule name")
	}
	if input.GroupName == "" {
		input.GroupName = "default"
	}
	if !namePattern.MatchString(input.GroupName) {
		return time.Time{}, validation("Invalid group name")
	}
	if !s.groups[input.GroupName] {
		return time.Time{}, absent()
	}
	if input.ClientToken != "" && !tokenPattern.MatchString(input.ClientToken) {
		return time.Time{}, validation("Invalid client token")
	}
	if utf8.RuneCountInString(input.Description) > 512 {
		return time.Time{}, validation("Description exceeds 512 characters")
	}
	if input.State == "" {
		input.State = "ENABLED"
	}
	if input.State != "ENABLED" && input.State != "DISABLED" {
		return time.Time{}, validation("Invalid State")
	}
	if input.ActionAfterCompletion == "" {
		input.ActionAfterCompletion = "NONE"
	}
	if input.ActionAfterCompletion != "NONE" && input.ActionAfterCompletion != "DELETE" {
		return time.Time{}, validation("Invalid ActionAfterCompletion")
	}
	for _, date := range []*float64{input.StartDate, input.EndDate} {
		if date != nil && (math.IsNaN(*date) || math.IsInf(*date, 0)) {
			return time.Time{}, validation("Invalid date")
		}
	}
	if input.KmsKeyArn != "" {
		return time.Time{}, validation("KMS encryption is not supported")
	}
	if input.FlexibleTimeWindow == nil || input.FlexibleTimeWindow.Mode != "OFF" || input.FlexibleTimeWindow.MaximumWindowInMinutes != nil {
		return time.Time{}, validation("Only FlexibleTimeWindow.Mode=OFF is supported")
	}
	if input.ScheduleExpressionTimezone == "" {
		input.ScheduleExpressionTimezone = "UTC"
	}
	if len(input.ScheduleExpressionTimezone) > 50 {
		return time.Time{}, validation("Invalid timezone")
	}
	location, err := time.LoadLocation(input.ScheduleExpressionTimezone)
	if err != nil {
		return time.Time{}, validation("Unknown IANA timezone")
	}
	if !strings.HasPrefix(input.ScheduleExpression, "at(") || !strings.HasSuffix(input.ScheduleExpression, ")") || len(input.ScheduleExpression) != len("at(2006-01-02T15:04:05)") {
		return time.Time{}, validation("Only one-time at(yyyy-mm-ddThh:mm:ss) schedules are supported")
	}
	expression := strings.TrimSuffix(strings.TrimPrefix(input.ScheduleExpression, "at("), ")")
	due, err := time.ParseInLocation("2006-01-02T15:04:05", expression, location)
	if err != nil || due.Format("2006-01-02T15:04:05") != expression {
		return time.Time{}, validation("Invalid local date/time")
	}
	if input.Target == nil {
		return time.Time{}, validation("Target is required")
	}
	target := input.Target
	parts := functionPattern.FindStringSubmatch(target.Arn)
	if parts == nil || parts[1] != s.region || parts[2] != s.account {
		return time.Time{}, validation("Only local registered Lambda ARNs are supported")
	}
	if len(target.RoleArn) > 1600 || !rolePattern.MatchString(target.RoleArn) {
		return time.Time{}, validation("A valid IAM RoleArn is required as local metadata")
	}
	if target.Input == nil {
		return time.Time{}, validation("Explicit Target.Input is required; default Scheduler notifications are not supported")
	}
	if len(*target.Input) == 0 || len(*target.Input) > 256<<10 || !json.Valid([]byte(*target.Input)) {
		return time.Time{}, validation("Lambda input must be valid JSON of at most 256 KiB")
	}
	for _, extra := range []json.RawMessage{target.DeadLetterConfig, target.EcsParameters, target.EventBridgeParameters, target.KinesisParameters, target.SageMakerPipelineParameters, target.SqsParameters} {
		if selected(extra) {
			return time.Time{}, validation("Selected target options are not supported")
		}
	}
	if policy := target.RetryPolicy; policy != nil {
		if policy.MaximumRetryAttempts != nil && (*policy.MaximumRetryAttempts < 0 || *policy.MaximumRetryAttempts > 185) {
			return time.Time{}, validation("MaximumRetryAttempts must be 0..185")
		}
		if policy.MaximumEventAgeInSeconds != nil && (*policy.MaximumEventAgeInSeconds < 60 || *policy.MaximumEventAgeInSeconds > 86400) {
			return time.Time{}, validation("MaximumEventAgeInSeconds must be 60..86400")
		}
	}
	return due, nil
}

func cloneSchedule(schedule Schedule) Schedule {
	if schedule.FlexibleTimeWindow != nil {
		copy := *schedule.FlexibleTimeWindow
		schedule.FlexibleTimeWindow = &copy
	}
	if schedule.Target != nil {
		copy := *schedule.Target
		schedule.Target = &copy
		copy.DeadLetterConfig = append(json.RawMessage(nil), copy.DeadLetterConfig...)
		copy.EcsParameters = append(json.RawMessage(nil), copy.EcsParameters...)
		copy.EventBridgeParameters = append(json.RawMessage(nil), copy.EventBridgeParameters...)
		copy.KinesisParameters = append(json.RawMessage(nil), copy.KinesisParameters...)
		copy.SageMakerPipelineParameters = append(json.RawMessage(nil), copy.SageMakerPipelineParameters...)
		copy.SqsParameters = append(json.RawMessage(nil), copy.SqsParameters...)
		if copy.Input != nil {
			value := *copy.Input
			copy.Input = &value
			schedule.Target.Input = copy.Input
		}
		if copy.RetryPolicy != nil {
			policy := *copy.RetryPolicy
			schedule.Target.RetryPolicy = &policy
			if policy.MaximumEventAgeInSeconds != nil {
				value := *policy.MaximumEventAgeInSeconds
				policy.MaximumEventAgeInSeconds = &value
			}
			if policy.MaximumRetryAttempts != nil {
				value := *policy.MaximumRetryAttempts
				policy.MaximumRetryAttempts = &value
			}
		}
	}
	if schedule.StartDate != nil {
		value := *schedule.StartDate
		schedule.StartDate = &value
	}
	if schedule.EndDate != nil {
		value := *schedule.EndDate
		schedule.EndDate = &value
	}
	return schedule
}

func (s *Service) Create(ctx context.Context, input CreateInput) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	due, err := s.validate(&input)
	if err != nil {
		return "", err
	}
	scheduledAt := due
	if !s.dev.ExactSeconds {
		due = due.Truncate(time.Minute)
	}
	if input.ClientToken == "" {
		input.ClientToken = uuid.NewString()
	}
	encoded, _ := json.Marshal(input)
	fingerprint := sha256.Sum256(append([]byte(input.Name+"\x00"), encoded...))
	key := input.GroupName + "/" + input.Name
	s.mu.Lock()
	if prior, exists := s.creates[input.ClientToken]; exists {
		s.mu.Unlock()
		if prior.fingerprint != fingerprint {
			return "", &APIError{"ConflictException", 409, "ClientToken was used for a different request"}
		}
		return prior.arn, nil
	}
	if s.closing {
		s.mu.Unlock()
		return "", &APIError{"InternalServerException", 503, "Scheduler is closing"}
	}
	// Retries of an accepted token remain idempotent after its due time.
	if !due.Add(time.Minute).After(s.dev.Clock()) {
		s.mu.Unlock()
		return "", validation("Schedule time is in the past")
	}
	s.mu.Unlock()
	if s.invoker == nil {
		return "", validation("No local Lambda runtime is configured")
	}
	if err := s.invoker.ValidateTarget(ctx, input.Target.Arn); err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", &APIError{"ResourceNotFoundException", 404, "Target is not a registered local Lambda function"}
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if prior, exists := s.creates[input.ClientToken]; exists {
		if prior.fingerprint != fingerprint {
			return "", &APIError{"ConflictException", 409, "ClientToken was used for a different request"}
		}
		return prior.arn, nil
	}
	if s.closing {
		return "", &APIError{"InternalServerException", 503, "Scheduler is closing"}
	}
	if _, exists := s.entries[key]; exists {
		return "", &APIError{"ConflictException", 409, "Schedule already exists"}
	}
	if len(s.entries) >= s.dev.MaxSchedules || len(s.creates)+len(s.deletes) >= 2*s.dev.MaxSchedules {
		return "", &APIError{"ServiceQuotaExceededException", 402, "Local schedule or idempotency capacity is full"}
	}
	arn := fmt.Sprintf("arn:aws:scheduler:%s:%s:schedule/%s/%s", s.region, s.account, input.GroupName, input.Name)
	now := float64(s.dev.Clock().UnixMilli()) / 1000
	snapshot := cloneSchedule(Schedule{Arn: arn, Name: input.Name, GroupName: input.GroupName, Description: input.Description, ScheduleExpression: input.ScheduleExpression, ScheduleExpressionTimezone: input.ScheduleExpressionTimezone, FlexibleTimeWindow: input.FlexibleTimeWindow, Target: input.Target, State: input.State, ActionAfterCompletion: input.ActionAfterCompletion, CreationDate: now, LastModificationDate: now, StartDate: input.StartDate, EndDate: input.EndDate})
	workerCtx, cancel := context.WithCancel(s.ctx)
	item := &entry{schedule: snapshot, due: due, scheduledAt: scheduledAt, cancel: cancel, done: make(chan struct{})}
	s.entries[key] = item
	s.creates[input.ClientToken] = tokenResult{fingerprint: fingerprint, arn: arn}
	if input.State == "ENABLED" {
		s.wg.Add(1)
		go s.run(workerCtx, key, item)
	} else {
		close(item.done)
	}
	return arn, nil
}

func (s *Service) Get(group, name string) (Schedule, error) {
	if group == "" {
		group = "default"
	}
	if !namePattern.MatchString(group) || !namePattern.MatchString(name) {
		return Schedule{}, validation("Invalid group or schedule name")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.entries[group+"/"+name]
	if item == nil {
		return Schedule{}, absent()
	}
	return cloneSchedule(item.schedule), nil
}

func (s *Service) Delete(ctx context.Context, group, name, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if group == "" {
		group = "default"
	}
	if !namePattern.MatchString(group) || !namePattern.MatchString(name) || (token != "" && !tokenPattern.MatchString(token)) {
		return validation("Invalid group, schedule name or client token")
	}
	fingerprint := sha256.Sum256([]byte(group + "/" + name))
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	if token != "" {
		if prior, ok := s.deletes[token]; ok {
			s.mu.Unlock()
			if prior.fingerprint != fingerprint {
				return &APIError{"ConflictException", 409, "ClientToken was used for a different deletion"}
			}
			return joinDeletion(ctx, prior.done)
		}
	}
	if s.closing {
		s.mu.Unlock()
		return &APIError{"InternalServerException", 503, "Scheduler is closing"}
	}
	key := group + "/" + name
	item := s.entries[key]
	if item == nil {
		s.mu.Unlock()
		return absent()
	}
	if token != "" && len(s.creates)+len(s.deletes) >= 2*s.dev.MaxSchedules {
		s.mu.Unlock()
		return &APIError{"ServiceQuotaExceededException", 402, "Local idempotency capacity is full"}
	}
	delete(s.entries, key)
	if token != "" {
		s.deletes[token] = tokenResult{fingerprint: fingerprint, arn: item.schedule.Arn, done: item.done}
	}
	item.cancel()
	s.mu.Unlock()
	return joinDeletion(ctx, item.done)
}

func joinDeletion(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		<-done // A deleted schedule owns its callback until it has joined.
		return ctx.Err()
	}
}

func (s *Service) wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(max(0, duration))
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

func (s *Service) run(ctx context.Context, key string, item *entry) {
	defer s.wg.Done()
	var completeSource func(error)
	// The source lifetime includes observation and item completion, but excludes
	// dormant waiting. Register before the existing finalizers so release cannot
	// expose a fixture-safe barrier while those owners are still running.
	defer func() {
		if recovered := recover(); recovered != nil {
			if completeSource != nil {
				completeSource(errors.New("Scheduler source ownership panicked"))
			}
			panic(recovered)
		}
		if completeSource != nil {
			completeSource(nil)
		}
	}()
	defer close(item.done)
	defer item.cancel()
	outcome := Outcome{ScheduleARN: item.schedule.Arn, Status: "canceled"}
	defer func() {
		if s.dev.Observe != nil {
			s.dev.Observe(outcome)
		}
	}()
	if !s.wait(ctx, item.due.Sub(s.dev.Clock())) {
		return
	}
	var admitted bool
	completeSource, admitted = s.beginSource(ctx, item.schedule.Name)
	if !admitted {
		return
	}
	attempts, age := 185, 86400
	if policy := item.schedule.Target.RetryPolicy; policy != nil {
		if policy.MaximumRetryAttempts != nil {
			attempts = *policy.MaximumRetryAttempts
		}
		if policy.MaximumEventAgeInSeconds != nil {
			age = *policy.MaximumEventAgeInSeconds
		}
	}
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			outcome.Status, outcome.Code = "canceled", ""
			return
		}
		if s.dev.Clock().Sub(item.due) >= time.Duration(age)*time.Second {
			outcome.Status = "failed"
			outcome.Code = "EventAgeExceeded"
			break
		}
		outcome.Attempts++
		// Context attributes are native Scheduler template substitutions. Plain
		// JSON input is passed byte-for-byte when it selects no placeholders.
		replacer := strings.NewReplacer(
			"<aws.scheduler.schedule-arn>", item.schedule.Arn,
			"<aws.scheduler.scheduled-time>", item.scheduledAt.UTC().Format(time.RFC3339),
			"<aws.scheduler.execution-id>", uuid.NewString(),
			"<aws.scheduler.attempt-number>", strconv.Itoa(attempt+1),
		)
		payload := []byte(replacer.Replace(*item.schedule.Target.Input))
		err := s.invoker.AdmitTarget(ctx, item.schedule.Target.Arn, payload)
		// Successful publication transfers ownership to Lambda. A later cancellation
		// cannot turn that accepted API invocation into a canceled schedule.
		if err == nil {
			outcome.Status, outcome.Code = "accepted", ""
			break
		}
		if ctx.Err() != nil {
			outcome.Status, outcome.Code = "canceled", ""
			return
		}
		outcome.Status, outcome.Code = "failed", "TargetAdmissionFailed"
		var failure targetFailure
		if attempt >= attempts || (errors.As(err, &failure) && !failure.Retryable()) {
			break
		}
		delay := min(time.Second*time.Duration(1<<min(attempt, 8)), 5*time.Minute)
		if s.dev.RetryDelay > 0 {
			delay = s.dev.RetryDelay
		}
		if !s.wait(ctx, min(delay, item.due.Add(time.Duration(age)*time.Second).Sub(s.dev.Clock()))) {
			outcome.Status, outcome.Code = "canceled", ""
			return
		}
	}
	s.mu.Lock()
	if s.entries[key] == item && item.schedule.ActionAfterCompletion == "DELETE" {
		delete(s.entries, key)
	}
	s.mu.Unlock()
}

// Close stops admission, cancels future schedules and joins target callbacks.
// Accepted Lambda events remain owned by Lambda, which must be closed afterward.
func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		s.cancel()
		go func() { s.wg.Wait(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
	case <-ctx.Done():
		s.mu.Lock()
		select {
		case <-s.done:
			result := s.closeErr
			s.mu.Unlock()
			return result
		default:
		}
		if s.closeErr == nil {
			s.closeErr = fmt.Errorf("join Scheduler target admissions: %w", ctx.Err())
		}
		s.mu.Unlock()
		<-s.done // Cancellation joins every owned target callback before returning.
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}
