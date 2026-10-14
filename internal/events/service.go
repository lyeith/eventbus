package events

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var namePattern = regexp.MustCompile(`^[.\-_A-Za-z0-9]{1,64}$`)
var targetPattern = regexp.MustCompile(`^[.\-_A-Za-z0-9]{1,64}$`)
var functionPattern = regexp.MustCompile(`^arn:aws:lambda:([a-z0-9-]+):([0-9]{12}):function:([A-Za-z0-9_-]{1,64})(?::[A-Za-z0-9_$-]{1,128})?$`)
var ratePattern = regexp.MustCompile(`^rate\(([1-9][0-9]*) (minute|minutes|hour|hours|day|days)\)$`)

type entry struct {
	// gate serializes first target admissions with native mutation barriers.
	// No global registry lock is held during a target callback.
	gate     sync.Mutex
	rule     Rule
	targets  map[string]Target
	interval time.Duration
	next     time.Time
	changed  chan struct{}
}
type Service struct {
	mu                    sync.Mutex
	region, account       string
	entries               map[string]*entry
	invoker               TargetInvoker
	dev                   DevOptions
	ctx                   context.Context
	cancel                context.CancelFunc
	deliveryCtx           context.Context
	deliveryCancel        context.CancelFunc
	wg                    sync.WaitGroup
	closing               bool
	done                  chan struct{}
	pending               int
	capacityChanged       chan struct{}
	closeErr, evidenceErr error
	tokenKey              []byte
}

func New(options Options, invoker TargetInvoker) (*Service, error) {
	if options.Region == "" || !regexp.MustCompile(`^[0-9]{12}$`).MatchString(options.AccountID) {
		return nil, validation("Region and twelve-digit AccountID are required")
	}
	if options.Dev.Clock == nil {
		options.Dev.Clock = systemClock{}
	}
	if options.Dev.MaxRules == 0 {
		options.Dev.MaxRules = 300
	}
	if options.Dev.MaxPending == 0 {
		options.Dev.MaxPending = 1500
	}
	if options.Dev.MaxRules < 1 || options.Dev.MaxPending < 1 {
		return nil, validation("Invalid local rule or pending delivery capacity")
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	deliveryCtx, deliveryCancel := context.WithCancel(context.Background())
	return &Service{region: options.Region, account: options.AccountID, entries: map[string]*entry{}, invoker: invoker, dev: options.Dev, ctx: ctx, cancel: cancel, deliveryCtx: deliveryCtx, deliveryCancel: deliveryCancel, done: make(chan struct{}), capacityChanged: make(chan struct{}), tokenKey: key}, nil
}

func rateDuration(expression string) (time.Duration, error) {
	parts := ratePattern.FindStringSubmatch(expression)
	if parts == nil {
		return 0, validation("Only rate(value minute|minutes|hour|hours|day|days) scheduled rules are supported")
	}
	value, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || value == 0 {
		return 0, validation("Invalid positive rate value")
	}
	plural := strings.HasSuffix(parts[2], "s")
	if (value == 1 && plural) || (value > 1 && !plural) {
		return 0, validation("Rate unit must be singular for one and plural for larger values")
	}
	unit := time.Minute
	if strings.HasPrefix(parts[2], "hour") {
		unit = time.Hour
	}
	if strings.HasPrefix(parts[2], "day") {
		unit = 24 * time.Hour
	}
	if value > uint64(math.MaxInt64/int64(unit)) {
		return 0, validation("Rate duration exceeds supported range")
	}
	return time.Duration(value) * unit, nil
}
func (s *Service) validateBus(bus string) error {
	if bus == "" || bus == "default" || bus == fmt.Sprintf("arn:aws:events:%s:%s:event-bus/default", s.region, s.account) {
		return nil
	}
	return validation("Only the local default event bus is supported")
}
func (s *Service) validateRuleInput(input RuleInput) error {
	if !namePattern.MatchString(input.Name) {
		return validation("Invalid rule name")
	}
	return s.validateBus(input.EventBusName)
}
func changed(item *entry) { close(item.changed); item.changed = make(chan struct{}) }

func (s *Service) PutRule(ctx context.Context, input PutRuleInput) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.validateRuleInput(RuleInput{Name: input.Name, EventBusName: input.EventBusName}); err != nil {
		return "", err
	}
	if input.EventPattern != nil {
		return "", validation("Event patterns are not supported")
	}
	if input.RoleArn != nil || len(input.Tags) > 0 {
		return "", validation("Rule IAM roles and tags are not supported")
	}
	if utf8.RuneCountInString(input.Description) > 512 {
		return "", validation("Description exceeds 512 characters")
	}
	if input.State == "" {
		input.State = "ENABLED"
	}
	if input.State != "ENABLED" && input.State != "DISABLED" {
		return "", validation("Only ENABLED or DISABLED State is supported")
	}
	interval, err := rateDuration(input.ScheduleExpression)
	if err != nil {
		return "", err
	}
	rule := Rule{Name: input.Name, Arn: s.ruleARN(input.Name), Description: input.Description, ScheduleExpression: input.ScheduleExpression, State: input.State, EventBusName: "default", CreatedBy: s.account}
	// Existing-entry mutations take their barrier before registry state. Creation
	// retries if a competing writer changed which entry owns the rule name.
	for {
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			return "", unavailable()
		}
		item := s.entries[input.Name]
		if item == nil {
			if len(s.entries) >= s.dev.MaxRules {
				s.mu.Unlock()
				return "", &APIError{"LimitExceededException", 400, "Local rule capacity is full"}
			}
			item = &entry{rule: rule, targets: map[string]Target{}, interval: interval, next: s.dev.Clock.Now().Add(interval), changed: make(chan struct{})}
			s.entries[input.Name] = item
			s.wg.Add(1)
			go s.run(input.Name, item)
			s.mu.Unlock()
			return rule.Arn, nil
		}
		s.mu.Unlock()
		item.gate.Lock()
		s.mu.Lock()
		if s.entries[input.Name] != item {
			s.mu.Unlock()
			item.gate.Unlock()
			continue
		}
		if s.closing {
			s.mu.Unlock()
			item.gate.Unlock()
			return "", unavailable()
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			item.gate.Unlock()
			return "", err
		}
		item.rule = rule
		item.interval = interval
		item.next = s.dev.Clock.Now().Add(interval)
		changed(item)
		s.mu.Unlock()
		item.gate.Unlock()
		return rule.Arn, nil
	}
}
func (s *Service) DescribeRule(input RuleInput) (Rule, error) {
	if err := s.validateRuleInput(input); err != nil {
		return Rule{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if item := s.entries[input.Name]; item != nil {
		return item.rule, nil
	}
	return Rule{}, absent()
}

func (s *Service) mutate(ctx context.Context, input RuleInput, mutation func(*entry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.validateRuleInput(input); err != nil {
		return err
	}
	for {
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			return unavailable()
		}
		item := s.entries[input.Name]
		if item == nil {
			s.mu.Unlock()
			return absent()
		}
		s.mu.Unlock()
		item.gate.Lock()
		s.mu.Lock()
		if s.entries[input.Name] != item {
			s.mu.Unlock()
			item.gate.Unlock()
			continue
		}
		var err error
		if s.closing {
			err = unavailable()
		} else if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			err = mutation(item)
		}
		s.mu.Unlock()
		item.gate.Unlock()
		return err
	}
}
func (s *Service) EnableRule(ctx context.Context, input RuleInput) error {
	return s.mutate(ctx, input, func(item *entry) error {
		if item.rule.State != "ENABLED" {
			item.rule.State = "ENABLED"
			if !item.next.After(s.dev.Clock.Now()) {
				s.advanceLocked(item, item.next)
			}
			changed(item)
		}
		return nil
	})
}
func (s *Service) DisableRule(ctx context.Context, input RuleInput) error {
	return s.mutate(ctx, input, func(item *entry) error { item.rule.State = "DISABLED"; changed(item); return nil })
}
func (s *Service) DeleteRule(ctx context.Context, input RuleInput) error {
	err := s.mutate(ctx, input, func(item *entry) error {
		if len(item.targets) > 0 {
			return validation("Rule cannot be deleted while targets are attached")
		}
		delete(s.entries, input.Name)
		changed(item)
		return nil
	})
	var failure *APIError
	if errors.As(err, &failure) && failure.Code == "ResourceNotFoundException" {
		return nil
	}
	return err
}

func cloneTarget(target Target) Target {
	// Native targets contain only JSON data; serialization provides detached
	// nested snapshots and preserves all supported optional field presence.
	data, _ := json.Marshal(target)
	var copied Target
	_ = json.Unmarshal(data, &copied)
	return copied
}
func validateTargetShape(target Target) error {
	if !targetPattern.MatchString(target.Id) {
		return validation("Invalid target Id")
	}
	if target.RoleArn != nil {
		return validation("Lambda targets do not support RoleArn")
	}
	for _, raw := range []json.RawMessage{target.DeadLetterConfig, target.AppSyncParameters, target.BatchParameters, target.EcsParameters, target.HttpParameters, target.KinesisParameters, target.RedshiftDataParameters, target.RunCommandParameters, target.SageMakerPipelineParameters, target.SqsParameters} {
		if len(raw) > 0 {
			return validation("Dead-letter queues and target-specific parameters are not supported")
		}
	}
	if policy := target.RetryPolicy; policy != nil {
		if policy.MaximumEventAgeInSeconds != nil && (*policy.MaximumEventAgeInSeconds < 60 || *policy.MaximumEventAgeInSeconds > 86400) {
			return validation("MaximumEventAgeInSeconds must be 60..86400")
		}
		if policy.MaximumRetryAttempts != nil && (*policy.MaximumRetryAttempts < 0 || *policy.MaximumRetryAttempts > 185) {
			return validation("MaximumRetryAttempts must be 0..185")
		}
	}
	return validateTargetInput(target)
}
func (s *Service) PutTargets(ctx context.Context, input PutTargetsInput) (TargetsResult, error) {
	result := TargetsResult{FailedEntries: []FailedEntry{}}
	if err := s.validateRuleInput(RuleInput{Name: input.Rule, EventBusName: input.EventBusName}); err != nil {
		return result, err
	}
	if len(input.Targets) < 1 || len(input.Targets) > 10 {
		return result, validation("PutTargets requires 1..10 entries")
	}
	seen := map[string]bool{}
	for _, target := range input.Targets {
		if seen[target.Id] {
			return result, validation("Duplicate target Id")
		}
		seen[target.Id] = true
		if err := validateTargetShape(target); err != nil {
			return result, err
		}
	}
	// Validate registration outside registry and mutation locks. Failures are
	// per-target; malformed common input is rejected before any mutation.
	valid := make([]Target, 0, len(input.Targets))
	for _, target := range input.Targets {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		parts := functionPattern.FindStringSubmatch(target.Arn)
		code, message := "", ""
		if parts == nil || parts[1] != s.region || parts[2] != s.account {
			code, message = "ValidationException", "Only Lambda targets in the local account and region are supported"
		} else if s.invoker == nil {
			code, message = "ResourceNotFoundException", "Target is not a registered local Lambda function"
		} else if err := s.invoker.ValidateTarget(ctx, target.Arn); err != nil {
			if ctx.Err() != nil {
				return result, ctx.Err()
			}
			code, message = "ResourceNotFoundException", "Target is not a registered local Lambda function"
		}
		if code != "" {
			result.FailedEntries = append(result.FailedEntries, FailedEntry{target.Id, code, message})
		} else {
			valid = append(valid, cloneTarget(target))
		}
	}
	err := s.mutate(ctx, RuleInput{Name: input.Rule, EventBusName: input.EventBusName}, func(item *entry) error {
		for _, target := range valid {
			if _, exists := item.targets[target.Id]; !exists && len(item.targets) >= 5 {
				result.FailedEntries = append(result.FailedEntries, FailedEntry{target.Id, "LimitExceededException", "A rule supports at most five targets"})
				continue
			}
			item.targets[target.Id] = target
		}
		return nil
	})
	result.FailedEntryCount = len(result.FailedEntries)
	return result, err
}
func (s *Service) RemoveTargets(ctx context.Context, input RemoveTargetsInput) (TargetsResult, error) {
	result := TargetsResult{FailedEntries: []FailedEntry{}}
	if len(input.Ids) < 1 || len(input.Ids) > 10 {
		return result, validation("RemoveTargets requires 1..10 Ids")
	}
	seen := map[string]bool{}
	for _, id := range input.Ids {
		if !targetPattern.MatchString(id) || seen[id] {
			return result, validation("Invalid or duplicate target Id")
		}
		seen[id] = true
	}
	err := s.mutate(ctx, RuleInput{Name: input.Rule, EventBusName: input.EventBusName}, func(item *entry) error {
		for _, id := range input.Ids {
			delete(item.targets, id)
		}
		return nil
	})
	return result, err
}

type cursor struct{ Kind, Scope, After string }

func (s *Service) encodeCursor(value cursor) string {
	data, _ := json.Marshal(value)
	mac := hmac.New(sha256.New, s.tokenKey)
	mac.Write(data)
	return base64.RawURLEncoding.EncodeToString(append(mac.Sum(nil), data...))
}
func (s *Service) decodeCursor(token, kind, scope string) (string, error) {
	if token == "" {
		return "", nil
	}
	data, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(data) <= sha256.Size {
		return "", validation("Invalid NextToken")
	}
	mac := hmac.New(sha256.New, s.tokenKey)
	mac.Write(data[sha256.Size:])
	if !hmac.Equal(data[:sha256.Size], mac.Sum(nil)) {
		return "", validation("Invalid NextToken")
	}
	var value cursor
	if json.Unmarshal(data[sha256.Size:], &value) != nil || value.Kind != kind || value.Scope != scope {
		return "", validation("NextToken does not belong to this listing")
	}
	return value.After, nil
}
func pageLimit(limit *int) (int, error) {
	if limit == nil {
		return 100, nil
	}
	if *limit < 1 || *limit > 100 {
		return 0, validation("Limit must be 1..100")
	}
	return *limit, nil
}
func (s *Service) ListRules(input ListRulesInput) (ListRulesOutput, error) {
	result := ListRulesOutput{Rules: []Rule{}}
	if err := s.validateBus(input.EventBusName); err != nil {
		return result, err
	}
	if input.NamePrefix != "" && !namePattern.MatchString(input.NamePrefix) {
		return result, validation("Invalid NamePrefix")
	}
	limit, err := pageLimit(input.Limit)
	if err != nil {
		return result, err
	}
	after, err := s.decodeCursor(input.NextToken, "rules", input.NamePrefix)
	if err != nil {
		return result, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{}
	for name := range s.entries {
		if strings.HasPrefix(name, input.NamePrefix) && name > after {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names[:min(limit, len(names))] {
		result.Rules = append(result.Rules, s.entries[name].rule)
	}
	if len(names) > limit {
		result.NextToken = s.encodeCursor(cursor{"rules", input.NamePrefix, names[limit-1]})
	}
	return result, nil
}
func (s *Service) ListTargetsByRule(input ListTargetsInput) (ListTargetsOutput, error) {
	result := ListTargetsOutput{Targets: []Target{}}
	if err := s.validateRuleInput(RuleInput{Name: input.Rule, EventBusName: input.EventBusName}); err != nil {
		return result, err
	}
	limit, err := pageLimit(input.Limit)
	if err != nil {
		return result, err
	}
	after, err := s.decodeCursor(input.NextToken, "targets", input.Rule)
	if err != nil {
		return result, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.entries[input.Rule]
	if item == nil {
		return result, absent()
	}
	ids := []string{}
	for id := range item.targets {
		if id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids[:min(limit, len(ids))] {
		result.Targets = append(result.Targets, cloneTarget(item.targets[id]))
	}
	if len(ids) > limit {
		result.NextToken = s.encodeCursor(cursor{"targets", input.Rule, ids[limit-1]})
	}
	return result, nil
}
