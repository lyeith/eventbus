package events

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testARN = "arn:aws:lambda:us-east-1:000000000000:function:scheduled"

var initialTime = time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

type fakeClock struct {
	mu         sync.Mutex
	now        time.Time
	timers     map[*fakeTimer]time.Time
	registered chan time.Duration
}
type fakeTimer struct {
	clock *fakeClock
	ch    chan time.Time
}

func newClock() *fakeClock {
	return &fakeClock{now: initialTime, timers: map[*fakeTimer]time.Time{}, registered: make(chan time.Duration, 100)}
}
func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) NewTimer(duration time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeTimer{c, make(chan time.Time, 1)}
	if duration <= 0 {
		timer.ch <- c.now
	} else {
		c.timers[timer] = c.now.Add(duration)
	}
	c.registered <- duration
	return timer
}
func (t *fakeTimer) C() <-chan time.Time { return t.ch }
func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	_, ok := t.clock.timers[t]
	delete(t.clock.timers, t)
	return ok
}
func (c *fakeClock) advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
	for timer, due := range c.timers {
		if !due.After(c.now) {
			timer.ch <- c.now
			delete(c.timers, timer)
		}
	}
}

type invoker struct {
	validate func(context.Context, string) error
	admit    func(context.Context, string, []byte) error
}

func (i invoker) ValidateTarget(ctx context.Context, arn string) error {
	if i.validate != nil {
		return i.validate(ctx, arn)
	}
	return ctx.Err()
}
func (i invoker) AdmitTarget(ctx context.Context, arn string, payload []byte) error {
	if i.admit != nil {
		return i.admit(ctx, arn, payload)
	}
	return ctx.Err()
}

type admissionError bool

func (e admissionError) Error() string   { return "private admission failure" }
func (e admissionError) Retryable() bool { return bool(e) }
func serviceFor(t *testing.T, clock *fakeClock, inv TargetInvoker, dev DevOptions) *Service {
	t.Helper()
	dev.Clock = clock
	service, err := New(Options{Region: "us-east-1", AccountID: "000000000000", Dev: dev}, inv)
	require.NoError(t, err)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	return service
}
func putRule(t *testing.T, s *Service, name string) string {
	t.Helper()
	arn, err := s.PutRule(t.Context(), PutRuleInput{Name: name, ScheduleExpression: "rate(1 minute)"})
	require.NoError(t, err)
	return arn
}
func putTarget(t *testing.T, s *Service, name string, target Target) {
	t.Helper()
	if target.Id == "" {
		target.Id = "target"
	}
	if target.Arn == "" {
		target.Arn = testARN
	}
	result, err := s.PutTargets(t.Context(), PutTargetsInput{Rule: name, Targets: []Target{target}})
	require.NoError(t, err)
	require.Zero(t, result.FailedEntryCount)
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("owned component did not reach expected boundary")
		var value T
		return value
	}
}
func cleanupRelease(t *testing.T, gate chan struct{}) func() {
	t.Helper()
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return release
}
func nativeError(t *testing.T, err error, code string) {
	t.Helper()
	var failure *APIError
	require.ErrorAs(t, err, &failure)
	assert.Equal(t, code, failure.Code)
	assert.Equal(t, 400, failure.Status)
}
func pointer[T any](v T) *T { return &v }

func TestRateContractMinimumUnitsAndOverflow(t *testing.T) {
	for _, test := range []struct {
		expression string
		duration   time.Duration
	}{
		{"rate(1 minute)", time.Minute}, {"rate(2 minutes)", 2 * time.Minute}, {"rate(1 hour)", time.Hour}, {"rate(3 hours)", 3 * time.Hour}, {"rate(1 day)", 24 * time.Hour}, {"rate(2 days)", 48 * time.Hour},
	} {
		duration, err := rateDuration(test.expression)
		require.NoError(t, err)
		assert.Equal(t, test.duration, duration)
	}
	for _, expression := range []string{"rate(0 minutes)", "rate(1 minutes)", "rate(2 minute)", "rate(1 second)", "rate(1 hours)", "rate(2 day)", "rate(1.5 minutes)", "rate(-1 minute)", "rate(18446744073709551615 days)", "cron(0 0 * * ? *)", "at(2030-01-02T03:04:05)"} {
		_, err := rateDuration(expression)
		nativeError(t, err, "ValidationException")
	}
}
func TestRuleUpdatesRetainTargetsAndReplaceOmittedMetadata(t *testing.T) {
	clock := newClock()
	s := serviceFor(t, clock, invoker{}, DevOptions{})
	arn, err := s.PutRule(t.Context(), PutRuleInput{Name: "owned", Description: "prior", ScheduleExpression: "rate(1 minute)", State: "DISABLED", EventBusName: "arn:aws:events:us-east-1:000000000000:event-bus/default"})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:events:us-east-1:000000000000:rule/owned", arn)
	target := Target{Id: "target", Arn: testARN, Input: pointer(`{"original":true}`), RetryPolicy: &RetryPolicy{MaximumRetryAttempts: pointer(2)}}
	putTarget(t, s, "owned", target)
	*target.Input = "{}"
	*target.RetryPolicy.MaximumRetryAttempts = 0
	_, err = s.PutRule(t.Context(), PutRuleInput{Name: "owned", ScheduleExpression: "rate(2 minutes)"})
	require.NoError(t, err)
	rule, err := s.DescribeRule(RuleInput{Name: "owned"})
	require.NoError(t, err)
	assert.Equal(t, "", rule.Description)
	assert.Equal(t, "ENABLED", rule.State)
	assert.Equal(t, "default", rule.EventBusName)
	targets, err := s.ListTargetsByRule(ListTargetsInput{Rule: "owned"})
	require.NoError(t, err)
	require.Len(t, targets.Targets, 1)
	assert.Equal(t, `{"original":true}`, *targets.Targets[0].Input)
	assert.Equal(t, 2, *targets.Targets[0].RetryPolicy.MaximumRetryAttempts)
	*targets.Targets[0].Input = "changed"
	*targets.Targets[0].RetryPolicy.MaximumRetryAttempts = 100
	targets, err = s.ListTargetsByRule(ListTargetsInput{Rule: "owned"})
	require.NoError(t, err)
	assert.Equal(t, 2, *targets.Targets[0].RetryPolicy.MaximumRetryAttempts)
	putTarget(t, s, "owned", Target{Id: "target", Arn: testARN})
	targets, err = s.ListTargetsByRule(ListTargetsInput{Rule: "owned"})
	require.NoError(t, err)
	assert.Nil(t, targets.Targets[0].Input)
	assert.Nil(t, targets.Targets[0].RetryPolicy)
	nativeError(t, s.DeleteRule(t.Context(), RuleInput{Name: "owned"}), "ValidationException")
	removed, err := s.RemoveTargets(t.Context(), RemoveTargetsInput{Rule: "owned", Ids: []string{"target", "absent"}, Force: true})
	require.NoError(t, err)
	assert.Empty(t, removed.FailedEntries)
	require.NoError(t, s.DeleteRule(t.Context(), RuleInput{Name: "owned", Force: true}))
	require.NoError(t, s.DeleteRule(t.Context(), RuleInput{Name: "owned"}))
	_, err = s.DescribeRule(RuleInput{Name: "owned"})
	nativeError(t, err, "ResourceNotFoundException")
}
func TestRuleAndTargetUnsupportedContractsHaveNoMutation(t *testing.T) {
	s := serviceFor(t, newClock(), invoker{validate: func(_ context.Context, arn string) error {
		if arn == testARN {
			return nil
		}
		return errors.New("private registration")
	}}, DevOptions{})
	for _, input := range []PutRuleInput{
		{Name: "bad/name", ScheduleExpression: "rate(1 minute)"},
		{Name: "owned", ScheduleExpression: "rate(1 minute)", EventBusName: "other"},
		{Name: "owned", ScheduleExpression: "rate(1 minute)", EventPattern: pointer("{}")},
		{Name: "owned", ScheduleExpression: "rate(1 minute)", RoleArn: pointer("arn:aws:iam::000000000000:role/unsupported")},
		{Name: "owned", ScheduleExpression: "rate(1 minute)", Tags: json.RawMessage("[]")},
		{Name: "owned", ScheduleExpression: "rate(1 minute)", State: "ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS"},
	} {
		_, err := s.PutRule(t.Context(), input)
		nativeError(t, err, "ValidationException")
	}
	putRule(t, s, "owned")
	for _, target := range []Target{
		{Id: "target", Arn: testARN, Input: pointer("{}"), InputPath: pointer("$.detail")},
		{Id: "target", Arn: testARN, RoleArn: pointer("role")},
		{Id: "target", Arn: testARN, DeadLetterConfig: json.RawMessage("{}")},
		{Id: "target", Arn: testARN, RetryPolicy: &RetryPolicy{MaximumRetryAttempts: pointer(186)}},
		{Id: "target", Arn: testARN, RetryPolicy: &RetryPolicy{MaximumEventAgeInSeconds: pointer(59)}},
	} {
		_, err := s.PutTargets(t.Context(), PutTargetsInput{Rule: "owned", Targets: []Target{{Id: "good", Arn: testARN}, target}})
		nativeError(t, err, "ValidationException")
		out, err := s.ListTargetsByRule(ListTargetsInput{Rule: "owned"})
		require.NoError(t, err)
		assert.Empty(t, out.Targets)
	}
	result, err := s.PutTargets(t.Context(), PutTargetsInput{Rule: "owned", Targets: []Target{{Id: "good", Arn: testARN}, {Id: "missing", Arn: testARN + "Missing"}, {Id: "other", Arn: "arn:aws:sqs:us-east-1:000000000000:unsupported"}}})
	require.NoError(t, err)
	require.Equal(t, 2, result.FailedEntryCount)
	assert.Equal(t, "ResourceNotFoundException", result.FailedEntries[0].ErrorCode)
	assert.Equal(t, "ValidationException", result.FailedEntries[1].ErrorCode)
	out, err := s.ListTargetsByRule(ListTargetsInput{Rule: "owned"})
	require.NoError(t, err)
	require.Len(t, out.Targets, 1)
	assert.Equal(t, "good", out.Targets[0].Id)
	for _, id := range []string{"2", "3", "4", "5"} {
		putTarget(t, s, "owned", Target{Id: id, Arn: testARN})
	}
	result, err = s.PutTargets(t.Context(), PutTargetsInput{Rule: "owned", Targets: []Target{{Id: "6", Arn: testARN}, {Id: "good", Arn: testARN, Input: pointer("{}")}}})
	require.NoError(t, err)
	require.Equal(t, 1, result.FailedEntryCount)
	assert.Equal(t, "LimitExceededException", result.FailedEntries[0].ErrorCode)
}
func TestListsScopeDetachedPaginationToOperationAndInstance(t *testing.T) {
	s := serviceFor(t, newClock(), invoker{}, DevOptions{})
	for _, name := range []string{"prefix-c", "prefix-a", "prefix-b", "other"} {
		putRule(t, s, name)
	}
	first, err := s.ListRules(ListRulesInput{NamePrefix: "prefix-", Limit: pointer(1)})
	require.NoError(t, err)
	require.Len(t, first.Rules, 1)
	assert.Equal(t, "prefix-a", first.Rules[0].Name)
	require.NotEmpty(t, first.NextToken)
	second, err := s.ListRules(ListRulesInput{NamePrefix: "prefix-", Limit: pointer(2), NextToken: first.NextToken})
	require.NoError(t, err)
	require.Len(t, second.Rules, 2)
	assert.Equal(t, "prefix-b", second.Rules[0].Name)
	assert.Empty(t, second.NextToken)
	_, err = s.ListRules(ListRulesInput{NextToken: first.NextToken})
	nativeError(t, err, "ValidationException")
	other := serviceFor(t, newClock(), invoker{}, DevOptions{})
	_, err = other.ListRules(ListRulesInput{NamePrefix: "prefix-", NextToken: first.NextToken})
	nativeError(t, err, "ValidationException")
	for _, id := range []string{"c", "a", "b"} {
		putTarget(t, s, "prefix-a", Target{Id: id})
	}
	targets, err := s.ListTargetsByRule(ListTargetsInput{Rule: "prefix-a", Limit: pointer(1)})
	require.NoError(t, err)
	require.NotEmpty(t, targets.NextToken)
	assert.Equal(t, "a", targets.Targets[0].Id)
	_, err = s.ListTargetsByRule(ListTargetsInput{Rule: "prefix-b", NextToken: targets.NextToken})
	nativeError(t, err, "ValidationException")
	_, err = s.ListRules(ListRulesInput{NextToken: targets.NextToken})
	nativeError(t, err, "ValidationException")
}
func TestScheduledOccurrencesCreateNativeIdentityAtFiringAndStopAfterMutation(t *testing.T) {
	clock := newClock()
	payloads := make(chan []byte, 10)
	outcomes := make(chan Outcome, 10)
	s := serviceFor(t, clock, invoker{admit: func(ctx context.Context, _ string, payload []byte) error {
		require.NoError(t, ctx.Err())
		payloads <- append([]byte(nil), payload...)
		return nil
	}}, DevOptions{Observe: func(outcome Outcome) { outcomes <- outcome }})
	arn := putRule(t, s, "recurring")
	putTarget(t, s, "recurring", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	first := await(t, payloads)
	await(t, outcomes)
	var event scheduledEvent
	require.NoError(t, json.Unmarshal(first, &event))
	assert.Equal(t, "0", event.Version)
	require.NotEmpty(t, event.ID)
	assert.Equal(t, "Scheduled Event", event.DetailType)
	assert.Equal(t, "aws.events", event.Source)
	assert.Equal(t, []string{arn}, event.Resources)
	assert.Empty(t, event.Detail)
	assert.Equal(t, initialTime.Add(time.Minute).Format(time.RFC3339), event.Time)
	clock.advance(time.Minute)
	second := await(t, payloads)
	await(t, outcomes)
	var another scheduledEvent
	require.NoError(t, json.Unmarshal(second, &another))
	assert.NotEqual(t, event.ID, another.ID)
	assert.Equal(t, initialTime.Add(2*time.Minute).Format(time.RFC3339), another.Time)
	require.NoError(t, s.DisableRule(t.Context(), RuleInput{Name: "recurring"}))
	clock.advance(2 * time.Minute)
	require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.pending == 0 }, time.Second, time.Millisecond)
	select {
	case <-payloads:
		t.Fatal("disabled rule admitted a new occurrence")
	default:
	}
	require.NoError(t, s.EnableRule(t.Context(), RuleInput{Name: "recurring"}))
	clock.advance(time.Minute)
	await(t, payloads)
	await(t, outcomes)
	_, err := s.RemoveTargets(t.Context(), RemoveTargetsInput{Rule: "recurring", Ids: []string{"target"}})
	require.NoError(t, err)
	clock.advance(time.Minute)
	require.NoError(t, s.DeleteRule(t.Context(), RuleInput{Name: "recurring"}))
	clock.advance(5 * time.Minute)
	require.NoError(t, s.Close(t.Context()))
	select {
	case <-payloads:
		t.Fatal("removed target or deleted rule admitted a new occurrence")
	default:
	}
}
func TestMutationBarrierJoinsCurrentFirstAdmissionWithoutHoldingRegistry(t *testing.T) {
	clock := newClock()
	entered, release := make(chan struct{}), make(chan struct{})
	outcomes := make(chan Outcome, 1)
	s := serviceFor(t, clock, invoker{admit: func(ctx context.Context, _ string, _ []byte) error { close(entered); <-release; return ctx.Err() }}, DevOptions{Observe: func(out Outcome) { outcomes <- out }})
	releaseCallback := cleanupRelease(t, release)
	putRule(t, s, "held")
	putTarget(t, s, "held", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	await(t, entered)
	disabled := make(chan error, 1)
	go func() { disabled <- s.DisableRule(context.Background(), RuleInput{Name: "held"}) }()
	rule, err := s.DescribeRule(RuleInput{Name: "held"})
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)
	select {
	case err := <-disabled:
		t.Fatalf("mutation crossed active admission: %v", err)
	default:
	}
	releaseCallback()
	require.NoError(t, await(t, disabled))
	assert.Equal(t, "accepted", await(t, outcomes).Status)
	clock.advance(time.Hour)
	require.NoError(t, s.Close(t.Context()))
}
func TestAcceptedAdmissionRetryRetainsPayloadAcrossDisableRemovalAndClose(t *testing.T) {
	clock := newClock()
	initial := make(chan []byte, 1)
	retry := make(chan []byte, 1)
	release := make(chan struct{})
	outcomes := make(chan Outcome, 1)
	var mu sync.Mutex
	calls := 0
	s := serviceFor(t, clock, invoker{admit: func(ctx context.Context, _ string, payload []byte) error {
		mu.Lock()
		calls++
		attempt := calls
		mu.Unlock()
		if attempt == 1 {
			initial <- append([]byte(nil), payload...)
			return admissionError(true)
		}
		retry <- append([]byte(nil), payload...)
		<-release
		return ctx.Err()
	}}, DevOptions{Observe: func(out Outcome) { outcomes <- out }})
	releaseCallback := cleanupRelease(t, release)
	putRule(t, s, "retry")
	putTarget(t, s, "retry", Target{RetryPolicy: &RetryPolicy{MaximumRetryAttempts: pointer(1)}})
	await(t, clock.registered)
	clock.advance(time.Minute)
	original := await(t, initial)
	// Wait for the retry timer, irrespective of the next cadence timer order.
	for duration := await(t, clock.registered); duration >= time.Minute; duration = await(t, clock.registered) {
	}
	require.NoError(t, s.DisableRule(t.Context(), RuleInput{Name: "retry"}))
	_, err := s.RemoveTargets(t.Context(), RemoveTargetsInput{Rule: "retry", Ids: []string{"target"}})
	require.NoError(t, err)
	require.NoError(t, s.DeleteRule(t.Context(), RuleInput{Name: "retry"}))
	closing := make(chan error, 1)
	go func() { closing <- s.Close(context.Background()) }()
	clock.advance(time.Second)
	retried := await(t, retry)
	assert.JSONEq(t, string(original), string(retried))
	select {
	case err := <-closing:
		t.Fatalf("close returned before accepted retry joined: %v", err)
	default:
	}
	releaseCallback()
	require.NoError(t, await(t, closing))
	out := await(t, outcomes)
	assert.Equal(t, 2, out.Attempts)
	assert.Equal(t, "accepted", out.Status)
	require.NoError(t, s.DevEvidence())
}
func TestCloseCancellationStillJoinsCallbackAndRetainsUncertainty(t *testing.T) {
	clock := newClock()
	entered, release := make(chan struct{}), make(chan struct{})
	s := serviceFor(t, clock, invoker{admit: func(ctx context.Context, _ string, _ []byte) error {
		close(entered)
		<-release
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		return ctx.Err()
	}}, DevOptions{})
	releaseCallback := cleanupRelease(t, release)
	putRule(t, s, "held")
	putTarget(t, s, "held", Target{})
	await(t, clock.registered)
	clock.advance(time.Minute)
	await(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closed := make(chan error, 1)
	go func() { closed <- s.Close(ctx) }()
	require.Eventually(t, func() bool { return s.DevEvidence() != nil }, time.Second, time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("close returned before callback join: %v", err)
	default:
	}
	releaseCallback()
	require.ErrorIs(t, await(t, closed), context.Canceled)
	require.ErrorIs(t, s.Close(context.Background()), context.Canceled)
}
func TestInterruptedDeliveryOwnershipIsSticky(t *testing.T) {
	for _, interrupt := range []func(){func() { panic("private panic") }, runtime.Goexit} {
		clock := newClock()
		s := serviceFor(t, clock, invoker{admit: func(context.Context, string, []byte) error { interrupt(); return nil }}, DevOptions{})
		putRule(t, s, "owned")
		putTarget(t, s, "owned", Target{})
		await(t, clock.registered)
		clock.advance(time.Minute)
		require.Eventually(t, func() bool { return s.DevEvidence() != nil }, time.Second, time.Millisecond)
		err := s.Close(context.Background())
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "private")
	}
}
func TestTargetRetryFailureDoesNotDelayOtherTargetRetry(t *testing.T) {
	clock := newClock()
	blocked := make(chan struct{})
	release := make(chan struct{})
	ready := make(chan Outcome, 1)
	var mu sync.Mutex
	calls := map[string]int{}
	s := serviceFor(t, clock, invoker{admit: func(_ context.Context, _ string, payload []byte) error {
		label := string(payload)
		mu.Lock()
		calls[label]++
		attempt := calls[label]
		mu.Unlock()
		if attempt == 1 {
			return admissionError(true)
		}
		if label == `"blocked"` {
			close(blocked)
			<-release
		}
		return nil
	}}, DevOptions{Observe: func(out Outcome) {
		if out.TargetID == "b" {
			ready <- out
		}
	}})
	releaseCallback := cleanupRelease(t, release)
	putRule(t, s, "parallel")
	putTarget(t, s, "parallel", Target{Id: "a", Input: pointer(`"blocked"`), RetryPolicy: &RetryPolicy{MaximumRetryAttempts: pointer(1)}})
	putTarget(t, s, "parallel", Target{Id: "b", Input: pointer(`"ready"`), RetryPolicy: &RetryPolicy{MaximumRetryAttempts: pointer(1)}})
	await(t, clock.registered)
	clock.advance(time.Minute)
	retries := 0
	for retries < 2 {
		duration := await(t, clock.registered)
		if duration < time.Minute {
			retries++
		}
	}
	clock.advance(time.Second)
	await(t, blocked)
	outcome := await(t, ready)
	assert.Equal(t, "accepted", outcome.Status)
	assert.Equal(t, 2, outcome.Attempts)
	releaseCallback()
	require.NoError(t, s.Close(t.Context()))
}
