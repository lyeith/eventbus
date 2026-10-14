package events

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type scheduledEvent struct {
	Version    string         `json:"version"`
	ID         string         `json:"id"`
	DetailType string         `json:"detail-type"`
	Source     string         `json:"source"`
	Account    string         `json:"account"`
	Time       string         `json:"time"`
	Region     string         `json:"region"`
	Resources  []string       `json:"resources"`
	Detail     map[string]any `json:"detail"`
}

func (s *Service) wait(ctx context.Context, duration time.Duration, changed <-chan struct{}) bool {
	timer := s.dev.Clock.NewTimer(max(0, duration))
	defer timer.Stop()
	select {
	case <-timer.C():
		return ctx.Err() == nil
	case <-changed:
		return false
	case <-ctx.Done():
		return false
	}
}
func (s *Service) run(name string, item *entry) {
	defer s.wg.Done()
	returned := false
	defer func() {
		if recover() != nil || !returned {
			s.recordEvidence(errors.New("EventBridge recurring intake ownership interrupted"))
		}
	}()
	s.runLoop(name, item)
	returned = true
}
func (s *Service) runLoop(name string, item *entry) {
	for s.ctx.Err() == nil {
		s.mu.Lock()
		if s.entries[name] != item || s.closing {
			s.mu.Unlock()
			return
		}
		enabled := item.rule.State == "ENABLED"
		due, signal := item.next, item.changed
		s.mu.Unlock()
		if !enabled {
			select {
			case <-signal:
				continue
			case <-s.ctx.Done():
				return
			}
		}
		if !s.wait(s.ctx, due.Sub(s.dev.Clock.Now()), signal) {
			continue
		}
		complete, allowed := s.beginSource(s.ctx, signal, name)
		if !allowed {
			continue
		}
		// Publish one owned occurrence. The entry gate will re-check current native
		// state before first delivery, so a mutation can fence an occurrence selected
		// just before its barrier. Missed dormant ticks coalesce rather than burst.
		s.mu.Lock()
		if s.entries[name] != item || s.closing || item.rule.State != "ENABLED" || !item.next.Equal(due) {
			s.mu.Unlock()
			if complete != nil {
				s.releaseSource(complete, nil)
			}
			continue
		}
		if s.pending >= s.dev.MaxPending {
			capacityChanged := s.capacityChanged
			s.mu.Unlock()
			if complete != nil {
				s.releaseSource(complete, nil)
			}
			// Capacity delays an occurrence instead of silently discarding it.
			// No source lifetime is retained while dormant behind this bound.
			select {
			case <-capacityChanged:
			case <-signal:
			case <-s.ctx.Done():
				return
			}
			continue
		}
		s.advanceLocked(item, due)
		s.pending++
		s.wg.Add(1)
		s.mu.Unlock()
		go s.deliver(name, item, complete)
	}
}
func (s *Service) advanceLocked(item *entry, due time.Time) {
	now := s.dev.Clock.Now()
	elapsed := now.Sub(due)
	periods := int64(1)
	if elapsed >= 0 {
		periods += int64(elapsed / item.interval)
	}
	item.next = due.Add(time.Duration(periods) * item.interval)
}

type targetFailure interface{ Retryable() bool }
type targetDelivery struct {
	target  Target
	payload []byte
	outcome Outcome
	failure error
}

func (s *Service) deliver(name string, item *entry, complete func(error)) {
	defer s.wg.Done()
	finished := false
	var ownershipErr error
	defer func() {
		if recover() != nil || !finished {
			ownershipErr = errors.New("EventBridge delivery ownership interrupted")
		}
		if ownershipErr != nil {
			s.recordEvidence(ownershipErr)
		}
		s.mu.Lock()
		s.pending--
		close(s.capacityChanged)
		s.capacityChanged = make(chan struct{})
		s.mu.Unlock()
		// All native publication is complete before the source lease is released.
		// Keep the waitgroup until release returns, so Close joins this finalizer.
		if complete != nil {
			s.releaseSource(complete, ownershipErr)
		}
	}()
	// Accepted EventBridge occurrences and retries are not canceled by later rule
	// mutations or service intake shutdown. Lambda owns accepted handler lifetime.
	ctx := s.deliveryCtx
	item.gate.Lock()
	locked := true
	defer func() {
		if locked {
			item.gate.Unlock()
		}
	}()
	s.mu.Lock()
	if s.closing || s.entries[name] != item || item.rule.State != "ENABLED" {
		s.mu.Unlock()
		finished = true
		return
	}
	rule := item.rule
	targets := make([]Target, 0, len(item.targets))
	for _, target := range item.targets {
		targets = append(targets, cloneTarget(target))
	}
	s.mu.Unlock()
	sort.Slice(targets, func(i, j int) bool { return targets[i].Id < targets[j].Id })
	now := s.dev.Clock.Now().UTC()
	eventID := uuid.NewString()
	event, _ := json.Marshal(scheduledEvent{"0", eventID, "Scheduled Event", "aws.events", s.account, now.Format(time.RFC3339), s.region, []string{rule.Arn}, map[string]any{}})
	deliveries := make([]targetDelivery, 0, len(targets))
	for _, target := range targets {
		delivery := targetDelivery{target: target, outcome: Outcome{RuleARN: rule.Arn, EventID: eventID, TargetID: target.Id, Status: "failed"}}
		payload, err := transformTarget(target, event, rule.Arn, rule.Name, now)
		if err != nil {
			delivery.outcome.Code = "InvalidTargetInput"
			deliveries = append(deliveries, delivery)
			continue
		}
		delivery.payload = payload
		delivery.outcome.Attempts = 1
		delivery.failure = s.invoker.AdmitTarget(ctx, target.Arn, payload)
		if delivery.failure == nil {
			delivery.outcome.Status = "accepted"
		} else {
			delivery.outcome.Code = "TargetAdmissionFailed"
		}
		deliveries = append(deliveries, delivery)
	}
	// Every first call has passed the mutation barrier. Retried occurrences retain
	// their original target and payload snapshot even after target removal.
	item.gate.Unlock()
	locked = false
	var targetsJoined sync.WaitGroup
	interrupted := make(chan error, len(deliveries))
	for index := range deliveries {
		targetsJoined.Add(1)
		go func(delivery *targetDelivery) {
			defer targetsJoined.Done()
			returned := false
			defer func() {
				if recover() != nil || !returned {
					interrupted <- errors.New("EventBridge target delivery ownership interrupted")
				}
			}()
			if delivery.failure != nil {
				s.retry(ctx, now, delivery)
			}
			if s.dev.Observe != nil {
				s.dev.Observe(delivery.outcome)
			}
			returned = true
		}(&deliveries[index])
	}
	targetsJoined.Wait()
	close(interrupted)
	for err := range interrupted {
		ownershipErr = errors.Join(ownershipErr, err)
	}
	finished = true
}
func (s *Service) retry(ctx context.Context, received time.Time, delivery *targetDelivery) {
	attempts, age := 185, 86400
	if policy := delivery.target.RetryPolicy; policy != nil {
		if policy.MaximumRetryAttempts != nil {
			attempts = *policy.MaximumRetryAttempts
		}
		if policy.MaximumEventAgeInSeconds != nil {
			age = *policy.MaximumEventAgeInSeconds
		}
	}
	for retry := 0; retry < attempts; retry++ {
		if ctx.Err() != nil {
			delivery.outcome.Status, delivery.outcome.Code = "canceled", "TargetAdmissionCanceled"
			return
		}
		var failure targetFailure
		if errors.As(delivery.failure, &failure) && !failure.Retryable() {
			return
		}
		delay := min(time.Second*time.Duration(1<<min(retry, 8)), 5*time.Minute)
		// EventBridge uses exponential backoff and jitter for retryable API delivery.
		delay = time.Duration(rand.Int64N(int64(delay/2)+1)) + delay/2
		remaining := received.Add(time.Duration(age) * time.Second).Sub(s.dev.Clock.Now())
		if remaining <= 0 {
			delivery.outcome.Code = "EventAgeExceeded"
			return
		}
		if !s.wait(ctx, min(delay, remaining), nil) {
			delivery.outcome.Status, delivery.outcome.Code = "canceled", "TargetAdmissionCanceled"
			return
		}
		if !s.dev.Clock.Now().Before(received.Add(time.Duration(age) * time.Second)) {
			delivery.outcome.Code = "EventAgeExceeded"
			return
		}
		delivery.outcome.Attempts++
		delivery.failure = s.invoker.AdmitTarget(ctx, delivery.target.Arn, delivery.payload)
		if delivery.failure == nil {
			delivery.outcome.Status, delivery.outcome.Code = "accepted", ""
			return
		}
	}
}
func (s *Service) recordEvidence(err error) {
	s.mu.Lock()
	s.evidenceErr = errors.Join(s.evidenceErr, err)
	s.mu.Unlock()
}

// Close fences recurring intake and joins every existing target admission/retry.
// Lambda must drain accepted asynchronous executions after this owner closes.
func (s *Service) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		s.cancel()
		go func() { s.wg.Wait(); s.deliveryCancel(); close(s.done) }()
	}
	s.mu.Unlock()
	select {
	case <-s.done:
	case <-ctx.Done():
		s.mu.Lock()
		select {
		case <-s.done:
		default:
			if s.closeErr == nil {
				s.closeErr = fmtCloseError(ctx.Err())
				s.evidenceErr = errors.Join(s.evidenceErr, s.closeErr)
			}
			// Permanent shutdown budget expiry aborts unaccepted API retry waits.
			// Real callbacks still join; already accepted events remain Lambda-owned.
			s.deliveryCancel()
		}
		s.mu.Unlock()
		<-s.done
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return errors.Join(s.closeErr, s.evidenceErr)
}
func fmtCloseError(err error) error {
	return errors.Join(errors.New("join EventBridge target admissions"), err)
}

// Release callback failures remain sticky, while the outer waitgroup still
// joins the callback even if it exits its goroutine without returning.
func (s *Service) releaseSource(complete func(error), ownershipErr error) {
	returned := false
	defer func() {
		if recover() != nil || !returned {
			s.recordEvidence(errors.New("EventBridge source release ownership interrupted"))
		}
	}()
	complete(ownershipErr)
	returned = true
}
