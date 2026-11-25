package messaging

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
)

// SNSRetryOptions configures local observation and optional test timing. Zero
// uses the protocol's native retry count and delay. Positive test overrides only
// shorten the retry budget and cannot add attempts.
type SNSRetryOptions struct {
	Activity        devactivity.Activity
	RetryDelayScale float64
	MaxAttempts     int
}

const (
	snsMaxPendingDeliveries = 4096
	snsMaxPendingBytes      = 128 << 20
	snsDeliveryWorkers      = 4
	snsAttemptTimeout       = 10 * time.Second
)

type snsRetryKey struct{ subscription, group string }

type snsDeliveryTask struct {
	input    SNSPublishInput
	result   SNSPublishResult
	plan     snsPlannedDelivery
	dedupID  string
	key      snsRetryKey
	size     int
	due      time.Time
	attempts int
	complete func(error)
}

// The first send to a native destination may complete inline. Retries and
// webhooks run outside the topic locks with bounded concurrency. A FIFO lane
// stays reserved for its head through retry delays and terminal DLQ settlement.
type snsRetryOwner struct {
	broker  *Broker
	options SNSRetryOptions
	ctx     context.Context
	cancel  context.CancelFunc

	mu                       sync.Mutex
	count, bytes             int
	pending                  []*snsDeliveryTask
	lanes                    map[snsRetryKey][]*snsDeliveryTask
	wake                     chan struct{}
	idle                     chan struct{}
	workers                  int
	running, sealed, discard bool
	evidenceErr              error

	closeOnce sync.Once
	closeErr  error
}

func newSNSRetryOwner(b *Broker, options SNSRetryOptions) *snsRetryOwner {
	ctx, cancel := context.WithCancel(context.Background())
	idle := make(chan struct{})
	close(idle)
	return &snsRetryOwner{broker: b, options: options, ctx: ctx, cancel: cancel, lanes: make(map[snsRetryKey][]*snsDeliveryTask), wake: make(chan struct{}, 1), idle: idle}
}

func (b *Broker) SetSNSRetryOptions(options SNSRetryOptions) error {
	if math.IsNaN(options.RetryDelayScale) || options.RetryDelayScale < 0 || options.RetryDelayScale > 1 || options.MaxAttempts < 0 {
		return errors.New("invalid local SNS retry options")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.snsRetry != nil {
		return errors.New("configure SNS retries before first delivery")
	}
	b.snsRetry = newSNSRetryOwner(b, options)
	return nil
}

func (b *Broker) snsRetryOwner() *snsRetryOwner {
	b.mu.RLock()
	owner := b.snsRetry
	b.mu.RUnlock()
	if owner != nil {
		return owner
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.snsRetry == nil {
		b.snsRetry = newSNSRetryOwner(b, SNSRetryOptions{})
	}
	return b.snsRetry
}

// DrainSNSDeliveries joins all current deliveries while downstream callback
// listeners and targets are available. It leaves admission open for callbacks.
func (b *Broker) DrainSNSDeliveries(ctx context.Context) error {
	return b.snsRetryOwner().wait(ctx)
}

// SNSDeliveriesIdle includes reserved work and completed worker finalizers.
func (b *Broker) SNSDeliveriesIdle() bool {
	owner := b.snsRetryOwner()
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.count == 0 && !owner.running && owner.workers == 0
}

// CloseSNSDeliveries seals admission and joins the owner. A deadline cancels
// outstanding attempts and discards pending work before returning the error.
func (b *Broker) CloseSNSDeliveries(ctx context.Context) error {
	owner := b.snsRetryOwner()
	owner.closeOnce.Do(func() {
		owner.mu.Lock()
		owner.sealed = true
		owner.mu.Unlock()
		owner.closeErr = owner.wait(ctx)
		if ctx.Err() != nil {
			owner.cancel()
			owner.discardPending()
			owner.closeErr = errors.Join(owner.closeErr, owner.wait(context.Background()))
		}
		owner.cancel()
	})
	return owner.closeErr
}

// DevDiscardPendingSNSDeliveries discards pending attempts without interrupting
// any active attempt. A new native synchronous attempt can still be accepted
// from a callback already admitted by the application's shutdown owner.
func (b *Broker) DevDiscardPendingSNSDeliveries() {
	b.snsRetryOwner().discardPending()
}

func (o *snsRetryOwner) discardPending() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.discard = true
	o.signalLocked()
}

func (o *snsRetryOwner) wait(ctx context.Context) error {
	for {
		o.mu.Lock()
		if o.count == 0 && !o.running && o.workers == 0 {
			err := o.evidenceErr
			o.mu.Unlock()
			return err
		}
		idle := o.idle
		o.mu.Unlock()
		select {
		case <-idle:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Admission budgets and optional activity leases are reserved before intent
// capture and any FIFO/archive commit. Abandoning a candidate has no effects.
func (o *snsRetryOwner) reserve(tasks []*snsDeliveryTask) error {
	if len(tasks) == 0 {
		return nil
	}
	size := 0
	for _, t := range tasks {
		if t.size > snsMaxPendingBytes-size {
			return snsInternal("SNS delivery capacity is full")
		}
		size += t.size
	}
	o.mu.Lock()
	if o.sealed || len(tasks) > snsMaxPendingDeliveries-o.count || size > snsMaxPendingBytes-o.bytes {
		o.mu.Unlock()
		return snsInternal("SNS delivery capacity is unavailable")
	}
	if o.count == 0 && !o.running && o.workers == 0 {
		o.idle = make(chan struct{})
	}
	o.count += len(tasks)
	o.bytes += size
	o.mu.Unlock()
	if o.options.Activity != nil {
		for _, t := range tasks {
			complete, err := o.options.Activity.BeginActivity("sns-delivery", t.result.MessageID)
			if err != nil || complete == nil {
				o.abandon(tasks)
				return snsInternal("SNS delivery ownership admission is unavailable")
			}
			t.complete = complete
		}
	}
	return nil
}

func (o *snsRetryOwner) abandon(tasks []*snsDeliveryTask) {
	for _, t := range tasks {
		o.finish(t, nil, false)
	}
}

func (o *snsRetryOwner) finish(t *snsDeliveryTask, evidenceErr error, submitted bool) {
	if t.complete != nil {
		t.complete(evidenceErr)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if evidenceErr != nil && o.evidenceErr == nil {
		o.evidenceErr = evidenceErr
	}
	if submitted && t.key != (snsRetryKey{}) {
		queued := o.lanes[t.key]
		if len(queued) == 0 {
			delete(o.lanes, t.key)
		} else {
			next := queued[0]
			queued[0] = nil
			o.lanes[t.key] = queued[1:]
			o.enqueueLocked(next, time.Time{})
		}
	}
	o.count--
	o.bytes -= t.size
	o.markIdleLocked()
}

func (o *snsRetryOwner) markIdleLocked() {
	if o.count == 0 && !o.running && o.workers == 0 {
		select {
		case <-o.idle:
		default:
			close(o.idle)
		}
	}
}

func (o *snsRetryOwner) submit(ctx context.Context, t *snsDeliveryTask) error {
	o.mu.Lock()
	if t.key != (snsRetryKey{}) {
		if queued, exists := o.lanes[t.key]; exists {
			o.lanes[t.key] = append(queued, t)
			o.mu.Unlock()
			return nil
		}
		o.lanes[t.key] = nil
	}
	if t.plan.webhook != nil {
		o.enqueueLocked(t, time.Time{})
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()
	return o.attempt(ctx, t, true)
}

func (o *snsRetryOwner) signalLocked() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *snsRetryOwner) enqueueLocked(t *snsDeliveryTask, due time.Time) {
	t.due = due
	o.pending = append(o.pending, t)
	if !o.running {
		o.running = true
		go o.run()
	}
	o.signalLocked()
}

func (o *snsRetryOwner) run() {
	o.mu.Lock()
	for {
		if len(o.pending) == 0 {
			o.running = false
			o.markIdleLocked()
			o.mu.Unlock()
			return
		}
		var next time.Time
		selected := -1
		if o.workers < snsDeliveryWorkers {
			now := time.Now()
			for i, p := range o.pending {
				if o.discard || !p.due.After(now) {
					selected = i
					break
				}
				if next.IsZero() || p.due.Before(next) {
					next = p.due
				}
			}
		}
		if selected != -1 {
			t := o.pending[selected]
			// Preserve enqueue order between equally due tasks.
			copy(o.pending[selected:], o.pending[selected+1:])
			o.pending[len(o.pending)-1] = nil
			o.pending = o.pending[:len(o.pending)-1]
			o.workers++
			go o.runAttempt(t)
			continue
		}
		o.mu.Unlock()
		if next.IsZero() {
			<-o.wake
		} else {
			timer := time.NewTimer(time.Until(next))
			select {
			case <-o.wake:
			case <-timer.C:
			}
			timer.Stop()
		}
		o.mu.Lock()
	}
}

func (o *snsRetryOwner) runAttempt(t *snsDeliveryTask) {
	ctx, cancel := context.WithTimeout(o.ctx, snsAttemptTimeout)
	_ = o.attempt(ctx, t, false)
	cancel()
	o.mu.Lock()
	o.workers--
	o.signalLocked()
	o.markIdleLocked()
	o.mu.Unlock()
}

func (o *snsRetryOwner) attempt(ctx context.Context, t *snsDeliveryTask, inline bool) error {
	o.mu.Lock()
	discarded := o.discard && !inline
	o.mu.Unlock()
	if discarded || o.ctx.Err() != nil {
		err := o.broker.snsCaptureTaskOutcome(t, "DeliveryCanceled", "canceled", "")
		o.finish(t, err, true)
		return err
	}
	if webhook := t.plan.webhook; webhook != nil {
		now := time.Now()
		if due := webhook.earliestAttempt(now); due.After(now) {
			o.mu.Lock()
			o.enqueueLocked(t, due)
			o.mu.Unlock()
			return nil
		}
	}
	t.attempts++
	err, evidenceErr := o.broker.snsAttempt(ctx, t)
	if err == nil {
		o.finish(t, evidenceErr, true)
		return evidenceErr
	}
	if t.retryable(err) && o.ctx.Err() == nil {
		if delay, ok := o.retryDelay(t); ok {
			o.mu.Lock()
			if !o.discard {
				o.enqueueLocked(t, time.Now().Add(delay))
				o.mu.Unlock()
				return nil
			}
			o.mu.Unlock()
			// A final shutdown policy may abandon the pending retry without
			// pretending that AWS's native retry budget was exhausted.
			evidenceErr = o.broker.snsCaptureTaskOutcome(t, "DeliveryCanceled", "canceled", "")
			o.finish(t, evidenceErr, true)
			return evidenceErr
		}
	}
	if o.ctx.Err() != nil {
		evidenceErr = o.broker.snsCaptureTaskOutcome(t, "DeliveryCanceled", "canceled", "")
	} else {
		evidenceErr = o.broker.snsFailDelivery(t, err)
	}
	o.finish(t, evidenceErr, true)
	return evidenceErr
}

func (t *snsDeliveryTask) retryable(err error) bool {
	if t.plan.failure != nil || t.plan.firehose != nil {
		return false
	}
	var classified interface{ Retryable() bool }
	if errors.As(err, &classified) {
		return classified.Retryable()
	}
	var queueError *sqsError
	if errors.As(err, &queueError) {
		switch queueError.Code {
		case "RequestThrottled", "ServiceUnavailable", "InternalError":
			return true
		}
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func (o *snsRetryOwner) retryDelay(t *snsDeliveryTask) (time.Duration, bool) {
	if limit := o.options.MaxAttempts; limit > 0 && t.attempts >= limit {
		return 0, false
	}
	var delay time.Duration
	var ok bool
	if t.plan.webhook != nil {
		delay, ok = t.plan.webhook.retryDelay(t.attempts)
	} else {
		delay, ok = snsManagedRetryDelay(t.attempts)
	}
	if !ok || delay <= 0 {
		return delay, ok
	}
	scale := o.options.RetryDelayScale
	if scale == 0 {
		scale = 1
	}
	// SNS jitters waits. Keep the published policy duration as the upper bound.
	return time.Duration(float64(delay) * scale * (0.8 + 0.2*rand.Float64())), true
}

// The first failure requests retry index one. The AWS managed delivery policy
// has three immediate, two one-second, ten exponential (1–20s) and 100,000
// twenty-second retries over roughly 23 days.
func snsManagedRetryDelay(retry int) (time.Duration, bool) {
	switch {
	case retry < 1 || retry > 100015:
		return 0, false
	case retry <= 3:
		return 0, true
	case retry <= 5:
		return time.Second, true
	case retry <= 15:
		return min(20*time.Second, time.Second<<uint(retry-6)), true
	default:
		return 20 * time.Second, true
	}
}
