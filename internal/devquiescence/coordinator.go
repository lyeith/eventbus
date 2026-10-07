// Package devquiescence owns the opt-in, process-exclusive retained-suite fence.
// It joins tracked ownership, not business success, and owns no service runtime.
package devquiescence

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
)

type State string

const (
	Open                 State = "open"
	Draining             State = "draining"
	Held                 State = "held"
	Shutdown             State = "shutdown"
	maxVisibleActivities       = 64
)

var (
	ErrFenced     = errors.New("retained-owner admission is fenced")
	ErrNotSafe    = errors.New("retained owner is not held and fixture-safe")
	ErrGeneration = errors.New("retained-owner generation does not match")
	ErrShutdown   = errors.New("retained owner is shutting down")
	ErrEvidence   = errors.New("retained-owner ownership evidence is incomplete")
)

// DrainHook requests an instantaneous reversible service transition. Start
// requests flushing already accepted work; neither callback joins service work.
// Both callbacks run outside the coordinator lock with a tracked transition.
// They must not block waiting for quiescence or call Quiesce/Resume themselves.
type DrainHook struct {
	Start  func() error
	Resume func() error
}

type Options struct {
	Checks          []func() error
	DrainHooks      []DrainHook
	MaxSourceLeases int
}

type Activity struct {
	ID        uint64    `json:"id"`
	Kind      string    `json:"kind"`
	RequestID string    `json:"request_id,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

type Snapshot struct {
	SchemaVersion      string     `json:"schema_version"`
	OwnerID            string     `json:"owner_id"`
	CallbackOrigin     string     `json:"callback_origin,omitempty"`
	State              State      `json:"state"`
	Generation         uint64     `json:"generation"`
	FixtureSafe        bool       `json:"fixture_safe"`
	WorkCount          int        `json:"work_count"`
	CleanupEnvelopes   int        `json:"cleanup_envelopes"`
	Activities         []Activity `json:"activities"`
	UnlistedActivities int        `json:"unlisted_activities"`
	EvidenceFailure    string     `json:"evidence_failure,omitempty"`
	LastTimeout        string     `json:"last_timeout,omitempty"`
}

type Coordinator struct {
	mu                       sync.Mutex
	state                    State
	generation, next         uint64
	work, cleanup            int
	transitions              int
	closing, evidenceFailure bool
	drainStarted, resuming   bool
	used                     bool
	ownerID, callbackOrigin  string
	evidenceCode             string
	lastTimeout              string
	activities               map[uint64]Activity
	checks                   []func() error
	hooks                    []DrainHook
	wake, sourceFence        chan struct{}
	sourceFenced             bool
	maxSourceLeases          int
	sourceLeases             map[string]*sourceLease
	retiredSourceLeases      map[sourceLeaseIdentity]sourceLeaseReceiptState
}

func New(checks ...func() error) *Coordinator {
	return NewWithOptions(Options{Checks: checks})
}

func NewWithOptions(options Options) *Coordinator {
	capacity := options.MaxSourceLeases
	if capacity < 1 {
		capacity = 4096
	}
	return &Coordinator{state: Open, generation: 1, ownerID: uuid.NewString(),
		activities: make(map[uint64]Activity), wake: make(chan struct{}), sourceFence: make(chan struct{}),
		checks: append([]func() error(nil), options.Checks...), hooks: append([]DrainHook(nil), options.DrainHooks...),
		maxSourceLeases: capacity, sourceLeases: make(map[string]*sourceLease),
		retiredSourceLeases: make(map[sourceLeaseIdentity]sourceLeaseReceiptState)}
}

var safeIdentity = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var safeKind = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func (c *Coordinator) wakeLocked() { close(c.wake); c.wake = make(chan struct{}) }

func (c *Coordinator) fenceSourcesLocked() {
	if c.state == Open {
		c.state = Draining
	}
	if !c.sourceFenced {
		close(c.sourceFence)
		c.sourceFenced = true
	}
	c.wakeLocked()
}

func (c *Coordinator) failEvidenceLocked(code string) {
	c.evidenceFailure = true
	if c.evidenceCode == "" {
		c.evidenceCode = code
	}
	if c.state == Open {
		c.fenceSourcesLocked()
	} else {
		c.wakeLocked()
	}
}

func (c *Coordinator) addWorkLocked(kind, requestID string) uint64 {
	c.next++
	id := c.next
	c.work++
	if !safeKind.MatchString(kind) {
		kind = "activity"
	}
	if !safeIdentity.MatchString(requestID) {
		requestID = ""
	}
	if len(c.activities) < maxVisibleActivities {
		c.activities[id] = Activity{ID: id, Kind: kind, RequestID: requestID, StartedAt: time.Now().UTC()}
	}
	c.wakeLocked()
	return id
}

func (c *Coordinator) finishWorkLocked(id uint64, evidenceErr error) {
	if evidenceErr != nil {
		c.failEvidenceLocked("incomplete_ownership_evidence")
	}
	c.work--
	delete(c.activities, id)
	c.wakeLocked()
}

func (c *Coordinator) beginWorkLocked(kind, requestID string) func(error) {
	id := c.addWorkLocked(kind, requestID)
	var once sync.Once
	return func(evidenceErr error) {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.finishWorkLocked(id, evidenceErr)
		})
	}
}

func (c *Coordinator) admissionErrorLocked() error {
	if c.closing {
		return ErrShutdown
	}
	if c.evidenceFailure {
		return ErrEvidence
	}
	return ErrFenced
}

func (c *Coordinator) sourceOpenLocked() bool {
	return c.state == Open && !c.closing && !c.evidenceFailure && !c.resuming
}

func (c *Coordinator) descendantsAllowedLocked() bool {
	return c.sourceOpenLocked() || c.state == Draining && !c.resuming && c.work > c.transitions
}

// BeginSource admits a new autonomous root only in the healthy open epoch.
// Success changed closes only when intake is fenced. Refusal returns the current
// all-work change signal atomically, so callers can park without losing wakes.
func (c *Coordinator) BeginSource(kind, requestID string) (func(error), <-chan struct{}, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = true
	if c.sourceOpenLocked() {
		return c.beginWorkLocked(kind, requestID), c.sourceFence, nil
	}
	return nil, c.wake, c.admissionErrorLocked()
}

// BeginActivity transfers one whole accepted descendant/task lifetime into this
// owner. Retries retain the same lease. Completion denotes ownership/evidence,
// never ordinary business success or failure. Transition hooks alone cannot
// admit unrelated descendants.
func (c *Coordinator) BeginActivity(kind, requestID string) (func(error), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = true
	if c.descendantsAllowedLocked() {
		return c.beginWorkLocked(kind, requestID), nil
	}
	return nil, c.admissionErrorLocked()
}

// BeginCleanup starts one app-validated exact cleanup root in the held epoch.
// Its received CleanupOnly envelope may already be counted. Sources stay fenced;
// descendants are admitted until joined, then an explicit Quiesce re-establishes
// the held evidence barrier before fixture attestation or Resume.
func (c *Coordinator) BeginCleanup(expectedGeneration uint64, kind, requestID string) (func(error), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = true
	if c.closing {
		return nil, ErrShutdown
	}
	if c.generation != expectedGeneration {
		return nil, ErrGeneration
	}
	if c.evidenceFailure {
		return nil, ErrEvidence
	}
	if c.state != Held || c.work != 0 || c.resuming {
		return nil, ErrNotSafe
	}
	c.state = Draining
	return c.beginWorkLocked(kind, requestID), nil
}

func (c *Coordinator) snapshotLocked() Snapshot {
	state := c.state
	if c.closing {
		state = Shutdown
	}
	result := Snapshot{SchemaVersion: "eventbus.retained-owner.v1", OwnerID: c.ownerID, CallbackOrigin: c.callbackOrigin,
		State: state, Generation: c.generation,
		FixtureSafe: c.state == Held && !c.closing && !c.evidenceFailure && !c.resuming && c.work == 0 && c.cleanup == 0,
		WorkCount:   c.work, CleanupEnvelopes: c.cleanup, Activities: make([]Activity, 0, len(c.activities)), LastTimeout: c.lastTimeout}
	for _, activity := range c.activities {
		result.Activities = append(result.Activities, activity)
	}
	sort.Slice(result.Activities, func(i, j int) bool { return result.Activities[i].ID < result.Activities[j].ID })
	result.UnlistedActivities = c.work - len(result.Activities)
	if c.evidenceFailure {
		result.EvidenceFailure = c.evidenceCode
	}
	return result
}

func (c *Coordinator) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.snapshotLocked()
}

func (c *Coordinator) markUsed() {
	c.mu.Lock()
	c.used = true
	c.mu.Unlock()
}

// checkEvidence uses immutable startup dependencies without holding c.mu.
func (c *Coordinator) checkEvidence() error {
	var evidenceErr error
	for _, check := range c.checks {
		if check != nil {
			evidenceErr = errors.Join(evidenceErr, safeCallback(check))
		}
	}
	return evidenceErr
}

func safeCallback(callback func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrEvidence
		}
	}()
	return callback()
}

func (c *Coordinator) runHooks(resume bool) error {
	var result error
	for _, hook := range c.hooks {
		callback := hook.Start
		if resume {
			callback = hook.Resume
		}
		if callback != nil {
			result = errors.Join(result, safeCallback(callback))
		}
	}
	return result
}

func (c *Coordinator) hasHooks(resume bool) bool {
	for _, hook := range c.hooks {
		if resume && hook.Resume != nil || !resume && hook.Start != nil {
			return true
		}
	}
	return false
}

func (c *Coordinator) beginTransitionLocked(kind string) uint64 {
	c.transitions++
	return c.addWorkLocked(kind, "")
}

func (c *Coordinator) finishTransitionLocked(id uint64, evidenceErr error) {
	c.transitions--
	c.finishWorkLocked(id, evidenceErr)
}

// Quiesce fences sources before waiting and keeps accepted callback chains live.
// Cancellation never cancels business work or opens the fence. Retry explicitly
// after a timeout; sticky evidence failures can never grant cleanup or resume.
func (c *Coordinator) Quiesce(ctx context.Context) (Snapshot, error) {
	c.mu.Lock()
	c.used = true
	generation := c.generation
	if c.resuming {
		result := c.snapshotLocked()
		c.mu.Unlock()
		return result, ErrNotSafe
	}
	if c.state == Open {
		c.fenceSourcesLocked()
	}
	if !c.drainStarted && c.state != Held {
		c.drainStarted = true
		if c.hasHooks(false) {
			id := c.beginTransitionLocked("dev.drain")
			c.mu.Unlock()
			err := c.runHooks(false)
			c.mu.Lock()
			c.finishTransitionLocked(id, err)
		}
	}
	for {
		if c.generation != generation {
			result := c.snapshotLocked()
			c.mu.Unlock()
			return result, ErrGeneration
		}
		if c.resuming {
			result := c.snapshotLocked()
			c.mu.Unlock()
			return result, ErrNotSafe
		}
		if c.evidenceFailure && c.work == 0 && c.cleanup == 0 {
			result := c.snapshotLocked()
			c.mu.Unlock()
			return result, ErrEvidence
		}
		if c.state == Held && c.work == 0 && c.cleanup == 0 {
			result := c.snapshotLocked()
			c.mu.Unlock()
			return result, nil
		}
		if err := ctx.Err(); err != nil {
			c.lastTimeout = "canceled"
			if errors.Is(err, context.DeadlineExceeded) {
				c.lastTimeout = "deadline_exceeded"
			}
			result := c.snapshotLocked()
			c.mu.Unlock()
			return result, err
		}
		if c.work == 0 && c.cleanup == 0 {
			c.mu.Unlock()
			evidenceErr := c.checkEvidence()
			c.mu.Lock()
			if evidenceErr != nil {
				c.failEvidenceLocked("evidence_unavailable")
			}
			if c.generation != generation || c.resuming || c.evidenceFailure || c.work != 0 || c.cleanup != 0 {
				continue
			}
			if err := ctx.Err(); err != nil {
				continue
			}
			c.state = Held
			c.lastTimeout = ""
			c.wakeLocked()
			result := c.snapshotLocked()
			c.mu.Unlock()
			return result, nil
		}
		wake := c.wake
		c.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
		}
		c.mu.Lock()
	}
}

func (c *Coordinator) openNextGenerationLocked() Snapshot {
	c.retireSourceLeasesLocked()
	c.generation++
	c.state = Open
	c.drainStarted = false
	c.sourceFence = make(chan struct{})
	c.sourceFenced = false
	c.lastTimeout = ""
	c.wakeLocked()
	return c.snapshotLocked()
}

func (c *Coordinator) Resume(expectedGeneration uint64) (Snapshot, error) {
	c.mu.Lock()
	c.used = true
	if c.closing {
		result := c.snapshotLocked()
		c.mu.Unlock()
		return result, ErrShutdown
	}
	if c.generation != expectedGeneration {
		result := c.snapshotLocked()
		c.mu.Unlock()
		return result, ErrGeneration
	}
	if c.evidenceFailure {
		result := c.snapshotLocked()
		c.mu.Unlock()
		return result, ErrEvidence
	}
	if c.state != Held || c.work != 0 || c.cleanup != 0 || c.resuming {
		result := c.snapshotLocked()
		c.mu.Unlock()
		return result, ErrNotSafe
	}
	if !c.hasHooks(true) {
		result := c.openNextGenerationLocked()
		c.mu.Unlock()
		return result, nil
	}
	c.resuming = true
	c.state = Draining
	id := c.beginTransitionLocked("dev.resume")
	c.mu.Unlock()
	err := c.runHooks(true)
	c.mu.Lock()
	c.resuming = false
	c.finishTransitionLocked(id, err)
	if c.closing || c.evidenceFailure || c.cleanup != 0 || c.work != 0 {
		c.drainStarted = false // A later join reasserts drain after partial resume.
		result := c.snapshotLocked()
		failure := ErrNotSafe
		if c.closing {
			failure = ErrShutdown
		} else if c.evidenceFailure {
			failure = ErrEvidence
		}
		c.mu.Unlock()
		return result, failure
	}
	result := c.openNextGenerationLocked()
	c.mu.Unlock()
	return result, nil
}

// Shutdown irreversibly fences sources/resume. Existing work may still create
// callbacks while its work count is positive; Quiesce joins without authorizing
// fixture cleanup, and the application subsequently closes its real owners.
func (c *Coordinator) Shutdown() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = true
	c.closing = true
	c.fenceSourcesLocked()
	return c.snapshotLocked()
}
