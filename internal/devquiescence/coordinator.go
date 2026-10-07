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

type Activity struct {
	ID        uint64    `json:"id"`
	Kind      string    `json:"kind"`
	RequestID string    `json:"request_id,omitempty"`
	StartedAt time.Time `json:"started_at"`
}

type Snapshot struct {
	SchemaVersion      string     `json:"schema_version"`
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
	closing, evidenceFailure bool
	evidenceCode             string
	lastTimeout              string
	activities               map[uint64]Activity
	checks                   []func() error
	wake                     chan struct{}
}

func New(checks ...func() error) *Coordinator {
	return &Coordinator{state: Open, generation: 1, activities: make(map[uint64]Activity), wake: make(chan struct{}), checks: append([]func() error(nil), checks...)}
}

var safeIdentity = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var safeKind = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

func (c *Coordinator) wakeLocked() { close(c.wake); c.wake = make(chan struct{}) }

func (c *Coordinator) beginWorkLocked(kind, requestID string) func(error) {
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
	var once sync.Once
	return func(evidenceErr error) {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.work--
			delete(c.activities, id)
			if evidenceErr != nil {
				c.evidenceFailure = true
				if c.evidenceCode == "" {
					c.evidenceCode = "incomplete_ownership_evidence"
				}
				if c.state == Open {
					c.state = Draining
				}
			}
			c.wakeLocked()
		})
	}
}

// BeginActivity transfers one whole invocation/task lifetime into this owner.
// Retries retain the same lease. Complete only after every owned runtime/child
// joins; its error denotes unreliable evidence/ownership, not a business error.
func (c *Coordinator) BeginActivity(kind, requestID string) (func(error), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == Open && !c.closing && !c.evidenceFailure || c.state == Draining && c.work > 0 {
		return c.beginWorkLocked(kind, requestID), nil
	}
	if c.closing {
		return nil, ErrShutdown
	}
	if c.evidenceFailure {
		return nil, ErrEvidence
	}
	return nil, ErrFenced
}

func (c *Coordinator) snapshotLocked() Snapshot {
	state := c.state
	if c.closing {
		state = Shutdown
	}
	result := Snapshot{SchemaVersion: "eventbus.retained-owner.v1", State: state, Generation: c.generation,
		FixtureSafe: c.state == Held && !c.closing && !c.evidenceFailure && c.work == 0 && c.cleanup == 0,
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

// checkEvidence uses immutable startup dependencies without holding c.mu.
func (c *Coordinator) checkEvidence() error {
	var evidenceErr error
	for _, check := range c.checks {
		if check != nil {
			evidenceErr = errors.Join(evidenceErr, check())
		}
	}
	return evidenceErr
}

// Quiesce fences sources before waiting and keeps accepted callback chains live.
// Cancellation never cancels business work or opens the fence. Retry explicitly
// after a timeout; sticky evidence failures can never grant cleanup or resume.
func (c *Coordinator) Quiesce(ctx context.Context) (Snapshot, error) {
	c.mu.Lock()
	generation := c.generation
	if c.state == Open {
		c.state = Draining
		c.wakeLocked()
	}
	for {
		if c.generation != generation {
			result := c.snapshotLocked()
			c.mu.Unlock()
			return result, ErrGeneration
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
			// Sink/service evidence checks are immutable startup dependencies;
			// never call them while holding the coordinator's ownership lock.
			c.mu.Unlock()
			evidenceErr := c.checkEvidence()
			c.mu.Lock()
			if evidenceErr != nil {
				c.evidenceFailure = true
				if c.evidenceCode == "" {
					c.evidenceCode = "evidence_unavailable"
				}
				if c.state == Open {
					c.state = Draining
				}
			}
			if c.generation != generation || c.evidenceFailure || c.work != 0 || c.cleanup != 0 {
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

func (c *Coordinator) Resume(expectedGeneration uint64) (Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return c.snapshotLocked(), ErrShutdown
	}
	if c.generation != expectedGeneration {
		return c.snapshotLocked(), ErrGeneration
	}
	if c.evidenceFailure {
		return c.snapshotLocked(), ErrEvidence
	}
	if c.state != Held || c.work != 0 || c.cleanup != 0 {
		return c.snapshotLocked(), ErrNotSafe
	}
	c.generation++
	c.state = Open
	c.lastTimeout = ""
	c.wakeLocked()
	return c.snapshotLocked(), nil
}

// Shutdown irreversibly fences sources/resume. Existing work may still create
// callbacks while its work count is positive; Quiesce joins without authorizing
// fixture cleanup, and the application subsequently closes its real owners.
func (c *Coordinator) Shutdown() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closing = true
	if c.state == Open {
		c.state = Draining
	}
	c.wakeLocked()
	return c.snapshotLocked()
}
