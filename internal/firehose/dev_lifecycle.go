package firehose

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/lyeith/eventbus/internal/devactivity"
)

// devDelivery owns optional retained-suite observation and force flushing.
// Native stream workers and their delivery/retry policy remain authoritative.
type devDelivery struct {
	activity         devactivity.Activity
	mu               sync.Mutex
	forcing, aborted bool
	leases, attempts int
	evidence         error
	changed          chan struct{}
	wake             chan struct{}
	forceContext     context.Context
	forceCancel      context.CancelFunc
	forceDone        chan struct{}
	abortContext     context.Context
	abortCancel      context.CancelFunc
	abortDone        chan struct{}
	abortResult      error
}

type devForceContextKey struct{}

type devRecordLease struct {
	owner    *devDelivery
	complete func(error)
	once     sync.Once
}

func (lease *devRecordLease) finish(err error) {
	if lease == nil {
		return
	}
	lease.once.Do(func() {
		owner := lease.owner
		owner.mu.Lock()
		owner.leases--
		if err != nil && owner.evidence == nil {
			owner.evidence = err
		}
		owner.signalLocked()
		owner.mu.Unlock()
		lease.complete(err)
	})
}

func (owner *devDelivery) signalLocked() {
	close(owner.changed)
	owner.changed = make(chan struct{})
}

func (owner *devDelivery) beginRecord(recordID string) (*devRecordLease, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.aborted {
		return nil, errors.New("Firehose delivery owner was aborted")
	}
	complete, err := owner.activity.BeginActivity("firehose.record", recordID)
	if err != nil {
		return nil, err
	}
	if complete == nil {
		return nil, errors.New("Firehose activity observer refused ownership")
	}
	owner.leases++
	owner.signalLocked()
	return &devRecordLease{owner: owner, complete: complete}, nil
}

// SetDevActivity configures optional development ownership before streams exist.
// The observer must only account for activity and must not reenter Firehose.
func (fm *FirehoseManager) SetDevActivity(activity devactivity.Activity) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	if fm.stopping || len(fm.streams) != 0 || fm.dev != nil {
		return errors.New("Firehose development activity must be configured before stream creation")
	}
	if activity == nil {
		return nil
	}
	if fm.httpClient.Transport == nil {
		transport, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return errors.New("tracked Firehose requires an owned HTTP transport")
		}
		// Abort closes only this manager's pool, never another service's shared
		// default transport. Explicitly injected transports retain caller policy.
		fm.httpClient.Transport = transport.Clone()
	}
	forceContext, forceCancel := context.WithCancel(context.Background())
	abortContext, abortCancel := context.WithCancel(context.Background())
	fm.dev = &devDelivery{
		activity: activity, changed: make(chan struct{}), wake: make(chan struct{}, 1),
		forceContext: forceContext, forceCancel: forceCancel, forceDone: make(chan struct{}),
		abortContext: abortContext, abortCancel: abortCancel, abortDone: make(chan struct{}),
	}
	fm.dev.forceContext = context.WithValue(forceContext, devForceContextKey{}, fm.dev)
	go fm.devForceLoop(fm.dev)
	return nil
}

func (fm *FirehoseManager) devOwner() (*devDelivery, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if fm.dev == nil {
		return nil, errors.New("Firehose development activity is not configured")
	}
	return fm.dev, nil
}

// DevBeginDrain immediately enables forcing, without closing stream admission.
// Accepted callbacks and held-cleanup puts wake the same force loop until resume.
func (fm *FirehoseManager) DevBeginDrain() error {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if fm.dev == nil || fm.stopping {
		return errors.New("Firehose development delivery is unavailable")
	}
	owner := fm.dev
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.aborted || owner.evidence != nil {
		return errors.Join(errors.New("Firehose development delivery is unavailable"), owner.evidence)
	}
	owner.forcing = true
	select {
	case owner.wake <- struct{}{}:
	default:
	}
	return nil
}

// DevResume restores native buffering on the same streams after settled work.
func (fm *FirehoseManager) DevResume() error {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	if fm.dev == nil || fm.stopping {
		return errors.New("Firehose development delivery is unavailable")
	}
	owner := fm.dev
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.aborted || owner.evidence != nil || owner.leases != 0 {
		return errors.Join(errors.New("Firehose delivery ownership is not settled"), owner.evidence)
	}
	owner.forcing = false
	return nil
}

// DevEvidence reports terminal ownership uncertainty without closing providers.
// Pending records are counted through Activity; they are healthy accepted work,
// including after a cleanup envelope returns and before explicit re-quiescence.
func (fm *FirehoseManager) DevEvidence() error {
	owner, err := fm.devOwner()
	if err != nil {
		return err
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.evidence != nil {
		return owner.evidence
	}
	if owner.aborted {
		return errors.New("Firehose delivery owner was aborted")
	}
	return nil
}

func (fm *FirehoseManager) devForceLoop(owner *devDelivery) {
	defer close(owner.forceDone)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-owner.forceContext.Done():
			return
		case <-owner.wake:
		case <-ticker.C:
		}
		owner.mu.Lock()
		forcing := owner.forcing && !owner.aborted
		owner.mu.Unlock()
		if !forcing || owner.forceContext.Err() != nil {
			continue
		}
		fm.mu.RLock()
		streams := make([]*DeliveryStream, 0, len(fm.streams))
		for _, stream := range fm.streams {
			streams = append(streams, stream)
		}
		fm.mu.RUnlock()
		for _, stream := range streams {
			owner.mu.Lock()
			forcing = owner.forcing && !owner.aborted
			owner.mu.Unlock()
			if !forcing || owner.forceContext.Err() != nil {
				break
			}
			stream.mu.Lock()
			hasRecords := len(stream.buffer) != 0 || len(stream.pending) != 0
			stream.mu.Unlock()
			if hasRecords {
				// Includes failed DELETING streams whose native worker has joined.
				// Transient errors retain the original object and its record leases.
				_ = fm.flushReady(owner.forceContext, stream, true)
			}
		}
	}
}

// devAttempt joins queued and active flush calls, including actual body cleanup.
// Its context is canceled only by irreversible abort, never a drain timeout.
func (fm *FirehoseManager) devAttempt(ctx context.Context) (context.Context, func(), error) {
	owner := fm.dev
	if owner == nil {
		return ctx, func() {}, nil
	}
	owner.mu.Lock()
	if owner.aborted {
		owner.mu.Unlock()
		return ctx, nil, errors.New("Firehose delivery owner was aborted")
	}
	owner.attempts++
	owner.signalLocked()
	owner.mu.Unlock()
	attemptContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(owner.abortContext, cancel)
	return attemptContext, func() {
		stop()
		cancel()
		owner.mu.Lock()
		owner.attempts--
		owner.signalLocked()
		owner.mu.Unlock()
	}, nil
}

func (fm *FirehoseManager) devWake() {
	if owner := fm.dev; owner != nil {
		select {
		case owner.wake <- struct{}{}:
		default:
		}
	}
}

func (fm *FirehoseManager) devStopForce(ctx context.Context) error {
	if fm.dev == nil {
		return nil
	}
	fm.dev.forceCancel()
	select {
	case <-fm.dev.forceDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DevAbortJoin irreversibly stops admission and cancels delivery owners. A wait
// timeout leaves all leases counted until the background join actually finishes.
// Original records and delivery diagnostics remain available after abort.
func (fm *FirehoseManager) DevAbortJoin(ctx context.Context) error {
	fm.mu.Lock()
	owner := fm.dev
	if owner == nil {
		fm.mu.Unlock()
		return errors.New("Firehose development activity is not configured")
	}
	owner.mu.Lock()
	if !owner.aborted {
		owner.aborted = true
		owner.forcing = false
		closeManager := !fm.stopping
		fm.stopping = true
		streams := make([]*DeliveryStream, 0, len(fm.streams))
		for _, stream := range fm.streams {
			stream.cancel()
			streams = append(streams, stream)
		}
		owner.forceCancel()
		owner.abortCancel()
		owner.signalLocked()
		go fm.devJoinAbort(owner, streams, closeManager)
	}
	owner.mu.Unlock()
	fm.mu.Unlock()
	select {
	case <-owner.abortDone:
		owner.mu.Lock()
		result := owner.abortResult
		owner.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (fm *FirehoseManager) devJoinAbort(owner *devDelivery, streams []*DeliveryStream, closeManager bool) {
	for _, stream := range streams {
		<-stream.done
	}
	<-owner.forceDone
	owner.mu.Lock()
	for owner.attempts != 0 {
		changed := owner.changed
		owner.mu.Unlock()
		<-changed
		owner.mu.Lock()
	}
	owner.mu.Unlock()
	var leases []*devRecordLease
	for _, stream := range streams {
		stream.mu.Lock()
		for _, record := range stream.buffer {
			if record.lease != nil {
				leases = append(leases, record.lease)
			}
		}
		for _, object := range stream.pending {
			for _, record := range object.records {
				if record.lease != nil {
					leases = append(leases, record.lease)
				}
			}
		}
		stream.mu.Unlock()
	}
	var result error
	if len(leases) != 0 {
		result = fmt.Errorf("Firehose aborted delivery of %d retained records after joining all attempts", len(leases))
	}
	for _, lease := range leases {
		lease.finish(result)
	}
	fm.httpClient.CloseIdleConnections()
	owner.mu.Lock()
	owner.abortResult = result
	owner.signalLocked()
	owner.mu.Unlock()
	if closeManager {
		fm.mu.Lock()
		fm.shutdownErr = result
		close(fm.done)
		fm.mu.Unlock()
	}
	close(owner.abortDone)
}
