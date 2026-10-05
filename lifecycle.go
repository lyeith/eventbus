package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// The listener is the only owner allowed to invoke Close after HTTP draining.
// Workers are dependencies of the stores: a failed join must withhold release.
// Unlike independent cleanup registrations, these stages deliberately stop on a
// failed prerequisite. Construction failure may call Close before workers start.
type eventBusLifecycle struct {
	store        *CognitoStore
	firehose     *FirehoseManager
	consumers    *ConsumerManager
	cancel       context.CancelFunc
	requeueDone  <-chan struct{}
	sessionsDone <-chan struct{}
	once         sync.Once
	err          error
}

func (owned *eventBusLifecycle) Close(ctx context.Context) error {
	if owned == nil {
		return nil
	}
	owned.once.Do(func() { owned.err = owned.close(ctx) })
	return owned.err
}

func (owned *eventBusLifecycle) close(ctx context.Context) error {
	if owned.cancel != nil {
		owned.cancel()
	}
	for _, worker := range []struct {
		name string
		done <-chan struct{}
	}{
		{"broker requeue", owned.requeueDone}, {"Cognito session cleanup", owned.sessionsDone},
	} {
		if worker.done == nil {
			continue
		}
		select {
		case <-worker.done:
		case <-ctx.Done():
			return fmt.Errorf("join %s; stores retained: %w", worker.name, ctx.Err())
		}
	}
	if owned.consumers != nil {
		if err := owned.consumers.Wait(ctx); err != nil {
			return fmt.Errorf("join consumers; stores retained: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("resource cleanup not started: %w", err)
	}
	var flushErr, storeErr error
	if owned.firehose != nil {
		flushErr = owned.firehose.ShutdownContext(ctx)
	}
	// Firehose owns no Cognito connection. Once every Cognito user has joined,
	// SQLite can close even when final stream delivery failed; preserve both errors.
	if owned.store != nil {
		storeErr = owned.store.Close()
	}
	return errors.Join(flushErr, storeErr)
}

func startChallengeCleanup(ctx context.Context, store *CognitoStore, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count, err := store.DeleteExpiredChallengeSessions(ctx, time.Now().Unix())
				if err != nil && ctx.Err() == nil {
					log.Warn().Err(err).Msg("challenge_sessions cleanup failed")
				} else if count > 0 {
					log.Debug().Int("deleted", count).Msg("challenge_sessions cleanup")
				}
			}
		}
	}()
	return done
}
