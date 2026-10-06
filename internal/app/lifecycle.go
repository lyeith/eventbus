package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/consumer"
	"github.com/lyeith/eventbus/internal/firehose"
)

type contextCloser interface {
	Close(context.Context) error
}

// The listener is the only owner allowed to invoke Close after HTTP draining.
// Workers are dependencies of the stores: a failed join must withhold release.
// Unlike independent cleanup registrations, these stages deliberately stop on a
// failed prerequisite. Construction failure may call Close before workers start.
type eventBusLifecycle struct {
	store        *cognito.CognitoStore
	firehose     *firehose.FirehoseManager
	ses          io.Closer
	triggers     contextCloser
	functions    contextCloser
	consumers    *consumer.ConsumerManager
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
	if owned.functions != nil {
		if err := owned.functions.Close(ctx); err != nil {
			return fmt.Errorf("join Lambda functions; stores retained: %w", err)
		}
	}
	if owned.triggers != nil {
		if err := owned.triggers.Close(ctx); err != nil {
			return fmt.Errorf("join Cognito triggers; stores retained: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("resource cleanup not started after trigger join: %w", err)
	}
	var flushErr, storeErr, captureErr error
	if owned.firehose != nil {
		flushErr = owned.firehose.ShutdownContext(ctx)
	}
	// Firehose owns no Cognito connection. Once every Cognito user has joined,
	// SQLite can close even when final stream delivery failed; preserve both errors.
	if owned.store != nil {
		storeErr = owned.store.Close()
	}
	// SES captures have no delivery workers and no dependency on Firehose or
	// Cognito. After admission is drained, close even if another finalizer failed.
	if owned.ses != nil {
		captureErr = owned.ses.Close()
	}
	return errors.Join(flushErr, storeErr, captureErr)
}
