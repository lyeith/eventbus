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

// The listener quiesces background callers while HTTP remains available, then
// invokes Close after HTTP draining.
// Workers are dependencies of the stores: a failed join must withhold release.
// Quiescing continues across independent owners after errors; resource release
// still stops when a prerequisite failed. Construction failure may call Close before workers start.
type backgroundDrainer interface {
	Drain(context.Context) error
	Close(context.Context) error
}

type eventBusLifecycle struct {
	store         *cognito.CognitoStore
	firehose      *firehose.FirehoseManager
	ses           io.Closer
	sns           io.Closer
	notifications io.Closer
	mappings      contextCloser
	scheduler     contextCloser
	rotation      backgroundDrainer
	triggers      contextCloser
	functions     contextCloser
	consumers     *consumer.ConsumerManager
	cancel        context.CancelFunc
	requeueDone   <-chan struct{}
	sessionsDone  <-chan struct{}
	quiesceOnce   sync.Once
	quiesceErr    error
	once          sync.Once
	err           error
}

func (owned *eventBusLifecycle) Close(ctx context.Context) error {
	if owned == nil {
		return nil
	}
	owned.once.Do(func() { owned.err = owned.close(ctx) })
	return owned.err
}

// Quiesce stops sources of background work and joins their SDK callbacks while
// the public AWS listener is still available. Stores and synchronous invocation
// remain usable until HTTP has drained.
func (owned *eventBusLifecycle) Quiesce(ctx context.Context) error {
	if owned == nil {
		return nil
	}
	owned.quiesceOnce.Do(func() { owned.quiesceErr = owned.quiesce(ctx) })
	return owned.quiesceErr
}

func (owned *eventBusLifecycle) quiesce(ctx context.Context) error {
	var failures []error
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
			failures = append(failures, fmt.Errorf("join %s; stores retained: %w", worker.name, ctx.Err()))
		}
	}
	if owned.consumers != nil {
		if err := owned.consumers.Wait(ctx); err != nil {
			failures = append(failures, fmt.Errorf("join consumers; stores retained: %w", err))
			// Polling was canceled above; join actual process cleanup even after
			// the budget expired. Preserve uncertainty discovered during that join
			// as well as the original waiting failure.
			if joinedErr := owned.consumers.Wait(context.WithoutCancel(ctx)); joinedErr != nil && !errors.Is(err, joinedErr) {
				failures = append(failures, fmt.Errorf("consumer ownership after join; stores retained: %w", joinedErr))
			}
		}
	}
	if err := ctx.Err(); err != nil {
		failures = append(failures, fmt.Errorf("resource cleanup not started: %w", err))
	}
	if owned.mappings != nil {
		if err := owned.mappings.Close(ctx); err != nil {
			failures = append(failures, fmt.Errorf("join SQS event-source mappings; resources retained: %w", err))
		}
	}
	if owned.scheduler != nil {
		if err := owned.scheduler.Close(ctx); err != nil {
			failures = append(failures, fmt.Errorf("join Scheduler callbacks; resources retained: %w", err))
		}
	}
	if owned.rotation != nil {
		if err := owned.rotation.Drain(ctx); err != nil {
			failures = append(failures, fmt.Errorf("drain Secrets rotations; resources retained: %w", err))
		}
	}
	if functions, ok := owned.functions.(interface{ DrainAsync(context.Context) error }); ok {
		if err := functions.DrainAsync(ctx); err != nil {
			failures = append(failures, fmt.Errorf("drain Lambda events; resources retained: %w", err))
		}
	}
	return errors.Join(failures...)
}

func (owned *eventBusLifecycle) close(ctx context.Context) error {
	if err := owned.Quiesce(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("resource cleanup not started: %w", err)
	}
	if owned.rotation != nil {
		if err := owned.rotation.Close(ctx); err != nil {
			return fmt.Errorf("close Secrets rotation owner; resources retained: %w", err)
		}
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
	var flushErr, storeErr, captureErr, snsCaptureErr, notificationErr error
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
	if owned.sns != nil {
		snsCaptureErr = owned.sns.Close()
	}
	if owned.notifications != nil {
		notificationErr = owned.notifications.Close()
	}
	return errors.Join(flushErr, storeErr, captureErr, snsCaptureErr, notificationErr)
}
