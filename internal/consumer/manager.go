package consumer

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/lyeith/eventbus/internal/sqsevent"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	baseRetryVisibility = 5 * time.Second
	maxRetryVisibility  = time.Minute
)

// QueueBroker is the queue behavior required by consumer polling and settlement.
// The application injects the same broker used by the SNS and SQS adapters.
type QueueBroker interface {
	GetQueue(name string) *messaging.Queue
	ReceiveMessagesContext(context.Context, *messaging.Queue, int, time.Duration) ([]*messaging.Message, error)
	ExtendMessageVisibility(queue *messaging.Queue, receipt string, timeout time.Duration) bool
	DeleteMessage(queue *messaging.Queue, receipt string) bool
	MoveMessage(source, destination *messaging.Queue, receipt string) bool
}

// ConsumerManager manages background consumer goroutines. An uncertain process
// join permanently fences its execution; ordinary handler failures still retry.
type ConsumerManager struct {
	broker  QueueBroker
	workDir string // project root for uv run

	mu           sync.Mutex
	running      int
	idle         chan struct{}
	ownershipErr error
	fenceCtx     context.Context
	fence        context.CancelFunc
	// The production boundary always uses localexec cleanup. Private tests wrap
	// that same cleanup to exercise uncertainty after real child/group joining.
	processCleanup func(*exec.Cmd) error
}

func NewConsumerManager(broker QueueBroker, workDir string) *ConsumerManager {
	idle := make(chan struct{})
	close(idle)
	fenceCtx, fence := context.WithCancel(context.Background())
	return &ConsumerManager{broker: broker, workDir: workDir, idle: idle, fenceCtx: fenceCtx, fence: fence, processCleanup: localexec.Cleanup}
}

// Start admits pollers before returning. Call Start before Wait; a manager with
// ownership uncertainty refuses subsequent starts as well as process launches.
func (cm *ConsumerManager) Start(ctx context.Context, consumers []ConsumerEntry) {
	cm.mu.Lock()
	if cm.ownershipErr != nil || len(consumers) == 0 {
		cm.mu.Unlock()
		return
	}
	if cm.running == 0 {
		cm.idle = make(chan struct{})
	}
	cm.running += len(consumers)
	cm.mu.Unlock()
	for _, c := range consumers {
		entry := c
		pollCtx, cancel := context.WithCancel(ctx)
		stopFence := context.AfterFunc(cm.fenceCtx, cancel)
		go func() {
			defer func() {
				stopFence()
				cancel()
				cm.mu.Lock()
				cm.running--
				if cm.running == 0 {
					close(cm.idle)
				}
				cm.mu.Unlock()
			}()
			cm.pollLoop(pollCtx, entry)
		}()
	}
}

// Wait joins admitted pollers and their actual process cleanup. Ownership
// uncertainty remains an error after joining and on every subsequent Wait.
// A caller deadline only bounds waiting; it never releases consumer ownership.
func (cm *ConsumerManager) Wait(ctx context.Context) error {
	cm.mu.Lock()
	done := cm.idle
	cm.mu.Unlock()
	select {
	case <-done:
		return cm.ownershipFailure()
	default:
	}
	select {
	case <-done:
		return cm.ownershipFailure()
	case <-ctx.Done():
		select {
		case <-done:
			return cm.ownershipFailure()
		default:
			return ctx.Err()
		}
	}
}

type processOwnershipError struct{ cause error }

func (err *processOwnershipError) Error() string { return err.cause.Error() }
func (err *processOwnershipError) Unwrap() error { return err.cause }

func (cm *ConsumerManager) ownershipFailure() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.ownershipErr
}

func (cm *ConsumerManager) failOwnership(err error) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.ownershipErr == nil {
		cm.ownershipErr = &processOwnershipError{cause: err}
		cm.fence()
	}
	return cm.ownershipErr
}

// Admission and the ownership fence share this lock. A process which starts
// before a concurrent failure remains owned by its canceled poller through Wait.
func (cm *ConsumerManager) startHandler(command *exec.Cmd) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.ownershipErr != nil {
		return cm.ownershipErr
	}
	return command.Start()
}

func (cm *ConsumerManager) pollLoop(ctx context.Context, entry ConsumerEntry) {
	logger := log.With().Str("consumer", entry.Name).Str("queue", entry.Queue).Logger()
	logger.Info().Msg("Consumer started")

	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Consumer stopping")
			return
		default:
		}

		queue := cm.broker.GetQueue(entry.Queue)
		if queue == nil {
			logger.Warn().Msg("Queue not found, retrying in 5s")
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}

		// Preserve the legacy broker's recipe batch clamping while using its
		// stricter contextual native port.
		batchSize := max(1, min(entry.BatchSize, 10))
		messages, receiveErr := cm.broker.ReceiveMessagesContext(ctx, queue, batchSize, 5*time.Second)

		if receiveErr != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Warn().Err(receiveErr).Msg("Queue receive failed, retrying in 5s")
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
			continue
		}
		if len(messages) == 0 {
			continue
		}

		logger.Debug().Int("count", len(messages)).Msg("Received messages")
		visibility := time.Duration(entry.TimeoutSeconds)*time.Second + 30*time.Second
		for _, msg := range messages {
			cm.broker.ExtendMessageVisibility(queue, msg.ReceiptHandle, visibility)
		}

		event := buildLambdaEvent(messages, queue)
		result, err := cm.invokeHandlerResult(ctx, entry, event)

		if err != nil {
			var ownership *processOwnershipError
			if errors.As(err, &ownership) {
				logger.Error().Err(err).Msg("Consumer process ownership uncertain; execution fenced and records retained")
				return
			}
			logger.Error().Err(err).Int("count", len(messages)).Msg("Handler failed; retaining every record")
			cm.settleBatch(logger, entry, queue, messages, nil, true)
			continue
		}
		failedIDs, err := validateBatchFailures(result, messages)
		if err != nil {
			logger.Error().Err(err).Int("count", len(messages)).Msg("Handler returned an invalid batch response; retaining every record")
			cm.settleBatch(logger, entry, queue, messages, nil, true)
			continue
		}
		cm.settleBatch(logger, entry, queue, messages, failedIDs, false)
	}
}

type batchItemFailure struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

type handlerBatchResult struct {
	BatchItemFailures []batchItemFailure `json:"batchItemFailures"`
}

func validateBatchFailures(result *handlerBatchResult, messages []*messaging.Message) (map[string]struct{}, error) {
	known := make(map[string]struct{}, len(messages))
	for _, msg := range messages {
		known[msg.ID] = struct{}{}
	}
	failed := make(map[string]struct{}, len(result.BatchItemFailures))
	for _, item := range result.BatchItemFailures {
		if item.ItemIdentifier == "" {
			return nil, fmt.Errorf("batch failure has an empty itemIdentifier")
		}
		if _, ok := known[item.ItemIdentifier]; !ok {
			return nil, fmt.Errorf("batch failure refers to unknown message %q", item.ItemIdentifier)
		}
		if _, duplicate := failed[item.ItemIdentifier]; duplicate {
			return nil, fmt.Errorf("batch failure repeats message %q", item.ItemIdentifier)
		}
		failed[item.ItemIdentifier] = struct{}{}
	}
	return failed, nil
}

func (cm *ConsumerManager) settleBatch(
	logger zerolog.Logger,
	entry ConsumerEntry,
	queue *messaging.Queue,
	messages []*messaging.Message,
	failedIDs map[string]struct{},
	failAll bool,
) {
	dlq := cm.broker.GetQueue(entry.DeadLetterQueue)
	deleted, retried, deadLettered := 0, 0, 0
	for _, msg := range messages {
		_, failed := failedIDs[msg.ID]
		if !failAll && !failed {
			if cm.broker.DeleteMessage(queue, msg.ReceiptHandle) {
				deleted++
			}
			continue
		}
		if msg.ReceiveCount >= entry.MaxReceiveCount && dlq != nil {
			if cm.broker.MoveMessage(queue, dlq, msg.ReceiptHandle) {
				deadLettered++
			}
			continue
		}
		if cm.broker.ExtendMessageVisibility(queue, msg.ReceiptHandle, retryVisibility(msg.ReceiveCount)) {
			retried++
		}
	}
	logger.Debug().
		Int("deleted", deleted).
		Int("retried", retried).
		Int("dead_lettered", deadLettered).
		Msg("Settled consumer batch")
}

// retryVisibility applies a small bounded exponential delay between explicit
// handler failures. The broker's existing visibility requeue loop performs the
// eventual retry; no second scheduler or message copy is introduced.
func retryVisibility(receiveCount int) time.Duration {
	if receiveCount < 1 {
		receiveCount = 1
	}
	delay := baseRetryVisibility
	for attempt := 1; attempt < receiveCount && delay < maxRetryVisibility; attempt++ {
		delay *= 2
	}
	if delay > maxRetryVisibility {
		return maxRetryVisibility
	}
	return delay
}

// buildLambdaEvent delegates native projection to the source owner.
func buildLambdaEvent(messages []*messaging.Message, queues ...*messaging.Queue) sqsevent.Event {
	var arn string
	if len(queues) > 0 && queues[0] != nil {
		arn = queues[0].ARN
	}
	return messaging.BuildSQSLambdaEvent(messages, arn)
}
