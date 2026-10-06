package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
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
	ReceiveMessages(queue *messaging.Queue, max int, wait time.Duration) []*messaging.Message
	ExtendMessageVisibility(queue *messaging.Queue, receipt string, timeout time.Duration) bool
	DeleteMessage(queue *messaging.Queue, receipt string) bool
	MoveMessage(source, destination *messaging.Queue, receipt string) bool
}

// ConsumerManager manages background consumer goroutines.
type ConsumerManager struct {
	broker  QueueBroker
	workDir string // project root for uv run
	wg      sync.WaitGroup
}

func NewConsumerManager(broker QueueBroker, workDir string) *ConsumerManager {
	return &ConsumerManager{broker: broker, workDir: workDir}
}

// Start launches a goroutine per consumer entry and returns immediately.
func (cm *ConsumerManager) Start(ctx context.Context, consumers []ConsumerEntry) {
	for _, c := range consumers {
		entry := c
		cm.wg.Add(1)
		go func() {
			defer cm.wg.Done()
			cm.pollLoop(ctx, entry)
		}()
	}
}

// Wait blocks until every selected consumer has observed cancellation.
func (cm *ConsumerManager) Wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		cm.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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

		messages := cm.broker.ReceiveMessages(queue, entry.BatchSize, 5*time.Second)
		if len(messages) == 0 {
			continue
		}

		logger.Debug().Int("count", len(messages)).Msg("Received messages")
		visibility := time.Duration(entry.TimeoutSeconds)*time.Second + 30*time.Second
		for _, msg := range messages {
			cm.broker.ExtendMessageVisibility(queue, msg.ReceiptHandle, visibility)
		}

		event := buildLambdaEvent(messages)
		result, err := cm.invokeHandlerResult(ctx, entry, event)

		if err != nil {
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

// buildLambdaEvent creates an SQS Lambda event from messages.
// Format: {"Records": [{"messageId": "...", "body": "...SNS envelope...", "receiptHandle": "..."}]}
func buildLambdaEvent(messages []*messaging.Message) map[string]interface{} {
	records := make([]map[string]interface{}, 0, len(messages))
	for _, msg := range messages {
		records = append(records, map[string]interface{}{
			"messageId":     msg.ID,
			"receiptHandle": msg.ReceiptHandle,
			"body":          msg.Body,
		})
	}
	return map[string]interface{}{
		"Records": records,
	}
}
