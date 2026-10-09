package consumer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

// The adapter gates real native receives; it never substitutes lease semantics.
type consumerReceiveBroker struct {
	*messaging.Broker
	onReceive func(context.Context, *messaging.Queue, int, time.Duration) ([]*messaging.Message, error)
}

func (broker consumerReceiveBroker) ReceiveMessagesContext(ctx context.Context, queue *messaging.Queue, max int, wait time.Duration) ([]*messaging.Message, error) {
	return broker.onReceive(ctx, queue, max, wait)
}

func TestConsumerCancellationInterruptsExistingEmptyQueue(t *testing.T) {
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	queue := broker.CreateQueue("empty", 0, 0)
	entered := make(chan struct{})
	var once sync.Once
	adapter := consumerReceiveBroker{Broker: broker, onReceive: func(ctx context.Context, queue *messaging.Queue, max int, wait time.Duration) ([]*messaging.Message, error) {
		once.Do(func() { close(entered) })
		return broker.ReceiveMessagesContext(ctx, queue, max, wait)
	}}
	manager := NewConsumerManager(adapter, t.TempDir())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	manager.Start(ctx, []ConsumerEntry{{Name: "empty", Queue: queue.Name, Type: "go", Handler: "never-started", BatchSize: 1, TimeoutSeconds: 10}})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("consumer did not enter its native receive")
	}
	cancel()
	joined, stop := context.WithTimeout(t.Context(), time.Second)
	defer stop()
	require.NoError(t, manager.Wait(joined), "cancellation must interrupt the five-second native long poll")
	waiting, inFlight := broker.QueueDepth(queue)
	require.Zero(t, waiting)
	require.Zero(t, inFlight)
	canceled, stopCanceled := context.WithCancel(t.Context())
	stopCanceled()
	require.NoError(t, manager.Wait(canceled), "a completed join takes priority over a later canceled waiter")
}

func TestConsumerCanceledAcceptedBatchPreservesSettlement(t *testing.T) {
	for _, mode := range []string{"retry", "dead-letter"} {
		t.Run(mode, func(t *testing.T) {
			broker := messaging.NewBroker("us-east-1", "000000000000", 0)
			queue := broker.CreateQueue("accepted", 0, 0)
			dlq := broker.CreateQueue("accepted-dlq", 0, 0)
			_, err := broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: "owned"})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			received := make(chan []*messaging.Message, 1)
			adapter := consumerReceiveBroker{Broker: broker, onReceive: func(ctx context.Context, queue *messaging.Queue, max int, wait time.Duration) ([]*messaging.Message, error) {
				messages, err := broker.ReceiveMessagesContext(ctx, queue, max, wait)
				if len(messages) != 0 {
					// Cancellation after native receipt admission must not turn that
					// accepted lease into a success or start a new handler process.
					cancel()
					received <- messages
				}
				return messages, err
			}}
			directory := t.TempDir()
			marker := filepath.Join(directory, "started")
			handler := filepath.Join(directory, "handler")
			require.NoError(t, os.WriteFile(handler, []byte("#!/bin/sh\nprintf started > \"$STARTED_FILE\"\nprintf '{}\\n'\n"), 0700))
			manager := NewConsumerManager(adapter, directory)
			maxReceives := 2
			if mode == "dead-letter" {
				maxReceives = 1
			}
			manager.Start(ctx, []ConsumerEntry{{Name: "accepted", Queue: queue.Name, DeadLetterQueue: dlq.Name, MaxReceiveCount: maxReceives, Type: "go", Handler: handler, BatchSize: 1, TimeoutSeconds: 10, Env: map[string]string{"STARTED_FILE": marker}}})
			joined, stop := context.WithTimeout(t.Context(), 2*time.Second)
			defer stop()
			require.NoError(t, manager.Wait(joined), "joined caller cancellation is not sticky ownership uncertainty")
			var messages []*messaging.Message
			select {
			case messages = <-received:
			default:
				t.Fatal("fixture did not admit a real native receipt")
			}
			require.Len(t, messages, 1)
			_, err = os.Stat(marker)
			require.ErrorIs(t, err, os.ErrNotExist, "a canceled accepted batch must not start its handler")
			waiting, inFlight := broker.QueueDepth(queue)
			require.Zero(t, waiting)
			if mode == "retry" {
				require.Equal(t, 1, inFlight)
				waiting, inFlight = broker.QueueDepth(dlq)
				require.Zero(t, waiting)
				require.Zero(t, inFlight)
				require.True(t, broker.DeleteMessage(queue, messages[0].ReceiptHandle), "the original accepted lease remains current")
			} else {
				require.Zero(t, inFlight)
				deadLetters := broker.ReceiveMessages(dlq, 1, 0)
				require.Len(t, deadLetters, 1)
				require.Equal(t, messages[0].ID, deadLetters[0].ID)
				require.Equal(t, "owned", deadLetters[0].Body)
			}
		})
	}
}

func TestConsumerContextualReceivePreservesLegacyBatchClamping(t *testing.T) {
	for _, tc := range []struct{ configured, expected int }{{-1, 1}, {0, 1}, {11, 10}} {
		t.Run(fmt.Sprint(tc.configured), func(t *testing.T) {
			broker := messaging.NewBroker("us-east-1", "000000000000", 0)
			queue := broker.CreateQueue("batch-bounds", 0, 0)
			for range 11 {
				_, err := broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: "owned"})
				require.NoError(t, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			observed := make(chan int, 1)
			adapter := consumerReceiveBroker{Broker: broker, onReceive: func(ctx context.Context, queue *messaging.Queue, max int, wait time.Duration) ([]*messaging.Message, error) {
				messages, err := broker.ReceiveMessagesContext(ctx, queue, max, wait)
				observed <- max
				cancel()
				return messages, err
			}}
			manager := NewConsumerManager(adapter, t.TempDir())
			manager.Start(ctx, []ConsumerEntry{{Name: "bounds", Queue: queue.Name, Type: "go", Handler: "must-not-start", BatchSize: tc.configured, TimeoutSeconds: 10, MaxReceiveCount: 2}})
			joined, stop := context.WithTimeout(t.Context(), 2*time.Second)
			defer stop()
			require.NoError(t, manager.Wait(joined))
			select {
			case actual := <-observed:
				require.Equal(t, tc.expected, actual)
			default:
				t.Fatal("the consumer did not use the contextual native port")
			}
			waiting, inFlight := broker.QueueDepth(queue)
			require.Equal(t, 11-tc.expected, waiting)
			require.Equal(t, tc.expected, inFlight)
		})
	}
}
