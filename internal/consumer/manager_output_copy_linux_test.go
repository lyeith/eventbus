//go:build linux

package consumer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

func TestConsumerCancellationCannotHideForcedOutputCopyCutoff(t *testing.T) {
	for _, descriptor := range []int{1, 2} {
		t.Run(fmt.Sprint(descriptor), func(t *testing.T) {
			directory := t.TempDir()
			marker, handler := filepath.Join(directory, "native.pid"), filepath.Join(directory, "handler")
			require.NoError(t, os.WriteFile(handler, []byte("#!/bin/sh\necho \"$$\" > \"$PID_FILE\"\nexec sleep 60\n"), 0700))
			broker := messaging.NewBroker("us-east-1", "000000000000", 0)
			queue := broker.CreateQueue("cancel-cutoff", 0, 0)
			_, err := broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: "owned"})
			require.NoError(t, err)
			manager := NewConsumerManager(broker, directory)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(func() {
				cancel()
				joined, stop := context.WithTimeout(context.Background(), 5*time.Second)
				defer stop()
				_ = manager.Wait(joined)
			})
			manager.Start(ctx, []ConsumerEntry{{Name: "cancel-cutoff", Queue: queue.Name, Type: "go", Handler: handler, BatchSize: 1, TimeoutSeconds: 10, MaxReceiveCount: 3, Env: map[string]string{"PID_FILE": marker}}})
			pid := consumerChildPID(t, marker)
			// This test owns only an extra descriptor, not an escaped process.
			// Native group cancellation can succeed while that exact writer holds
			// a pipe open; os/exec must cut it off and the manager must stay dirty.
			held, err := os.OpenFile(fmt.Sprintf("/proc/%d/fd/%d", pid, descriptor), os.O_WRONLY, 0)
			require.NoError(t, err)
			t.Cleanup(func() { _ = held.Close() })
			cancel()
			joined, stop := context.WithTimeout(t.Context(), 3*time.Second)
			defer stop()
			require.ErrorIs(t, manager.Wait(joined), exec.ErrWaitDelay, "a native cancellation error cannot hide forced pipe closure")
			require.False(t, consumerChildRunning(pid), "the actual native process must be joined before sticky evidence returns")
			require.NoError(t, held.Close(), "the fixture explicitly releases its exact retained writer")
			waiting, inFlight := broker.QueueDepth(queue)
			require.Zero(t, waiting)
			require.Equal(t, 1, inFlight)
		})
	}
}
