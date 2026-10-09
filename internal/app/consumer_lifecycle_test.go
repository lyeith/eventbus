//go:build linux || darwin

package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/cognito"
	"github.com/lyeith/eventbus/internal/consumer"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

func TestConsumerOwnershipFailureRetainsAppStoreAndCaptureAfterActualJoin(t *testing.T) {
	directory := t.TempDir()
	store, err := cognito.OpenCognitoStore(filepath.Join(directory, "cognito.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	queue := broker.CreateQueue("consumer-owned", 0, 0)
	dlq := broker.CreateQueue("consumer-owned-dlq", 0, 0)
	_, err = broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: "retained"})
	require.NoError(t, err)
	marker := filepath.Join(directory, "children.pid")
	handler := filepath.Join(directory, "handler")
	// The real direct child exits successfully while its descendant retains
	// stdout/stderr. Native WaitDelay must detect that cutoff, cleanup must stop
	// the group, and the joined error must still prevent application release.
	require.NoError(t, os.WriteFile(handler, []byte("#!/bin/sh\nsleep 60 &\nprintf '%s %s\\n' \"$$\" \"$!\" > \"$CHILDREN_FILE\"\nprintf '{}\\n'\n"), 0700))
	manager := consumer.NewConsumerManager(broker, directory)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		joined, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		_ = manager.Wait(joined)
		// Fault fixtures retain no live native child beyond their owned cleanup.
		data, err := os.ReadFile(marker)
		if err == nil {
			for _, value := range strings.Fields(string(data)) {
				if pid, err := strconv.Atoi(value); err == nil && pid > 0 {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			}
		}
	})
	manager.Start(ctx, []consumer.ConsumerEntry{{Name: "owned", Type: "go", Queue: queue.Name, DeadLetterQueue: dlq.Name, MaxReceiveCount: 1, Handler: handler, BatchSize: 1, TimeoutSeconds: 10, Env: map[string]string{"CHILDREN_FILE": marker}}})
	joined, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	require.ErrorIs(t, manager.Wait(joined), exec.ErrWaitDelay)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	pids := strings.Fields(string(data))
	require.Len(t, pids, 2)
	parent, err := strconv.Atoi(pids[0])
	require.NoError(t, err)
	child, err := strconv.Atoi(pids[1])
	require.NoError(t, err)
	require.ErrorIs(t, syscall.Kill(parent, 0), syscall.ESRCH, "the actual direct child must be reaped before Wait returns")
	require.Eventually(t, func() bool { return !appConsumerChildRunning(child) }, 3*time.Second, 10*time.Millisecond, "the real descendant must stop before resource-retention assertions")
	captureClosed := false
	owned := &eventBusLifecycle{store: store, consumers: manager, cancel: cancel, ses: closeFunc(func() error { captureClosed = true; return nil })}
	require.ErrorIs(t, owned.Close(t.Context()), exec.ErrWaitDelay)
	require.ErrorIs(t, owned.Close(t.Context()), exec.ErrWaitDelay, "failed app close remains terminal")
	require.NoError(t, readCognitoStore(t.Context(), store), "joined ownership uncertainty must retain SQLite")
	require.False(t, captureClosed, "joined ownership uncertainty must retain capture ownership")
	waiting, inFlight := broker.QueueDepth(queue)
	require.Zero(t, waiting)
	require.Equal(t, 1, inFlight, "uncertain execution must not ACK or redrive the original lease")
	waiting, inFlight = broker.QueueDepth(dlq)
	require.Zero(t, waiting)
	require.Zero(t, inFlight)
}

func appConsumerChildRunning(pid int) bool {
	if errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) {
		return false
	}
	if data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		fields := strings.Fields(string(data))
		if len(fields) > 2 && fields[2] == "Z" {
			return false
		}
	}
	return true
}

func TestConsumerDeadlineFirstAppCloseRetainsLateOutputUncertainty(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the owned retained-writer fault fixture duplicates a Linux /proc descriptor")
	}
	directory := t.TempDir()
	store, err := cognito.OpenCognitoStore(filepath.Join(directory, "cognito.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	queue := broker.CreateQueue("deadline-cutoff", 0, 0)
	_, err = broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: "owned"})
	require.NoError(t, err)
	marker, handler := filepath.Join(directory, "native.pid"), filepath.Join(directory, "handler")
	require.NoError(t, os.WriteFile(handler, []byte("#!/bin/sh\necho \"$$\" > \"$PID_FILE\"\nexec sleep 60\n"), 0700))
	manager := consumer.NewConsumerManager(broker, directory)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		joined, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = manager.Wait(joined)
	})
	manager.Start(ctx, []consumer.ConsumerEntry{{Name: "deadline-cutoff", Queue: queue.Name, Type: "go", Handler: handler, BatchSize: 1, TimeoutSeconds: 10, MaxReceiveCount: 3, Env: map[string]string{"PID_FILE": marker}}})
	var pid int
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(marker)
		if err != nil {
			return false
		}
		pid, err = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil && pid > 0
	}, 3*time.Second, 5*time.Millisecond, "the real consumer process must begin before shutdown")
	// The test retains this exact native writer while group cancellation joins
	// the actual handler. No external child, PID lifetime or grace period is proof.
	held, err := os.OpenFile(fmt.Sprintf("/proc/%d/fd/1", pid), os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })
	captureClosed := false
	owned := &eventBusLifecycle{store: store, consumers: manager, cancel: cancel, ses: closeFunc(func() error { captureClosed = true; return nil })}
	deadline, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	err = owned.Close(deadline)
	require.ErrorIs(t, err, context.DeadlineExceeded, "retain the first waiting deadline")
	require.ErrorIs(t, err, exec.ErrWaitDelay, "retain forced-copy uncertainty discovered by the uncanceled second join")
	require.ErrorIs(t, syscall.Kill(pid, 0), syscall.ESRCH, "the native process must actually be reaped before app close returns")
	require.NoError(t, held.Close())
	require.NoError(t, readCognitoStore(t.Context(), store))
	require.False(t, captureClosed)
	require.ErrorIs(t, owned.Close(t.Context()), exec.ErrWaitDelay)
	waiting, inFlight := broker.QueueDepth(queue)
	require.Zero(t, waiting)
	require.Equal(t, 1, inFlight, "forced output cutoff must not ACK the accepted record")
}
