//go:build linux || darwin

package consumer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lyeith/eventbus/internal/localexec"
	"github.com/lyeith/eventbus/internal/messaging"
	"github.com/stretchr/testify/require"
)

func TestConsumerOwnershipFailureFencesAllPollersAndJoinsActualCleanup(t *testing.T) {
	for _, exitCode := range []int{0, 7} {
		t.Run(fmt.Sprint(exitCode), func(t *testing.T) { consumerOwnershipFailureFixture(t, exitCode) })
	}
}

func consumerOwnershipFailureFixture(t *testing.T, exitCode int) {

	broker := messaging.NewBroker("us-east-1", "000000000000", 0)
	faultQueue := broker.CreateQueue("fault", 0, 0)
	peerQueue := broker.CreateQueue("peer", 0, 0)
	dlq := broker.CreateQueue("fault-dlq", 0, 0)
	for _, queue := range []*messaging.Queue{faultQueue, peerQueue} {
		_, err := broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: queue.Name})
		require.NoError(t, err)
	}
	directory := t.TempDir()
	peerMarker, faultMarker := filepath.Join(directory, "peer.pid"), filepath.Join(directory, "fault.pid")
	cleanupConsumerChild(t, peerMarker)
	cleanupConsumerChild(t, faultMarker)
	peerHandler, faultHandler := filepath.Join(directory, "peer"), filepath.Join(directory, "fault")
	require.NoError(t, os.WriteFile(peerHandler, []byte("#!/bin/sh\nsleep 60 &\necho \"$!\" > \"$PEER_MARKER\"\nwait\n"), 0700))
	require.NoError(t, os.WriteFile(faultHandler, []byte(fmt.Sprintf("#!/bin/sh\nwhile [ ! -e \"$PEER_MARKER\" ]; do sleep 0.01; done\nsleep 60 &\necho \"$!\" > \"$FAULT_MARKER\"\nprintf '{}\\n'\nexit %d\n", exitCode)), 0700))
	manager := NewConsumerManager(broker, directory)
	faultCleaned, peerCleaned := make(chan *exec.Cmd, 1), make(chan *exec.Cmd, 1)
	releasePeer := make(chan struct{})
	var releaseOnce sync.Once
	openPeer := func() { releaseOnce.Do(func() { close(releasePeer) }) }
	manager.processCleanup = func(command *exec.Cmd) error {
		err := localexec.Cleanup(command)
		if command.Process != nil {
			switch command.Path {
			case faultHandler:
				faultCleaned <- command
			case peerHandler:
				peerCleaned <- command
				<-releasePeer
			}
		}
		return err
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		openPeer()
		joined, stop := context.WithTimeout(context.Background(), 8*time.Second)
		defer stop()
		_ = manager.Wait(joined)
	})
	manager.Start(ctx, []ConsumerEntry{
		{Name: "fault", Queue: faultQueue.Name, DeadLetterQueue: dlq.Name, MaxReceiveCount: 1, Type: "go", Handler: faultHandler, BatchSize: 1, TimeoutSeconds: 10, Env: map[string]string{"PEER_MARKER": peerMarker, "FAULT_MARKER": faultMarker}},
		{Name: "peer", Queue: peerQueue.Name, MaxReceiveCount: 3, Type: "go", Handler: peerHandler, BatchSize: 1, TimeoutSeconds: 10, Env: map[string]string{"PEER_MARKER": peerMarker}},
	})
	var faultCommand, peerCommand *exec.Cmd
	select {
	case faultCommand = <-faultCleaned:
	case <-time.After(5 * time.Second):
		t.Fatal("retained-pipe owner did not finish native process cleanup")
	}
	select {
	case peerCommand = <-peerCleaned:
	case <-time.After(3 * time.Second):
		t.Fatal("ownership failure did not cancel the other real handler")
	}
	require.NotNil(t, faultCommand.ProcessState, "the direct child must be reaped before ownership failure")
	require.NotNil(t, peerCommand.ProcessState, "the peer direct child must be reaped before the cleanup gate")
	for _, marker := range []string{faultMarker, peerMarker} {
		child := consumerChildPID(t, marker)
		require.Eventually(t, func() bool { return !consumerChildRunning(child) }, 3*time.Second, 10*time.Millisecond, "native group cleanup must stop the real descendant")
	}
	waiting, inFlight := broker.QueueDepth(faultQueue)
	require.Zero(t, waiting)
	require.Equal(t, 1, inFlight, "uncertainty retains the admitted original record")
	waiting, inFlight = broker.QueueDepth(dlq)
	require.Zero(t, waiting, "ownership failure must not be classified as a business failure/redrive")
	require.Zero(t, inFlight)
	deadline, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	require.ErrorIs(t, manager.Wait(deadline), context.DeadlineExceeded, "Wait must not finish while actual peer cleanup is gated")
	openPeer()
	joined, stopJoined := context.WithTimeout(t.Context(), 3*time.Second)
	defer stopJoined()
	require.ErrorIs(t, manager.Wait(joined), exec.ErrWaitDelay)
	require.ErrorIs(t, manager.Wait(t.Context()), exec.ErrWaitDelay, "joined uncertainty remains sticky")
	canceled, stopCanceled := context.WithCancel(t.Context())
	stopCanceled()
	require.ErrorIs(t, manager.Wait(canceled), exec.ErrWaitDelay, "completed join must not lose sticky evidence to caller cancellation")

	lateMarker, lateHandler := filepath.Join(directory, "late.started"), filepath.Join(directory, "late")
	require.NoError(t, os.WriteFile(lateHandler, []byte("#!/bin/sh\nprintf started > \"$LATE_MARKER\"\nprintf '{}\\n'\n"), 0700))
	late := ConsumerEntry{Name: "late", Queue: peerQueue.Name, Type: "go", Handler: lateHandler, BatchSize: 1, TimeoutSeconds: 10, Env: map[string]string{"LATE_MARKER": lateMarker}}
	manager.Start(t.Context(), []ConsumerEntry{late})
	_, err := manager.invokeHandlerResult(t.Context(), late, buildLambdaEvent(nil))
	require.ErrorIs(t, err, exec.ErrWaitDelay, "both poller admission and direct process launch remain fenced")
	_, err = os.Stat(lateMarker)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestConsumerCleanupFailureIsStickyAndPreventsLaterExecution(t *testing.T) {
	directory := t.TempDir()
	handler := filepath.Join(directory, "handler")
	require.NoError(t, os.WriteFile(handler, []byte("#!/bin/sh\nprintf '{}\\n'\n"), 0700))
	manager := NewConsumerManager(messaging.NewBroker("us-east-1", "000000000000", 0), directory)
	uncertain := errors.New("private process cleanup uncertainty")
	var actualCleanups atomic.Int32
	manager.processCleanup = func(command *exec.Cmd) error {
		err := localexec.Cleanup(command)
		if command.Process == nil {
			return err
		}
		actualCleanups.Add(1)
		require.NotNil(t, command.ProcessState, "the real direct child joined before injected uncertainty")
		return errors.Join(err, uncertain)
	}
	entry := ConsumerEntry{Name: "cleanup", Type: "go", Handler: handler, TimeoutSeconds: 10}
	_, err := manager.invokeHandlerResult(t.Context(), entry, buildLambdaEvent(nil))
	require.ErrorIs(t, err, uncertain)
	require.ErrorIs(t, manager.Wait(t.Context()), uncertain)
	entry.TimeoutSeconds = 0 // A simultaneous expired budget cannot erase the typed fence.
	_, err = manager.invokeHandlerResult(t.Context(), entry, buildLambdaEvent(nil))
	require.ErrorIs(t, err, uncertain)
	require.EqualValues(t, 1, actualCleanups.Load(), "a fenced manager must not start another actual child")
}

type consumerSettlementBroker struct {
	*messaging.Broker
	retry, success         chan struct{}
	retryOnce, successOnce sync.Once
}

func (broker *consumerSettlementBroker) ExtendMessageVisibility(queue *messaging.Queue, receipt string, timeout time.Duration) bool {
	settled := broker.Broker.ExtendMessageVisibility(queue, receipt, timeout)
	if settled && timeout == baseRetryVisibility {
		broker.retryOnce.Do(func() { close(broker.retry) })
	}
	return settled
}
func (broker *consumerSettlementBroker) DeleteMessage(queue *messaging.Queue, receipt string) bool {
	settled := broker.Broker.DeleteMessage(queue, receipt)
	if settled {
		broker.successOnce.Do(func() { close(broker.success) })
	}
	return settled
}

func TestConsumerBusinessFailurePreservesRetryAndOtherExecution(t *testing.T) {
	broker := &consumerSettlementBroker{Broker: messaging.NewBroker("us-east-1", "000000000000", 0), retry: make(chan struct{}), success: make(chan struct{})}
	failed, successful := broker.CreateQueue("business-failed", 0, 0), broker.CreateQueue("business-success", 0, 0)
	for _, queue := range []*messaging.Queue{failed, successful} {
		_, err := broker.SendQueueMessage(queue, messaging.QueueMessageInput{Body: queue.Name})
		require.NoError(t, err)
	}
	directory := t.TempDir()
	failHandler, successHandler := filepath.Join(directory, "fail"), filepath.Join(directory, "success")
	require.NoError(t, os.WriteFile(failHandler, []byte("#!/bin/sh\nexit 7\n"), 0700))
	require.NoError(t, os.WriteFile(successHandler, []byte("#!/bin/sh\nprintf '{}\\n'\n"), 0700))
	manager := NewConsumerManager(broker, directory)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() {
		cancel()
		joined, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = manager.Wait(joined)
	})
	manager.Start(ctx, []ConsumerEntry{
		{Name: "failed", Queue: failed.Name, MaxReceiveCount: 3, Type: "go", Handler: failHandler, BatchSize: 1, TimeoutSeconds: 10},
		{Name: "successful", Queue: successful.Name, MaxReceiveCount: 3, Type: "go", Handler: successHandler, BatchSize: 1, TimeoutSeconds: 10},
	})
	for _, done := range []chan struct{}{broker.retry, broker.success} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("ordinary business failure blocked retry or the peer's successful execution")
		}
	}
	cancel()
	joined, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	require.NoError(t, manager.Wait(joined), "ordinary handler failure must not poison joined ownership")
	waiting, inFlight := broker.QueueDepth(failed)
	require.Zero(t, waiting)
	require.Equal(t, 1, inFlight)
	waiting, inFlight = broker.QueueDepth(successful)
	require.Zero(t, waiting)
	require.Zero(t, inFlight)
}
