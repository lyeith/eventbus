package eventsource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type blockedAckQueue struct {
	*fakeQueue
	enteredAck chan string
	releaseAck chan struct{}
}

func (q *blockedAckQueue) Delete(ctx context.Context, receipt string) (bool, error) {
	q.enteredAck <- receipt
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-q.releaseAck:
		return q.fakeQueue.Delete(ctx, receipt)
	}
}

func TestUpdateDrainsActiveHandlerAndReceiptBeforeSwitchingSettings(t *testing.T) {
	q := &blockedAckQueue{fakeQueue: newFakeQueue(), enteredAck: make(chan string, 1), releaseAck: make(chan struct{})}
	started := make(chan string, 2)
	releaseHandler := make(chan struct{})
	functions := fakeInvoker{invoke: func(ctx context.Context, arn string, _ []byte) error {
		started <- arn
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-releaseHandler:
			return nil
		}
	}}
	s := newTestService(t, q, functions)
	created, err := s.Create(t.Context(), createInput())
	require.NoError(t, err)
	q.batches <- []Record{{MessageID: "message", ReceiptHandle: "original-receipt"}}
	select {
	case arn := <-started:
		require.Equal(t, targetARN, arn)
	case <-time.After(time.Second):
		t.Fatal("original worker did not invoke")
	}
	next := "worker:next"
	batchSize := 4
	responseTypes := []string{"ReportBatchItemFailures"}
	type updateResult struct {
		mapping Mapping
		err     error
	}
	updated := make(chan updateResult, 1)
	go func() {
		mapping, err := s.Update(t.Context(), created.UUID, UpdateInput{FunctionName: &next, BatchSize: &batchSize, FunctionResponseTypes: &responseTypes})
		updated <- updateResult{mapping, err}
	}()
	require.Eventually(t, func() bool {
		state, _ := s.Get(created.UUID)
		return state.State == "Updating"
	}, time.Second, time.Millisecond)
	_, err = s.Delete(t.Context(), created.UUID)
	assertCode(t, err, "ResourceInUseException", 400)
	conflicting := createInput()
	conflicting.FunctionName = next
	_, err = s.Create(t.Context(), conflicting)
	assertCode(t, err, "ResourceConflictException", 409)
	close(releaseHandler)
	select {
	case receipt := <-q.enteredAck:
		require.Equal(t, "original-receipt", receipt)
	case result := <-updated:
		t.Fatalf("update completed before acknowledging original delivery: %+v", result)
	case <-time.After(time.Second):
		t.Fatal("original ACK did not start")
	}
	select {
	case result := <-updated:
		t.Fatalf("update did not wait for ACK: %+v", result)
	default:
	}
	require.NoError(t, s.TagResource(created.EventSourceMappingARN, map[string]string{"during": "drain"}))
	close(q.releaseAck)
	select {
	case result := <-updated:
		require.NoError(t, result.err)
		require.Equal(t, created.UUID, result.mapping.UUID)
		require.Equal(t, created.EventSourceMappingARN, result.mapping.EventSourceMappingARN)
		require.Equal(t, "Enabled", result.mapping.State)
		require.Equal(t, 4, result.mapping.BatchSize)
		require.Equal(t, responseTypes, result.mapping.FunctionResponseTypes)
		require.Equal(t, "OK", result.mapping.LastProcessingResult)
	case <-time.After(time.Second):
		t.Fatal("update did not finish")
	}
	tags, err := s.ListTags(created.EventSourceMappingARN)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"during": "drain"}, tags)
	require.Equal(t, "original-receipt", <-q.deleted)
	q.batches <- []Record{{MessageID: "next", ReceiptHandle: "next-receipt"}}
	select {
	case arn := <-started:
		require.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:worker:next", arn)
	case <-time.After(time.Second):
		t.Fatal("replacement worker did not invoke")
	}
}

type invalidatedQueue struct {
	*fakeQueue
	invalid bool
}

func (q *invalidatedQueue) Info() (QueueInfo, error) {
	if q.invalid {
		return QueueInfo{}, errors.New("original queue removed")
	}
	return q.fakeQueue.Info()
}

func TestUpdateRejectsReplacementSourceAndInvalidCandidates(t *testing.T) {
	q := &invalidatedQueue{fakeQueue: newFakeQueue()}
	s := newTestService(t, q, fakeInvoker{})
	create := createInput()
	disabled := false
	create.Enabled = &disabled
	created, err := s.Create(t.Context(), create)
	require.NoError(t, err)
	invalidBatch := 11
	_, err = s.Update(t.Context(), created.UUID, UpdateInput{BatchSize: &invalidBatch})
	assertCode(t, err, "InvalidParameterValueException", 400)
	actual, err := s.Get(created.UUID)
	require.NoError(t, err)
	require.Equal(t, created, actual)
	q.invalid = true
	enabled := true
	_, err = s.Update(t.Context(), created.UUID, UpdateInput{Enabled: &enabled})
	assertCode(t, err, "ResourceNotFoundException", 404)
	actual, err = s.Get(created.UUID)
	require.NoError(t, err)
	require.Equal(t, created, actual)
}
