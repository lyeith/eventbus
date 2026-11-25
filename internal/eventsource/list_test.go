package eventsource

import (
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestListMappingsCapsPageSizeAndContinuesAfterDeletedMarker(t *testing.T) {
	q := newFakeQueue()
	s := newTestService(t, q, fakeInvoker{})
	disabled := false
	input := createInput()
	input.Enabled = &disabled
	const count = 103
	wantIDs := make([]string, 0, count)
	for i := range count {
		input.FunctionName = fmt.Sprintf("worker-%d", i)
		mapping, err := s.Create(t.Context(), input)
		require.NoError(t, err)
		wantIDs = append(wantIDs, mapping.UUID)
	}
	sort.Strings(wantIDs)

	requested := 10000
	first, err := s.List(ListInput{MaxItems: &requested})
	require.NoError(t, err)
	require.Len(t, first.EventSourceMappings, 100)
	require.NotEmpty(t, first.NextMarker)
	for i, mapping := range first.EventSourceMappings {
		require.Equal(t, wantIDs[i], mapping.UUID)
		require.Equal(t, "Disabled", mapping.State)
	}

	_, err = s.Delete(t.Context(), wantIDs[99])
	require.NoError(t, err)
	last, err := s.List(ListInput{Marker: first.NextMarker})
	require.NoError(t, err)
	require.Len(t, last.EventSourceMappings, 3)
	require.Empty(t, last.NextMarker)
	for i, mapping := range last.EventSourceMappings {
		require.Equal(t, wantIDs[100+i], mapping.UUID)
	}
}

func TestListMappingsFiltersNormalizeAndReturnDetachedSnapshots(t *testing.T) {
	q := newFakeQueue()
	s := newTestService(t, q, fakeInvoker{})
	disabled, maximum := false, 2
	input := createInput()
	input.Enabled = &disabled
	input.FunctionResponseTypes = []string{reportBatchItemFailures}
	input.ScalingConfig = &ScalingConfig{MaximumConcurrency: &maximum}
	mapping, err := s.Create(t.Context(), input)
	require.NoError(t, err)
	input.FunctionName = "worker"
	unqualified, err := s.Create(t.Context(), input)
	require.NoError(t, err)

	for _, reference := range []string{"worker:live", targetARN, "000000000000:function:worker:live"} {
		got, err := s.List(ListInput{FunctionName: reference, EventSourceARN: sourceARN})
		require.NoError(t, err)
		require.Equal(t, []Mapping{mapping}, got.EventSourceMappings)
		got.EventSourceMappings[0].FunctionResponseTypes[0] = "wrong"
		*got.EventSourceMappings[0].ScalingConfig.MaximumConcurrency = 999
	}
	got, err := s.Get(mapping.UUID)
	require.NoError(t, err)
	require.Equal(t, mapping, got)

	byBase, err := s.List(ListInput{FunctionName: "worker"})
	require.NoError(t, err)
	require.Equal(t, []Mapping{unqualified}, byBase.EventSourceMappings)

	byAbsentSource, err := s.List(ListInput{EventSourceARN: "arn:aws:sqs:us-east-1:000000000000:missing"})
	require.NoError(t, err)
	require.NotNil(t, byAbsentSource.EventSourceMappings)
	require.Empty(t, byAbsentSource.EventSourceMappings)
	require.Empty(t, byAbsentSource.NextMarker)
}

func TestListMappingsRejectsInvalidLimitsFiltersAndMismatchedMarker(t *testing.T) {
	q := newFakeQueue()
	s := newTestService(t, q, fakeInvoker{})
	disabled := false
	input := createInput()
	input.Enabled = &disabled
	for _, name := range []string{"worker:live", "worker:other"} {
		input.FunctionName = name
		_, err := s.Create(t.Context(), input)
		require.NoError(t, err)
	}
	one, zero, tooHigh := 1, 0, 10001
	first, err := s.List(ListInput{MaxItems: &one, EventSourceARN: sourceARN})
	require.NoError(t, err)
	require.NotEmpty(t, first.NextMarker)

	for _, input := range []ListInput{
		{MaxItems: &zero},
		{MaxItems: &tooHigh},
		{Marker: "malformed!"},
		{Marker: first.NextMarker},
		{Marker: first.NextMarker, EventSourceARN: sourceARN, FunctionName: "worker:live"},
		{EventSourceARN: "arn:aws:sqs:us-east-2:000000000000:other"},
		{FunctionName: "arn:aws:lambda:us-east-1:111111111111:function:worker"},
	} {
		_, err := s.List(input)
		assertCode(t, err, "InvalidParameterValueException", http.StatusBadRequest)
	}
}
