package devquiescence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func leaseInput(c *Coordinator, requestID string) SourceLeaseInput {
	snapshot := c.Snapshot()
	return SourceLeaseInput{OwnerID: snapshot.OwnerID, Generation: snapshot.Generation, RequestID: requestID, Kind: "http.gateway"}
}

func leaseRelease(input SourceLeaseInput, confirmed bool) SourceLeaseReleaseInput {
	return SourceLeaseReleaseInput{OwnerID: input.OwnerID, Generation: input.Generation, RequestID: input.RequestID, OwnershipConfirmed: &confirmed}
}

func leaseControl(t *testing.T, c *Coordinator, path string, body any) SourceLeaseReceipt {
	t.Helper()
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	response := httptest.NewRecorder()
	NewHandler(c).ServeHTTP(response, httptest.NewRequest(http.MethodPost, ControlPath+path, bytes.NewReader(encoded)))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	var receipt SourceLeaseReceipt
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &receipt))
	require.Equal(t, SourceLeaseSchema, receipt.SchemaVersion)
	return receipt
}

func TestRemoteSourceLeaseReplaysLostResponsesAndRetainsOwnerEpoch(t *testing.T) {
	c := New()
	input := leaseInput(c, "gateway-root")
	first := leaseControl(t, c, "/source-leases/acquire", input)
	require.Equal(t, SourceLeaseActive, first.Status)
	second := leaseControl(t, c, "/source-leases/acquire", input)
	require.Equal(t, first, second)
	require.Equal(t, 1, c.Snapshot().WorkCount, "lost acquire response replay never duplicates ownership")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := c.Quiesce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, first, leaseControl(t, c, "/source-leases/acquire", input), "already accepted roots can reconcile while draining")
	newRoot := input
	newRoot.RequestID = "unrelated-root"
	_, err = c.AcquireSourceLease(newRoot)
	require.ErrorIs(t, err, ErrFenced)
	completed := leaseControl(t, c, "/source-leases/release", leaseRelease(input, true))
	require.Equal(t, SourceLeaseCompleted, completed.Status)
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/release", leaseRelease(input, true)))
	require.Zero(t, c.Snapshot().WorkCount)
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/acquire", input), "completed receipt never grants execution again")
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	_, err = c.Resume(held.Generation)
	require.NoError(t, err)
	current := leaseInput(c, "new-generation")
	_, err = c.AcquireSourceLease(current)
	require.NoError(t, err)
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/release", leaseRelease(input, true)))
	require.Equal(t, 1, c.Snapshot().WorkCount, "old completed release cannot decrement current ownership")
	_, err = c.AcquireSourceLease(input)
	require.ErrorIs(t, err, ErrGeneration)
	staleKindSwap := input
	staleKindSwap.Kind = sourceLeaseGateway
	_, err = c.AcquireSourceLease(staleKindSwap)
	require.ErrorIs(t, err, ErrGeneration)
	require.Empty(t, c.Snapshot().EvidenceFailure, "stale requests cannot poison a new owner generation")
	_, err = New().AcquireSourceLease(current)
	require.ErrorIs(t, err, ErrOwnerIdentity, "a restarted owner cannot accept old process receipts")
	_, err = c.ReleaseSourceLease(leaseRelease(current, true))
	require.NoError(t, err)
}

func TestSourceLedgerCapacityNeverEvictsAndClearsOnlyOnSafeResume(t *testing.T) {
	c := NewWithOptions(Options{MaxSourceLeases: 2})
	first, second := leaseInput(c, "first"), leaseInput(c, "second")
	_, err := c.AcquireSourceLease(first)
	require.NoError(t, err)
	_, err = c.AcquireSourceLease(second)
	require.NoError(t, err)
	_, err = c.ReleaseSourceLease(leaseRelease(first, true))
	require.NoError(t, err)
	_, err = c.AcquireSourceLease(leaseInput(c, "third"))
	require.ErrorIs(t, err, ErrLeaseCapacity)
	require.Equal(t, 1, c.Snapshot().WorkCount)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.Quiesce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, err = c.Resume(c.Snapshot().Generation)
	require.ErrorIs(t, err, ErrNotSafe)
	_, err = c.ReleaseSourceLease(leaseRelease(second, true))
	require.NoError(t, err)
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	_, err = c.Resume(held.Generation)
	require.NoError(t, err)
	third := leaseInput(c, "third")
	_, err = c.AcquireSourceLease(third)
	require.NoError(t, err)
	_, err = c.ReleaseSourceLease(leaseRelease(third, true))
	require.NoError(t, err)
	unknown := leaseRelease(leaseInput(c, "unknown"), true)
	_, err = c.ReleaseSourceLease(unknown)
	require.ErrorIs(t, err, ErrUnknownLease)
	require.Zero(t, c.Snapshot().WorkCount)
}

func TestUnconfirmedRemoteCompletionPoisonsEvidenceAndIsIdempotent(t *testing.T) {
	c := New()
	input := leaseInput(c, "uncertain-root")
	_, err := c.AcquireSourceLease(input)
	require.NoError(t, err)
	completed, err := c.ReleaseSourceLease(leaseRelease(input, false))
	require.NoError(t, err)
	require.Equal(t, SourceLeaseCompleted, completed.Status)
	duplicate, err := c.ReleaseSourceLease(leaseRelease(input, false))
	require.NoError(t, err)
	require.Equal(t, completed, duplicate)
	require.Zero(t, c.Snapshot().WorkCount)
	snapshot, err := c.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrEvidence)
	require.False(t, snapshot.FixtureSafe)
	_, err = c.Resume(snapshot.Generation)
	require.ErrorIs(t, err, ErrEvidence)
	_, _, err = c.BeginSource("mapping.batch", "new-root")
	require.ErrorIs(t, err, ErrEvidence)
	_, err = c.ReleaseSourceLease(leaseRelease(input, true))
	require.ErrorIs(t, err, ErrLeaseConflict)
	require.Zero(t, c.Snapshot().WorkCount)
}

func TestRemoteAbandonmentRequiresShutdownAndStillJoinsLocalDescendants(t *testing.T) {
	c := New()
	input := leaseInput(c, "dead-gateway")
	_, err := c.AcquireSourceLease(input)
	require.NoError(t, err)
	child, err := c.BeginActivity("lambda.async", "native-child")
	require.NoError(t, err)
	require.ErrorIs(t, c.AbandonRemoteSources(), ErrNotSafe)
	require.Equal(t, 2, c.Snapshot().WorkCount, "resumable timeout cannot expire or abandon a foreign source")
	c.Shutdown()
	require.ErrorIs(t, c.AbandonRemoteSources(), ErrEvidence)
	require.Equal(t, 1, c.Snapshot().WorkCount)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.Quiesce(ctx)
	require.ErrorIs(t, err, context.Canceled, "sticky failure must not skip joining native children")
	child(nil)
	snapshot, err := c.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrEvidence)
	require.Equal(t, Shutdown, snapshot.State)
	require.False(t, snapshot.FixtureSafe)
	require.Zero(t, snapshot.WorkCount)
	require.NoError(t, c.AbandonRemoteSources())
	_, err = c.Resume(snapshot.Generation)
	require.ErrorIs(t, err, ErrShutdown)
}

func TestSourceLeaseControlsStrictJSONAndConfigurationBeforeUse(t *testing.T) {
	c := New()
	require.NoError(t, c.SetCallbackOrigin("http://127.0.0.1:1234/"))
	require.Equal(t, "http://127.0.0.1:1234", c.Snapshot().CallbackOrigin)
	handler := NewHandler(c)
	for _, scenario := range []struct{ path, body string }{
		{"/source-leases/acquire", `{}`},
		{"/source-leases/acquire", `{"owner_id":"x","generation":1,"request_id":"r","kind":"wrong"}`},
		{"/source-leases/acquire", `{"owner_id":"x","generation":1,"request_id":"r","kind":"http.gateway.continuation.extra"}`},
		{"/source-leases/acquire", `{"owner_id":"x","generation":1,"request_id":"r","kind":"http.gateway.continuation "}`},
		{"/source-leases/acquire", `{"owner_id":"x","generation":1,"request_id":"r","kind":"HTTP.gateway.continuation"}`},
		{"/source-leases/acquire", `{"owner_id":"x","generation":1,"request_id":"r","kind":"http.gateway","unknown":true}`},
		{"/source-leases/acquire", `{"owner_id":"x","OWNER_ID":"y","generation":1,"request_id":"r","kind":"http.gateway"}`},
		{"/source-leases/release", `{"owner_id":"x","generation":1,"request_id":"r"}`},
		{"/source-leases/release", `{"owner_id":"x","generation":1,"request_id":"r","ownership_confirmed":null}`},
		{"/source-leases/release", `{"owner_id":"x","generation":1,"request_id":"r","ownership_confirmed":"true"}`},
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, ControlPath+scenario.path, strings.NewReader(scenario.body)))
		require.Equal(t, http.StatusBadRequest, response.Code, scenario)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, ControlPath+"/source-leases/acquire", nil))
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
	require.Zero(t, c.Snapshot().WorkCount)
	require.ErrorIs(t, c.SetCallbackOrigin("http://127.0.0.1:5678"), ErrConfiguration)
	for _, invalid := range []string{"not-an-origin", "file:///tmp/file", "http://user:secret@localhost", "http://localhost/path", "http://localhost/?secret=yes"} {
		require.ErrorIs(t, New().SetCallbackOrigin(invalid), ErrConfiguration)
	}
}

func continuationLeaseInput(c *Coordinator, requestID string) SourceLeaseInput {
	input := leaseInput(c, requestID)
	input.Kind = sourceLeaseContinuation
	return input
}

func TestRemoteContinuationLeaseCountsAcceptedChainThroughDrainAndRetirement(t *testing.T) {
	c := New()
	root := leaseInput(c, "initial-public-root")
	leaseControl(t, c, "/source-leases/acquire", root)
	parent, err := c.BeginActivity("lambda_invoke", "accepted-native-parent")
	require.NoError(t, err)
	leaseControl(t, c, "/source-leases/release", leaseRelease(root, true))
	require.Equal(t, 1, c.Snapshot().WorkCount)

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.Quiesce(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, Draining, c.Snapshot().State)
	_, err = c.AcquireSourceLease(leaseInput(c, "fresh-public-root"))
	require.ErrorIs(t, err, ErrFenced)

	input := continuationLeaseInput(c, "downstream-http")
	active := leaseControl(t, c, "/source-leases/acquire", input)
	require.Equal(t, SourceLeaseActive, active.Status)
	require.Equal(t, 2, c.Snapshot().WorkCount)
	require.Equal(t, active, leaseControl(t, c, "/source-leases/acquire", input))
	require.Equal(t, 2, c.Snapshot().WorkCount, "lost acquisition response cannot duplicate continuation ownership")
	require.Equal(t, sourceLeaseContinuation, c.Snapshot().Activities[1].Kind)

	child, err := c.BeginActivity("lambda_invoke", "nested-native-integration")
	require.NoError(t, err)
	parent(nil)
	completed := leaseControl(t, c, "/source-leases/release", leaseRelease(input, true))
	require.Equal(t, SourceLeaseCompleted, completed.Status)
	require.Equal(t, 1, c.Snapshot().WorkCount, "gateway completion cannot settle its still-running native descendant")
	_, err = c.Quiesce(canceled)
	require.ErrorIs(t, err, context.Canceled)
	child(nil)

	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	require.True(t, held.FixtureSafe)
	require.Zero(t, held.WorkCount)
	_, err = c.AcquireSourceLease(continuationLeaseInput(c, "unrelated-held-continuation"))
	require.ErrorIs(t, err, ErrFenced)
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/acquire", input), "completed history never grants execution while held")
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/release", leaseRelease(input, true)))
	_, err = c.Resume(held.Generation)
	require.NoError(t, err)

	current := continuationLeaseInput(c, "resumed-continuation")
	_, err = c.AcquireSourceLease(current)
	require.ErrorIs(t, err, ErrFenced, "resuming open admission creates no accepted continuation ancestor")
	currentParent, err := c.BeginActivity("lambda_invoke", "resumed-native-parent")
	require.NoError(t, err)
	_, err = c.AcquireSourceLease(current)
	require.NoError(t, err, "trusted continuation requires an accepted open-generation ancestor")
	currentParent(nil)
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/release", leaseRelease(input, true)))
	require.Equal(t, 1, c.Snapshot().WorkCount, "retired continuation ACK cannot decrement current work")
	_, err = c.AcquireSourceLease(input)
	require.ErrorIs(t, err, ErrGeneration)
	staleKindSwap := input
	staleKindSwap.Kind = sourceLeaseGateway
	_, err = c.AcquireSourceLease(staleKindSwap)
	require.ErrorIs(t, err, ErrGeneration)
	require.Empty(t, c.Snapshot().EvidenceFailure, "stale requests cannot poison a new owner generation")
	_, err = New().AcquireSourceLease(current)
	require.ErrorIs(t, err, ErrOwnerIdentity)
	_, err = c.ReleaseSourceLease(leaseRelease(current, true))
	require.NoError(t, err)
}

func TestRemoteContinuationRejectsIdleDrainAndTransitionOnlyWork(t *testing.T) {
	t.Run("open-without-live-parent", func(t *testing.T) {
		c := New()
		_, err := c.AcquireSourceLease(continuationLeaseInput(c, "idle-open-continuation"))
		require.ErrorIs(t, err, ErrFenced)
		require.Equal(t, Open, c.Snapshot().State)
		require.Zero(t, c.Snapshot().WorkCount)
		parent, err := c.BeginActivity("lambda_invoke", "parent-finishes-after-precheck")
		require.NoError(t, err, "native activity admission remains unchanged")
		input := continuationLeaseInput(c, "parent-finished-continuation")
		require.Equal(t, 1, c.Snapshot().WorkCount, "gateway precheck sees accepted work")
		parent(nil)
		_, err = c.AcquireSourceLease(input)
		require.ErrorIs(t, err, ErrFenced, "authoritative acquisition rejects when the prechecked parent has joined")
		require.Zero(t, c.Snapshot().WorkCount)
		root := leaseInput(c, "ordinary-open-root")
		_, err = c.AcquireSourceLease(root)
		require.NoError(t, err, "public root admission still works in idle open state")
		_, err = c.ReleaseSourceLease(leaseRelease(root, true))
		require.NoError(t, err)
		require.Zero(t, c.Snapshot().WorkCount)
	})

	t.Run("idle-drain", func(t *testing.T) {
		c := New()
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		_, err := c.Quiesce(canceled)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, Draining, c.Snapshot().State)
		require.Zero(t, c.Snapshot().WorkCount)
		_, err = c.AcquireSourceLease(continuationLeaseInput(c, "unrelated-continuation"))
		require.ErrorIs(t, err, ErrFenced)
		require.Zero(t, c.Snapshot().WorkCount)
	})

	for _, phase := range []string{"drain-transition", "resume-transition"} {
		t.Run(phase, func(t *testing.T) {
			entered, gate := make(chan struct{}), make(chan struct{})
			var gateOnce sync.Once
			openGate := func() { gateOnce.Do(func() { close(gate) }) }
			t.Cleanup(openGate)
			hook := func() error { close(entered); <-gate; return nil }
			options := Options{DrainHooks: []DrainHook{{Start: hook}}}
			if phase == "resume-transition" {
				options.DrainHooks = []DrainHook{{Resume: hook}}
			}
			c := NewWithOptions(options)
			joined := make(chan error, 1)
			if phase == "resume-transition" {
				held, err := c.Quiesce(t.Context())
				require.NoError(t, err)
				go func() { _, err := c.Resume(held.Generation); joined <- err }()
			} else {
				go func() { _, err := c.Quiesce(t.Context()); joined <- err }()
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("transition callback never entered")
			}
			require.Equal(t, 1, c.Snapshot().WorkCount, "the hook is counted but owns no accepted application work")
			_, err := c.AcquireSourceLease(continuationLeaseInput(c, "transition-only-continuation"))
			require.ErrorIs(t, err, ErrFenced)
			_, err = c.AcquireSourceLease(leaseInput(c, "transition-only-root"))
			require.ErrorIs(t, err, ErrFenced)
			require.Equal(t, 1, c.Snapshot().WorkCount)
			openGate()
			require.NoError(t, <-joined)
			require.Zero(t, c.Snapshot().WorkCount)
		})
	}
}

func TestRemoteContinuationShutdownJoinsAcceptedDescendantsAndFencesRoots(t *testing.T) {
	c := New()
	parent, err := c.BeginActivity("lambda_async", "accepted-shutdown-parent")
	require.NoError(t, err)
	c.Shutdown()
	require.Equal(t, Shutdown, c.Snapshot().State)
	_, err = c.AcquireSourceLease(leaseInput(c, "unrelated-shutdown-root"))
	require.ErrorIs(t, err, ErrShutdown)

	input := continuationLeaseInput(c, "accepted-shutdown-continuation")
	active, err := c.AcquireSourceLease(input)
	require.NoError(t, err, "accepted native work needs its trusted HTTP continuation during owner shutdown")
	require.Equal(t, SourceLeaseActive, active.Status)
	require.Equal(t, active, leaseControl(t, c, "/source-leases/acquire", input))
	require.Equal(t, 2, c.Snapshot().WorkCount)
	parent(nil)
	require.Equal(t, 1, c.Snapshot().WorkCount)
	completed := leaseControl(t, c, "/source-leases/release", leaseRelease(input, true))
	require.Equal(t, SourceLeaseCompleted, completed.Status)
	require.Zero(t, c.Snapshot().WorkCount)
	_, err = c.AcquireSourceLease(continuationLeaseInput(c, "after-shutdown-join"))
	require.ErrorIs(t, err, ErrShutdown)
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/acquire", input), "completed reconciliation is an ACK, not shutdown admission")
	snapshot, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Shutdown, snapshot.State)
	require.False(t, snapshot.FixtureSafe)
	require.Zero(t, snapshot.WorkCount)
}

func TestRemoteContinuationDirtyEvidenceRejectsNewWorkButKeepsReceipts(t *testing.T) {
	c := New()
	parent, err := c.BeginActivity("lambda_invoke", "remaining-native-parent")
	require.NoError(t, err)
	input := continuationLeaseInput(c, "uncertain-continuation")
	_, err = c.AcquireSourceLease(input)
	require.NoError(t, err)
	completed, err := c.ReleaseSourceLease(leaseRelease(input, false))
	require.NoError(t, err)
	require.Equal(t, 1, c.Snapshot().WorkCount)
	require.NotEmpty(t, c.Snapshot().EvidenceFailure)
	_, err = c.AcquireSourceLease(continuationLeaseInput(c, "new-dirty-continuation"))
	require.ErrorIs(t, err, ErrEvidence, "a live parent cannot erase sticky ownership uncertainty")
	require.Equal(t, 1, c.Snapshot().WorkCount)
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/acquire", input))
	require.Equal(t, completed, leaseControl(t, c, "/source-leases/release", leaseRelease(input, false)))
	parent(nil)
	snapshot, err := c.Quiesce(t.Context())
	require.ErrorIs(t, err, ErrEvidence)
	require.False(t, snapshot.FixtureSafe)
	require.Zero(t, snapshot.WorkCount)
}

func TestRemoteContinuationSharesBoundedLedgerAndCompletedHistory(t *testing.T) {
	c := NewWithOptions(Options{MaxSourceLeases: 2})
	root := leaseInput(c, "root")
	input := continuationLeaseInput(c, "continuation")
	_, err := c.AcquireSourceLease(root)
	require.NoError(t, err)
	active, err := c.AcquireSourceLease(input)
	require.NoError(t, err)
	replayed, err := c.AcquireSourceLease(input)
	require.NoError(t, err)
	require.Equal(t, active, replayed)
	_, err = c.ReleaseSourceLease(leaseRelease(root, true))
	require.NoError(t, err)
	_, err = c.AcquireSourceLease(continuationLeaseInput(c, "capacity-overflow"))
	require.ErrorIs(t, err, ErrLeaseCapacity)
	require.Equal(t, 1, c.Snapshot().WorkCount)
	replayed, err = c.AcquireSourceLease(input)
	require.NoError(t, err, "capacity pressure cannot evict accepted continuation ownership")
	require.Equal(t, active, replayed)
	_, err = c.ReleaseSourceLease(leaseRelease(input, true))
	require.NoError(t, err)
	held, err := c.Quiesce(t.Context())
	require.NoError(t, err)
	_, err = c.Resume(held.Generation)
	require.NoError(t, err)
	current := continuationLeaseInput(c, "capacity-overflow")
	parent, err := c.BeginActivity("lambda_invoke", "resumed-capacity-parent")
	require.NoError(t, err)
	_, err = c.AcquireSourceLease(current)
	require.NoError(t, err, "only safe resume clears the shared completed ledger")
	parent(nil)
	_, err = c.ReleaseSourceLease(leaseRelease(current, true))
	require.NoError(t, err)
	require.Zero(t, c.Snapshot().WorkCount)
}

func TestRemoteSourceLeaseRejectsKindSwapOnActiveOrCompletedIdentity(t *testing.T) {
	for _, kind := range []string{sourceLeaseGateway, sourceLeaseContinuation} {
		for _, completed := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/completed=%t", kind, completed), func(t *testing.T) {
				c := New()
				var parent func(error)
				if kind == sourceLeaseContinuation {
					var err error
					parent, err = c.BeginActivity("lambda_invoke", "kind-conflict-parent")
					require.NoError(t, err)
				}
				input := leaseInput(c, "same-request")
				input.Kind = kind
				receipt, err := c.AcquireSourceLease(input)
				require.NoError(t, err)
				if completed {
					receipt, err = c.ReleaseSourceLease(leaseRelease(input, true))
					require.NoError(t, err)
				}
				work := c.Snapshot().WorkCount
				conflict := input
				conflict.Kind = sourceLeaseContinuation
				if kind == sourceLeaseContinuation {
					conflict.Kind = sourceLeaseGateway
				}
				_, err = c.AcquireSourceLease(conflict)
				require.ErrorIs(t, err, ErrLeaseConflict)
				require.Equal(t, work, c.Snapshot().WorkCount, "kind conflict cannot create or settle ownership")
				require.NotEmpty(t, c.Snapshot().EvidenceFailure)
				replay, err := c.AcquireSourceLease(input)
				require.NoError(t, err, "matching accepted identity remains reconcilable after a conflicting replay")
				require.Equal(t, receipt, replay)
				_, err = c.ReleaseSourceLease(leaseRelease(input, true))
				require.NoError(t, err)
				if parent != nil {
					parent(nil)
				}
				snapshot, err := c.Quiesce(t.Context())
				require.ErrorIs(t, err, ErrEvidence)
				require.False(t, snapshot.FixtureSafe)
				require.Zero(t, snapshot.WorkCount)
			})
		}
	}
}
