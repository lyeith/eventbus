package devquiescence

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
