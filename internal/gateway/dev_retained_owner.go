package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lyeith/eventbus/internal/devquiescence"
)

// This opt-in harness adapter transfers HTTP ownership to the retained broker
// before native routing. It changes no authorizer or Lambda payload.
var (
	errRetainedAdmission    = errors.New("retained gateway admission is unavailable")
	errRetainedOwnership    = errors.New("retained gateway ownership is unresolved")
	errRetainedOwnerChanged = errors.New("retained gateway owner identity changed")
)

const (
	retainedControlBudget           = 5 * time.Second
	retainedGatewayRootKind         = "http.gateway"
	retainedGatewayContinuationKind = "http.gateway.continuation"
)

type retainedPending struct {
	input     devquiescence.SourceLeaseInput
	acquiring bool
	confirmed bool
}

type retainedGateway struct {
	url, ownerID, callbackOrigin string
	continuations                bool
	client                       *http.Client
	transport                    *http.Transport
	budget                       time.Duration
	mu                           sync.Mutex
	closing                      bool
	fatal                        error
	active                       int
	generation                   uint64
	pending                      map[string]retainedPending
	wake                         chan struct{}
	reconcileGate                chan struct{}
	shutdownSignal               chan struct{}
	shutdownOnce                 sync.Once
	monitorCancel                context.CancelFunc
	monitorDone                  chan struct{}
}

func validateRetainedControlURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || !loopbackHTTP(parsed) || parsed.Path != devquiescence.ControlPath || parsed.EscapedPath() != devquiescence.ControlPath {
		return errors.New("retained_owner_control_url must be the absolute loopback HTTP retained-owner control URL without credentials, query or fragment")
	}
	return nil
}

func loopbackHTTP(parsed *url.URL) bool {
	if parsed == nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return false
	}
	address := net.ParseIP(parsed.Hostname())
	return address != nil && address.IsLoopback()
}

func newRetainedGateway(cfg Config, options Options) (*retainedGateway, error) {
	if options.FrontendProxy != "" {
		return nil, errors.New("retained gateway does not support an unjoined frontend proxy")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil // The exclusive loopback control plane must not use environment proxies.
	owner := &retainedGateway{url: cfg.RetainedOwnerControlURL, transport: transport, budget: retainedControlBudget,
		continuations: cfg.RetainedOwnerContinuationPort != 0,
		pending:       make(map[string]retainedPending), wake: make(chan struct{}), reconcileGate: make(chan struct{}, 1), shutdownSignal: make(chan struct{})}
	owner.client = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ctx, cancel := context.WithTimeout(context.Background(), owner.controlBudget())
	defer cancel()
	snapshot, err := owner.snapshot(ctx)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	callback, err := url.Parse(snapshot.CallbackOrigin)
	control, _ := url.Parse(owner.url)
	if err != nil || !loopbackHTTP(callback) || callback.Path != "" || callback.Host == control.Host {
		transport.CloseIdleConnections()
		return nil, errors.New("retained gateway requires the owner's separate loopback callback_origin")
	}
	owner.ownerID, owner.callbackOrigin, owner.generation = snapshot.OwnerID, snapshot.CallbackOrigin, snapshot.Generation
	for _, route := range cfg.Routes {
		if !sameOrigin(route.Integration.InvokeURL, callback) {
			transport.CloseIdleConnections()
			return nil, errors.New("retained gateway Lambda integrations must use the owner's callback_origin")
		}
	}
	for _, authorizer := range cfg.Authorizers {
		if !sameOrigin(authorizer.InvokeURL, callback) {
			transport.CloseIdleConnections()
			return nil, errors.New("retained gateway authorizers must use the owner's callback_origin")
		}
	}
	return owner, nil
}

func sameOrigin(raw string, origin *url.URL) bool {
	parsed, err := url.Parse(raw)
	return err == nil && parsed.Scheme == origin.Scheme && parsed.Host == origin.Host
}

func (owner *retainedGateway) controlBudget() time.Duration {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.budget
}

func (owner *retainedGateway) start() {
	ctx, cancel := context.WithCancel(context.Background())
	owner.monitorCancel, owner.monitorDone = cancel, make(chan struct{})
	go func() {
		defer close(owner.monitorDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		var shutdownDeadline time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			budget := owner.controlBudget()
			if !shutdownDeadline.IsZero() {
				remaining := time.Until(shutdownDeadline)
				if remaining <= 0 {
					owner.signalShutdown(errRetainedOwnership)
					return
				}
				budget = min(budget, remaining)
			}
			poll, stop := context.WithTimeout(ctx, budget)
			snapshot, err := owner.snapshot(poll)
			stop()
			if errors.Is(err, errRetainedOwnerChanged) {
				owner.signalShutdown(errRetainedOwnerChanged)
				return
			}
			if !shutdownDeadline.IsZero() && !time.Now().Before(shutdownDeadline) {
				owner.signalShutdown(errRetainedOwnership)
				return
			}
			if err == nil && snapshot.State == devquiescence.Shutdown {
				if owner.continuations && snapshot.EvidenceFailure != "" {
					owner.signalShutdown(errRetainedOwnership)
					return
				}
				if !owner.continuations || snapshot.WorkCount == 0 && snapshot.CleanupEnvelopes == 0 {
					owner.signalShutdown(nil)
					return
				}
				if shutdownDeadline.IsZero() {
					shutdownDeadline = time.Now().Add(owner.controlBudget())
				}
			}
		}
	}()
}

func (owner *retainedGateway) signalShutdown(failure error) {
	owner.mu.Lock()
	owner.closing = true
	if failure != nil && owner.fatal == nil {
		owner.fatal = failure
	}
	close(owner.wake)
	owner.wake = make(chan struct{})
	owner.mu.Unlock()
	owner.shutdownOnce.Do(func() { close(owner.shutdownSignal) })
}

func (owner *retainedGateway) changed() {
	owner.mu.Lock()
	owner.fatal = errRetainedOwnerChanged
	close(owner.wake)
	owner.wake = make(chan struct{})
	owner.mu.Unlock()
}

func (owner *retainedGateway) snapshot(ctx context.Context) (devquiescence.Snapshot, error) {
	var snapshot devquiescence.Snapshot
	if err := owner.exchange(ctx, http.MethodGet, owner.url, nil, &snapshot); err != nil {
		return snapshot, err
	}
	if snapshot.SchemaVersion != "eventbus.retained-owner.v1" || !identifier.MatchString(snapshot.OwnerID) || len(snapshot.OwnerID) > 128 || snapshot.Generation == 0 {
		return snapshot, errRetainedAdmission
	}
	if owner.ownerID != "" && (snapshot.OwnerID != owner.ownerID || snapshot.CallbackOrigin != owner.callbackOrigin) {
		owner.changed()
		return snapshot, errRetainedOwnerChanged
	}
	if owner.ownerID != "" {
		owner.mu.Lock()
		if snapshot.Generation > owner.generation {
			owner.generation = snapshot.Generation
		}
		owner.mu.Unlock()
	}
	return snapshot, nil
}

type retainedRefusal struct{}

func (retainedRefusal) Error() string { return errRetainedAdmission.Error() }

func (owner *retainedGateway) exchange(ctx context.Context, method, endpoint string, input, output any) error {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return errRetainedAdmission
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return errRetainedAdmission
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := owner.client.Do(request)
	if err != nil {
		return errRetainedOwnership
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || len(data) > 64<<10 {
		return errRetainedOwnership
	}
	if response.StatusCode != http.StatusOK {
		var failure struct {
			Error string `json:"error"`
		}
		if response.StatusCode >= 400 && response.StatusCode < 500 && json.Unmarshal(data, &failure) == nil && failure.Error != "" {
			return retainedRefusal{}
		}
		return errRetainedOwnership
	}
	if json.Unmarshal(data, output) != nil {
		return errRetainedOwnership
	}
	return nil
}

func (owner *retainedGateway) acquireOnce(ctx context.Context, input devquiescence.SourceLeaseInput) (string, error) {
	var receipt devquiescence.SourceLeaseReceipt
	err := owner.exchange(ctx, http.MethodPost, owner.url+"/source-leases/acquire", input, &receipt)
	if err != nil {
		return "", err
	}
	if !validSourceReceipt(receipt, input) || (receipt.Status != devquiescence.SourceLeaseActive && receipt.Status != devquiescence.SourceLeaseCompleted) {
		return "", errRetainedOwnership
	}
	return string(receipt.Status), nil
}

func (owner *retainedGateway) acquire(ctx context.Context, input devquiescence.SourceLeaseInput) (string, error) {
	for {
		status, err := owner.acquireOnce(ctx, input)
		var refusal retainedRefusal
		if err == nil || errors.As(err, &refusal) {
			return status, err
		}
		if !waitRetainedRetry(ctx) {
			return "", errRetainedOwnership
		}
	}
}

func waitRetainedRetry(ctx context.Context) bool {
	timer := time.NewTimer(25 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func validSourceReceipt(receipt devquiescence.SourceLeaseReceipt, input devquiescence.SourceLeaseInput) bool {
	return receipt.SchemaVersion == devquiescence.SourceLeaseSchema && receipt.OwnerID == input.OwnerID && receipt.Generation == input.Generation && receipt.RequestID == input.RequestID
}

func (owner *retainedGateway) releaseOnce(ctx context.Context, pending retainedPending) error {
	var receipt devquiescence.SourceLeaseReceipt
	confirmed := pending.confirmed
	input := devquiescence.SourceLeaseReleaseInput{OwnerID: pending.input.OwnerID, Generation: pending.input.Generation, RequestID: pending.input.RequestID, OwnershipConfirmed: &confirmed}
	if err := owner.exchange(ctx, http.MethodPost, owner.url+"/source-leases/release", input, &receipt); err != nil {
		return err
	}
	if !validSourceReceipt(receipt, pending.input) || receipt.Status != devquiescence.SourceLeaseCompleted {
		return errRetainedOwnership
	}
	return nil
}

func (owner *retainedGateway) release(ctx context.Context, pending retainedPending) error {
	for {
		err := owner.releaseOnce(ctx, pending)
		var refusal retainedRefusal
		if err == nil || errors.As(err, &refusal) {
			return err
		}
		if !waitRetainedRetry(ctx) {
			return errRetainedOwnership
		}
	}
}

func (owner *retainedGateway) enter() (uint64, error) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.closing || owner.fatal != nil {
		return 0, errRetainedAdmission
	}
	owner.active++
	return owner.generation, nil
}

func (owner *retainedGateway) executionAllowed() bool {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	return owner.fatal == nil && len(owner.pending) == 0
}

func (owner *retainedGateway) leave() {
	owner.mu.Lock()
	owner.active--
	close(owner.wake)
	owner.wake = make(chan struct{})
	owner.mu.Unlock()
}

func (owner *retainedGateway) remember(pending retainedPending) {
	owner.mu.Lock()
	owner.pending[pending.input.RequestID] = pending
	if !pending.confirmed {
		owner.fatal = errRetainedOwnership
	}
	owner.mu.Unlock()
}

// Reconciliation never runs application work. An uncertain acquire is either
// refused or resolved to its original lease and immediately completed unused.
func (owner *retainedGateway) reconcile(ctx context.Context) error {
	select {
	case owner.reconcileGate <- struct{}{}:
	case <-ctx.Done():
		return errRetainedOwnership
	}
	defer func() { <-owner.reconcileGate }()
	owner.mu.Lock()
	pending := make([]retainedPending, 0, len(owner.pending))
	for _, item := range owner.pending {
		pending = append(pending, item)
	}
	owner.mu.Unlock()
	for _, item := range pending {
		if item.acquiring {
			status, err := owner.acquire(ctx, item.input)
			var refusal retainedRefusal
			if errors.As(err, &refusal) {
				if _, checkErr := owner.snapshot(ctx); checkErr != nil {
					return checkErr
				}
			} else if err != nil {
				return err
			} else if status == devquiescence.SourceLeaseActive {
				item.acquiring = false
				owner.remember(item)
				if err := owner.release(ctx, item); err != nil {
					return err
				}
			}
		} else if err := owner.release(ctx, item); err != nil {
			return err
		}
		owner.mu.Lock()
		delete(owner.pending, item.input.RequestID)
		owner.mu.Unlock()
	}
	return nil
}

func (owner *retainedGateway) complete(input devquiescence.SourceLeaseInput, confirmed bool) {
	pending := retainedPending{input: input, confirmed: confirmed}
	owner.remember(pending)
	ctx, cancel := context.WithTimeout(context.Background(), owner.controlBudget())
	defer cancel()
	_ = owner.reconcile(ctx) // Failure leaves the receipt owned and new roots fenced.
}

func (owner *retainedGateway) serve(w http.ResponseWriter, r *http.Request, kind string, handler http.Handler) {
	generation, admissionErr := owner.enter()
	if admissionErr != nil {
		writeGatewayError(w, http.StatusServiceUnavailable)
		return
	}
	defer owner.leave()
	ctx, cancel := context.WithTimeout(r.Context(), owner.controlBudget())
	defer cancel()
	// The epoch was frozen at entry, before any control-plane exchange. Validate
	// it without migrating a received candidate to a later resumed epoch.
	snapshot, err := owner.snapshot(ctx)
	if err != nil || !retainedGatewaySnapshotAllows(snapshot, kind) || snapshot.Generation != generation {
		writeGatewayError(w, http.StatusServiceUnavailable)
		return
	}
	if owner.reconcile(ctx) != nil {
		writeGatewayError(w, http.StatusServiceUnavailable)
		return
	}
	input := devquiescence.SourceLeaseInput{OwnerID: owner.ownerID, Generation: generation, RequestID: uuid.NewString(), Kind: kind}
	status, err := owner.acquire(ctx, input)
	if err != nil {
		var refusal retainedRefusal
		if !errors.As(err, &refusal) {
			owner.remember(retainedPending{input: input, acquiring: true, confirmed: true})
			fresh, stop := context.WithTimeout(context.Background(), owner.controlBudget())
			_ = owner.reconcile(fresh)
			stop()
		} else {
			_, _ = owner.snapshot(ctx)
		}
		writeGatewayError(w, http.StatusServiceUnavailable)
		return
	}
	if status != devquiescence.SourceLeaseActive {
		writeGatewayError(w, http.StatusServiceUnavailable)
		return
	}
	normalCompletion := false
	defer func() {
		if recovered := recover(); recovered != nil {
			owner.complete(input, false)
			panic(recovered)
		}
		owner.complete(input, normalCompletion)
	}()
	// Completion uncertainty can arise while this grant is in flight. Resolve
	// it before starting application work, or complete this granted root unused.
	if owner.reconcile(ctx) != nil || r.Context().Err() != nil || !owner.executionAllowed() {
		writeGatewayError(w, http.StatusServiceUnavailable)
		normalCompletion = true // The granted but unused envelope returned normally.
		return
	}
	handler.ServeHTTP(w, r)
	normalCompletion = true
}

// Snapshot is only a precheck. AcquireSourceLease atomically decides whether
// accepted actual work still exists, excluding transition-only activity, before
// the gateway reads a body or invokes native authorization/integration.
func retainedGatewaySnapshotAllows(snapshot devquiescence.Snapshot, kind string) bool {
	if snapshot.EvidenceFailure != "" {
		return false
	}
	switch kind {
	case retainedGatewayRootKind:
		return snapshot.State == devquiescence.Open
	case retainedGatewayContinuationKind:
		return (snapshot.State == devquiescence.Open || snapshot.State == devquiescence.Draining || snapshot.State == devquiescence.Shutdown) && snapshot.WorkCount > 0
	default:
		return false
	}
}

func (owner *retainedGateway) close(ctx context.Context) error {
	if owner.monitorCancel != nil {
		owner.monitorCancel()
		select {
		case <-owner.monitorDone:
		case <-ctx.Done():
			return errRetainedOwnership
		}
	}
	owner.mu.Lock()
	owner.closing = true
	for owner.active != 0 {
		wake := owner.wake
		owner.mu.Unlock()
		select {
		case <-wake:
		case <-ctx.Done():
			return errRetainedOwnership
		}
		owner.mu.Lock()
	}
	owner.mu.Unlock()
	if err := owner.reconcile(ctx); err != nil {
		return errors.Join(errRetainedOwnership, err)
	}
	owner.mu.Lock()
	fatal := owner.fatal
	owner.mu.Unlock()
	owner.transport.CloseIdleConnections()
	return fatal
}

func (cfg Config) validateRetainedGateway() error {
	if cfg.RetainedOwnerContinuationPort < 0 || cfg.RetainedOwnerContinuationPort > 65535 {
		return errors.New("retained_owner_continuation_port must be 1..65535, or omitted")
	}
	if cfg.RetainedOwnerContinuationPort != 0 {
		if cfg.RetainedOwnerContinuationPort == cfg.Port {
			return errors.New("retained_owner_continuation_port must differ from the public gateway port")
		}
		if cfg.RetainedOwnerControlURL == "" {
			return errors.New("retained_owner_continuation_port requires retained_owner_control_url")
		}
	}
	if cfg.RetainedOwnerControlURL == "" {
		return nil
	}
	if err := validateRetainedControlURL(cfg.RetainedOwnerControlURL); err != nil {
		return err
	}
	for _, route := range cfg.Routes {
		if route.Integration.Type != "AWS_PROXY" {
			return errors.New("retained gateway requires joined AWS_PROXY Lambda integrations")
		}
	}
	return nil
}
