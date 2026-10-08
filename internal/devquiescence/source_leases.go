package devquiescence

import (
	"encoding/json"
	"errors"
	"net/http"
)

const (
	SourceLeaseSchema    = "eventbus.retained-source-lease.v1"
	SourceLeaseActive    = "active"
	SourceLeaseCompleted = "completed"

	sourceLeaseGateway      = "http.gateway"
	sourceLeaseContinuation = "http.gateway.continuation"
)

var (
	ErrOwnerIdentity = errors.New("retained-owner identity does not match")
	ErrUnknownLease  = errors.New("retained source lease is unknown")
	ErrLeaseCapacity = errors.New("retained source lease ledger is full")
	ErrLeaseConflict = errors.New("retained source lease completion conflicts")
)

type SourceLeaseInput struct {
	OwnerID    string `json:"owner_id"`
	Generation uint64 `json:"generation"`
	RequestID  string `json:"request_id"`
	Kind       string `json:"kind"`
}

type SourceLeaseReleaseInput struct {
	OwnerID            string `json:"owner_id"`
	Generation         uint64 `json:"generation"`
	RequestID          string `json:"request_id"`
	OwnershipConfirmed *bool  `json:"ownership_confirmed"`
}

type SourceLeaseReceipt struct {
	SchemaVersion string `json:"schema_version"`
	OwnerID       string `json:"owner_id"`
	Generation    uint64 `json:"generation"`
	RequestID     string `json:"request_id"`
	Status        string `json:"status"`
}

type sourceLeaseReceiptState struct {
	status    string
	confirmed bool
}

type sourceLease struct {
	sourceLeaseReceiptState
	workID uint64
	kind   string
}

type sourceLeaseIdentity struct {
	generation uint64
	requestID  string
}

func (c *Coordinator) sourceReceiptLocked(generation uint64, requestID, status string) SourceLeaseReceipt {
	return SourceLeaseReceipt{SchemaVersion: SourceLeaseSchema, OwnerID: c.ownerID,
		Generation: generation, RequestID: requestID, Status: status}
}

func validSourceLeaseKind(kind string) bool {
	return kind == sourceLeaseGateway || kind == sourceLeaseContinuation
}

// AcquireSourceLease is idempotent for one kind within an owner generation.
// Active leases have no expiry, including lost-response or dead-client intervals.
// A completed receipt acknowledges history and never grants execution again.
// Trusted continuations require live accepted work even in open admission;
// fresh gateway roots require healthy open admission.
func (c *Coordinator) AcquireSourceLease(input SourceLeaseInput) (SourceLeaseReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = true
	if input.OwnerID != c.ownerID {
		return SourceLeaseReceipt{}, ErrOwnerIdentity
	}
	if input.Generation != c.generation {
		return SourceLeaseReceipt{}, ErrGeneration
	}
	if !validSourceLeaseKind(input.Kind) || !safeIdentity.MatchString(input.RequestID) {
		return SourceLeaseReceipt{}, ErrLeaseConflict
	}
	if existing := c.sourceLeases[input.RequestID]; existing != nil {
		if existing.kind != input.Kind {
			c.failEvidenceLocked("incomplete_ownership_evidence")
			return SourceLeaseReceipt{}, ErrLeaseConflict
		}
		return c.sourceReceiptLocked(input.Generation, input.RequestID, existing.status), nil
	}
	allowed := c.sourceOpenLocked()
	if input.Kind == sourceLeaseContinuation {
		// Admission is authoritative if the parent joins after a gateway
		// precheck. Open state alone cannot create a continuation root.
		// Closing fences roots while accepted descendants still need callback
		// transports; transition hooks and sticky uncertainty admit no work.
		allowed = !c.evidenceFailure && c.work > c.transitions && c.descendantsAllowedLocked()
	}
	if !allowed {
		return SourceLeaseReceipt{}, c.admissionErrorLocked()
	}
	if len(c.sourceLeases) >= c.maxSourceLeases {
		return SourceLeaseReceipt{}, ErrLeaseCapacity
	}
	id := c.addWorkLocked(input.Kind, input.RequestID)
	c.sourceLeases[input.RequestID] = &sourceLease{workID: id, kind: input.Kind, sourceLeaseReceiptState: sourceLeaseReceiptState{status: SourceLeaseActive}}
	return c.sourceReceiptLocked(input.Generation, input.RequestID, SourceLeaseActive), nil
}

// ReleaseSourceLease records actual envelope/child join, not caller cancellation.
// An explicit uncertain completion releases the live count but poisons evidence;
// it can never authorize cleanup or Resume. Replaying the same completion is safe.
func (c *Coordinator) ReleaseSourceLease(input SourceLeaseReleaseInput) (SourceLeaseReceipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used = true
	if input.OwnerID != c.ownerID {
		return SourceLeaseReceipt{}, ErrOwnerIdentity
	}
	if input.OwnershipConfirmed == nil || !safeIdentity.MatchString(input.RequestID) {
		return SourceLeaseReceipt{}, ErrLeaseConflict
	}
	if input.Generation != c.generation {
		retired, ok := c.retiredSourceLeases[sourceLeaseIdentity{input.Generation, input.RequestID}]
		if !ok {
			return SourceLeaseReceipt{}, ErrGeneration
		}
		if retired.confirmed != *input.OwnershipConfirmed {
			c.failEvidenceLocked("incomplete_ownership_evidence")
			return SourceLeaseReceipt{}, ErrLeaseConflict
		}
		return c.sourceReceiptLocked(input.Generation, input.RequestID, retired.status), nil
	}
	lease := c.sourceLeases[input.RequestID]
	if lease == nil {
		return SourceLeaseReceipt{}, ErrUnknownLease
	}
	if lease.status == SourceLeaseCompleted {
		if lease.confirmed != *input.OwnershipConfirmed {
			c.failEvidenceLocked("incomplete_ownership_evidence")
			return SourceLeaseReceipt{}, ErrLeaseConflict
		}
		return c.sourceReceiptLocked(input.Generation, input.RequestID, SourceLeaseCompleted), nil
	}
	lease.status, lease.confirmed = SourceLeaseCompleted, *input.OwnershipConfirmed
	var evidenceErr error
	if !lease.confirmed {
		evidenceErr = ErrEvidence
	}
	c.finishWorkLocked(lease.workID, evidenceErr)
	return c.sourceReceiptLocked(input.Generation, input.RequestID, SourceLeaseCompleted), nil
}

// Completion receipts from the immediately preceding generation remain bounded
// for lost-release reconciliation. Older history is discarded only on a safe
// Resume; active leases can never reach this point or be evicted.
func (c *Coordinator) retireSourceLeasesLocked() {
	retired := make(map[sourceLeaseIdentity]sourceLeaseReceiptState, len(c.sourceLeases))
	for requestID, lease := range c.sourceLeases {
		retired[sourceLeaseIdentity{c.generation, requestID}] = lease.sourceLeaseReceiptState
	}
	c.retiredSourceLeases = retired
	c.sourceLeases = make(map[string]*sourceLease)
}

func (c *Coordinator) handleSourceLease(writer http.ResponseWriter, request *http.Request, acquire bool) {
	if request.Method != http.MethodPost {
		methodError(writer, http.MethodPost)
		return
	}
	var receipt SourceLeaseReceipt
	var err error
	if acquire {
		var input SourceLeaseInput
		if decodeControl(writer, request, &input) != nil || input.OwnerID == "" || input.Generation == 0 || !validSourceLeaseKind(input.Kind) || !safeIdentity.MatchString(input.RequestID) {
			writeFailure(writer, http.StatusBadRequest, errors.New("invalid source lease acquisition"))
			return
		}
		receipt, err = c.AcquireSourceLease(input)
	} else {
		var input SourceLeaseReleaseInput
		if decodeControl(writer, request, &input) != nil || input.OwnerID == "" || input.Generation == 0 || input.OwnershipConfirmed == nil || !safeIdentity.MatchString(input.RequestID) {
			writeFailure(writer, http.StatusBadRequest, errors.New("invalid source lease release"))
			return
		}
		receipt, err = c.ReleaseSourceLease(input)
	}
	if err != nil {
		writeFailure(writer, http.StatusConflict, err)
		return
	}
	_ = json.NewEncoder(writer).Encode(receipt)
}

// AbandonRemoteSources is a terminal shutdown escape after the application has
// aborted/joined local native owners and closed both transports. Foreign owners
// are never inferred dead or expired: every unresolved lease becomes sticky
// uncertainty. It cannot settle leases during resumable fencing.
func (c *Coordinator) AbandonRemoteSources() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closing {
		return ErrNotSafe
	}
	abandoned := false
	for _, lease := range c.sourceLeases {
		if lease.status != SourceLeaseActive {
			continue
		}
		lease.status, lease.confirmed = SourceLeaseCompleted, false
		c.finishWorkLocked(lease.workID, ErrEvidence)
		abandoned = true
	}
	if abandoned {
		return ErrEvidence
	}
	return nil
}
