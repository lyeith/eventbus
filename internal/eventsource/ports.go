// Package eventsource owns Lambda SQS event-source mappings. Queue leases and
// settlement belong to the queue source; function execution belongs to Lambda.
package eventsource

import (
	"context"
	"time"

	"github.com/lyeith/eventbus/internal/sqsevent"
)

// QueueSource resolves only explicitly owned local sources. The returned handle
// stays bound to that queue instance: deleting/recreating its ARN must not make
// an existing mapping consume the replacement queue.
type QueueSource interface {
	ResolveQueue(context.Context, string) (Queue, error)
}

type QueueInfo struct {
	ARN               string
	VisibilityTimeout time.Duration
}

// MaxBatchPayloadBytes is the synchronous Lambda Records JSON payload limit.
// Queue adapters enforce it while selecting records, before leasing messages.
const MaxBatchPayloadBytes = 6 << 20

// Queue retains the broker's native visibility, FIFO and redrive behavior.
// Receive is cancellation-aware and returns owned snapshots of at most max
// leased messages. The encoded SQSEvent must fit MaxBatchPayloadBytes; remaining
// records stay unleased in queue order. Delete joins acknowledgment of the
// original owned delivery: true means that exact receipt was settled while
// current, either now or previously by the handler. Expired, unknown, superseded
// or otherwise discarded receipts must not acknowledge a later lease. A replaced
// queue must fail rather than rebind. Cancellation cannot settle another receipt.
// Neither method may contact AWS. Receive must wait when empty rather than spin.
// Non-cancellation receive errors permanently stop this bound source; adapters
// retain transient waits locally.
type Queue interface {
	Info() QueueInfo
	Receive(context.Context, int) ([]Record, error)
	Delete(context.Context, string) (bool, error)
}

// FunctionInvoker delegates to the registered Lambda runner. InvokeTarget must
// join actual execution and child cleanup, returning an error on function
// failure/timeout. Asynchronous admission does not satisfy this contract.
type FunctionInvoker interface {
	ValidateTarget(context.Context, string) (time.Duration, error)
	InvokeTarget(context.Context, string, []byte) error
}

// Native SQS event records have one shared wire owner. Messaging projects queue
// snapshots; both event-source mappings and development consumers use it.
type Record = sqsevent.Record
type MessageAttribute = sqsevent.MessageAttribute
type SQSEvent = sqsevent.Event
