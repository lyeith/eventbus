// Prepared-object retry pacing and stream diagnostics share the native delivery
// owner. They neither settle records nor create another delivery queue.
package firehose

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const maxDeliveryRetryDelay = time.Minute
const duplicateDeliveryLogInterval = time.Minute

type destinationStatusError struct{ status int }

func (e *destinationStatusError) Error() string {
	return fmt.Sprintf("Firehose destination returned status %d", e.status)
}

func deliveryRetryDelay(failures int) time.Duration {
	if failures < 1 {
		return 0
	}
	return min(time.Second*time.Duration(1<<min(failures-1, 6)), maxDeliveryRetryDelay)
}

func deliveryFailureCode(err error) string {
	var status *destinationStatusError
	if errors.As(err, &status) {
		return fmt.Sprintf("destination_http_%d", status.status)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "destination_timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "delivery_canceled"
	}
	var transport *url.Error
	if errors.As(err, &transport) {
		var timeout net.Error
		if errors.As(transport.Err, &timeout) && timeout.Timeout() {
			return "destination_timeout"
		}
		var dns *net.DNSError
		if errors.As(transport.Err, &dns) {
			return "destination_dns"
		}
		return "destination_transport"
	}
	return "delivery_preparation_or_response"
}

type deliveryDiagnostics struct {
	signature        string
	lastLog          time.Time
	attemptsSinceLog int
	totalAttempts    int
}
type deliveryLogEvent struct {
	stream                          string
	reason                          string
	failureCodes                    []string
	retainedRecords, pendingObjects int
	attemptsSinceLog, totalAttempts int
	retryAfter                      time.Duration
}

// Caller holds ds.mu after a successful pending-object removal.
func refreshLastDeliveryError(ds *DeliveryStream) {
	ds.lastDeliveryError = ""
	for index := len(ds.pending) - 1; index >= 0; index-- {
		if ds.pending[index].lastError != "" {
			ds.lastDeliveryError = ds.pending[index].lastError
			return
		}
	}
}

func (fm *FirehoseManager) reportDelivery(ds *DeliveryStream, attempted int, flushErr error) {
	// A dormant cooldown pass is not a successful delivery or a fresh failure.
	if attempted == 0 && flushErr == nil {
		return
	}
	now := fm.now()
	ds.mu.Lock()
	codes := map[string]bool{}
	next := time.Time{}
	for _, object := range ds.pending {
		if object.failureCode != "" {
			codes[object.failureCode] = true
			if next.IsZero() || object.retryAt.Before(next) {
				next = object.retryAt
			}
		}
	}
	if flushErr != nil && (len(codes) == 0 || attempted == 0) {
		codes[deliveryFailureCode(flushErr)] = true
	}
	values := make([]string, 0, len(codes))
	for code := range codes {
		values = append(values, code)
	}
	sort.Strings(values)
	signature := strings.Join(values, ",")
	diagnostics := &ds.diagnostics
	if signature == "" && diagnostics.signature == "" {
		ds.mu.Unlock()
		return
	}
	diagnostics.attemptsSinceLog += attempted
	diagnostics.totalAttempts += attempted
	reason := ""
	switch {
	case signature == "":
		reason = "recovered"
	case diagnostics.signature == "":
		reason = "first_failure"
	case signature != diagnostics.signature:
		reason = "error_changed"
	case !now.Before(diagnostics.lastLog.Add(duplicateDeliveryLogInterval)):
		reason = "retry_summary"
	}
	if reason == "" {
		ds.mu.Unlock()
		return
	}
	event := deliveryLogEvent{stream: ds.Name, reason: reason, failureCodes: values, retainedRecords: ds.retainedRecords, pendingObjects: len(ds.pending), attemptsSinceLog: diagnostics.attemptsSinceLog, totalAttempts: diagnostics.totalAttempts}
	if !next.IsZero() {
		event.retryAfter = max(0, next.Sub(now))
	}
	diagnostics.signature = signature
	diagnostics.lastLog = now
	diagnostics.attemptsSinceLog = 0
	if reason == "recovered" {
		*diagnostics = deliveryDiagnostics{}
	}
	ds.mu.Unlock()
	// Construction freezes the sink. It sees only redacted aggregate state and
	// runs outside entity locks while the flush still owns its completion lifetime.
	fm.logDelivery(event)
}

func writeDeliveryLog(event deliveryLogEvent) {
	entry := log.Error()
	message := "Firehose delivery failed; records retained"
	if event.reason == "recovered" {
		entry = log.Info()
		message = "Firehose delivery recovered"
	}
	if event.reason == "retry_summary" {
		message = "Firehose delivery retry summary; records retained"
	}
	entry.Str("stream", event.stream).Str("delivery_state", event.reason).
		Strs("error_codes", event.failureCodes).
		Int("retained_records", event.retainedRecords).Int("pending_objects", event.pendingObjects).
		Int("attempts_since_log", event.attemptsSinceLog).Int("attempts", event.totalAttempts).
		Float64("retry_after_seconds", event.retryAfter.Seconds()).Msg(message)
}
