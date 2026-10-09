package messaging

import "time"

// pruneDedupEntries owns the shared map-expiry mechanics, independent of native
// SNS/SQS acceptance, key scope and result types. The caller holds its entity
// ownership lock. A conservative lower bound skips healthy histories; an unknown
// or due bound requires a sweep and recomputes the exact next expiry.
func pruneDedupEntries[T any](entries map[string]T, earliest *time.Time, now time.Time, expires func(T) time.Time) {
	if !earliest.IsZero() && now.Before(*earliest) {
		return
	}
	*earliest = time.Time{}
	for key, entry := range entries {
		expiry := expires(entry)
		if !now.Before(expiry) {
			delete(entries, key)
			continue
		}
		lowerDedupExpiry(earliest, expiry)
	}
}

// Replacement can leave an earlier lower bound, but insertion must never move
// it later. An extra early sweep is safe; delaying a due sweep is not.
func lowerDedupExpiry(earliest *time.Time, expiry time.Time) {
	if earliest.IsZero() || expiry.Before(*earliest) {
		*earliest = expiry
	}
}
