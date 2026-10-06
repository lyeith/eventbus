package cognito

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
)

// StartChallengeCleanup starts an owned session-reaper worker. Cancel its
// context and join the returned channel before closing the Cognito store.
func StartChallengeCleanup(ctx context.Context, store *CognitoStore, interval time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				count, err := store.DeleteExpiredChallengeSessions(ctx, time.Now().Unix())
				if err != nil && ctx.Err() == nil {
					log.Warn().Err(err).Msg("challenge_sessions cleanup failed")
				} else if count > 0 {
					log.Debug().Int("deleted", count).Msg("challenge_sessions cleanup")
				}
			}
		}
	}()
	return done
}
