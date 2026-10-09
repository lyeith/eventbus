// The store owns decoded signing keys and first-use generation. Internal callers
// borrow immutable keys; exported accessors detach mutable RSA snapshots.
package cognito

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
)

type signingKeyGeneration struct {
	done chan struct{}
	key  *SigningKey
	err  error
}

// EnsureSigningKey returns a detached pool key, generating and persisting real
// RSA-2048 material when necessary. Caller mutations cannot change the store.
func (s *CognitoStore) EnsureSigningKey(ctx context.Context, poolID string) (*SigningKey, error) {
	key, err := s.ensureSigningKey(ctx, poolID)
	if err != nil {
		return nil, err
	}
	return snapshotSigningKey(key), nil
}

// LoadSigningKey returns a detached persisted key without generating one.
// Missing pools and keys return sql.ErrNoRows.
func (s *CognitoStore) LoadSigningKey(ctx context.Context, poolID string) (*SigningKey, error) {
	key, err := s.loadSigningKey(ctx, poolID)
	if err != nil {
		return nil, err
	}
	return snapshotSigningKey(key), nil
}

// ensureSigningKey lends an immutable key only to same-package crypto callers.
// Expensive RSA generation does not hold mu or the single SQLite connection.
func (s *CognitoStore) ensureSigningKey(ctx context.Context, poolID string) (*SigningKey, error) {
	if poolID == "" {
		return nil, errors.New("pool id required")
	}
	for {
		s.mu.Lock()
		if err := s.signingKeyContextLocked(ctx); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		if key := s.signingKeys[poolID]; key != nil {
			s.mu.Unlock()
			return key, nil
		}
		if pending := s.signingKeyGenerations[poolID]; pending != nil {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-pending.done:
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				// A live caller can retry after the initiating caller cancelled.
				// Pool deletion/generation invalidation must still fail this call.
				if errors.Is(pending.err, context.Canceled) || errors.Is(pending.err, context.DeadlineExceeded) {
					continue
				}
				return pending.key, pending.err
			}
		}
		if key, err := s.loadSigningKeyLocked(ctx, poolID); err == nil {
			s.mu.Unlock()
			return key, nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			s.mu.Unlock()
			return nil, err
		}
		var parent string
		if err := s.db.QueryRowContext(ctx, "SELECT id FROM pools WHERE id=?", poolID).Scan(&parent); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		pending := &signingKeyGeneration{done: make(chan struct{})}
		s.signingKeyGenerations[poolID] = pending
		s.signingKeyTasks.Add(1)
		s.mu.Unlock()
		defer s.signingKeyTasks.Done()

		key, privatePEM, publicPEM, err := generateSigningKey(poolID)
		s.mu.Lock()
		defer s.mu.Unlock()
		s.completeSigningKeyGenerationLocked(ctx, poolID, pending, key, privatePEM, publicPEM, err)
		return pending.key, pending.err
	}
}

func generateSigningKey(poolID string) (*SigningKey, string, string, error) {
	private, err := rsa.GenerateKey(rand.Reader, rsaKeySize)
	if err != nil {
		return nil, "", "", fmt.Errorf("generate rsa key: %w", err)
	}
	kid, err := computeKid(&private.PublicKey)
	if err != nil {
		return nil, "", "", err
	}
	privatePEM, err := encodePrivateKeyPEM(private)
	if err != nil {
		return nil, "", "", err
	}
	publicPEM, err := encodePublicKeyPEM(&private.PublicKey)
	if err != nil {
		return nil, "", "", err
	}
	return &SigningKey{PoolID: poolID, Kid: kid, Private: private, Public: &private.PublicKey}, privatePEM, publicPEM, nil
}

// A pending entry is its generation identity. Successful parent deletion removes
// it only after commit; even an immediate reseed cannot adopt that older key.
func (s *CognitoStore) completeSigningKeyGenerationLocked(ctx context.Context, poolID string, pending *signingKeyGeneration, key *SigningKey, privatePEM, publicPEM string, generationErr error) {
	defer close(pending.done)
	if s.signingKeyGenerations[poolID] != pending {
		if s.closed {
			pending.err = sql.ErrConnDone
		} else {
			pending.err = sql.ErrNoRows
		}
		return
	}
	defer delete(s.signingKeyGenerations, poolID)
	if err := s.signingKeyContextLocked(ctx); err != nil {
		pending.err = err
		return
	}
	if generationErr != nil {
		pending.err = generationErr
		return
	}
	var parent string
	if err := s.db.QueryRowContext(ctx, "SELECT id FROM pools WHERE id=?", poolID).Scan(&parent); err != nil {
		pending.err = err
		return
	}
	// Recheck the persisted key before inserting.
	if existing, err := s.loadSigningKeyLocked(ctx, poolID); err == nil {
		pending.key = existing
		return
	} else if !errors.Is(err, sql.ErrNoRows) {
		pending.err = err
		return
	}
	key.CreatedAt = s.now().Unix()
	if _, err := s.db.ExecContext(ctx, "INSERT INTO signing_keys (pool_id,kid,private_pem,public_pem,created_at) VALUES (?,?,?,?,?)", poolID, key.Kid, privatePEM, publicPEM, key.CreatedAt); err != nil {
		pending.err = fmt.Errorf("persist signing key: %w", err)
		return
	}
	s.signingKeys[poolID] = key
	pending.key = key
}

func (s *CognitoStore) loadSigningKey(ctx context.Context, poolID string) (*SigningKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.signingKeyContextLocked(ctx); err != nil {
		return nil, err
	}
	return s.loadSigningKeyLocked(ctx, poolID)
}

func (s *CognitoStore) signingKeyContextLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return sql.ErrConnDone
	}
	return nil
}

func (s *CognitoStore) loadSigningKeyLocked(ctx context.Context, poolID string) (*SigningKey, error) {
	if key := s.signingKeys[poolID]; key != nil {
		return key, nil
	}
	var kid, privatePEM, publicPEM string
	var createdAt int64
	err := s.db.QueryRowContext(ctx, `
		SELECT k.kid,k.private_pem,k.public_pem,k.created_at
		FROM signing_keys k JOIN pools p ON p.id=k.pool_id WHERE k.pool_id=?
	`, poolID).Scan(&kid, &privatePEM, &publicPEM, &createdAt)
	if err != nil {
		return nil, err
	}
	private, err := decodePrivateKeyPEM(privatePEM)
	if err != nil {
		return nil, fmt.Errorf("decode private pem: %w", err)
	}
	public, err := decodePublicKeyPEM(publicPEM)
	if err != nil {
		return nil, fmt.Errorf("decode public pem: %w", err)
	}
	key := &SigningKey{PoolID: poolID, Kid: kid, Private: private, Public: public, CreatedAt: createdAt}
	s.signingKeys[poolID] = key
	return key, nil
}

// RSA keys contain mutable big integers and hidden precomputed state. Rebuild
// detached snapshots through the public crypto API instead of sharing either.
func snapshotSigningKey(key *SigningKey) *SigningKey {
	public := rsa.PublicKey{N: new(big.Int).Set(key.Public.N), E: key.Public.E}
	private := &rsa.PrivateKey{
		PublicKey: rsa.PublicKey{N: new(big.Int).Set(key.Private.N), E: key.Private.E},
		D:         new(big.Int).Set(key.Private.D),
		Primes:    make([]*big.Int, len(key.Private.Primes)),
	}
	for index, prime := range key.Private.Primes {
		private.Primes[index] = new(big.Int).Set(prime)
	}
	private.Precompute()
	return &SigningKey{PoolID: key.PoolID, Kid: key.Kid, Private: private, Public: &public, CreatedAt: key.CreatedAt}
}
