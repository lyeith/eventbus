// Cognito pagination owns authenticated, persisted-store-scoped keyset cursors.
package cognito

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"
)

type invalidListParameter struct{ message string }

func (e *invalidListParameter) Error() string { return e.message }

// Every list operation shares this envelope and codec. Query contains the
// operation's normalized request parameters; generation distinguishes a deleted
// and recreated pool even when a local fixture reuses its public ID.
type listCursor struct {
	Version    int    `json:"v"`
	Operation  string `json:"operation"`
	PoolID     string `json:"pool,omitempty"`
	Generation string `json:"generation,omitempty"`
	Query      string `json:"query,omitempty"`
	Limit      int    `json:"limit"`
	After      string `json:"after"`
	Expires    int64  `json:"expires,omitempty"`
}

func (s *CognitoStore) bootstrapPagination(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS cognito_store_metadata (
		name TEXT PRIMARY KEY, value BLOB NOT NULL
	)`); err != nil {
		return err
	}
	var key []byte
	err := tx.QueryRowContext(ctx, `SELECT value FROM cognito_store_metadata WHERE name='pagination_key'`).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		key = make([]byte, sha256.Size)
		if _, err = rand.Read(key); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO cognito_store_metadata(name,value) VALUES('pagination_key',?)`, key); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if len(key) != sha256.Size {
		return errors.New("invalid persisted Cognito pagination key")
	}
	copy(s.paginationKey[:], key)
	// Old databases gain a stable incarnation once. New pools receive one at
	// insertion; neither metadata updates nor process restarts replace it.
	_, err = tx.ExecContext(ctx, `UPDATE pools SET inventory_generation=lower(hex(randomblob(16))) WHERE inventory_generation=''`)
	return err
}

func (s *CognitoStore) beginList(operation, poolID, generation, query string, limit int, token string, lifetime time.Duration) (listCursor, error) {
	expected := listCursor{Version: 1, Operation: operation, PoolID: poolID, Generation: generation, Query: query, Limit: limit}
	if lifetime > 0 {
		expected.Expires = s.now().Add(lifetime).Unix()
	}
	if token == "" {
		return expected, nil
	}
	invalid := func() (listCursor, error) {
		return listCursor{}, &invalidListParameter{"invalid pagination token"}
	}
	if len(token) > 131072 {
		return invalid()
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(raw) <= sha256.Size {
		return invalid()
	}
	payload, signature := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	mac := hmac.New(sha256.New, s.paginationKey[:])
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return invalid()
	}
	var previous listCursor
	if err = json.Unmarshal(payload, &previous); err != nil ||
		previous.Version != expected.Version || previous.Operation != operation ||
		previous.PoolID != poolID || previous.Generation != generation ||
		previous.Query != query || previous.Limit != limit || previous.After == "" ||
		(previous.Expires != 0 && previous.Expires <= s.now().Unix()) ||
		(lifetime > 0 && previous.Expires == 0) {
		return invalid()
	}
	return previous, nil
}

func (s *CognitoStore) finishList(cursor listCursor, after string) (string, error) {
	cursor.After = after
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, s.paginationKey[:])
	_, _ = mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(append(payload, mac.Sum(nil)...)), nil
}

// Query encoding belongs to the shared Cognito cursor owner; arrays retain their
// original element boundaries rather than being joined with a delimiter.
func listQuery(value any) (string, error) {
	data, err := json.Marshal(value)
	return string(data), err
}
