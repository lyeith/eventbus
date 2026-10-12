// Native pool and app-client inventory. These summaries deliberately exclude
// the detailed configuration and secrets returned by Describe operations.
package cognito

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"regexp"
)

type listPoolsRequest struct {
	MaxResults *int    `json:"MaxResults"`
	NextToken  *string `json:"NextToken"`
}

type listClientsRequest struct {
	listPoolsRequest
	UserPoolID string `json:"UserPoolId"`
}

type poolSummary struct {
	ID           string         `json:"Id"`
	Name         string         `json:"Name,omitempty"`
	Status       string         `json:"Status"`
	LambdaConfig map[string]any `json:"LambdaConfig"`
	CreatedAt    int64          `json:"CreationDate"`
	ModifiedAt   int64          `json:"LastModifiedDate"`
}

type clientSummary struct {
	ID     string `json:"ClientId"`
	Name   string `json:"ClientName,omitempty"`
	PoolID string `json:"UserPoolId"`
}

var nativePoolIDPattern = regexp.MustCompile(`^[\w-]+_[0-9a-zA-Z]+$`)

func inventoryRequest(req listPoolsRequest, required bool) (int, string, error) {
	limit := 60
	if req.MaxResults != nil {
		limit = *req.MaxResults
	} else if required {
		return 0, "", &invalidListParameter{"MaxResults is required"}
	}
	if limit < 1 || limit > 60 {
		return 0, "", &invalidListParameter{"MaxResults must be between 1 and 60"}
	}
	token := ""
	if req.NextToken != nil {
		token = *req.NextToken
		if token == "" {
			return 0, "", &invalidListParameter{"NextToken must not be empty"}
		}
	}
	return limit, token, nil
}

func (s *Handler) handleListUserPools(w http.ResponseWriter, r *http.Request) {
	var req listPoolsRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	limit, token, err := inventoryRequest(req, true)
	if err != nil {
		s.inventoryError(w, err, "ListUserPools")
		return
	}
	pools, next, err := s.cognito.listPools(r.Context(), limit, token)
	if err != nil {
		s.inventoryError(w, err, "ListUserPools")
		return
	}
	body := map[string]any{"UserPools": pools}
	if next != "" {
		body["NextToken"] = next
	}
	cognitoJSONResponse(w, http.StatusOK, body)
}

func (s *Handler) handleListUserPoolClients(w http.ResponseWriter, r *http.Request) {
	var req listClientsRequest
	if !readCognitoJSON(w, r, &req) {
		return
	}
	if len(req.UserPoolID) > 55 || !nativePoolIDPattern.MatchString(req.UserPoolID) {
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", "UserPoolId must contain 1 to 55 characters and match [\\w-]+_[0-9a-zA-Z]+")
		return
	}
	limit, token, err := inventoryRequest(req.listPoolsRequest, false)
	if err != nil {
		s.inventoryError(w, err, "ListUserPoolClients")
		return
	}
	clients, next, err := s.cognito.listClients(r.Context(), req.UserPoolID, limit, token)
	if err != nil {
		s.inventoryError(w, err, "ListUserPoolClients")
		return
	}
	body := map[string]any{"UserPoolClients": clients}
	if next != "" {
		body["NextToken"] = next
	}
	cognitoJSONResponse(w, http.StatusOK, body)
}

func (s *Handler) inventoryError(w http.ResponseWriter, err error, operation string) {
	var invalid *invalidListParameter
	switch {
	case errors.As(err, &invalid):
		cognitoJSONError(w, http.StatusBadRequest, "InvalidParameterException", err.Error())
	case errors.Is(err, sql.ErrNoRows):
		cognitoJSONError(w, http.StatusBadRequest, "ResourceNotFoundException", "User pool does not exist")
	default:
		authInternalError(w, err, operation)
	}
}

// Stable ID keysets require only page-sized reads and remain meaningful after
// restart or deletion of the last returned item. Inventories are live, not
// retained snapshots: later insertions before the cursor belong to a new scan.
func (s *CognitoStore) listPools(ctx context.Context, limit int, token string) ([]poolSummary, string, error) {
	if limit < 1 || limit > 60 {
		return nil, "", &invalidListParameter{"MaxResults must be between 1 and 60"}
	}
	cursor, err := s.beginList("ListUserPools", "", "", "", limit, token, 0)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,created_at,last_modified_at,json_extract(metadata,'$.Name')
		FROM pools WHERE id>? ORDER BY id LIMIT ?`, cursor.After, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	pools := make([]poolSummary, 0, limit)
	for rows.Next() {
		p := poolSummary{Status: "Enabled", LambdaConfig: map[string]any{}}
		var name sql.NullString
		if err = rows.Scan(&p.ID, &p.CreatedAt, &p.ModifiedAt, &name); err != nil {
			return nil, "", err
		}
		p.Name = name.String
		if p.ModifiedAt == 0 {
			p.ModifiedAt = p.CreatedAt
		}
		pools = append(pools, p)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	if len(pools) <= limit {
		return pools, "", nil
	}
	pools = pools[:limit]
	next, err := s.finishList(cursor, pools[len(pools)-1].ID)
	return pools, next, err
}

func (s *CognitoStore) listClients(ctx context.Context, poolID string, limit int, token string) ([]clientSummary, string, error) {
	if limit < 1 || limit > 60 {
		return nil, "", &invalidListParameter{"MaxResults must be between 1 and 60"}
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, "", err
	}
	defer tx.Rollback()
	var generation string
	if err = tx.QueryRowContext(ctx, `SELECT inventory_generation FROM pools WHERE id=?`, poolID).Scan(&generation); err != nil {
		return nil, "", err
	}
	cursor, err := s.beginList("ListUserPoolClients", poolID, generation, "", limit, token, 0)
	if err != nil {
		return nil, "", err
	}
	// Select summary columns only. ClientSecret never enters the read model.
	rows, err := tx.QueryContext(ctx, `SELECT id,pool_id,json_extract(metadata,'$.Name')
		FROM clients WHERE pool_id=? AND id>? ORDER BY id LIMIT ?`, poolID, cursor.After, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	clients := make([]clientSummary, 0, limit)
	for rows.Next() {
		var c clientSummary
		var name sql.NullString
		if err = rows.Scan(&c.ID, &c.PoolID, &name); err != nil {
			return nil, "", err
		}
		c.Name = name.String
		clients = append(clients, c)
	}
	if err = rows.Err(); err != nil {
		return nil, "", err
	}
	if len(clients) <= limit {
		return clients, "", nil
	}
	clients = clients[:limit]
	next, err := s.finishList(cursor, clients[len(clients)-1].ID)
	return clients, next, err
}
