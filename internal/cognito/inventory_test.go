package cognito

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func inventoryPage(t *testing.T, e *nativeProvisioningEnvironment, action string, request map[string]any, field string) ([]map[string]any, string) {
	t.Helper()
	status, body := postCognito(t, e.server.URL, action, request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	items, ok := body[field].([]any)
	require.True(t, ok, "empty inventories must be JSON arrays: %v", body)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		out = append(out, item.(map[string]any))
	}
	token, _ := body["NextToken"].(string)
	return out, token
}

func poolInventorySummary(pool map[string]any) map[string]any {
	return map[string]any{
		"Id": pool["Id"], "Name": pool["Name"], "CreationDate": pool["CreationDate"],
		"LastModifiedDate": pool["LastModifiedDate"], "Status": "Enabled", "LambdaConfig": map[string]any{},
	}
}

func clientInventorySummary(client map[string]any) map[string]any {
	return map[string]any{"ClientId": client["ClientId"], "ClientName": client["ClientName"], "UserPoolId": client["UserPoolId"]}
}

func TestCognitoInventoryNativePaginationAndRestart(t *testing.T) {
	env := newNativeProvisioningEnvironment(t, Options{})
	pools, token := inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 2}, "UserPools")
	require.Empty(t, pools)
	require.Empty(t, token)

	expectedPools := []map[string]any{}
	var poolID string
	expectedClients := []map[string]any{}
	for index, name := range []string{"same display name", "same display name", "empty pool"} {
		env.clock.Add(10)
		pool := env.createPool(t, map[string]any{"PoolName": name})
		expectedPools = append(expectedPools, poolInventorySummary(pool))
		if index == 0 {
			poolID = pool["Id"].(string)
			for _, clientName := range []string{"same client name", "same client name", "secret client"} {
				env.clock.Add(7)
				client := env.createClient(t, poolID, map[string]any{"ClientName": clientName, "GenerateSecret": true})
				require.NotEmpty(t, client["ClientSecret"])
				expectedClients = append(expectedClients, clientInventorySummary(client))
			}
		} else if index == 1 {
			env.createClient(t, pool["Id"].(string), map[string]any{"ClientName": "foreign client", "GenerateSecret": true})
		} else {
			clients, next := inventoryPage(t, env, "ListUserPoolClients", map[string]any{"UserPoolId": pool["Id"]}, "UserPoolClients")
			require.Empty(t, clients)
			require.Empty(t, next)
		}
	}
	sort.Slice(expectedPools, func(i, j int) bool { return expectedPools[i]["Id"].(string) < expectedPools[j]["Id"].(string) })
	sort.Slice(expectedClients, func(i, j int) bool {
		return expectedClients[i]["ClientId"].(string) < expectedClients[j]["ClientId"].(string)
	})

	// Persisted tokens resume on the same file even when process defaults change.
	firstPools, poolToken := inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 1}, "UserPools")
	firstClients, clientToken := inventoryPage(t, env, "ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 1}, "UserPoolClients")
	require.NotEmpty(t, poolToken)
	require.NotEmpty(t, clientToken)
	env.clock.Add(86400)
	env.options.Region = "ap-southeast-1"
	env.options.AccountID = "999999999999"
	env.restart(t)
	allPools := append([]map[string]any{}, firstPools...)
	for page := 0; poolToken != ""; page++ {
		require.Less(t, page, len(expectedPools), "pagination must terminate")
		var rows []map[string]any
		rows, poolToken = inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 1, "NextToken": poolToken}, "UserPools")
		allPools = append(allPools, rows...)
	}
	allClients := append([]map[string]any{}, firstClients...)
	for page := 0; clientToken != ""; page++ {
		require.Less(t, page, len(expectedClients), "pagination must terminate")
		var rows []map[string]any
		rows, clientToken = inventoryPage(t, env, "ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 1, "NextToken": clientToken}, "UserPoolClients")
		allClients = append(allClients, rows...)
	}
	require.Equal(t, expectedPools, allPools)
	require.Equal(t, expectedClients, allClients)
	clients, next := inventoryPage(t, env, "ListUserPoolClients", map[string]any{"UserPoolId": poolID}, "UserPoolClients")
	require.Equal(t, expectedClients, clients)
	require.Empty(t, next)
	// Native summary reads remain detached from both SDK/handler maps and SQL.
	clients[0]["ClientName"] = "changed outside the store"
	clients, _ = inventoryPage(t, env, "ListUserPoolClients", map[string]any{"UserPoolId": poolID}, "UserPoolClients")
	require.Equal(t, expectedClients, clients)

	deletedID := expectedClients[1]["ClientId"].(string)
	status, body := postCognito(t, env.server.URL, "DeleteUserPoolClient", map[string]any{"UserPoolId": poolID, "ClientId": deletedID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	clients, _ = inventoryPage(t, env, "ListUserPoolClients", map[string]any{"UserPoolId": poolID}, "UserPoolClients")
	require.Equal(t, []map[string]any{expectedClients[0], expectedClients[2]}, clients)
	status, body = postCognito(t, env.server.URL, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	pools, _ = inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 60}, "UserPools")
	require.Len(t, pools, 2)
	for _, p := range pools {
		require.NotEqual(t, poolID, p["Id"])
	}
	status, body = postCognito(t, env.server.URL, "ListUserPoolClients", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ResourceNotFoundException", body["__type"])
}

func TestCognitoInventoryValidationAndTokenBoundaries(t *testing.T) {
	first := newNativeProvisioningEnvironment(t, Options{})
	second := newNativeProvisioningEnvironment(t, Options{})
	pool := first.createPool(t, nil)
	poolID := pool["Id"].(string)
	other := first.createPool(t, nil)["Id"].(string)
	for range 3 {
		first.createClient(t, poolID, map[string]any{"GenerateSecret": true})
	}
	first.createClient(t, other, nil)
	_, poolToken := inventoryPage(t, first, "ListUserPools", map[string]any{"MaxResults": 1}, "UserPools")
	_, clientToken := inventoryPage(t, first, "ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 1}, "UserPoolClients")
	userOne, err := first.store.CreateUserIdentity(t.Context(), poolID, "one", "", "Password1!", "CONFIRMED", nil)
	require.NoError(t, err)
	_, err = first.store.CreateUserIdentity(t.Context(), poolID, "two", "", "Password1!", "CONFIRMED", nil)
	require.NoError(t, err)
	_, userToken, err := first.store.ListPoolUsers(t.Context(), poolID, UserListOptions{Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, userToken)

	raw, err := base64.RawURLEncoding.DecodeString(clientToken)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 1
	tampered := base64.RawURLEncoding.EncodeToString(raw)
	cases := []struct {
		action  string
		request map[string]any
		code    string
	}{
		{"ListUserPools", map[string]any{}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": nil}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 0}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 61}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": -1}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 1.5}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": "1"}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": true}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 1, "NextToken": ""}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 1, "NextToken": "a b"}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 1, "NextToken": 123}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 1, "NextToken": clientToken}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 2, "NextToken": poolToken}, "InvalidParameterException"},
		{"ListUserPools", map[string]any{"MaxResults": 1, "NextToken": userToken}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": "not_a_pool!"}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": "eu-west-1_" + strings.Repeat("x", 50)}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 0}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 61}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": poolID, "NextToken": "malformed"}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": poolID, "NextToken": ""}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 1, "NextToken": tampered}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 1, "NextToken": poolToken}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": other, "MaxResults": 1, "NextToken": clientToken}, "InvalidParameterException"},
		{"ListUserPoolClients", map[string]any{"UserPoolId": "eu-west-1_Missing"}, "ResourceNotFoundException"},
	}
	for _, c := range cases {
		status, body := postCognito(t, first.server.URL, c.action, c.request)
		require.Equal(t, http.StatusBadRequest, status, "action=%s request=%v body=%v", c.action, c.request, body)
		require.Equal(t, c.code, body["__type"])
	}
	status, body := postCognito(t, first.server.URL, "ListUsers", map[string]any{"UserPoolId": poolID, "Limit": 1, "PaginationToken": clientToken})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	// The same public resource IDs in a different database cannot admit tokens.
	require.NoError(t, second.store.UpsertPool(t.Context(), poolID, "eu-west-1"))
	second.createClient(t, poolID, nil)
	status, body = postCognito(t, second.server.URL, "ListUserPools", map[string]any{"MaxResults": 1, "NextToken": poolToken})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	status, body = postCognito(t, second.server.URL, "ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 1, "NextToken": clientToken})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	// Deleted/recreated fixture IDs are a new pool incarnation.
	ok, err := first.store.DeletePool(t.Context(), poolID)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, first.store.UpsertPool(t.Context(), poolID, "eu-west-1"))
	first.createClient(t, poolID, nil)
	status, body = postCognito(t, first.server.URL, "ListUserPoolClients", map[string]any{"UserPoolId": poolID, "MaxResults": 1, "NextToken": clientToken})
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	_, _, err = first.store.ListPoolUsers(t.Context(), poolID, UserListOptions{Limit: 1, PaginationToken: userToken})
	var invalid *invalidListParameter
	require.ErrorAs(t, err, &invalid)
	_, err = first.store.LookupUserBySub(t.Context(), userOne.Sub)
	require.Error(t, err, "delete cascade still removes the original users")
}

func TestCognitoInventoryKeysetsRemainLiveAndBounded(t *testing.T) {
	env := newNativeProvisioningEnvironment(t, Options{})
	// Seed through the store's existing fixture seam only to choose key order.
	for _, id := range []string{"eu-west-1_B", "eu-west-1_D", "eu-west-1_F"} {
		require.NoError(t, env.store.UpsertPool(t.Context(), id, "eu-west-1"))
	}
	first, token := inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 1}, "UserPools")
	require.Equal(t, "eu-west-1_B", first[0]["Id"])
	_, err := env.store.DeletePool(t.Context(), "eu-west-1_B")
	require.NoError(t, err)
	require.NoError(t, env.store.UpsertPool(t.Context(), "eu-west-1_A", "eu-west-1"))
	require.NoError(t, env.store.UpsertPool(t.Context(), "eu-west-1_C", "eu-west-1"))
	second, next := inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 1, "NextToken": token}, "UserPools")
	require.Equal(t, "eu-west-1_C", second[0]["Id"])
	require.NotEmpty(t, next)
	third, next := inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 1, "NextToken": next}, "UserPools")
	require.Equal(t, "eu-west-1_D", third[0]["Id"])
	fourth, next := inventoryPage(t, env, "ListUserPools", map[string]any{"MaxResults": 1, "NextToken": next}, "UserPools")
	require.Equal(t, "eu-west-1_F", fourth[0]["Id"])
	require.Empty(t, next)
}

func TestCognitoPaginationPersistedKeyCorruptionDoesNotResetState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cognito.db")
	store, err := OpenCognitoStore(path)
	require.NoError(t, err)
	require.NoError(t, store.UpsertPool(t.Context(), "eu-west-1_Preserved", "eu-west-1"))
	_, err = store.db.Exec(`UPDATE cognito_store_metadata SET value=? WHERE name='pagination_key'`, []byte("broken"))
	require.NoError(t, err)
	require.NoError(t, store.Close())
	store, err = OpenCognitoStore(path)
	require.ErrorContains(t, err, "invalid persisted Cognito pagination key")
	require.Nil(t, store)
	db, err := openPaginationTestDB(path)
	require.NoError(t, err)
	defer db.Close()
	var count int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM pools WHERE id='eu-west-1_Preserved'`).Scan(&count))
	require.Equal(t, 1, count)
}

func TestCognitoPaginationLegacySchemaMigrationAndUserRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cognito.db")
	store, err := OpenCognitoStore(path)
	require.NoError(t, err)
	require.NoError(t, store.UpsertPool(t.Context(), "eu-west-1_Legacy", "eu-west-1"))
	require.NoError(t, store.UpsertClient(t.Context(), "old-client", "eu-west-1_Legacy", "secret"))
	require.NoError(t, store.Close())
	// A legacy file has neither the store cursor key nor pool-incarnation column.
	db, err := openPaginationTestDB(path)
	require.NoError(t, err)
	_, err = db.Exec(`DROP TABLE cognito_store_metadata`)
	require.NoError(t, err)
	_, err = db.Exec(`ALTER TABLE pools DROP COLUMN inventory_generation`)
	require.NoError(t, err)
	require.NoError(t, db.Close())
	store, err = OpenCognitoStore(path)
	require.NoError(t, err)
	pools, token, err := store.listPools(t.Context(), 60, "")
	require.NoError(t, err)
	require.Empty(t, token)
	require.Len(t, pools, 1)
	require.Equal(t, "eu-west-1_Legacy", pools[0].ID)
	require.Equal(t, pools[0].CreatedAt, pools[0].ModifiedAt)
	clients, token, err := store.listClients(t.Context(), "eu-west-1_Legacy", 60, "")
	require.NoError(t, err)
	require.Empty(t, token)
	require.Equal(t, []clientSummary{{ID: "old-client", PoolID: "eu-west-1_Legacy"}}, clients)
	for _, username := range []string{"one", "two"} {
		_, err = store.CreateUserIdentity(t.Context(), "eu-west-1_Legacy", username, "", "Password1!", "CONFIRMED", nil)
		require.NoError(t, err)
	}
	users, userToken, err := store.ListPoolUsers(t.Context(), "eu-west-1_Legacy", UserListOptions{Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, userToken)
	require.NoError(t, store.Close())
	store, err = OpenCognitoStore(path)
	require.NoError(t, err)
	defer store.Close()
	more, next, err := store.ListPoolUsers(t.Context(), "eu-west-1_Legacy", UserListOptions{Limit: 1, PaginationToken: userToken})
	require.NoError(t, err)
	require.Len(t, more, 1)
	require.Empty(t, next)
	require.NotEqual(t, users[0].Sub, more[0].Sub)

	// The signed payload is a cursor, never a credential or retained inventory.
	data, err := base64.RawURLEncoding.DecodeString(userToken)
	require.NoError(t, err)
	var cursor listCursor
	require.NoError(t, json.Unmarshal(data[:len(data)-32], &cursor))
	require.Equal(t, "ListUsers", cursor.Operation)
}

func openPaginationTestDB(path string) (*sql.DB, error) { return sql.Open("sqlite", path) }
