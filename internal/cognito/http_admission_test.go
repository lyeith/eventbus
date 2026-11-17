package cognito

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This is the documented local receiving policy, independent of the production
// constant so these controls also exercise the unbounded released implementation.
const cognitoAdmissionBudget = 1 << 20

type cognitoAdmissionBody struct {
	reader io.Reader
	read   int
	closed bool
}

func (body *cognitoAdmissionBody) Read(buffer []byte) (int, error) {
	n, err := body.reader.Read(buffer)
	body.read += n
	return n, err
}

func (body *cognitoAdmissionBody) Close() error {
	body.closed = true
	return nil
}

type cognitoAdmissionReadFailure struct{}

func (cognitoAdmissionReadFailure) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func assertCognitoAdmissionError(t *testing.T, response *httptest.ResponseRecorder, message string) {
	t.Helper()
	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "application/x-amz-json-1.1", response.Header().Get("Content-Type"))
	assert.NotEmpty(t, response.Header().Get("X-Amzn-RequestId"))
	var envelope map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
	assert.Equal(t, map[string]any{
		"__type": "InvalidParameterException", "message": message,
	}, envelope)
}

func cognitoAdmissionPoolCount(t *testing.T, store *CognitoStore) int {
	t.Helper()
	var count int
	require.NoError(t, store.DB().QueryRowContext(t.Context(), "SELECT COUNT(*) FROM pools").Scan(&count))
	return count
}

func TestCognitoJSONAdmissionRejectsOversizeWithoutEffects(t *testing.T) {
	const valid = `{"PoolName":"bounded-receiving"}`
	oversize := valid + strings.Repeat(" ", cognitoAdmissionBudget+4096-len(valid))
	// The first budget bytes form a valid document. Truncating there would accept
	// the operation; the excess byte must instead produce the size refusal.
	excessSuffix := valid + strings.Repeat(" ", cognitoAdmissionBudget-len(valid)) + "{}"
	for _, scenario := range []struct {
		name          string
		payload       string
		contentLength int64
	}{
		{"known_length", oversize, int64(len(oversize))},
		{"unknown_length", oversize, -1},
		{"zero_length_hint", oversize, 0},
		{"underreported_length", oversize, int64(len(valid))},
		{"overreported_length", oversize, 8 * cognitoAdmissionBudget},
		{"valid_prefix_with_excess_suffix", excessSuffix, -1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, _ := newCognitoTestStore(t)
			require.NoError(t, store.UpsertPool(t.Context(), "sentinel-pool", "us-east-1"))
			sentinel, err := store.LookupPool(t.Context(), "sentinel-pool")
			require.NoError(t, err)
			handler := NewHandler(store, Options{})
			body := &cognitoAdmissionBody{reader: strings.NewReader(scenario.payload)}
			request := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(t.Context())
			request.Header.Set("X-Amz-Target", cognitoTargetPrefix+"CreateUserPool")
			request.Header.Set("Content-Type", "application/x-amz-json-1.1")
			request.Body = body
			request.ContentLength = scenario.contentLength
			response := httptest.NewRecorder()

			// Invoke the real service HTTP route. Direct request delivery preserves
			// misleading Content-Length hints and permits exact stream accounting.
			handler.ServeAction(response, request, Action(request.Header.Get("X-Amz-Target")))

			assertCognitoAdmissionError(t, response, "request body exceeds 1048576 bytes")
			assert.Equal(t, cognitoAdmissionBudget+1, body.read)
			assert.True(t, body.closed)
			assert.Equal(t, 1, cognitoAdmissionPoolCount(t, store), "refusal must create no pool")
			after, err := store.LookupPool(t.Context(), "sentinel-pool")
			require.NoError(t, err)
			assert.Equal(t, sentinel, after, "refusal must preserve the existing pool")
		})
	}
}

func TestCognitoJSONAdmissionReadFailurePrecedesEffects(t *testing.T) {
	const valid = `{"PoolName":"read-failure"}`
	for _, scenario := range []struct {
		name   string
		prefix string
	}{
		{"before_input", ""},
		{"interrupted_document", valid[:len(valid)/2]},
		{"after_complete_document", valid},
		{"at_exact_budget", valid + strings.Repeat(" ", cognitoAdmissionBudget-len(valid))},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, _ := newCognitoTestStore(t)
			handler := NewHandler(store, Options{})
			body := &cognitoAdmissionBody{reader: io.MultiReader(strings.NewReader(scenario.prefix), cognitoAdmissionReadFailure{})}
			request := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(t.Context())
			request.Header.Set("X-Amz-Target", cognitoTargetPrefix+"CreateUserPool")
			request.Body = body
			request.ContentLength = -1
			response := httptest.NewRecorder()

			handler.ServeAction(response, request, Action(request.Header.Get("X-Amz-Target")))

			assertCognitoAdmissionError(t, response, "failed to read request body")
			assert.Equal(t, len(scenario.prefix), body.read)
			assert.LessOrEqual(t, body.read, cognitoAdmissionBudget+1)
			assert.True(t, body.closed)
			assert.Zero(t, cognitoAdmissionPoolCount(t, store), "incomplete reads must create no pool")
		})
	}
}

func TestCognitoJSONAdmissionPreservesHTTPWireBehavior(t *testing.T) {
	const valid = `{"PoolName":"ordinary-native-pool"}`
	for _, scenario := range []struct {
		name    string
		payload string
		message string
	}{
		{"small_supported_request", valid, ""},
		{"exact_budget_supported_request", valid + strings.Repeat(" ", cognitoAdmissionBudget-len(valid)), ""},
		{"empty_body_owner_validation", "", "PoolName is required"},
		{"malformed_json", `{"PoolName":`, "malformed JSON request body"},
		{"complete_input_required", valid + "{}", "malformed JSON request body"},
		{"typed_capability_error", `{"PoolName":"unsupported","MfaConfiguration":"ON"}`, "MfaConfiguration is not supported by this Cognito emulator"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			_, server, store := newCognitoTestServer(t)
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/", strings.NewReader(scenario.payload))
			require.NoError(t, err)
			request.Header.Set("X-Amz-Target", cognitoTargetPrefix+"CreateUserPool")
			request.Header.Set("Content-Type", "application/x-amz-json-1.1")
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			defer func() { _ = response.Body.Close() }()
			assert.Equal(t, "application/x-amz-json-1.1", response.Header.Get("Content-Type"))
			assert.NotEmpty(t, response.Header.Get("X-Amzn-RequestId"))
			var envelope map[string]any
			require.NoError(t, json.NewDecoder(response.Body).Decode(&envelope))
			if scenario.message != "" {
				assert.Equal(t, http.StatusBadRequest, response.StatusCode)
				assert.Equal(t, map[string]any{
					"__type": "InvalidParameterException", "message": scenario.message,
				}, envelope)
				assert.Zero(t, cognitoAdmissionPoolCount(t, store))
				return
			}
			require.Equal(t, http.StatusOK, response.StatusCode, "body=%v", envelope)
			pool, ok := envelope["UserPool"].(map[string]any)
			require.True(t, ok)
			id, ok := pool["Id"].(string)
			require.True(t, ok)
			require.NotEmpty(t, id)
			assert.Equal(t, "ordinary-native-pool", pool["Name"])
			stored, err := store.LookupPool(t.Context(), id)
			require.NoError(t, err)
			assert.True(t, stored.Native)
			assert.Equal(t, "ordinary-native-pool", stored.Name)
			assert.Equal(t, 1, cognitoAdmissionPoolCount(t, store))
		})
	}
}

func TestCognitoJSONAdmissionContentLengthIsOnlyAHint(t *testing.T) {
	const payload = `{"PoolName":"length-hint"}`
	for _, scenario := range []struct {
		name   string
		length int64
	}{
		{"unknown_length", -1},
		{"zero_length_hint", 0},
		{"underreported_length", 1},
		{"overreported_length", 1 << 40},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			store, _ := newCognitoTestStore(t)
			handler := NewHandler(store, Options{})
			body := &cognitoAdmissionBody{reader: strings.NewReader(payload)}
			request := httptest.NewRequest(http.MethodPost, "/", nil).WithContext(t.Context())
			request.Header.Set("X-Amz-Target", cognitoTargetPrefix+"CreateUserPool")
			request.Body = body
			request.ContentLength = scenario.length
			response := httptest.NewRecorder()

			handler.ServeAction(response, request, Action(request.Header.Get("X-Amz-Target")))

			require.Equal(t, http.StatusOK, response.Code, "body=%s", response.Body.String())
			assert.Equal(t, len(payload), body.read)
			assert.True(t, body.closed)
			assert.Equal(t, 1, cognitoAdmissionPoolCount(t, store))
			var envelope struct {
				UserPool struct{ Id, Name string }
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &envelope))
			require.NotEmpty(t, envelope.UserPool.Id)
			stored, err := store.LookupPool(t.Context(), envelope.UserPool.Id)
			require.NoError(t, err)
			assert.True(t, stored.Native)
			assert.Equal(t, "length-hint", stored.Name)
		})
	}
}
