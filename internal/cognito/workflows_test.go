package cognito

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type workflowCapture struct {
	mu      sync.Mutex
	records []Notification
	failure error
}

func (c *workflowCapture) Deliver(_ context.Context, n Notification) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure != nil {
		return c.failure
	}
	c.records = append(c.records, n)
	return nil
}
func (c *workflowCapture) last(t *testing.T) Notification {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	require.NotEmpty(t, c.records)
	return c.records[len(c.records)-1]
}
func (c *workflowCapture) count() int     { c.mu.Lock(); defer c.mu.Unlock(); return len(c.records) }
func (c *workflowCapture) fail(err error) { c.mu.Lock(); defer c.mu.Unlock(); c.failure = err }

type workflowEnvironment struct {
	store                *CognitoStore
	server               *httptest.Server
	capture              *workflowCapture
	clock                atomic.Int64
	pool, client, secret string
}

func newWorkflowEnvironment(t *testing.T, poolSettings, clientSettings map[string]interface{}) *workflowEnvironment {
	t.Helper()
	store, _ := newCognitoTestStore(t)
	env := &workflowEnvironment{store: store, capture: &workflowCapture{}}
	env.clock.Store(time.Now().Unix())
	handler := NewHandler(store, Options{IssuerBase: "http://localhost:4100", Notifications: env.capture, Clock: func() time.Time { return time.Unix(env.clock.Load(), 0) }})
	env.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeAction(w, r, Action(r.Header.Get("X-Amz-Target")))
	}))
	t.Cleanup(env.server.Close)
	poolRequest := map[string]interface{}{"PoolName": "workflow-pool", "AutoVerifiedAttributes": []string{"email"}, "AccountRecoverySetting": map[string]interface{}{"RecoveryMechanisms": []map[string]interface{}{{"Name": "verified_email", "Priority": 1}}}}
	for name, value := range poolSettings {
		poolRequest[name] = value
	}
	status, body := postCognito(t, env.server.URL, "CreateUserPool", poolRequest)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	env.pool = body["UserPool"].(map[string]interface{})["Id"].(string)
	clientRequest := map[string]interface{}{"UserPoolId": env.pool, "ClientName": "workflow-client", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"}}
	for name, value := range clientSettings {
		clientRequest[name] = value
	}
	status, body = postCognito(t, env.server.URL, "CreateUserPoolClient", clientRequest)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	client := body["UserPoolClient"].(map[string]interface{})
	env.client = client["ClientId"].(string)
	env.secret, _ = client["ClientSecret"].(string)
	return env
}
func (e *workflowEnvironment) request(username string) map[string]interface{} {
	request := map[string]interface{}{"ClientId": e.client, "Username": username}
	if e.secret != "" {
		request["SecretHash"] = computeSecretHash(e.secret, username, e.client)
	}
	return request
}
func (e *workflowEnvironment) signup(t *testing.T, username, email string) Notification {
	t.Helper()
	request := e.request(username)
	request["Password"] = "InitialPass1!"
	request["UserAttributes"] = []map[string]string{{"Name": "email", "Value": email}}
	status, body := postCognito(t, e.server.URL, "SignUp", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Equal(t, false, body["UserConfirmed"])
	require.NotEmpty(t, body["UserSub"])
	return e.capture.last(t)
}
func (e *workflowEnvironment) confirm(t *testing.T, username, code string) {
	t.Helper()
	request := e.request(username)
	request["ConfirmationCode"] = code
	status, body := postCognito(t, e.server.URL, "ConfirmSignUp", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
}
func differentWorkflowCode(code string) string {
	if code == "000000" {
		return "000001"
	}
	return "000000"
}

func TestWorkflowSignupConfirmationAndRecovery(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, map[string]interface{}{"GenerateSecret": true})
	capture := env.signup(t, "stable-user", "invitee@example.test")
	require.Equal(t, "SignUp", capture.Operation)
	require.Equal(t, "signup", capture.Purpose)
	require.Equal(t, "EMAIL", capture.DeliveryMedium)
	require.Equal(t, "invitee@example.test", capture.Destination)
	require.Len(t, capture.Code, 6)
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "stable-user")
	require.NoError(t, err)
	require.Equal(t, "UNCONFIRMED", user.Status)
	require.Equal(t, "stable-user", user.Username)
	require.NotEmpty(t, user.SRPVerifier)
	var storedHash string
	require.NoError(t, env.store.DB().QueryRow(`SELECT code_hash FROM verification_codes WHERE sub=? AND purpose='signup'`, user.Sub).Scan(&storedHash))
	require.NotEqual(t, capture.Code, storedHash)
	request := env.request(user.Username)
	request["ConfirmationCode"] = differentWorkflowCode(capture.Code)
	status, body := postCognito(t, env.server.URL, "ConfirmSignUp", request)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "CodeMismatchException", body["__type"])
	env.confirm(t, user.Username, capture.Code)
	attrs, err := env.store.LoadUserAttributes(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "true", attrs["email_verified"])
	request["ConfirmationCode"] = capture.Code
	status, body = postCognito(t, env.server.URL, "ConfirmSignUp", request)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NotAuthorizedException", body["__type"])
	require.NoError(t, env.store.CreateChallengeSession(t.Context(), "pending-recovery", user.Sub, env.pool, env.client, "CUSTOM_CHALLENGE", time.Minute))
	status, body = postCognito(t, env.server.URL, "ForgotPassword", env.request(user.Username))
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	recovery := env.capture.last(t)
	require.Equal(t, "recovery", recovery.Purpose)
	require.Equal(t, "i***@e***", body["CodeDeliveryDetails"].(map[string]interface{})["Destination"])
	request = env.request(user.Username)
	request["ConfirmationCode"] = recovery.Code
	request["Password"] = "RecoveredPass2!"
	status, body = postCognito(t, env.server.URL, "ConfirmForgotPassword", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	updated, err := env.store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, "CONFIRMED", updated.Status)
	require.Equal(t, user.AuthVersion+1, updated.AuthVersion)
	require.NoError(t, compareUserPasswordHash(updated.PasswordHash, "RecoveredPass2!"))
	require.NotEqual(t, user.SRPVerifier, updated.SRPVerifier)
	_, err = env.store.LookupChallengeSession(t.Context(), "pending-recovery")
	require.Error(t, err)
	status, body = postCognito(t, env.server.URL, "ConfirmForgotPassword", request)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ExpiredCodeException", body["__type"])
}

func TestWorkflowCodesExpireResendAndInvalidateAcrossDisable(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, nil)
	capture := env.signup(t, "expiry-user", "expiry@example.test")
	env.clock.Add(int64((24 * time.Hour) / time.Second))
	request := env.request("expiry-user")
	request["ConfirmationCode"] = capture.Code
	status, body := postCognito(t, env.server.URL, "ConfirmSignUp", request)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ExpiredCodeException", body["__type"])
	status, body = postCognito(t, env.server.URL, "ResendConfirmationCode", env.request("expiry-user"))
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	fresh := env.capture.last(t)
	env.confirm(t, "expiry-user", fresh.Code)
	status, body = postCognito(t, env.server.URL, "ForgotPassword", env.request("expiry-user"))
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	recovery := env.capture.last(t)
	env.clock.Add(int64(time.Hour / time.Second))
	request = env.request("expiry-user")
	request["ConfirmationCode"] = recovery.Code
	request["Password"] = "RecoveredPass2!"
	status, body = postCognito(t, env.server.URL, "ConfirmForgotPassword", request)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ExpiredCodeException", body["__type"])
	status, body = postCognito(t, env.server.URL, "ForgotPassword", env.request("expiry-user"))
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	recovery = env.capture.last(t)
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "expiry-user")
	require.NoError(t, err)
	require.NoError(t, env.store.SetUserEnabled(t.Context(), user.Sub, false))
	require.NoError(t, env.store.SetUserEnabled(t.Context(), user.Sub, true))
	request["ConfirmationCode"] = recovery.Code
	status, body = postCognito(t, env.server.URL, "ConfirmForgotPassword", request)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "ExpiredCodeException", body["__type"])
}

func TestWorkflowSignupPolicyPermissionsAndUnsupportedOptions(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]interface{}{"Schema": []map[string]interface{}{{"Name": "email", "AttributeDataType": "String", "Required": true}}}, map[string]interface{}{"WriteAttributes": []string{"email"}})
	for _, scenario := range []struct {
		name       string
		attributes []map[string]string
		extra      map[string]interface{}
		code       string
	}{
		{"required-email", nil, nil, "InvalidParameterException"},
		{"forbidden-name", []map[string]string{{"Name": "email", "Value": "ok@example.test"}, {"Name": "name", "Value": "Forbidden"}}, nil, "NotAuthorizedException"},
		{"forbidden-verified", []map[string]string{{"Name": "email", "Value": "ok@example.test"}, {"Name": "email_verified", "Value": "true"}}, nil, "NotAuthorizedException"},
		{"risk-context", []map[string]string{{"Name": "email", "Value": "ok@example.test"}}, map[string]interface{}{"UserContextData": map[string]string{"EncodedData": "risk"}}, "InvalidParameterException"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			request := env.request(scenario.name)
			request["Password"] = "InitialPass1!"
			request["UserAttributes"] = scenario.attributes
			for key, value := range scenario.extra {
				request[key] = value
			}
			status, body := postCognito(t, env.server.URL, "SignUp", request)
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, scenario.code, body["__type"])
			_, err := env.store.LookupPoolUser(t.Context(), env.pool, scenario.name)
			require.Error(t, err)
		})
	}
	require.Zero(t, env.capture.count())
	restricted := newWorkflowEnvironment(t, map[string]interface{}{"AdminCreateUserConfig": map[string]bool{"AllowAdminCreateUserOnly": true}}, nil)
	request := restricted.request("not-admitted")
	request["Password"] = "InitialPass1!"
	status, body := postCognito(t, restricted.server.URL, "SignUp", request)
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "NotAuthorizedException", body["__type"])
	manual := newWorkflowEnvironment(t, map[string]interface{}{"AutoVerifiedAttributes": []string{}}, nil)
	request = manual.request("manual-user")
	request["Password"] = "InitialPass1!"
	status, body = postCognito(t, manual.server.URL, "SignUp", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.NotContains(t, body, "CodeDeliveryDetails")
	require.Zero(t, manual.capture.count())
	status, body = postCognito(t, manual.server.URL, "AdminConfirmSignUp", map[string]string{"UserPoolId": manual.pool, "Username": "manual-user"})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err := manual.store.LookupPoolUser(t.Context(), manual.pool, "manual-user")
	require.NoError(t, err)
	require.Equal(t, "CONFIRMED", user.Status)
}

func TestWorkflowRecoveryPriorityAndCaptureFailure(t *testing.T) {
	env := newWorkflowEnvironment(t, map[string]interface{}{"AccountRecoverySetting": map[string]interface{}{"RecoveryMechanisms": []map[string]interface{}{{"Name": "verified_phone_number", "Priority": 2}, {"Name": "verified_email", "Priority": 1}}}}, nil)
	capture := env.signup(t, "recovery-user", "recovery@example.test")
	env.confirm(t, "recovery-user", capture.Code)
	status, body := postCognito(t, env.server.URL, "AdminUpdateUserAttributes", map[string]interface{}{"UserPoolId": env.pool, "Username": "recovery-user", "UserAttributes": []map[string]string{{"Name": "phone_number", "Value": "+12025550123"}, {"Name": "phone_number_verified", "Value": "true"}}})
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	status, body = postCognito(t, env.server.URL, "ForgotPassword", env.request("recovery-user"))
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	require.Equal(t, "email", env.capture.last(t).AttributeName)
	// A failed capture cannot replace the active recovery code with an invisible one.
	prior := env.capture.last(t)
	env.capture.fail(errors.New("disk full"))
	status, body = postCognito(t, env.server.URL, "ForgotPassword", env.request("recovery-user"))
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "CodeDeliveryFailureException", body["__type"])
	request := env.request("recovery-user")
	request["ConfirmationCode"] = prior.Code
	request["Password"] = "StillVisiblePass2!"
	status, body = postCognito(t, env.server.URL, "ConfirmForgotPassword", request)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	blocked := newWorkflowEnvironment(t, map[string]interface{}{"AccountRecoverySetting": map[string]interface{}{"RecoveryMechanisms": []map[string]interface{}{{"Name": "admin_only", "Priority": 1}}}}, nil)
	capture = blocked.signup(t, "blocked-recovery", "blocked@example.test")
	blocked.confirm(t, "blocked-recovery", capture.Code)
	before := blocked.capture.count()
	status, body = postCognito(t, blocked.server.URL, "ForgotPassword", blocked.request("blocked-recovery"))
	require.Equal(t, http.StatusBadRequest, status)
	require.Equal(t, "InvalidParameterException", body["__type"])
	require.Equal(t, before, blocked.capture.count())
}

func TestWorkflowRecoveryConfirmationConcurrentSingleUse(t *testing.T) {
	env := newWorkflowEnvironment(t, nil, nil)
	capture := env.signup(t, "race-user", "race@example.test")
	env.confirm(t, "race-user", capture.Code)
	status, body := postCognito(t, env.server.URL, "ForgotPassword", env.request("race-user"))
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	user, err := env.store.LookupPoolUser(t.Context(), env.pool, "race-user")
	require.NoError(t, err)
	code := env.capture.last(t).Code
	start := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			results <- env.store.confirmPasswordRecovery(context.Background(), user, code, "WinningPass2!")
		}()
	}
	close(start)
	wins := 0
	for range 2 {
		if err := <-results; err == nil {
			wins++
		} else {
			require.ErrorIs(t, err, errTokenRevoked)
		}
	}
	require.Equal(t, 1, wins)
	updated, err := env.store.LookupUserBySub(t.Context(), user.Sub)
	require.NoError(t, err)
	require.Equal(t, user.AuthVersion+1, updated.AuthVersion)
}

func TestWorkflowRecoveryDestinationDefaultsAndFallback(t *testing.T) {
	attrs := map[string]string{"email": "mail@example.test", "email_verified": "true", "phone_number": "+12025550123", "phone_number_verified": "true"}
	attribute, _, err := selectRecoveryDestination(&CognitoPool{}, attrs)
	require.NoError(t, err)
	require.Equal(t, "phone_number", attribute)
	pool := &CognitoPool{AccountRecoverySetting: &AccountRecoverySetting{RecoveryMechanisms: []RecoveryMechanism{{"verified_email", 1}, {"verified_phone_number", 2}}}}
	attribute, _, err = selectRecoveryDestination(pool, attrs)
	require.NoError(t, err)
	require.Equal(t, "email", attribute)
	attrs["email_verified"] = "false"
	attribute, _, err = selectRecoveryDestination(pool, attrs)
	require.NoError(t, err)
	require.Equal(t, "phone_number", attribute)
	attrs["phone_number_verified"] = "false"
	_, _, err = selectRecoveryDestination(pool, attrs)
	var workflow *workflowError
	require.ErrorAs(t, err, &workflow)
	require.Equal(t, "InvalidParameterException", workflow.Code)
}

// Assert wire payloads continue to contain native keys rather than capture keys.
func TestWorkflowDeliveryDetailsWireShape(t *testing.T) {
	encoded, err := json.Marshal(deliveryDetails("phone_number", "+12025550123"))
	require.NoError(t, err)
	require.JSONEq(t, `{"Destination":"***0123","DeliveryMedium":"SMS","AttributeName":"phone_number"}`, string(encoded))
}
