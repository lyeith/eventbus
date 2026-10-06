package cognito

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/pquerna/otp/totp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	customTestPool     = "us-east-1_CustomController"
	customTestClient   = "custom-controller-client"
	customTestSecret   = "custom-controller-secret"
	customTestUsername = "canonical-controller-user"
	customTestEmail    = "controller@example.test"
	customTestPassword = "ControllerPass!42"
	customTestAnswer   = "private-expected-answer"
	customTestMetadata = "EMAIL_CODE:controller-test"
)

type customTriggerCall struct {
	name  string
	event map[string]any
}

// This fake models an application's challenge policy, not Cognito's state
// machine. Its hooks let each test supply one hostile response or side effect.
type customTestTriggers struct {
	mu        sync.Mutex
	calls     []customTriggerCall
	hook      func(context.Context, string, map[string]any) (map[string]any, error, bool)
	temporary bool
}

func (f *customTestTriggers) Supports(poolID string) bool { return poolID == customTestPool }

func (f *customTestTriggers) Invoke(ctx context.Context, poolID, name string, event map[string]any) (map[string]any, error) {
	if poolID != customTestPool {
		return nil, errors.New("unexpected pool")
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	var copy map[string]any
	if err := json.Unmarshal(encoded, &copy); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.calls = append(f.calls, customTriggerCall{name: name, event: copy})
	f.mu.Unlock()
	if f.hook != nil {
		if result, err, handled := f.hook(ctx, name, copy); handled {
			return result, err
		}
	}
	request, ok := copy["request"].(map[string]any)
	if !ok {
		return nil, errors.New("request missing")
	}
	var response map[string]any
	switch name {
	case defineAuthTrigger:
		history, ok := request["session"].([]any)
		if !ok || len(history) == 0 {
			return nil, errors.New("history missing")
		}
		last := history[len(history)-1].(map[string]any)
		challenge, result := last["challengeName"], last["challengeResult"]
		switch {
		case challenge == "SRP_A":
			response = customDecision("PASSWORD_VERIFIER", false, false)
		case result == false:
			response = customDecision("", false, true)
		case challenge == "PASSWORD_VERIFIER" && f.temporary:
			response = customDecision("NEW_PASSWORD_REQUIRED", false, false)
		case challenge == "CUSTOM_CHALLENGE":
			response = customDecision("", true, false)
		default:
			response = customDecision("CUSTOM_CHALLENGE", false, false)
		}
	case createAuthTrigger:
		response = map[string]any{
			"publicChallengeParameters":  map[string]string{"delivery": "email", "hint": "c***@example.test"},
			"privateChallengeParameters": map[string]string{"answer": customTestAnswer},
			"challengeMetadata":          customTestMetadata,
		}
	case verifyAuthTrigger:
		private, ok := request["privateChallengeParameters"].(map[string]any)
		response = map[string]any{"answerCorrect": ok && request["challengeAnswer"] == private["answer"]}
	default:
		return nil, fmt.Errorf("unexpected trigger %q", name)
	}
	return map[string]any{"response": response}, nil
}

func customDecision(name string, issue, fail bool) map[string]any {
	return map[string]any{"challengeName": name, "issueTokens": issue, "failAuthentication": fail}
}

func (f *customTestTriggers) recorded() []customTriggerCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]customTriggerCall(nil), f.calls...)
}

func (f *customTestTriggers) count(name string) int {
	count := 0
	for _, call := range f.recorded() {
		if call.name == name {
			count++
		}
	}
	return count
}

type customTestEnvironment struct {
	server   *httptest.Server
	store    *CognitoStore
	user     *CognitoUser
	triggers *customTestTriggers
}

func newCustomTestEnvironment(t *testing.T, temporary bool) customTestEnvironment {
	t.Helper()
	store, _ := newCognitoTestStore(t)
	require.NoError(t, store.UpsertPool(t.Context(), customTestPool, "us-east-1"))
	require.NoError(t, store.SetPoolSignInConfig(t.Context(), customTestPool, PoolSignInConfig{EmailAlias: true, CaseSensitive: true}))
	require.NoError(t, store.UpsertClient(t.Context(), customTestClient, customTestPool, customTestSecret))
	require.NoError(t, store.SetClientAuthConfig(t.Context(), customTestClient, []string{"ALLOW_CUSTOM_AUTH", "ALLOW_USER_SRP_AUTH"}, 3))
	status := "CONFIRMED"
	if temporary {
		status = "FORCE_CHANGE_PASSWORD"
	}
	user, err := store.CreateUserIdentity(t.Context(), customTestPool, customTestUsername, customTestEmail, customTestPassword, status,
		map[string]string{"email": customTestEmail, "email_verified": "true", "custom:role": "developer"})
	require.NoError(t, err)
	triggers := &customTestTriggers{temporary: temporary}
	handler := NewHandler(store, Options{IssuerBase: "http://localhost:4100", Triggers: triggers})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeAction(w, r, Action(r.Header.Get("X-Amz-Target")))
	}))
	t.Cleanup(server.Close)
	return customTestEnvironment{server: server, store: store, user: user, triggers: triggers}
}

func customStart(t *testing.T, env customTestEnvironment) (int, map[string]interface{}, *big.Int) {
	t.Helper()
	privateA := big.NewInt(123456789123456789)
	publicA := new(big.Int).Exp(big.NewInt(2), privateA, srpN).Text(16)
	status, body := postCognito(t, env.server.URL, "AdminInitiateAuth", map[string]interface{}{
		"UserPoolId": customTestPool, "ClientId": customTestClient, "AuthFlow": "CUSTOM_AUTH",
		"ClientMetadata": map[string]string{"initiation-only": "must-not-reach-auth-triggers"},
		"AuthParameters": map[string]string{
			"USERNAME": customTestEmail, "CHALLENGE_NAME": "SRP_A", "SRP_A": publicA,
			"SECRET_HASH": computeSecretHash(customTestSecret, customTestEmail, customTestClient),
		},
	})
	return status, body, privateA
}

func customProofResponses(t *testing.T, challenge map[string]interface{}, privateA *big.Int, password string) map[string]string {
	t.Helper()
	params := challenge["ChallengeParameters"].(map[string]interface{})
	state := SRPState{SRPB: params["SRP_B"].(string), Salt: params["SALT"].(string), SecretBlock: params["SECRET_BLOCK"].(string)}
	timestamp := time.Now().UTC().Format(srpTimestampLayout)
	_, signature := srpIndependentClientProof(t, "CustomController", customTestUsername, password, privateA, state, timestamp)
	return map[string]string{
		"USERNAME": customTestUsername, "PASSWORD_CLAIM_SIGNATURE": signature,
		"PASSWORD_CLAIM_SECRET_BLOCK": state.SecretBlock, "TIMESTAMP": timestamp,
		"SECRET_HASH": computeSecretHash(customTestSecret, customTestUsername, customTestClient),
	}
}

func customResponsePayload(challenge map[string]interface{}, responses, metadata map[string]string) map[string]interface{} {
	return map[string]interface{}{
		"ClientId": customTestClient, "Session": challenge["Session"], "ChallengeName": challenge["ChallengeName"],
		"ChallengeResponses": responses, "ClientMetadata": metadata,
	}
}

func customRespond(t *testing.T, env customTestEnvironment, challenge map[string]interface{}, responses, metadata map[string]string) (int, map[string]interface{}) {
	t.Helper()
	return postCognito(t, env.server.URL, "RespondToAuthChallenge", customResponsePayload(challenge, responses, metadata))
}

func customAnswerResponses(answer string) map[string]string {
	return map[string]string{"USERNAME": customTestUsername, "ANSWER": answer, "SECRET_HASH": computeSecretHash(customTestSecret, customTestUsername, customTestClient)}
}

func customReachChallenge(t *testing.T, env customTestEnvironment) map[string]interface{} {
	t.Helper()
	status, passwordChallenge, privateA := customStart(t, env)
	require.Equal(t, http.StatusOK, status, "body=%v", passwordChallenge)
	require.Equal(t, "PASSWORD_VERIFIER", passwordChallenge["ChallengeName"])
	status, custom := customRespond(t, env, passwordChallenge, customProofResponses(t, passwordChallenge, privateA, customTestPassword), nil)
	require.Equal(t, http.StatusOK, status, "body=%v", custom)
	require.Equal(t, "CUSTOM_CHALLENGE", custom["ChallengeName"])
	return custom
}

func TestCustomAuthController_EventsHistoryMetadataAndPrivateParameters(t *testing.T) {
	env := newCustomTestEnvironment(t, false)
	status, proofChallenge, privateA := customStart(t, env)
	require.Equal(t, http.StatusOK, status, "body=%v", proofChallenge)
	assert.NotContains(t, proofChallenge, "AuthenticationResult")
	proofMetadata := map[string]string{"correlation": "password-proof", "app-context": "first"}
	status, challenge := customRespond(t, env, proofChallenge, customProofResponses(t, proofChallenge, privateA, customTestPassword), proofMetadata)
	require.Equal(t, http.StatusOK, status, "body=%v", challenge)
	require.Equal(t, "CUSTOM_CHALLENGE", challenge["ChallengeName"])
	assert.Equal(t, map[string]interface{}{"delivery": "email", "hint": "c***@example.test", "USERNAME": customTestUsername}, challenge["ChallengeParameters"])
	encoded, err := json.Marshal(challenge)
	require.NoError(t, err)
	assert.NotContains(t, string(encoded), customTestAnswer)
	assert.NotContains(t, string(encoded), customTestMetadata)
	assert.NotContains(t, string(encoded), "derived_key")
	answerMetadata := map[string]string{"correlation": "custom-answer"}
	status, body := customRespond(t, env, challenge, customAnswerResponses(customTestAnswer), answerMetadata)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	readAuthResult(t, body)
	calls := env.triggers.recorded()
	require.Len(t, calls, 5)
	assert.Equal(t, []string{defineAuthTrigger, defineAuthTrigger, createAuthTrigger, verifyAuthTrigger, defineAuthTrigger}, []string{calls[0].name, calls[1].name, calls[2].name, calls[3].name, calls[4].name})
	for _, call := range calls {
		assert.Equal(t, "1", call.event["version"])
		assert.Equal(t, "us-east-1", call.event["region"])
		assert.Equal(t, customTestPool, call.event["userPoolId"])
		assert.Equal(t, customTestUsername, call.event["userName"])
		assert.Equal(t, call.name+"_Authentication", call.event["triggerSource"])
		assert.Equal(t, customTestClient, call.event["callerContext"].(map[string]any)["clientId"])
		request := call.event["request"].(map[string]any)
		assert.Equal(t, false, request["userNotFound"])
		attributes := request["userAttributes"].(map[string]any)
		assert.Equal(t, env.user.Sub, attributes["sub"])
		assert.Equal(t, customTestEmail, attributes["email"])
		assert.Equal(t, "developer", attributes["custom:role"])
		if call.name != verifyAuthTrigger {
			assert.NotContains(t, request, "privateChallengeParameters")
		}
	}
	initialRequest := calls[0].event["request"].(map[string]any)
	assert.NotContains(t, initialRequest, "clientMetadata", "AWS does not forward InitiateAuth metadata to auth challenge triggers")
	assert.Equal(t, []any{map[string]any{"challengeName": "SRP_A", "challengeResult": true}}, initialRequest["session"])
	for _, index := range []int{1, 2} {
		request := calls[index].event["request"].(map[string]any)
		assert.Equal(t, map[string]any{"correlation": "password-proof", "app-context": "first"}, request["clientMetadata"])
		assert.Equal(t, []any{map[string]any{"challengeName": "SRP_A", "challengeResult": true}, map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true}}, request["session"])
	}
	assert.Equal(t, "CUSTOM_CHALLENGE", calls[2].event["request"].(map[string]any)["challengeName"])
	verifyRequest := calls[3].event["request"].(map[string]any)
	assert.Equal(t, map[string]any{"answer": customTestAnswer}, verifyRequest["privateChallengeParameters"])
	assert.Equal(t, customTestAnswer, verifyRequest["challengeAnswer"])
	assert.Equal(t, map[string]any{"correlation": "custom-answer"}, verifyRequest["clientMetadata"])
	finalRequest := calls[4].event["request"].(map[string]any)
	assert.Equal(t, map[string]any{"correlation": "custom-answer"}, finalRequest["clientMetadata"])
	assert.Equal(t, []any{
		map[string]any{"challengeName": "SRP_A", "challengeResult": true},
		map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true},
		map[string]any{"challengeName": "CUSTOM_CHALLENGE", "challengeResult": true, "challengeMetadata": customTestMetadata},
	}, finalRequest["session"])
}

func TestCustomAuthController_CannotBypassSRPPasswordProof(t *testing.T) {
	for _, attempt := range []string{"early-token", "skip-verifier", "wrong-password-then-token"} {
		t.Run(attempt, func(t *testing.T) {
			env := newCustomTestEnvironment(t, false)
			env.triggers.hook = func(ctx context.Context, name string, event map[string]any) (map[string]any, error, bool) {
				if name != defineAuthTrigger {
					return nil, nil, false
				}
				history := event["request"].(map[string]any)["session"].([]any)
				if attempt == "wrong-password-then-token" && len(history) == 1 {
					return nil, nil, false
				}
				decision := customDecision("", true, false)
				if attempt == "skip-verifier" {
					decision = customDecision("CUSTOM_CHALLENGE", false, false)
				}
				return map[string]any{"response": decision}, nil, true
			}
			status, body, privateA := customStart(t, env)
			if attempt == "wrong-password-then-token" {
				require.Equal(t, http.StatusOK, status, "body=%v", body)
				status, body = customRespond(t, env, body, customProofResponses(t, body, privateA, "WrongPassword!42"), nil)
				assert.Equal(t, "NotAuthorizedException", body["__type"])
			} else {
				assert.Equal(t, "InvalidLambdaResponseException", body["__type"])
			}
			assert.Equal(t, http.StatusBadRequest, status)
			assert.NotContains(t, body, "AuthenticationResult")
			assert.Equal(t, 0, env.triggers.count(createAuthTrigger))
			assert.Equal(t, 0, env.triggers.count(verifyAuthTrigger))
		})
	}
}

func TestCustomAuthController_TemporaryPasswordContinuesCustomHistory(t *testing.T) {
	env := newCustomTestEnvironment(t, true)
	status, proofChallenge, privateA := customStart(t, env)
	require.Equal(t, http.StatusOK, status, "body=%v", proofChallenge)
	status, replacement := customRespond(t, env, proofChallenge, customProofResponses(t, proofChallenge, privateA, customTestPassword), nil)
	require.Equal(t, http.StatusOK, status, "body=%v", replacement)
	require.Equal(t, "NEW_PASSWORD_REQUIRED", replacement["ChallengeName"])
	assert.NotContains(t, replacement, "AuthenticationResult")
	assert.Equal(t, 0, env.triggers.count(createAuthTrigger))
	responses := customAnswerResponses("")
	delete(responses, "ANSWER")
	responses["NEW_PASSWORD"] = "NewControllerPass!42"
	status, challenge := customRespond(t, env, replacement, responses, map[string]string{"stage": "password-replaced"})
	require.Equal(t, http.StatusOK, status, "body=%v", challenge)
	require.Equal(t, "CUSTOM_CHALLENGE", challenge["ChallengeName"])
	assert.NotContains(t, challenge, "AuthenticationResult")
	calls := env.triggers.recorded()
	require.Len(t, calls, 4)
	assert.Equal(t, defineAuthTrigger, calls[1].name)
	assert.Equal(t, []any{
		map[string]any{"challengeName": "SRP_A", "challengeResult": true},
		map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true},
	}, calls[1].event["request"].(map[string]any)["session"])
	request := calls[2].event["request"].(map[string]any)
	assert.Equal(t, []any{
		map[string]any{"challengeName": "SRP_A", "challengeResult": true},
		map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true},
		map[string]any{"challengeName": "NEW_PASSWORD_REQUIRED", "challengeResult": true},
	}, request["session"])
	status, body := customRespond(t, env, challenge, customAnswerResponses(customTestAnswer), nil)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	updated, err := env.store.LookupUserBySub(t.Context(), env.user.Sub)
	require.NoError(t, err)
	assert.Equal(t, "CONFIRMED", updated.Status)
	require.NoError(t, compareUserPasswordHash(updated.PasswordHash, "NewControllerPass!42"))
	claims := parseClaimsUnverified(t, readAuthResult(t, body)["AccessToken"].(string))
	assert.Equal(t, float64(updated.AuthVersion), claims[authVersionClaim])
	status, body = customRespond(t, env, replacement, responses, nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
}

func TestCustomAuthController_TemporaryPasswordPolicyCannotSkipReplacement(t *testing.T) {
	for _, attempt := range []string{"issue-tokens", "skip-to-custom", "fail-authentication"} {
		t.Run(attempt, func(t *testing.T) {
			env := newCustomTestEnvironment(t, true)
			env.triggers.hook = func(ctx context.Context, name string, event map[string]any) (map[string]any, error, bool) {
				if name != defineAuthTrigger {
					return nil, nil, false
				}
				history := event["request"].(map[string]any)["session"].([]any)
				if len(history) == 1 {
					return nil, nil, false
				}
				decision := customDecision("", true, false)
				if attempt == "skip-to-custom" {
					decision = customDecision("CUSTOM_CHALLENGE", false, false)
				}
				if attempt == "fail-authentication" {
					decision = customDecision("", false, true)
				}
				return map[string]any{"response": decision}, nil, true
			}
			status, challenge, privateA := customStart(t, env)
			require.Equal(t, http.StatusOK, status, "body=%v", challenge)
			status, body := customRespond(t, env, challenge, customProofResponses(t, challenge, privateA, customTestPassword), nil)
			assert.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			wantCode := "InvalidLambdaResponseException"
			if attempt == "fail-authentication" {
				wantCode = "NotAuthorizedException"
			}
			assert.Equal(t, wantCode, body["__type"])
			assert.NotContains(t, body, "AuthenticationResult")
			assert.NotContains(t, body, "Session")
			assert.Equal(t, 0, env.triggers.count(createAuthTrigger))
			user, err := env.store.LookupUserBySub(t.Context(), env.user.Sub)
			require.NoError(t, err)
			assert.Equal(t, "FORCE_CHANGE_PASSWORD", user.Status)
		})
	}
}

func TestCustomAuthController_FailedAnswerRetryUsesFreshSessionAndHistory(t *testing.T) {
	env := newCustomTestEnvironment(t, false)
	env.triggers.hook = func(ctx context.Context, name string, event map[string]any) (map[string]any, error, bool) {
		if name != defineAuthTrigger {
			return nil, nil, false
		}
		history := event["request"].(map[string]any)["session"].([]any)
		last := history[len(history)-1].(map[string]any)
		if last["challengeName"] == "CUSTOM_CHALLENGE" && last["challengeResult"] == false {
			return map[string]any{"response": customDecision("CUSTOM_CHALLENGE", false, false)}, nil, true
		}
		return nil, nil, false
	}
	first := customReachChallenge(t, env)
	status, next := customRespond(t, env, first, customAnswerResponses("wrong-answer"), nil)
	require.Equal(t, http.StatusOK, status, "configured policy retries a failed answer: %v", next)
	require.Equal(t, "CUSTOM_CHALLENGE", next["ChallengeName"])
	assert.NotContains(t, next, "AuthenticationResult")
	assert.NotEqual(t, first["Session"], next["Session"])
	status, body := customRespond(t, env, first, customAnswerResponses(customTestAnswer), nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
	status, body = customRespond(t, env, next, customAnswerResponses(customTestAnswer), nil)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	readAuthResult(t, body)
	calls := env.triggers.recorded()
	assert.Equal(t, 2, env.triggers.count(createAuthTrigger))
	assert.Equal(t, 2, env.triggers.count(verifyAuthTrigger))
	assert.Equal(t, []any{
		map[string]any{"challengeName": "SRP_A", "challengeResult": true},
		map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true},
		map[string]any{"challengeName": "CUSTOM_CHALLENGE", "challengeResult": false, "challengeMetadata": customTestMetadata},
		map[string]any{"challengeName": "CUSTOM_CHALLENGE", "challengeResult": true, "challengeMetadata": customTestMetadata},
	}, calls[len(calls)-1].event["request"].(map[string]any)["session"])
}

func TestCustomAuthController_ExpiredTemporaryPasswordCannotContinue(t *testing.T) {
	for _, expireAt := range []string{"before-srp-proof", "before-new-password"} {
		t.Run(expireAt, func(t *testing.T) {
			env := newCustomTestEnvironment(t, true)
			status, challenge, privateA := customStart(t, env)
			require.Equal(t, http.StatusOK, status, "body=%v", challenge)
			responses := customProofResponses(t, challenge, privateA, customTestPassword)
			if expireAt == "before-new-password" {
				status, challenge = customRespond(t, env, challenge, responses, nil)
				require.Equal(t, http.StatusOK, status, "body=%v", challenge)
				require.Equal(t, "NEW_PASSWORD_REQUIRED", challenge["ChallengeName"])
				responses = customAnswerResponses("")
				delete(responses, "ANSWER")
				responses["NEW_PASSWORD"] = "NewControllerPass!42"
			}
			_, err := env.store.DB().ExecContext(t.Context(), `UPDATE users SET password_changed_at=? WHERE sub=?`, time.Now().Add(-8*24*time.Hour).Unix(), env.user.Sub)
			require.NoError(t, err)
			status, body := customRespond(t, env, challenge, responses, nil)
			assert.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			assert.Equal(t, "NotAuthorizedException", body["__type"])
			assert.NotContains(t, body, "AuthenticationResult")
			assert.NotContains(t, body, "Session")
			assert.Equal(t, 0, env.triggers.count(createAuthTrigger))
			user, err := env.store.LookupUserBySub(t.Context(), env.user.Sub)
			require.NoError(t, err)
			assert.Equal(t, "FORCE_CHANGE_PASSWORD", user.Status)
		})
	}
}

func TestCustomAuthController_SessionBindingAndReplay(t *testing.T) {
	env := newCustomTestEnvironment(t, false)
	challenge := customReachChallenge(t, env)
	require.NoError(t, env.store.UpsertClient(t.Context(), "other-client", customTestPool, customTestSecret))
	for _, attempt := range []string{"other-client", "other-user", "email-alias", "wrong-secret", "wrong-challenge", "tampered-session", "wrong-admin-pool"} {
		payload := customResponsePayload(challenge, customAnswerResponses(customTestAnswer), nil)
		responses := payload["ChallengeResponses"].(map[string]string)
		operation := "RespondToAuthChallenge"
		switch attempt {
		case "other-client":
			payload["ClientId"] = "other-client"
			responses["SECRET_HASH"] = computeSecretHash(customTestSecret, customTestUsername, "other-client")
		case "other-user", "email-alias":
			responses["USERNAME"] = "other-user"
			if attempt == "email-alias" {
				responses["USERNAME"] = customTestEmail
			}
			responses["SECRET_HASH"] = computeSecretHash(customTestSecret, responses["USERNAME"], customTestClient)
		case "wrong-secret":
			responses["SECRET_HASH"] = "incorrect-hash"
		case "wrong-challenge":
			payload["ChallengeName"] = "PASSWORD_VERIFIER"
		case "tampered-session":
			session := challenge["Session"].(string)
			payload["Session"] = session[:len(session)-6] + "AAAAAA"
		case "wrong-admin-pool":
			operation = "AdminRespondToAuthChallenge"
			payload["UserPoolId"] = "foreign-pool"
		}
		status, body := postCognito(t, env.server.URL, operation, payload)
		assert.Equal(t, http.StatusBadRequest, status, "attempt=%s body=%v", attempt, body)
		assert.NotContains(t, body, "AuthenticationResult")
		assert.Equal(t, 0, env.triggers.count(verifyAuthTrigger), "no trigger side effects for %s", attempt)
	}
	status, body := customRespond(t, env, challenge, customAnswerResponses(customTestAnswer), nil)
	require.Equal(t, http.StatusOK, status, "refused attempts must not consume the owner's session: %v", body)
	readAuthResult(t, body)
	status, body = customRespond(t, env, challenge, customAnswerResponses(customTestAnswer), nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "NotAuthorizedException", body["__type"])
	assert.Equal(t, 1, env.triggers.count(verifyAuthTrigger))
}

func TestCustomAuthController_InvalidatedAndExpiredSessionsHaveNoTriggerEffects(t *testing.T) {
	for _, mutate := range []string{"disable", "reset", "delete", "expire"} {
		t.Run(mutate, func(t *testing.T) {
			env := newCustomTestEnvironment(t, false)
			challenge := customReachChallenge(t, env)
			switch mutate {
			case "disable":
				require.NoError(t, env.store.SetUserEnabled(t.Context(), env.user.Sub, false))
			case "reset":
				require.NoError(t, env.store.SetUserPassword(t.Context(), env.user.Sub, "ResetControllerPass!42", "CONFIRMED"))
			case "delete":
				_, err := env.store.DB().ExecContext(t.Context(), `DELETE FROM users WHERE sub=?`, env.user.Sub)
				require.NoError(t, err)
			case "expire":
				_, err := env.store.DB().ExecContext(t.Context(), `UPDATE challenge_sessions SET expires_at=? WHERE sub=? AND used=0`, time.Now().Add(-time.Second).Unix(), env.user.Sub)
				require.NoError(t, err)
			}
			status, body := customRespond(t, env, challenge, customAnswerResponses(customTestAnswer), nil)
			assert.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			assert.Equal(t, "NotAuthorizedException", body["__type"])
			assert.Equal(t, 0, env.triggers.count(verifyAuthTrigger))
		})
	}
}

type customRawResult struct {
	status int
	body   map[string]interface{}
	err    error
}

func customPostRaw(baseURL string, payload map[string]interface{}) customRawResult {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return customRawResult{err: err}
	}
	request, err := http.NewRequest(http.MethodPost, baseURL+"/", bytes.NewReader(encoded))
	if err != nil {
		return customRawResult{err: err}
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService.RespondToAuthChallenge")
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return customRawResult{err: err}
	}
	defer response.Body.Close()
	var body map[string]interface{}
	err = json.NewDecoder(response.Body).Decode(&body)
	return customRawResult{status: response.StatusCode, body: body, err: err}
}

func TestCustomAuthController_ConcurrentResponsesConsumeOnceBeforeTriggerEffects(t *testing.T) {
	env := newCustomTestEnvironment(t, false)
	challenge := customReachChallenge(t, env)
	entered, release := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	env.triggers.hook = func(ctx context.Context, name string, event map[string]any) (map[string]any, error, bool) {
		if name != verifyAuthTrigger {
			return nil, nil, false
		}
		enteredOnce.Do(func() { close(entered) })
		select {
		case <-release:
			return nil, nil, false
		case <-ctx.Done():
			return nil, ctx.Err(), true
		}
	}
	results := make(chan customRawResult, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		payload := customResponsePayload(challenge, customAnswerResponses(customTestAnswer), nil)
		go func() { <-start; results <- customPostRaw(env.server.URL, payload) }()
	}
	close(start)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Verify trigger was never invoked")
	}
	// The losing request must return while the winning trigger is blocked.
	select {
	case loser := <-results:
		require.NoError(t, loser.err)
		assert.Equal(t, http.StatusBadRequest, loser.status, "body=%v", loser.body)
		assert.Equal(t, "NotAuthorizedException", loser.body["__type"])
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent loser did not return before trigger release")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case winner := <-results:
		require.NoError(t, winner.err)
		require.Equal(t, http.StatusOK, winner.status, "body=%v", winner.body)
		readAuthResult(t, winner.body)
	case <-time.After(5 * time.Second):
		t.Fatal("winning response did not finish")
	}
	assert.Equal(t, 1, env.triggers.count(verifyAuthTrigger), "application verification side effect occurs once")
}

type customClassifiedFailure struct{}

func (customClassifiedFailure) Error() string       { return "malformed trigger output" }
func (customClassifiedFailure) FailureKind() string { return "invalid_response" }

func TestCustomAuthController_TriggerErrorsAndMalformedResponses(t *testing.T) {
	for _, test := range []struct {
		name, trigger, wantCode string
		response                map[string]any
		err                     error
	}{
		{name: "missing-envelope", trigger: defineAuthTrigger, wantCode: "InvalidLambdaResponseException", response: map[string]any{}},
		{name: "contradictory-decision", trigger: defineAuthTrigger, wantCode: "InvalidLambdaResponseException", response: map[string]any{"response": customDecision("", true, true)}},
		{name: "wrong-decision-types", trigger: defineAuthTrigger, wantCode: "InvalidLambdaResponseException", response: map[string]any{"response": map[string]any{"issueTokens": "true", "failAuthentication": false}}},
		{name: "non-string-private", trigger: createAuthTrigger, wantCode: "InvalidLambdaResponseException", response: map[string]any{"response": map[string]any{"privateChallengeParameters": map[string]any{"answer": 42}}}},
		{name: "non-string-metadata", trigger: createAuthTrigger, wantCode: "InvalidLambdaResponseException", response: map[string]any{"response": map[string]any{"challengeMetadata": 42}}},
		{name: "non-boolean-verification", trigger: verifyAuthTrigger, wantCode: "InvalidLambdaResponseException", response: map[string]any{"response": map[string]any{"answerCorrect": "true"}}},
		{name: "handler-failure", trigger: defineAuthTrigger, wantCode: "UnexpectedLambdaException", err: errors.New("application handler failed")},
		{name: "handler-timeout", trigger: verifyAuthTrigger, wantCode: "UnexpectedLambdaException", err: context.DeadlineExceeded},
		{name: "invalid-runner-output", trigger: createAuthTrigger, wantCode: "InvalidLambdaResponseException", err: customClassifiedFailure{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := newCustomTestEnvironment(t, false)
			env.triggers.hook = func(ctx context.Context, name string, event map[string]any) (map[string]any, error, bool) {
				if name == test.trigger {
					return test.response, test.err, true
				}
				return nil, nil, false
			}
			status, body, privateA := customStart(t, env)
			if test.trigger != defineAuthTrigger {
				require.Equal(t, http.StatusOK, status, "body=%v", body)
				status, body = customRespond(t, env, body, customProofResponses(t, body, privateA, customTestPassword), nil)
				if test.trigger == verifyAuthTrigger {
					require.Equal(t, http.StatusOK, status, "body=%v", body)
					status, body = customRespond(t, env, body, customAnswerResponses(customTestAnswer), nil)
				}
			}
			assert.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			assert.Equal(t, test.wantCode, body["__type"])
			assert.NotContains(t, body, "AuthenticationResult")
		})
	}
}

func TestCustomAuthController_EmptyPrivateParametersReachVerifyAsObject(t *testing.T) {
	env := newCustomTestEnvironment(t, false)
	env.triggers.hook = func(ctx context.Context, name string, event map[string]any) (map[string]any, error, bool) {
		switch name {
		case createAuthTrigger:
			return map[string]any{"response": map[string]any{
				"publicChallengeParameters":  map[string]string{"prompt": "app-owned challenge"},
				"privateChallengeParameters": map[string]string{},
			}}, nil, true
		case verifyAuthTrigger:
			return map[string]any{"response": map[string]any{"answerCorrect": true}}, nil, true
		default:
			return nil, nil, false
		}
	}
	challenge := customReachChallenge(t, env)
	status, body := customRespond(t, env, challenge, customAnswerResponses("app-answer"), nil)
	require.Equal(t, http.StatusOK, status, "body=%v", body)
	readAuthResult(t, body)
	for _, call := range env.triggers.recorded() {
		if call.name == verifyAuthTrigger {
			request := call.event["request"].(map[string]any)
			private, ok := request["privateChallengeParameters"].(map[string]any)
			require.True(t, ok, "empty private parameters must encode as {}, not null: %v", request)
			assert.Empty(t, private)
			return
		}
	}
	t.Fatal("VerifyAuthChallengeResponse was not invoked")
}

func TestCustomAuthController_InvalidRestoredPasswordVerifierStateFailsClosed(t *testing.T) {
	for _, corruption := range []string{"already-password-verified", "missing-srp", "foreign-username", "foreign-pool"} {
		t.Run(corruption, func(t *testing.T) {
			env := newCustomTestEnvironment(t, false)
			status, challenge, privateA := customStart(t, env)
			require.Equal(t, http.StatusOK, status, "body=%v", challenge)
			responses := customProofResponses(t, challenge, privateA, customTestPassword)
			var session, encoded string
			require.NoError(t, env.store.DB().QueryRowContext(t.Context(),
				"SELECT session,state_json FROM challenge_sessions WHERE sub=? AND challenge_name='PASSWORD_VERIFIER' AND used=0", env.user.Sub).Scan(&session, &encoded))
			var state authChallengeState
			require.NoError(t, json.Unmarshal([]byte(encoded), &state))
			require.NotNil(t, state.SRP)
			switch corruption {
			case "already-password-verified":
				state.PasswordVerified = true
			case "missing-srp":
				state.SRP = nil
			case "foreign-username":
				state.SRP.Username = "foreign-user"
			case "foreign-pool":
				state.SRP.PoolSuffix = "ForeignPool"
			}
			changed, err := json.Marshal(state)
			require.NoError(t, err)
			_, err = env.store.DB().ExecContext(t.Context(), "UPDATE challenge_sessions SET state_json=? WHERE session=?", string(changed), session)
			require.NoError(t, err)
			// A hostile approval must not authenticate corrupted restored state.
			env.triggers.hook = func(ctx context.Context, name string, event map[string]any) (map[string]any, error, bool) {
				if name == defineAuthTrigger {
					return map[string]any{"response": customDecision("", true, false)}, nil, true
				}
				return nil, nil, false
			}
			status, body := customRespond(t, env, challenge, responses, nil)
			assert.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			assert.Equal(t, "NotAuthorizedException", body["__type"])
			assert.NotContains(t, body, "AuthenticationResult")
			assert.NotContains(t, body, "Session")
			assert.Equal(t, 0, env.triggers.count(createAuthTrigger))
		})
	}
}

func customMFAResponses(code string) map[string]string {
	return map[string]string{
		"USERNAME": customTestUsername, "SOFTWARE_TOKEN_MFA_CODE": code,
		"SECRET_HASH": computeSecretHash(customTestSecret, customTestUsername, customTestClient),
	}
}

// Include one extra window on each side so a period boundary during the request
// cannot turn the selected wrong six-digit value into an accepted code.
func customIncorrectTOTP(t *testing.T, secret string) string {
	t.Helper()
	now := time.Now()
	accepted := map[string]bool{}
	for _, delta := range []time.Duration{-60 * time.Second, -30 * time.Second, 0, 30 * time.Second, 60 * time.Second} {
		code, err := totp.GenerateCode(secret, now.Add(delta))
		require.NoError(t, err)
		accepted[code] = true
	}
	for value := 0; ; value++ {
		code := fmt.Sprintf("%06d", value)
		if !accepted[code] {
			return code
		}
	}
}

func TestCustomAuthController_RealMFAIsRequiredBeforeCustomPolicyContinues(t *testing.T) {
	for _, temporary := range []bool{false, true} {
		t.Run(map[bool]string{false: "confirmed-password", true: "temporary-password"}[temporary], func(t *testing.T) {
			env := newCustomTestEnvironment(t, temporary)
			require.NoError(t, env.store.SetUserTOTPSecret(t.Context(), env.user.Sub, testTOTPSecret))
			_, err := env.store.DB().ExecContext(t.Context(), "UPDATE users SET mfa_enabled=1 WHERE sub=?", env.user.Sub)
			require.NoError(t, err)
			status, challenge, privateA := customStart(t, env)
			require.Equal(t, http.StatusOK, status, "body=%v", challenge)
			status, next := customRespond(t, env, challenge, customProofResponses(t, challenge, privateA, customTestPassword), nil)
			require.Equal(t, http.StatusOK, status, "body=%v", next)
			defineBeforeMFA := 1
			expectedHistory := []any{
				map[string]any{"challengeName": "SRP_A", "challengeResult": true},
				map[string]any{"challengeName": "PASSWORD_VERIFIER", "challengeResult": true},
			}
			if temporary {
				require.Equal(t, "NEW_PASSWORD_REQUIRED", next["ChallengeName"])
				responses := map[string]string{
					"USERNAME": customTestUsername, "NEW_PASSWORD": "NewMFAControllerPass!42",
					"SECRET_HASH": computeSecretHash(customTestSecret, customTestUsername, customTestClient),
				}
				status, next = customRespond(t, env, next, responses, nil)
				require.Equal(t, http.StatusOK, status, "body=%v", next)
				defineBeforeMFA = 2
				expectedHistory = append(expectedHistory, map[string]any{"challengeName": "NEW_PASSWORD_REQUIRED", "challengeResult": true})
			}
			require.Equal(t, "SOFTWARE_TOKEN_MFA", next["ChallengeName"])
			assert.NotContains(t, next, "AuthenticationResult")
			assert.Equal(t, 0, env.triggers.count(createAuthTrigger), "custom application side effects wait for built-in MFA")
			assert.Equal(t, defineBeforeMFA, env.triggers.count(defineAuthTrigger), "Define waits for built-in MFA proof")
			status, body := customRespond(t, env, next, customMFAResponses(customIncorrectTOTP(t, testTOTPSecret)), nil)
			assert.Equal(t, http.StatusBadRequest, status, "body=%v", body)
			assert.Equal(t, "CodeMismatchException", body["__type"])
			assert.NotContains(t, body, "AuthenticationResult")
			assert.Equal(t, defineBeforeMFA, env.triggers.count(defineAuthTrigger))
			assert.Equal(t, 0, env.triggers.count(createAuthTrigger))
			status, custom := customRespond(t, env, next, customMFAResponses(totpCode(t, testTOTPSecret)), map[string]string{"stage": "mfa"})
			require.Equal(t, http.StatusOK, status, "body=%v", custom)
			require.Equal(t, "CUSTOM_CHALLENGE", custom["ChallengeName"])
			assert.NotContains(t, custom, "AuthenticationResult")
			expectedHistory = append(expectedHistory, map[string]any{"challengeName": "SOFTWARE_TOKEN_MFA", "challengeResult": true})
			calls := env.triggers.recorded()
			request := calls[defineBeforeMFA].event["request"].(map[string]any)
			assert.Equal(t, expectedHistory, request["session"])
			assert.Equal(t, map[string]any{"stage": "mfa"}, request["clientMetadata"])
			events := make([]map[string]any, 0, len(calls))
			for _, call := range calls {
				events = append(events, call.event)
			}
			encoded, err := json.Marshal(events)
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), testTOTPSecret)
			status, body = customRespond(t, env, custom, customAnswerResponses(customTestAnswer), nil)
			require.Equal(t, http.StatusOK, status, "body=%v", body)
			readAuthResult(t, body)
			finalCalls := env.triggers.recorded()
			expectedHistory = append(expectedHistory, map[string]any{"challengeName": "CUSTOM_CHALLENGE", "challengeResult": true, "challengeMetadata": customTestMetadata})
			assert.Equal(t, expectedHistory, finalCalls[len(finalCalls)-1].event["request"].(map[string]any)["session"])
		})
	}
}

func TestCustomAuthController_EnabledMFAWithoutTOTPSecretHasNoFixtureBypass(t *testing.T) {
	env := newCustomTestEnvironment(t, false)
	_, err := env.store.DB().ExecContext(t.Context(), "UPDATE users SET mfa_enabled=1,totp_secret='' WHERE sub=?", env.user.Sub)
	require.NoError(t, err)
	status, body, _ := customStart(t, env)
	require.Equal(t, http.StatusBadRequest, status, "body=%v", body)
	assert.Equal(t, "InvalidParameterException", body["__type"])
	assert.NotContains(t, body, "AuthenticationResult")
	assert.NotContains(t, body, "Session")
	assert.Empty(t, env.triggers.recorded(), "invalid MFA configuration must not execute app handlers")
}
