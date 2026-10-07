package cognito

import (
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
	"time"
)

func TestPerClientTokenValidityAfterCustomAndSRP(t *testing.T) {
	for _, flow := range []string{"CUSTOM_AUTH", "USER_SRP_AUTH"} {
		t.Run(flow, func(t *testing.T) {
			env := newCustomTestEnvironment(t, false)
			client, err := env.store.LookupClient(t.Context(), customTestClient)
			require.NoError(t, err)
			client.TokenValidity = &ClientTokenValidity{AccessTokenValidity: 13, IdTokenValidity: 17, RefreshTokenValidity: 2, TokenValidityUnits: TokenValidityUnits{AccessToken: "minutes", IdToken: "minutes", RefreshToken: "hours"}}
			require.NoError(t, env.store.SaveClient(t.Context(), client))
			var status int
			var body map[string]interface{}
			if flow == "CUSTOM_AUTH" {
				challenge := customReachChallenge(t, env)
				status, body = customRespond(t, env, challenge, customAnswerResponses(customTestAnswer), nil)
			} else {
				privateA := big.NewInt(123456789123456789)
				publicA := new(big.Int).Exp(big.NewInt(2), privateA, srpN).Text(16)
				status, body = postCognito(t, env.server.URL, "InitiateAuth", map[string]any{"ClientId": customTestClient, "AuthFlow": flow, "AuthParameters": map[string]string{"USERNAME": customTestEmail, "SRP_A": publicA, "SECRET_HASH": computeSecretHash(customTestSecret, customTestEmail, customTestClient)}})
				require.Equal(t, 200, status, "%v", body)
				status, body = customRespond(t, env, body, customProofResponses(t, body, privateA, customTestPassword), nil)
			}
			require.Equal(t, 200, status, "%v", body)
			result := readAuthResult(t, body)
			require.Equal(t, float64(13*60), result["ExpiresIn"])
			for _, entry := range []struct {
				name     string
				duration time.Duration
			}{{"AccessToken", 13 * time.Minute}, {"IdToken", 17 * time.Minute}, {"RefreshToken", 2 * time.Hour}} {
				claims := parseClaimsUnverified(t, result[entry.name].(string))
				require.Equal(t, entry.duration.Seconds(), claims["exp"].(float64)-claims["iat"].(float64), entry.name)
			}
		})
	}
}
