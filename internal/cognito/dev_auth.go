// Explicit fixture policy preserves supported legacy scenarios without making
// digit-only MFA acceptance part of ordinary AWS-created client behavior.
package cognito

const DevProfileLegacyFixtures = "legacy-fixtures"

func (s *Handler) allowLegacyFixtureAuth(client *CognitoClient) bool {
	return s.devProfile == DevProfileLegacyFixtures && !client.Native
}
func (s *Handler) allowLegacyFixturePool(pool *CognitoPool) bool {
	return s.devProfile == DevProfileLegacyFixtures && !pool.Native
}
