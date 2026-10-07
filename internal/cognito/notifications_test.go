package cognito

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWorkflowConfigurationRejectsUnsupportedSelections(t *testing.T) {
	lifetime := 5
	for _, scenario := range []struct {
		name     string
		auto     []string
		recovery *AccountRecoverySetting
		admin    *AdminCreateUserConfig
	}{
		{"unknown-auto", []string{"preferred_username"}, nil, nil},
		{"duplicate-auto", []string{"email", "email"}, nil, nil},
		{"empty-recovery", nil, &AccountRecoverySetting{}, nil},
		{"duplicate-priority", nil, &AccountRecoverySetting{[]RecoveryMechanism{{"verified_email", 1}, {"verified_phone_number", 1}}}, nil},
		{"unknown-recovery", nil, &AccountRecoverySetting{[]RecoveryMechanism{{"custom", 1}}}, nil},
		{"admin-with-other", nil, &AccountRecoverySetting{[]RecoveryMechanism{{"admin_only", 1}, {"verified_email", 2}}}, nil},
		{"custom-invite", nil, nil, &AdminCreateUserConfig{InviteMessageTemplate: &InviteMessageTemplate{EmailMessage: "Customized {####}"}}},
		{"deprecated-lifetime", nil, nil, &AdminCreateUserConfig{UnusedAccountValidityDays: &lifetime}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			require.Error(t, NormalizePoolWorkflowConfig(scenario.auto, scenario.recovery, scenario.admin))
		})
	}
	require.NoError(t, NormalizePoolWorkflowConfig([]string{"email", "phone_number"}, &AccountRecoverySetting{[]RecoveryMechanism{{"verified_phone_number", 2}, {"verified_email", 1}}}, &AdminCreateUserConfig{AllowAdminCreateUserOnly: true}))
	require.NoError(t, NormalizePoolWorkflowConfig(nil, &AccountRecoverySetting{[]RecoveryMechanism{{"admin_only", 1}}}, nil))
}
