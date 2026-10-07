package cognito

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// NotificationSink is the delivery boundary for Cognito-managed messages.
// The core accepts the sink's delivery result without prescribing files,
// SMTP, SES, or a process runner. The development harness supplies local capture.
type NotificationSink interface {
	Deliver(context.Context, Notification) error
}

type Notification struct {
	Operation         string    `json:"operation"`
	PoolID            string    `json:"pool_id"`
	ClientID          string    `json:"client_id,omitempty"`
	Username          string    `json:"username"`
	UserSub           string    `json:"user_sub"`
	Destination       string    `json:"destination"`
	DeliveryMedium    string    `json:"delivery_medium"`
	AttributeName     string    `json:"attribute_name"`
	Purpose           string    `json:"purpose"`
	Code              string    `json:"code,omitempty"`
	TemporaryPassword string    `json:"temporary_password,omitempty"`
	Timestamp         time.Time `json:"timestamp"`
}

type AccountRecoverySetting struct {
	RecoveryMechanisms []RecoveryMechanism `json:"RecoveryMechanisms"`
}
type RecoveryMechanism struct {
	Name     string `json:"Name"`
	Priority int    `json:"Priority"`
}
type AdminCreateUserConfig struct {
	AllowAdminCreateUserOnly  bool                   `json:"AllowAdminCreateUserOnly"`
	InviteMessageTemplate     *InviteMessageTemplate `json:"InviteMessageTemplate,omitempty"`
	UnusedAccountValidityDays *int                   `json:"UnusedAccountValidityDays,omitempty"`
}
type InviteMessageTemplate struct {
	EmailMessage string `json:"EmailMessage,omitempty"`
	EmailSubject string `json:"EmailSubject,omitempty"`
	SMSMessage   string `json:"SMSMessage,omitempty"`
}

// NormalizePoolWorkflowConfig rejects unsupported selections before mutation.
// Verification and recovery support email/SMS codes captured by the sink.
func NormalizePoolWorkflowConfig(auto []string, recovery *AccountRecoverySetting, admin *AdminCreateUserConfig) error {
	seen := map[string]bool{}
	for _, attribute := range auto {
		if attribute != "email" && attribute != "phone_number" {
			return fmt.Errorf("unsupported AutoVerifiedAttributes value %q", attribute)
		}
		if seen[attribute] {
			return fmt.Errorf("duplicate AutoVerifiedAttributes value %q", attribute)
		}
		seen[attribute] = true
	}
	if recovery != nil {
		if len(recovery.RecoveryMechanisms) < 1 || len(recovery.RecoveryMechanisms) > 2 {
			return errors.New("AccountRecoverySetting must contain one or two recovery mechanisms")
		}
		names, priorities := map[string]bool{}, map[int]bool{}
		for _, mechanism := range recovery.RecoveryMechanisms {
			if mechanism.Name != "verified_email" && mechanism.Name != "verified_phone_number" && mechanism.Name != "admin_only" {
				return fmt.Errorf("unsupported account recovery mechanism %q", mechanism.Name)
			}
			if mechanism.Priority < 1 || mechanism.Priority > 2 || priorities[mechanism.Priority] || names[mechanism.Name] {
				return errors.New("recovery names and priorities must be unique, with priorities between 1 and 2")
			}
			if mechanism.Name == "admin_only" && (len(recovery.RecoveryMechanisms) != 1 || mechanism.Priority != 1) {
				return errors.New("admin_only must be the sole recovery mechanism at priority 1")
			}
			names[mechanism.Name], priorities[mechanism.Priority] = true, true
		}
	}
	if admin != nil {
		if template := admin.InviteMessageTemplate; template != nil && (template.EmailMessage != "" || template.EmailSubject != "" || template.SMSMessage != "") {
			return errors.New("custom InviteMessageTemplate is not supported")
		}
		if admin.UnusedAccountValidityDays != nil && *admin.UnusedAccountValidityDays != 7 {
			return errors.New("UnusedAccountValidityDays is deprecated; use PasswordPolicy.TemporaryPasswordValidityDays")
		}
	}
	return nil
}

type codeDeliveryDetails struct {
	Destination    string `json:"Destination"`
	DeliveryMedium string `json:"DeliveryMedium"`
	AttributeName  string `json:"AttributeName"`
}

func deliveryDetails(attribute, destination string) codeDeliveryDetails {
	medium, masked := "EMAIL", "***"
	if attribute == "phone_number" {
		medium = "SMS"
		if runes := []rune(destination); len(runes) > 4 {
			masked += string(runes[len(runes)-4:])
		}
	} else if local, domain, ok := strings.Cut(destination, "@"); ok {
		if runes := []rune(local); len(runes) != 0 {
			masked = string(runes[0]) + "***"
		}
		masked += "@"
		if runes := []rune(domain); len(runes) != 0 {
			masked += string(runes[0]) + "***"
		} else {
			masked += "***"
		}
	}
	return codeDeliveryDetails{masked, medium, attribute}
}

func validateInvitationMediums(desiredMediums []string) error {
	seen := map[string]bool{}
	for _, medium := range desiredMediums {
		if medium != "EMAIL" && medium != "SMS" {
			return workflowCodeError("InvalidParameterException", "DesiredDeliveryMediums supports only EMAIL or SMS")
		}
		if seen[medium] {
			return workflowCodeError("InvalidParameterException", "DesiredDeliveryMediums must not contain duplicates")
		}
		seen[medium] = true
	}
	return nil
}

func validateInvitationDelivery(attributes map[string]string, desiredMediums []string) error {
	if err := validateInvitationMediums(desiredMediums); err != nil {
		return err
	}
	if len(desiredMediums) == 0 {
		desiredMediums = []string{"SMS"}
	}
	for _, medium := range desiredMediums {
		attribute := "email"
		if medium == "SMS" {
			attribute = "phone_number"
		}
		if attributes[attribute] == "" {
			return workflowCodeError("InvalidParameterException", "Missing "+attribute+" for invitation delivery")
		}
	}
	return nil
}

func (s *Handler) deliverInvitation(ctx context.Context, clientID string, user *CognitoUser, temporaryPassword string, desiredMediums []string) error {
	if len(desiredMediums) == 0 {
		desiredMediums = []string{"SMS"}
	}
	attributes, err := s.cognito.LoadUserAttributes(ctx, user.Sub)
	if err != nil {
		return err
	}
	if err = validateInvitationDelivery(attributes, desiredMediums); err != nil {
		return err
	}
	for _, medium := range desiredMediums {
		attribute := "email"
		if medium == "SMS" {
			attribute = "phone_number"
		}
		notification := Notification{Operation: "AdminCreateUser", PoolID: user.PoolID, ClientID: clientID, Username: user.Username, UserSub: user.Sub, Destination: attributes[attribute], DeliveryMedium: medium, AttributeName: attribute, Purpose: "invitation", TemporaryPassword: temporaryPassword, Timestamp: s.cognito.now().UTC()}
		if s.notifications == nil || s.notifications.Deliver(ctx, notification) != nil {
			return &workflowError{"CodeDeliveryFailureException", "Failed to deliver invitation message"}
		}
	}
	return nil
}
