package ses

import (
	"maps"
	"net/http"
	"net/mail"
	"slices"
	"strings"
)

type SESManager struct {
	fixtures SESFixtures
	capture  *SESCapture
}

func NewSESManager(fixtures SESFixtures, capture *SESCapture) *SESManager {
	// Fixtures contain no mutable runtime state; make the caller's maps/pointers
	// independent before concurrent HTTP readers are admitted.
	owned := fixtures
	owned.Templates = maps.Clone(fixtures.Templates)
	owned.CustomVerificationTemplates = maps.Clone(fixtures.CustomVerificationTemplates)
	owned.ConfigurationSets = slices.Clone(fixtures.ConfigurationSets)
	owned.VerifiedIdentities = slices.Clone(fixtures.VerifiedIdentities)
	owned.ReceivedMessages = maps.Clone(fixtures.ReceivedMessages)
	for id, message := range owned.ReceivedMessages {
		message.Recipients = slices.Clone(message.Recipients)
		owned.ReceivedMessages[id] = message
	}
	if fixtures.SendingEnabled != nil {
		enabled := *fixtures.SendingEnabled
		owned.SendingEnabled = &enabled
	}
	return &SESManager{fixtures: owned, capture: capture}
}
func (manager *SESManager) Close() error {
	if manager == nil {
		return nil
	}
	return manager.capture.Close()
}
func (manager *SESManager) checkSender(api, sender string) *sesAPIError {
	if err := sesValidateAddress(api, "Sender", sender); err != nil {
		return err
	}
	if !manager.fixtures.RequireVerifiedIdentities {
		return nil
	}
	parsed, _ := mail.ParseAddress(sender)
	domain := parsed.Address[strings.LastIndex(parsed.Address, "@")+1:]
	for _, identity := range manager.fixtures.VerifiedIdentities {
		if strings.EqualFold(identity, parsed.Address) || strings.EqualFold(identity, domain) {
			return nil
		}
	}
	return &sesAPIError{Code: "MessageRejected", Message: "Email address is not verified", Status: http.StatusBadRequest}
}
func (manager *SESManager) checkConfigurationSet(api, name string) *sesAPIError {
	if name == "" {
		return nil
	}
	for _, configured := range manager.fixtures.ConfigurationSets {
		if configured == name {
			return nil
		}
	}
	code, status := "NotFoundException", http.StatusNotFound
	if api == "v1" {
		code, status = "ConfigurationSetDoesNotExist", http.StatusBadRequest
	}
	return &sesAPIError{Code: code, Message: "Configuration set does not exist: " + name, Status: status}
}
func (manager *SESManager) template(name string) (SESTemplate, bool) {
	if strings.HasPrefix(name, "arn:") {
		if index := strings.LastIndex(name, "/"); index >= 0 {
			name = name[index+1:]
		}
	}
	template, ok := manager.fixtures.Templates[name]
	return template, ok
}
func (manager *SESManager) receivedMessage(id string) (SESReceivedMessage, bool) {
	value, ok := manager.fixtures.ReceivedMessages[id]
	return value, ok
}
