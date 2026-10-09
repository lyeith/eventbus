package ses

import (
	"container/list"
	"maps"
	"net/http"
	"net/mail"
	"slices"
	"strings"
	"sync"
)

type SESManager struct {
	fixtures SESFixtures
	capture  *SESCapture

	mu                sync.Mutex
	configurationSets map[string]*configurationSet
	eventPublisher    EventPublisher
	region, accountID string
	accepted          map[string]*acceptedMessage
	acceptedOrder     *list.List
	acceptedBytes     int
}

func NewSESManager(fixtures SESFixtures, capture *SESCapture, options ...ManagerOption) *SESManager {
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
	manager := &SESManager{fixtures: owned, capture: capture, configurationSets: make(map[string]*configurationSet),
		region: "us-east-1", accountID: "000000000000", accepted: make(map[string]*acceptedMessage), acceptedOrder: list.New()}
	for _, name := range owned.ConfigurationSets {
		manager.configurationSets[name] = &configurationSet{name: name, destinations: make(map[string]*eventDestination)}
	}
	for _, option := range options {
		option(manager)
	}
	return manager
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
	manager.mu.Lock()
	_, exists := manager.configurationSets[name]
	manager.mu.Unlock()
	if exists {
		return nil
	}
	if api == "v1" {
		return missingConfigurationSet(name)
	}
	return &sesAPIError{Code: "NotFoundException", Message: "Configuration set does not exist: " + name, Status: http.StatusNotFound}
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
