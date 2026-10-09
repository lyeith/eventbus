package ses

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type SESTemplate struct {
	SubjectPart string `json:"SubjectPart" yaml:"SubjectPart"`
	TextPart    string `json:"TextPart" yaml:"TextPart"`
	HtmlPart    string `json:"HtmlPart" yaml:"HtmlPart"`
}
type SESCustomVerificationTemplate struct {
	FromEmailAddress      string `json:"FromEmailAddress" yaml:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject" yaml:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent" yaml:"TemplateContent"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL" yaml:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL" yaml:"FailureRedirectionURL"`
}
type SESReceivedMessage struct {
	From       string    `json:"from" yaml:"from"`
	ReceivedAt time.Time `json:"received_at" yaml:"received_at"`
	Recipients []string  `json:"recipients" yaml:"recipients"`
}

// Fixtures supply sending prerequisites. Configuration-set names seed the native
// mutable registry; fixtures perform no DNS checks or external email delivery.
type SESFixtures struct {
	Templates                   map[string]SESTemplate                   `json:"templates" yaml:"templates"`
	CustomVerificationTemplates map[string]SESCustomVerificationTemplate `json:"custom_verification_templates" yaml:"custom_verification_templates"`
	ConfigurationSets           []string                                 `json:"configuration_sets" yaml:"configuration_sets"`
	VerifiedIdentities          []string                                 `json:"verified_identities" yaml:"verified_identities"`
	RequireVerifiedIdentities   bool                                     `json:"require_verified_identities" yaml:"require_verified_identities"`
	SendingEnabled              *bool                                    `json:"sending_enabled" yaml:"sending_enabled"`
	ReceivedMessages            map[string]SESReceivedMessage            `json:"received_messages" yaml:"received_messages"`
}

func LoadSESFixtures(path string) (SESFixtures, error) {
	var fixtures SESFixtures
	file, err := os.Open(path)
	if err != nil {
		return fixtures, err
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&fixtures); err != nil {
		return fixtures, fmt.Errorf("load SES fixtures: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fixtures, errors.New("SES fixtures must contain exactly one document")
	}
	for id, message := range fixtures.ReceivedMessages {
		if id == "" || message.ReceivedAt.IsZero() || message.From == "" {
			return fixtures, errors.New("SES received-message fixtures require id, from, and received_at")
		}
	}
	return fixtures, nil
}
