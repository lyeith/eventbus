// Package cognitotrigger executes explicitly configured application-owned
// Cognito custom authentication handlers. Cognito owns event construction,
// challenge state and operation-specific response validation.
package cognitotrigger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/lyeith/eventbus/internal/devactivity"

	"gopkg.in/yaml.v3"
)

const (
	DefineAuthChallenge         = "DefineAuthChallenge"
	CreateAuthChallenge         = "CreateAuthChallenge"
	VerifyAuthChallengeResponse = "VerifyAuthChallengeResponse"
	defaultTimeoutSeconds       = 5
)

type Config struct {
	Node  string          `yaml:"node"`
	Pools map[string]Pool `yaml:"pools"`
	// DevActivity is supplied by the application before startup, never YAML.
	DevActivity devactivity.Activity `yaml:"-"`
}

type Pool struct {
	DefineAuthChallenge         *Entry `yaml:"DefineAuthChallenge"`
	CreateAuthChallenge         *Entry `yaml:"CreateAuthChallenge"`
	VerifyAuthChallengeResponse *Entry `yaml:"VerifyAuthChallengeResponse"`
}

type Entry struct {
	Handler        string            `yaml:"handler"`
	TimeoutSeconds int               `yaml:"timeout_seconds"`
	Env            map[string]string `yaml:"env"`
}

func (p Pool) entries() map[string]*Entry {
	return map[string]*Entry{
		DefineAuthChallenge:         p.DefineAuthChallenge,
		CreateAuthChallenge:         p.CreateAuthChallenge,
		VerifyAuthChallengeResponse: p.VerifyAuthChallengeResponse,
	}
}

// Load accepts exactly one YAML document and rejects unknown configuration
// fields. Relative handler paths are resolved later against New's workDir.
func Load(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read Cognito trigger config: %w", err)
	}
	defer file.Close()
	var config Config
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("parse Cognito trigger config: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("Cognito trigger config must contain exactly one document")
	}
	if err := validateConfig(&config); err != nil {
		return nil, err
	}
	return &config, nil
}

var identifier = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*$`)
var envName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func handlerReference(reference string) (module, exported string, err error) {
	module, exported, found := strings.Cut(reference, "#")
	if !found {
		exported = "handler"
	}
	if strings.TrimSpace(module) == "" || strings.ContainsRune(module, 0) || !identifier.MatchString(exported) {
		return "", "", errors.New("handler must be a module path with an optional #export name")
	}
	return module, exported, nil
}

func validateConfig(config *Config) error {
	if config == nil || len(config.Pools) == 0 {
		return errors.New("Cognito trigger config requires at least one pool")
	}
	for poolID, pool := range config.Pools {
		if strings.TrimSpace(poolID) == "" {
			return errors.New("Cognito trigger pool ID must not be empty")
		}
		for name, entry := range pool.entries() {
			if entry == nil {
				return fmt.Errorf("Cognito trigger pool %q requires %s", poolID, name)
			}
			if _, _, err := handlerReference(entry.Handler); err != nil {
				return fmt.Errorf("Cognito trigger pool %q %s: %w", poolID, name, err)
			}
			if entry.TimeoutSeconds < 0 || entry.TimeoutSeconds > defaultTimeoutSeconds {
				return fmt.Errorf("Cognito trigger pool %q %s timeout_seconds must be between 1 and 5, or omitted", poolID, name)
			}
			for key, value := range entry.Env {
				if !envName.MatchString(key) || strings.ContainsRune(value, 0) {
					return fmt.Errorf("Cognito trigger pool %q %s has an invalid environment entry", poolID, name)
				}
			}
		}
	}
	return nil
}
