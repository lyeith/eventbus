// Development harness adapter: local YAML recipes.
// This file does not implement AWS resource-management APIs. Native request,
// event, response and resource semantics remain in the service core.
package lambda

import (
	"errors"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

// LoadConfig rejects unknown fields and extra YAML documents.
func LoadConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("read Lambda configuration: %w", err)
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("parse Lambda configuration: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, errors.New("Lambda configuration must contain exactly one YAML document")
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}
