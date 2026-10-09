package app

import (
	"context"
	"path/filepath"

	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/lyeith/eventbus/internal/devactivity"
)

// Trigger recipe paths and application handler references use --work-dir.
func loadCognitoTriggers(path, workDir string) (*cognitotrigger.Runner, error) {
	return loadCognitoTriggersWithActivity(path, workDir, nil)
}
func loadCognitoTriggersWithActivity(path, workDir string, activity devactivity.Activity) (*cognitotrigger.Runner, error) {
	if path == "" {
		return nil, nil
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(workDir, path)
	}
	config, err := cognitotrigger.Load(path)
	if err != nil {
		return nil, err
	}
	config.DevActivity = activity
	return NewCognitoTriggers(config, workDir)
}

// NewCognitoTriggers composes registered handlers with the private managed
// runtime. Embedded hosts and SDK suites use this same application boundary.
func NewCognitoTriggers(config *cognitotrigger.Config, workDir string) (*cognitotrigger.Runner, error) {
	execution, err := newCognitoTriggerExecution(config, workDir)
	if err != nil {
		return nil, err
	}
	runner, err := cognitotrigger.New(config, execution)
	if err != nil {
		_ = execution.Close(context.Background())
		return nil, err
	}
	return runner, nil
}
