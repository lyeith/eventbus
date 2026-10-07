package app

import (
	"path/filepath"

	"github.com/lyeith/eventbus/internal/cognitotrigger"
	"github.com/lyeith/eventbus/internal/devactivity"
)

// Trigger configuration is application-owned. Both its relative path and its
// handler references use the application root selected by --work-dir.
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
	return cognitotrigger.New(config, workDir)
}
