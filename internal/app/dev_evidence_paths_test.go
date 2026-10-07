package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	lambdaservice "github.com/lyeith/eventbus/internal/lambda"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDevDiagnosticsPathsStaySeparateFromEveryServiceCapture(t *testing.T) {
	directory := t.TempDir()
	private := filepath.Join(directory, "diagnostics.jsonl")
	require.NoError(t, os.WriteFile(private, nil, 0600))
	hardlink, symlink := filepath.Join(directory, "hardlink.jsonl"), filepath.Join(directory, "symlink.jsonl")
	require.NoError(t, os.Link(private, hardlink))
	require.NoError(t, os.Symlink(private, symlink))
	for index, name := range []string{"SQS delivery", "SNS", "SES", "Cognito"} {
		for _, alias := range []string{private, hardlink, symlink, filepath.Join(directory, "unused", "..", "diagnostics.jsonl")} {
			t.Run(name+"/"+filepath.Base(alias), func(t *testing.T) {
				paths := []string{"", "-", "-", "-"}
				paths[index] = alias
				err := validateDevEvidencePaths(private, paths[0], paths[1], paths[2], paths[3])
				require.ErrorContains(t, err, "separate private file from "+name+" capture")
			})
		}
	}
	require.NoError(t, validateDevEvidencePaths("", private, private, private, private))
	require.NoError(t, validateDevEvidencePaths(private, filepath.Join(directory, "delivery.jsonl"), "-", "-", "-"))
}

func TestRunRejectsPrivateDiagnosticsAliasingDeliveryCaptureBeforeListening(t *testing.T) {
	cfg := runTestConfig(t)
	cfg.port = retainedTestPort(t)
	cfg.sqsDeliveryLog = filepath.Join(cfg.workDir, "shared.jsonl")
	executable, err := os.Executable()
	require.NoError(t, err)
	recipe, err := yaml.Marshal(lambdaservice.Config{
		Functions:      map[string]lambdaservice.Function{"configured": {Runtime: "provided", Command: []string{executable, "-test.run=^TestMessagingCompositionProvidedProcess$"}}},
		DevDiagnostics: &lambdaservice.DevDiagnosticsConfig{LogPath: cfg.sqsDeliveryLog},
	})
	require.NoError(t, err)
	cfg.lambdaFunctions = filepath.Join(cfg.workDir, "functions.yaml")
	require.NoError(t, os.WriteFile(cfg.lambdaFunctions, recipe, 0600))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = run(ctx, cfg)
	require.ErrorContains(t, err, "separate private file from SQS delivery capture")
}

func TestRunDeliveryEvidenceRequiresFunctionsBeforeOpeningFilesInBothProfiles(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "retained"}[retained], func(t *testing.T) {
			cfg := runTestConfig(t)
			if retained {
				cfg.port = retainedTestPort(t)
				for cfg.retainedCallbackPort == 0 || cfg.retainedCallbackPort == cfg.port {
					cfg.retainedCallbackPort = retainedTestPort(t)
				}
			}
			cfg.sqsDeliveryLog = filepath.Join(cfg.workDir, "deliveries.jsonl")
			err := run(t.Context(), cfg)
			require.ErrorContains(t, err, "sqs-delivery-log requires lambda-functions")
			_, err = os.Stat(cfg.sqsDeliveryLog)
			require.ErrorIs(t, err, os.ErrNotExist)
			_, err = os.Stat(cfg.cognitoDB)
			require.ErrorIs(t, err, os.ErrNotExist, "invalid opt-in must fail before opening stores")
		})
	}
}

func TestRunDefaultEvidenceWithoutFunctionsInBothProfiles(t *testing.T) {
	for _, retained := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "retained"}[retained], func(t *testing.T) {
			cfg := runTestConfig(t)
			if retained {
				cfg.port = retainedTestPort(t)
				for cfg.retainedCallbackPort == 0 || cfg.retainedCallbackPort == cfg.port {
					cfg.retainedCallbackPort = retainedTestPort(t)
				}
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.NoError(t, run(ctx, cfg), "absent optional runtimes must not become typed-nil evidence failures")
		})
	}
}
