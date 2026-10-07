package secrets

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func stringValue(value string) SecretValue { return SecretValue{String: &value} }
func requireAPICode(t *testing.T, err error, code string) {
	t.Helper()
	var failure *APIError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, code, failure.Code)
}

func TestStageSelectorsAndImmutableVersions(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	initialTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return initialTime }
	original, err := store.Create(CreateInput{Name: "versions", ClientRequestToken: strings.Repeat("1", 32), Value: stringValue("initial")})
	require.NoError(t, err)
	_, err = store.GetValue("versions", "", "AWSPREVIOUS")
	requireAPICode(t, err, "ResourceNotFoundException")

	nextTime := initialTime.Add(time.Hour)
	store.now = func() time.Time { return nextTime }
	pending, err := store.PutValue(PutInput{SecretID: original.ARN, ClientRequestToken: strings.Repeat("2", 32), Value: stringValue("candidate"), VersionStages: []string{"AWSPENDING", "CUSTOM"}})
	require.NoError(t, err)
	require.Equal(t, []string{"AWSPENDING", "CUSTOM"}, pending.VersionStages)
	current, err := store.GetValue("versions", "", "")
	require.NoError(t, err)
	require.Equal(t, original.VersionID, current.VersionID)
	require.Equal(t, initialTime, current.CreatedDate)
	selected, err := store.GetValue("versions", pending.VersionID, "CUSTOM")
	require.NoError(t, err)
	require.Equal(t, nextTime, selected.CreatedDate)
	_, err = store.GetValue("versions", original.VersionID, "AWSPENDING")
	requireAPICode(t, err, "InvalidParameterException")
	_, err = store.GetValue("versions", strings.Repeat("9", 32), "")
	requireAPICode(t, err, "ResourceNotFoundException")

	replay, err := store.PutValue(PutInput{SecretID: "versions", ClientRequestToken: pending.VersionID, Value: stringValue("candidate"), VersionStages: []string{"AWSCURRENT"}})
	require.NoError(t, err)
	require.Equal(t, pending.VersionStages, replay.VersionStages, "idempotent value replay must not mutate labels")
	_, err = store.PutValue(PutInput{SecretID: "versions", ClientRequestToken: pending.VersionID, Value: stringValue("conflict")})
	requireAPICode(t, err, "ResourceExistsException")
	_, err = store.UpdateVersionStage(StageInput{SecretID: "versions", VersionStage: "AWSCURRENT", MoveToVersionID: pending.VersionID})
	requireAPICode(t, err, "InvalidParameterException")
	_, err = store.UpdateVersionStage(StageInput{SecretID: "versions", VersionStage: "AWSCURRENT", MoveToVersionID: pending.VersionID, RemoveFromVersionID: original.VersionID})
	require.NoError(t, err)
	previous, err := store.GetValue("versions", "", "AWSPREVIOUS")
	require.NoError(t, err)
	require.Equal(t, original.VersionID, previous.VersionID)
	require.Equal(t, "initial", previous.SecretString)
	_, err = store.UpdateVersionStage(StageInput{SecretID: "versions", VersionStage: "CUSTOM", RemoveFromVersionID: pending.VersionID})
	require.NoError(t, err)
	_, err = store.GetValue("versions", "", "CUSTOM")
	requireAPICode(t, err, "ResourceNotFoundException")
}

func TestMetadataOnlySecretAndOwnedSnapshots(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	tags := map[string]string{"owner": "original"}
	metadata, err := store.Create(CreateInput{Name: "metadata", Tags: tags})
	require.NoError(t, err)
	require.Empty(t, metadata.VersionID)
	require.Empty(t, metadata.VersionIdsToStages)
	require.False(t, metadata.HasValue)
	tags["owner"] = "input-mutated"
	metadata.Tags["owner"] = "snapshot-mutated"
	_, err = store.GetValue("metadata", "", "")
	requireAPICode(t, err, "ResourceNotFoundException")
	description := "metadata-only change"
	updated, err := store.Update(UpdateInput{SecretID: "metadata", Description: &description})
	require.NoError(t, err)
	require.Empty(t, updated.VersionID)
	require.Empty(t, updated.VersionIdsToStages)
	require.Equal(t, "original", updated.Tags["owner"])

	bytes := []byte{0, 1, 255, 10}
	stored, err := store.PutValue(PutInput{SecretID: "metadata", Value: SecretValue{Binary: bytes}, VersionStages: []string{"CUSTOM"}})
	require.NoError(t, err)
	require.Equal(t, []string{"AWSCURRENT", "CUSTOM"}, stored.VersionStages, "first stored value is always current")
	bytes[0] = 99
	stored.SecretBinary[1] = 98
	stored.VersionIdsToStages[stored.VersionID][0] = "FORGED"
	stored.VersionStages[0] = "FORGED"
	got, err := store.GetValue("metadata", "", "AWSCURRENT")
	require.NoError(t, err)
	require.Equal(t, []byte{0, 1, 255, 10}, got.SecretBinary)
	require.Equal(t, []string{"AWSCURRENT", "CUSTOM"}, got.VersionStages)
	described, err := store.Describe("metadata")
	require.NoError(t, err)
	require.False(t, described.HasValue)
	require.Empty(t, described.SecretBinary)
	require.Empty(t, described.SecretString)
	require.Equal(t, "metadata-only change", described.Description)
}

func TestLabelQuotaFailuresAreAtomic(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	first, err := store.Create(CreateInput{Name: "quota", Value: stringValue("initial")})
	require.NoError(t, err)
	stages := make([]string, 19)
	for index := range stages {
		stages[index] = fmt.Sprintf("CUSTOM-%d", index)
	}
	pending, err := store.PutValue(PutInput{SecretID: "quota", Value: stringValue("candidate"), VersionStages: stages})
	require.NoError(t, err)
	before, err := store.Describe("quota")
	require.NoError(t, err)
	_, err = store.UpdateVersionStage(StageInput{SecretID: "quota", VersionStage: "AWSCURRENT", MoveToVersionID: pending.VersionID, RemoveFromVersionID: first.VersionID})
	requireAPICode(t, err, "LimitExceededException")
	after, err := store.Describe("quota")
	require.NoError(t, err)
	require.Equal(t, before.VersionIdsToStages, after.VersionIdsToStages)
	_, _, err = store.beginRotation(RotateInput{SecretID: "quota"}, "arn:aws:lambda:us-east-1:000000000000:function:rotate", strings.Repeat("3", 32))
	requireAPICode(t, err, "LimitExceededException")
	after, err = store.Describe("quota")
	require.NoError(t, err)
	require.Nil(t, after.RotationEnabled, "failed admission must not enable rotation")
	require.Equal(t, before.VersionIdsToStages, after.VersionIdsToStages)
}

func TestConcurrentStoreSnapshotsAndVersionMoves(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	_, err := store.Create(CreateInput{Name: "concurrent", Value: stringValue("initial"), Tags: map[string]string{"owner": "fixed"}})
	require.NoError(t, err)
	var workers sync.WaitGroup
	failures := make(chan error, 16)
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for iteration := 0; iteration < 15; iteration++ {
				if worker%2 == 0 {
					id := fmt.Sprintf("%032d", worker*100+iteration)
					_, err := store.PutValue(PutInput{SecretID: "concurrent", ClientRequestToken: id, Value: stringValue(id)})
					if err != nil {
						failures <- err
						return
					}
				} else {
					result, err := store.GetValue("concurrent", "", "")
					if err != nil {
						failures <- err
						return
					}
					if !result.HasValue || len(result.VersionIdsToStages) == 0 {
						failures <- fmt.Errorf("incomplete snapshot")
						return
					}
					result.Tags["owner"] = "mutated"
					for id := range result.VersionIdsToStages {
						result.VersionIdsToStages[id] = []string{"FORGED"}
					}
				}
			}
		}(worker)
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	final, err := store.Describe("concurrent")
	require.NoError(t, err)
	require.Equal(t, "fixed", final.Tags["owner"])
	require.Len(t, final.VersionIdsToStages, 2)
}

func TestPutCurrentPreviousLabelsHaveOrderIndependentSemantics(t *testing.T) {
	for _, stages := range [][]string{{"AWSCURRENT", "AWSPREVIOUS"}, {"AWSPREVIOUS", "AWSCURRENT"}} {
		store := NewSecretsStore("us-east-1", "000000000000")
		original, err := store.Create(CreateInput{Name: "native-labels", Value: stringValue("original")})
		require.NoError(t, err)
		next, err := store.PutValue(PutInput{SecretID: "native-labels", Value: stringValue("next"), VersionStages: stages})
		require.NoError(t, err)
		require.Equal(t, []string{"AWSCURRENT"}, next.VersionStages)
		previous, err := store.GetValue("native-labels", "", "AWSPREVIOUS")
		require.NoError(t, err)
		require.Equal(t, original.VersionID, previous.VersionID)
	}
}
