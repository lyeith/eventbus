package secrets

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/stretchr/testify/require"
)

func TestCanonicalARNAndNameShareNativeVersionState(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	created, err := store.CreateSecret("owned/alias", "original", strings.Repeat("1", 32), nil)
	require.NoError(t, err)
	replayed, err := store.CreateSecret(created.Name, "original", created.VersionID, nil)
	require.NoError(t, err)
	require.Equal(t, created.ARN, replayed.ARN)

	pendingToken := strings.Repeat("2", 32)
	_, err = store.PutValue(PutInput{SecretID: created.ARN, ClientRequestToken: pendingToken, Value: stringValue("pending"), VersionStages: []string{"AWSPENDING"}})
	require.NoError(t, err)
	pending, err := store.GetValue(created.Name, "", "AWSPENDING")
	require.NoError(t, err)
	require.Equal(t, pendingToken, pending.VersionID)
	require.Equal(t, "pending", pending.SecretString)
	_, err = store.UpdateVersionStage(StageInput{SecretID: created.Name, VersionStage: "AWSCURRENT", MoveToVersionID: pendingToken, RemoveFromVersionID: created.VersionID})
	require.NoError(t, err)
	current, err := store.GetValue(created.ARN, "", "")
	require.NoError(t, err)
	require.Equal(t, pendingToken, current.VersionID)
	previous, err := store.GetValue(created.ARN, "", "AWSPREVIOUS")
	require.NoError(t, err)
	require.Equal(t, "original", previous.SecretString)

	description := "updated by name"
	_, err = store.Update(UpdateInput{SecretID: created.Name, Description: &description})
	require.NoError(t, err)
	byARN, err := store.Describe(created.ARN)
	require.NoError(t, err)
	require.Equal(t, description, byARN.Description)
	store.mu.RLock()
	nameState, arnState := store.findLocked(created.Name), store.findLocked(created.ARN)
	store.mu.RUnlock()
	require.Same(t, nameState, arnState, "both identities must own one version/rotation state")
}

func TestCanonicalARNDeleteReuseAndForeignIdentities(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	first, err := store.CreateSecret("reuse", "first", "", nil)
	require.NoError(t, err)
	for _, id := range []string{
		first.ARN[:len(first.ARN)-7],
		first.ARN + "-other",
		strings.Replace(first.ARN, ":us-east-1:", ":us-west-2:", 1),
		strings.Replace(first.ARN, ":000000000000:", ":123456789012:", 1),
	} {
		_, err := store.GetValue(id, "", "")
		requireAPICode(t, err, "ResourceNotFoundException")
		requireAPICode(t, store.DeleteSecret(id, true), "ResourceNotFoundException")
	}
	require.NoError(t, store.DeleteSecret(first.ARN, true))
	_, err = store.Describe(first.Name)
	requireAPICode(t, err, "ResourceNotFoundException")
	second, err := store.CreateSecret(first.Name, "second", "", nil)
	require.NoError(t, err)
	require.NotEqual(t, first.ARN, second.ARN)
	_, err = store.GetValue(first.ARN, "", "")
	requireAPICode(t, err, "ResourceNotFoundException")
	_, err = store.PutValue(PutInput{SecretID: first.ARN, Value: stringValue("stale")})
	requireAPICode(t, err, "ResourceNotFoundException")
	requireAPICode(t, store.finishRotation(first.ARN, first.VersionID), "ResourceNotFoundException")
	store.removeTestPending(first.ARN, first.VersionID)
	_, err = store.cancelRotation(first.ARN)
	requireAPICode(t, err, "ResourceNotFoundException")
	requireAPICode(t, store.DeleteSecret(first.ARN, true), "ResourceNotFoundException")
	got, err := store.GetValue(second.Name, "", "")
	require.NoError(t, err)
	require.Equal(t, second.ARN, got.ARN)
	require.Equal(t, "second", got.SecretString)
	require.NoError(t, store.DeleteSecret(second.Name, true))
	_, err = store.Describe(second.ARN)
	requireAPICode(t, err, "ResourceNotFoundException")
}

func TestCanonicalARNConcurrentCreateDeleteNeverResolvesNewNameOwner(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	const workers, generations = 4, 20
	failures := make(chan error, workers*generations*2)
	var joined sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		joined.Add(1)
		go func(worker int) {
			defer joined.Done()
			name := fmt.Sprintf("concurrent/w%d", worker)
			stale := ""
			for generation := 0; generation < generations; generation++ {
				value := fmt.Sprintf("generation-%d", generation)
				created, err := store.CreateSecret(name, value, "", nil)
				if err != nil {
					failures <- err
					return
				}
				if stale != "" {
					old, err := store.GetValue(stale, "", "")
					if api, ok := err.(*APIError); !ok || api.Code != "ResourceNotFoundException" || old != nil {
						failures <- fmt.Errorf("stale ARN resolved replacement: %s value=%v error=%v", stale, old, err)
					}
				}
				got, err := store.GetValue(created.ARN, "", "")
				if err != nil || got.ARN != created.ARN || got.SecretString != value {
					failures <- fmt.Errorf("fresh ARN lookup failed: value=%v error=%v", got, err)
				}
				if err := store.DeleteSecret(name, true); err != nil {
					failures <- err
					return
				}
				stale = created.ARN
			}
		}(worker)
	}
	joined.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestCanonicalARNLookupDuringNativeSDKRotation(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	outcomes := make(chan RotationOutcome, 1)
	entered, gate := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	var client *secretsmanager.Client
	invoker := rotationInvoker{invoke: func(ctx context.Context, _ string, event RotationEvent) error {
		if event.Step == "testSecret" {
			close(entered)
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return rotateThroughSDK(ctx, client, event)
	}}
	service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	serving := httptest.NewServer(NewHandler(store, service))
	defer serving.Close()
	defer closeRotation(t, service)
	defer release()
	client = secretsSDK(serving.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	created, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String("concurrent-sdk-rotation"), SecretString: aws.String("original")})
	require.NoError(t, err)
	token := strings.Repeat("2", 32)
	_, err = client.RotateSecret(ctx, &secretsmanager.RotateSecretInput{SecretId: created.ARN, RotationLambdaARN: aws.String(rotationARN), ClientRequestToken: aws.String(token)})
	require.NoError(t, err)
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("rotation did not enter its owned gated step")
	}
	failures := make(chan error, 80)
	var joined sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		joined.Add(1)
		go func(worker int) {
			defer joined.Done()
			id := created.ARN
			if worker%2 == 0 {
				id = created.Name
			}
			for operation := 0; operation < 10; operation++ {
				current, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: id})
				if err != nil || aws.ToString(current.SecretString) != "original" || aws.ToString(current.ARN) != aws.ToString(created.ARN) {
					failures <- fmt.Errorf("current snapshot changed during rotation: id=%s result=%v error=%v", aws.ToString(id), current, err)
				}
				pending, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: id, VersionStage: aws.String("AWSPENDING")})
				if err != nil || aws.ToString(pending.VersionId) != token || aws.ToString(pending.ARN) != aws.ToString(created.ARN) {
					failures <- fmt.Errorf("pending snapshot changed during rotation: id=%s result=%v error=%v", aws.ToString(id), pending, err)
				}
			}
		}(worker)
	}
	joined.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	release()
	require.Equal(t, "succeeded", waitOutcome(t, outcomes).Status)
	for _, id := range []*string{created.Name, created.ARN} {
		current, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: id})
		require.NoError(t, err)
		require.Equal(t, token, aws.ToString(current.VersionId))
	}
	_, err = client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{SecretId: created.ARN, ForceDeleteWithoutRecovery: aws.Bool(true)})
	require.NoError(t, err)
	replacement, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: created.Name, SecretString: aws.String("replacement")})
	require.NoError(t, err)
	require.NotEqual(t, aws.ToString(created.ARN), aws.ToString(replacement.ARN))
	_, err = client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: created.ARN})
	var absent *smtypes.ResourceNotFoundException
	require.ErrorAs(t, err, &absent)
	current, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: replacement.ARN})
	require.NoError(t, err)
	require.Equal(t, "replacement", aws.ToString(current.SecretString))
}

func TestRotationBindsResolvedARNThroughExternalValidation(t *testing.T) {
	store := NewSecretsStore("us-east-1", "000000000000")
	validating, gate := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	var validations, invocations atomic.Int32
	var client *secretsmanager.Client
	outcomes := make(chan RotationOutcome, 1)
	invoker := rotationInvoker{
		validate: func(ctx context.Context, function string) error {
			if function != rotationARN {
				return fmt.Errorf("unexpected rotation function %s", function)
			}
			if validations.Add(1) == 1 {
				close(validating)
				select {
				case <-gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
		invoke: func(ctx context.Context, _ string, event RotationEvent) error {
			invocations.Add(1)
			return rotateThroughSDK(ctx, client, event)
		},
	}
	service := NewRotationService(store, invoker, RotationOptions{MaxAttempts: 1, MaxConcurrent: 1, MaxPending: 1,
		Observer: func(outcome RotationOutcome) { outcomes <- outcome }})
	serving := httptest.NewServer(NewHandler(store, service))
	defer serving.Close()
	defer closeRotation(t, service)
	client = secretsSDK(serving.URL)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	original, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String("generation-bound-rotation"), SecretString: aws.String("original")})
	require.NoError(t, err)
	token := strings.Repeat("3", 32)
	rotated, joined := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(joined)
		_, err := client.RotateSecret(ctx, &secretsmanager.RotateSecretInput{SecretId: original.Name, RotationLambdaARN: aws.String(rotationARN), ClientRequestToken: aws.String(token)})
		rotated <- err
	}()
	defer func() {
		release()
		cancel()
		<-joined
	}()
	select {
	case <-validating:
	case <-ctx.Done():
		t.Fatal("rotation did not reach its external validation seam")
	}
	_, err = client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{SecretId: original.ARN, ForceDeleteWithoutRecovery: aws.Bool(true)})
	require.NoError(t, err)
	replacement, err := client.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: original.Name, SecretString: aws.String("replacement")})
	require.NoError(t, err)
	require.NotEqual(t, aws.ToString(original.ARN), aws.ToString(replacement.ARN))
	release()
	var absent *smtypes.ResourceNotFoundException
	require.ErrorAs(t, <-rotated, &absent)
	<-joined
	require.Zero(t, invocations.Load(), "an old generation must not launch against its name replacement")
	metadata, err := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: replacement.ARN})
	require.NoError(t, err)
	require.Nil(t, metadata.RotationEnabled)
	require.Nil(t, metadata.RotationLambdaARN)
	require.Len(t, metadata.VersionIdsToStages, 1)
	require.Equal(t, []string{"AWSCURRENT"}, metadata.VersionIdsToStages[aws.ToString(replacement.VersionId)])
	current, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: original.Name})
	require.NoError(t, err)
	require.Equal(t, "replacement", aws.ToString(current.SecretString))
	require.Equal(t, aws.ToString(replacement.ARN), aws.ToString(current.ARN))

	// The refused transition must release admission capacity. The replacement
	// can independently rotate through the native SDK and its canonical ARN.
	_, err = client.RotateSecret(ctx, &secretsmanager.RotateSecretInput{SecretId: replacement.ARN, RotationLambdaARN: aws.String(rotationARN), ClientRequestToken: aws.String(token)})
	require.NoError(t, err)
	require.Equal(t, "succeeded", waitOutcome(t, outcomes).Status)
	require.Equal(t, int32(4), invocations.Load())
	current, err = client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: replacement.ARN})
	require.NoError(t, err)
	require.Equal(t, token, aws.ToString(current.VersionId))
}
