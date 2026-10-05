package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

type SecretsStore struct {
	secrets map[string]*Secret // name → secret
	mu      sync.RWMutex
}

type Secret struct {
	ARN          string
	Name         string
	SecretString string
	VersionID    string
	CreatedDate  time.Time
	Tags         map[string]string
	Versions     map[string]string
}

func NewSecretsStore(region, accountID string) *SecretsStore {
	return &SecretsStore{
		secrets: make(map[string]*Secret),
	}
}

func (ss *SecretsStore) CreateSecret(
	name, secretString, versionID string,
	tags map[string]string,
	region, accountID string,
) (*Secret, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	if _, exists := ss.secrets[name]; exists {
		return nil, fmt.Errorf("ResourceExistsException")
	}

	versionID = requestedOrGeneratedVersionID(versionID)
	secret := &Secret{
		ARN:          fmt.Sprintf("arn:aws:secretsmanager:%s:%s:secret:%s-%s", region, accountID, name, uuid.New().String()[:6]),
		Name:         name,
		SecretString: secretString,
		VersionID:    versionID,
		CreatedDate:  time.Now(),
		Tags:         tags,
		Versions:     map[string]string{versionID: secretString},
	}
	ss.secrets[name] = secret
	return secret, nil
}

func (ss *SecretsStore) GetSecretValue(nameOrARN, versionID string) (*Secret, error) {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	secret := ss.findLocked(nameOrARN)
	if secret == nil {
		return nil, fmt.Errorf("ResourceNotFoundException")
	}
	if versionID == "" || versionID == secret.VersionID {
		return secret, nil
	}
	secretString, ok := secret.Versions[versionID]
	if !ok {
		return nil, fmt.Errorf("ResourceNotFoundException")
	}
	version := *secret
	version.SecretString = secretString
	version.VersionID = versionID
	return &version, nil
}

func (ss *SecretsStore) UpdateSecret(nameOrARN, secretString, versionID string) (*Secret, error) {
	return ss.PutSecretValue(nameOrARN, secretString, versionID)
}

func (ss *SecretsStore) PutSecretValue(nameOrARN, secretString, versionID string) (*Secret, error) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	secret := ss.findLocked(nameOrARN)
	if secret == nil {
		return nil, fmt.Errorf("ResourceNotFoundException")
	}

	versionID = requestedOrGeneratedVersionID(versionID)
	if existing, ok := secret.Versions[versionID]; ok {
		if existing != secretString {
			return nil, fmt.Errorf("ResourceExistsException")
		}
		version := *secret
		version.SecretString = existing
		version.VersionID = versionID
		return &version, nil
	}

	secret.SecretString = secretString
	secret.VersionID = versionID
	secret.Versions[versionID] = secretString
	return secret, nil
}

func requestedOrGeneratedVersionID(versionID string) string {
	if versionID != "" {
		return versionID
	}
	return uuid.New().String()
}

func (ss *SecretsStore) DeleteSecret(nameOrARN string, forceDelete bool) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	secret := ss.findLocked(nameOrARN)
	if secret == nil {
		return fmt.Errorf("ResourceNotFoundException")
	}

	delete(ss.secrets, secret.Name)
	return nil
}

// findLocked finds a secret by name or ARN. Caller must hold the lock.
func (ss *SecretsStore) findLocked(nameOrARN string) *Secret {
	if secret, ok := ss.secrets[nameOrARN]; ok {
		return secret
	}
	for _, secret := range ss.secrets {
		if secret.ARN == nameOrARN {
			return secret
		}
	}
	return nil
}
