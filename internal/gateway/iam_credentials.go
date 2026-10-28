package gateway

import (
	"sync/atomic"
	"time"
)

// One immutable credential record owns the identity, signing material and
// effective permissions used by one authentication attempt.
type iamCredential struct {
	secretAccessKey    string
	sessionToken       string
	identity           iamIdentity
	expiresAt          time.Time
	issuedAt           time.Time
	policyPrincipalARN string
	principalTags      map[string]string
	mfaPresent         *bool
	policy             invokePolicy
}

type iamCredentialSnapshot struct{ entries map[string]*iamCredential }

// A fixture replacement publishes a complete generation. An attempt holding an
// old record cannot acquire admission after replacement, even for the same key.
type iamCredentialStore struct {
	active atomic.Pointer[iamCredentialSnapshot]
}

func newIAMCredentialStore(input map[string]iamCredential) *iamCredentialStore {
	store := &iamCredentialStore{}
	store.replace(input)
	return store
}

func (store *iamCredentialStore) replace(input map[string]iamCredential) {
	snapshot := &iamCredentialSnapshot{entries: make(map[string]*iamCredential, len(input))}
	for key, credential := range input {
		credential.identity.AccessKey = key
		if credential.principalTags != nil {
			tags := make(map[string]string, len(credential.principalTags))
			for name, value := range credential.principalTags {
				tags[name] = value
			}
			credential.principalTags = tags
		}
		if credential.mfaPresent != nil {
			value := *credential.mfaPresent
			credential.mfaPresent = &value
		}
		snapshot.entries[key] = &credential
	}
	store.active.Store(snapshot)
}

func (store *iamCredentialStore) lookup(key string, now time.Time) (*iamCredential, bool) {
	snapshot := store.active.Load()
	if snapshot == nil {
		return nil, false
	}
	credential, ok := snapshot.entries[key]
	return credential, ok && (credential.expiresAt.IsZero() || now.Before(credential.expiresAt))
}

func (store *iamCredentialStore) current(credential *iamCredential, now time.Time) bool {
	if credential == nil {
		return false
	}
	current, ok := store.lookup(credential.identity.AccessKey, now)
	return ok && current == credential
}
