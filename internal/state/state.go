package state

import "sync"

var (
	signatureStore   SignatureStore
	signatureStoreMu sync.RWMutex
)

// GetSignatureStore returns the active signature store.
// Lazily initializes to in-memory store if not set.
func GetSignatureStore() SignatureStore {
	signatureStoreMu.RLock()
	if signatureStore != nil {
		defer signatureStoreMu.RUnlock()
		return signatureStore
	}
	signatureStoreMu.RUnlock()

	signatureStoreMu.Lock()
	defer signatureStoreMu.Unlock()
	if signatureStore == nil {
		signatureStore = NewMemorySignatureStore()
	}
	return signatureStore
}

// SetSignatureStore replaces the default store.
// Must be called before any Get/Set operations (typically in init or main).
// Not safe for concurrent use with GetSignatureStore.
func SetSignatureStore(store SignatureStore) {
	signatureStoreMu.Lock()
	defer signatureStoreMu.Unlock()
	if store != nil {
		signatureStore = store
	}
}

// ResetSignatureStore resets to nil (for testing).
func ResetSignatureStore() {
	signatureStoreMu.Lock()
	defer signatureStoreMu.Unlock()
	signatureStore = nil
}
