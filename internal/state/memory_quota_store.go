package state

import (
	"context"
	"sync"
	"time"
)

// memoryQuotaStore is the default in-memory implementation.
type memoryQuotaStore struct {
	data sync.Map
}

// NewMemoryQuotaStore creates a new in-memory quota store.
func NewMemoryQuotaStore() QuotaStore {
	return &memoryQuotaStore{}
}

// SetExceeded marks a credential as quota-exceeded until the given time.
func (s *memoryQuotaStore) SetExceeded(_ context.Context, key string, until time.Time, reason string) error {
	s.data.Store(key, &QuotaEntry{
		Until:  until,
		Reason: reason,
	})
	return nil
}

// IsExceeded checks if the credential is still in cooldown.
func (s *memoryQuotaStore) IsExceeded(_ context.Context, key string) (bool, time.Time, error) {
	val, ok := s.data.Load(key)
	if !ok {
		return false, time.Time{}, nil
	}
	entry := val.(*QuotaEntry)
	if entry.IsExpired() {
		s.data.Delete(key)
		return false, time.Time{}, nil
	}
	return true, entry.Until, nil
}

// Clear removes the cooldown status.
func (s *memoryQuotaStore) Clear(_ context.Context, key string) error {
	s.data.Delete(key)
	return nil
}

// Global quota store accessor
var (
	quotaStore   QuotaStore
	quotaStoreMu sync.RWMutex
)

// GetQuotaStore returns the active quota store.
func GetQuotaStore() QuotaStore {
	quotaStoreMu.RLock()
	if quotaStore != nil {
		defer quotaStoreMu.RUnlock()
		return quotaStore
	}
	quotaStoreMu.RUnlock()

	quotaStoreMu.Lock()
	defer quotaStoreMu.Unlock()
	if quotaStore == nil {
		quotaStore = NewMemoryQuotaStore()
	}
	return quotaStore
}

// SetQuotaStore replaces the default quota store.
func SetQuotaStore(store QuotaStore) {
	quotaStoreMu.Lock()
	defer quotaStoreMu.Unlock()
	if store != nil {
		quotaStore = store
	}
}
