package state

import (
	"context"
	"sync"
	"sync/atomic"
)

// memoryOffsetStore is the default in-memory implementation using atomic counters.
type memoryOffsetStore struct {
	offsets sync.Map
}

// NewMemoryOffsetStore creates a new in-memory offset store.
func NewMemoryOffsetStore() OffsetStore {
	return &memoryOffsetStore{}
}

// Increment atomically increments the offset and returns the new value.
func (s *memoryOffsetStore) Increment(_ context.Context, key string) (int, error) {
	val, _ := s.offsets.LoadOrStore(key, new(int64))
	ptr := val.(*int64)
	return int(atomic.AddInt64(ptr, 1)), nil
}

// Get returns the current offset without incrementing.
func (s *memoryOffsetStore) Get(_ context.Context, key string) (int, error) {
	val, ok := s.offsets.Load(key)
	if !ok {
		return 0, nil
	}
	return int(atomic.LoadInt64(val.(*int64))), nil
}

// Reset sets the offset back to 0.
func (s *memoryOffsetStore) Reset(_ context.Context, key string) error {
	s.offsets.Delete(key)
	return nil
}

// Global offset store accessor
var (
	offsetStore   OffsetStore
	offsetStoreMu sync.RWMutex
)

// GetOffsetStore returns the active offset store.
func GetOffsetStore() OffsetStore {
	offsetStoreMu.RLock()
	if offsetStore != nil {
		defer offsetStoreMu.RUnlock()
		return offsetStore
	}
	offsetStoreMu.RUnlock()

	offsetStoreMu.Lock()
	defer offsetStoreMu.Unlock()
	if offsetStore == nil {
		offsetStore = NewMemoryOffsetStore()
	}
	return offsetStore
}

// SetOffsetStore replaces the default offset store.
func SetOffsetStore(store OffsetStore) {
	offsetStoreMu.Lock()
	defer offsetStoreMu.Unlock()
	if store != nil {
		offsetStore = store
	}
}

// ResetOffsetStore resets the global offset store to nil (for testing).
func ResetOffsetStore() {
	offsetStoreMu.Lock()
	defer offsetStoreMu.Unlock()
	offsetStore = nil
}
