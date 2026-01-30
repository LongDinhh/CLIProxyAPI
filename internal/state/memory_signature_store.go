package state

import (
	"context"
	"sync"
	"time"
)

// memorySignatureStore is the default in-memory implementation using sync.Map.
type memorySignatureStore struct {
	data sync.Map
}

// NewMemorySignatureStore creates a new in-memory signature store.
func NewMemorySignatureStore() SignatureStore {
	return &memorySignatureStore{}
}

// Get retrieves a cached signature entry.
func (s *memorySignatureStore) Get(_ context.Context, key string) (*SignatureEntry, bool) {
	val, ok := s.data.Load(key)
	if !ok {
		return nil, false
	}
	entry, ok := val.(*SignatureEntry)
	if !ok || entry.IsExpired() {
		s.data.Delete(key)
		return nil, false
	}
	return entry, true
}

// Set stores a signature entry with TTL.
func (s *memorySignatureStore) Set(_ context.Context, key string, entry *SignatureEntry, ttl time.Duration) error {
	if entry == nil {
		return nil
	}
	entry.ExpiresAt = time.Now().Add(ttl)
	s.data.Store(key, entry)
	return nil
}

// Delete removes a specific signature.
func (s *memorySignatureStore) Delete(_ context.Context, key string) error {
	s.data.Delete(key)
	return nil
}

// Clear removes all signatures.
func (s *memorySignatureStore) Clear(_ context.Context) error {
	s.data.Range(func(key, _ any) bool {
		s.data.Delete(key)
		return true
	})
	return nil
}

// Close releases resources (no-op for in-memory store).
func (s *memorySignatureStore) Close() error {
	return s.Clear(context.Background())
}
