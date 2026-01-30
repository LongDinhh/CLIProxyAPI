package state

import (
	"context"
	"time"
)

// SignatureStore abstracts signature persistence for horizontal scaling.
// The signature cache is used for Claude's extended thinking feature,
// where multi-turn conversations require the thinking signature from
// previous responses to be included in subsequent requests.
//
// Default implementation uses sync.Map (in-memory).
// For horizontal scaling, inject a distributed implementation (e.g., Redis).
type SignatureStore interface {
	// Get retrieves a cached signature entry.
	// Returns (entry, true) if found and not expired, (nil, false) otherwise.
	Get(ctx context.Context, key string) (*SignatureEntry, bool)

	// Set stores a signature entry with TTL.
	Set(ctx context.Context, key string, entry *SignatureEntry, ttl time.Duration) error

	// Delete removes a specific signature.
	Delete(ctx context.Context, key string) error

	// Clear removes all signatures (for cleanup/testing).
	Clear(ctx context.Context) error

	// Close releases any resources held by the store.
	Close() error
}

// SignatureEntry represents a cached thinking signature.
type SignatureEntry struct {
	Signature string    `json:"signature"`
	ExpiresAt time.Time `json:"expires_at"`
	ModelID   string    `json:"model_id,omitempty"`
}

// IsExpired checks if the entry has exceeded its TTL.
func (e *SignatureEntry) IsExpired() bool {
	return e == nil || time.Now().After(e.ExpiresAt)
}

// SignatureKey generates a cache key from model group and text hash.
func SignatureKey(modelGroup, textHash string) string {
	return modelGroup + ":" + textHash
}
