package state

import "context"

// OffsetStore manages round-robin counters for provider load balancing.
// Used to fairly distribute requests across multiple provider credentials.
//
// Default implementation uses sync.Map with atomic counters (in-memory).
// For horizontal scaling, inject a distributed implementation (e.g., Redis INCR).
type OffsetStore interface {
	// Increment atomically increments the offset and returns the new value.
	// This is used for round-robin selection of providers.
	Increment(ctx context.Context, key string) (int, error)

	// Get returns the current offset without incrementing.
	Get(ctx context.Context, key string) (int, error)

	// Reset sets the offset back to 0.
	Reset(ctx context.Context, key string) error
}

// OffsetKey generates a key for offset tracking.
func OffsetKey(model string) string {
	return "offset:" + model
}
