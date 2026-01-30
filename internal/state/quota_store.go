package state

import (
	"context"
	"time"
)

// QuotaStore tracks rate limit cooldowns across instances.
// When a provider returns a quota exceeded error, the cooldown is recorded
// so other instances know to skip this credential temporarily.
//
// Default implementation uses sync.Map (in-memory).
// For horizontal scaling, inject a distributed implementation (e.g., Redis).
type QuotaStore interface {
	// SetExceeded marks a credential as quota-exceeded until the given time.
	SetExceeded(ctx context.Context, key string, until time.Time, reason string) error

	// IsExceeded checks if the credential is still in cooldown.
	// Returns (exceeded, cooldownEnd, error).
	IsExceeded(ctx context.Context, key string) (bool, time.Time, error)

	// Clear removes the cooldown status.
	Clear(ctx context.Context, key string) error
}

// QuotaEntry represents a cooldown record.
type QuotaEntry struct {
	Until  time.Time `json:"until"`
	Reason string    `json:"reason,omitempty"`
}

// IsExpired checks if the cooldown has passed.
func (e *QuotaEntry) IsExpired() bool {
	return e == nil || time.Now().After(e.Until)
}

// QuotaKey generates a key for quota tracking.
func QuotaKey(authID, model string) string {
	if model == "" {
		return "quota:" + authID
	}
	return "quota:" + authID + ":" + model
}
