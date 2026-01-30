package redisstore

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/state"
)

// RedisQuotaStore implements state.QuotaStore using Redis for distributed cooldowns.
type RedisQuotaStore struct {
	client    *redis.Client
	keyPrefix string
}

// NewRedisQuotaStore creates a Redis-backed quota store.
func NewRedisQuotaStore(client *redis.Client, keyPrefix string) *RedisQuotaStore {
	if keyPrefix == "" {
		keyPrefix = "cliproxy:quota:"
	}
	return &RedisQuotaStore{client: client, keyPrefix: keyPrefix}
}

// SetExceeded marks a credential as quota-exceeded until the given time.
func (s *RedisQuotaStore) SetExceeded(ctx context.Context, key string, until time.Time, reason string) error {
	ttl := time.Until(until)
	if ttl <= 0 {
		return nil // Already expired
	}
	// Store reason as value, use TTL for automatic expiration
	return s.client.Set(ctx, s.keyPrefix+key, reason, ttl).Err()
}

// IsExceeded checks if the credential is still in cooldown.
func (s *RedisQuotaStore) IsExceeded(ctx context.Context, key string) (bool, time.Time, error) {
	ttl, err := s.client.TTL(ctx, s.keyPrefix+key).Result()
	if err != nil {
		return false, time.Time{}, err
	}
	if ttl <= 0 {
		// Key doesn't exist or has no TTL
		return false, time.Time{}, nil
	}
	cooldownEnd := time.Now().Add(ttl)
	return true, cooldownEnd, nil
}

// Clear removes the cooldown status.
func (s *RedisQuotaStore) Clear(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.keyPrefix+key).Err()
}

// Ensure RedisQuotaStore implements state.QuotaStore
var _ state.QuotaStore = (*RedisQuotaStore)(nil)
