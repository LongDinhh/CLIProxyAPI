package state

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisQuotaStore implements QuotaStore using Redis with TTL.
type redisQuotaStore struct {
	client    *redis.Client
	keyPrefix string
}

// newRedisQuotaStore creates a Redis-backed quota store.
func newRedisQuotaStore(client *redis.Client, keyPrefix string) QuotaStore {
	return &redisQuotaStore{client: client, keyPrefix: keyPrefix}
}

// SetExceeded marks a credential as quota-exceeded until the given time.
func (s *redisQuotaStore) SetExceeded(ctx context.Context, key string, until time.Time, reason string) error {
	ttl := time.Until(until)
	if ttl <= 0 {
		return nil
	}
	entry := QuotaEntry{
		Until:  until,
		Reason: reason,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.keyPrefix+key, data, ttl).Err()
}

// IsExceeded checks if the credential is still in cooldown.
func (s *redisQuotaStore) IsExceeded(ctx context.Context, key string) (bool, time.Time, error) {
	data, err := s.client.Get(ctx, s.keyPrefix+key).Bytes()
	if err == redis.Nil {
		return false, time.Time{}, nil
	}
	if err != nil {
		return false, time.Time{}, err
	}

	var entry QuotaEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return false, time.Time{}, err
	}

	if entry.IsExpired() {
		return false, time.Time{}, nil
	}

	return true, entry.Until, nil
}

// Clear removes the cooldown status.
func (s *redisQuotaStore) Clear(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.keyPrefix+key).Err()
}
