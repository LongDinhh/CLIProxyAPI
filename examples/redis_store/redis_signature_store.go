package redisstore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/state"
)

// RedisSignatureStore implements state.SignatureStore using Redis.
type RedisSignatureStore struct {
	client    *redis.Client
	keyPrefix string
}

// NewRedisSignatureStore creates a Redis-backed signature store.
func NewRedisSignatureStore(client *redis.Client, keyPrefix string) *RedisSignatureStore {
	if keyPrefix == "" {
		keyPrefix = "cliproxy:sig:"
	}
	return &RedisSignatureStore{client: client, keyPrefix: keyPrefix}
}

// Get retrieves a cached signature entry.
func (s *RedisSignatureStore) Get(ctx context.Context, key string) (*state.SignatureEntry, bool) {
	data, err := s.client.Get(ctx, s.keyPrefix+key).Bytes()
	if err != nil {
		return nil, false
	}
	var entry state.SignatureEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, false
	}
	if entry.IsExpired() {
		return nil, false
	}
	return &entry, true
}

// Set stores a signature entry with TTL.
func (s *RedisSignatureStore) Set(ctx context.Context, key string, entry *state.SignatureEntry, ttl time.Duration) error {
	if entry == nil {
		return nil
	}
	entry.ExpiresAt = time.Now().Add(ttl)
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return s.client.Set(ctx, s.keyPrefix+key, data, ttl).Err()
}

// Delete removes a specific signature.
func (s *RedisSignatureStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.keyPrefix+key).Err()
}

// Clear removes all signatures with the store's prefix.
func (s *RedisSignatureStore) Clear(ctx context.Context) error {
	iter := s.client.Scan(ctx, 0, s.keyPrefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		s.client.Del(ctx, iter.Val())
	}
	return iter.Err()
}

// Close is a no-op since Redis client lifecycle is managed externally.
func (s *RedisSignatureStore) Close() error {
	return nil
}
