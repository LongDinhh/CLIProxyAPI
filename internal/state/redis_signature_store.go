package state

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisSignatureStore implements SignatureStore using Redis.
type redisSignatureStore struct {
	client    *redis.Client
	keyPrefix string
}

// newRedisSignatureStore creates a Redis-backed signature store.
func newRedisSignatureStore(client *redis.Client, keyPrefix string) SignatureStore {
	return &redisSignatureStore{client: client, keyPrefix: keyPrefix}
}

// Get retrieves a cached signature entry.
func (s *redisSignatureStore) Get(ctx context.Context, key string) (*SignatureEntry, bool) {
	data, err := s.client.Get(ctx, s.keyPrefix+key).Bytes()
	if err != nil {
		return nil, false
	}
	var entry SignatureEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, false
	}
	if entry.IsExpired() {
		return nil, false
	}
	return &entry, true
}

// Set stores a signature entry with TTL.
func (s *redisSignatureStore) Set(ctx context.Context, key string, entry *SignatureEntry, ttl time.Duration) error {
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
func (s *redisSignatureStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.keyPrefix+key).Err()
}

// Clear removes all signatures with the store's prefix.
func (s *redisSignatureStore) Clear(ctx context.Context) error {
	iter := s.client.Scan(ctx, 0, s.keyPrefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		s.client.Del(ctx, iter.Val())
	}
	return iter.Err()
}

// Close is a no-op since Redis client lifecycle is managed externally.
func (s *redisSignatureStore) Close() error {
	return nil
}
