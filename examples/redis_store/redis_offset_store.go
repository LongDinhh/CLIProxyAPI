package redisstore

import (
	"context"

	"github.com/redis/go-redis/v9"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/state"
)

// RedisOffsetStore implements state.OffsetStore using Redis INCR for atomic counters.
type RedisOffsetStore struct {
	client    *redis.Client
	keyPrefix string
}

// NewRedisOffsetStore creates a Redis-backed offset store.
func NewRedisOffsetStore(client *redis.Client, keyPrefix string) *RedisOffsetStore {
	if keyPrefix == "" {
		keyPrefix = "cliproxy:offset:"
	}
	return &RedisOffsetStore{client: client, keyPrefix: keyPrefix}
}

// Increment atomically increments the offset and returns the new value.
func (s *RedisOffsetStore) Increment(ctx context.Context, key string) (int, error) {
	val, err := s.client.Incr(ctx, s.keyPrefix+key).Result()
	if err != nil {
		return 0, err
	}
	return int(val), nil
}

// Get returns the current offset without incrementing.
func (s *RedisOffsetStore) Get(ctx context.Context, key string) (int, error) {
	val, err := s.client.Get(ctx, s.keyPrefix+key).Int()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return val, nil
}

// Reset sets the offset back to 0.
func (s *RedisOffsetStore) Reset(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.keyPrefix+key).Err()
}

// Ensure RedisOffsetStore implements state.OffsetStore
var _ state.OffsetStore = (*RedisOffsetStore)(nil)
