package state

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// redisOffsetStore implements OffsetStore using Redis INCR.
type redisOffsetStore struct {
	client    *redis.Client
	keyPrefix string
}

// newRedisOffsetStore creates a Redis-backed offset store.
func newRedisOffsetStore(client *redis.Client, keyPrefix string) OffsetStore {
	return &redisOffsetStore{client: client, keyPrefix: keyPrefix}
}

// Increment atomically increments the offset and returns the new value.
func (s *redisOffsetStore) Increment(ctx context.Context, key string) (int, error) {
	val, err := s.client.Incr(ctx, s.keyPrefix+key).Result()
	if err != nil {
		return 0, err
	}
	// Handle overflow - reset to 1 if approaching int32 max
	if val >= 2_147_483_640 {
		s.client.Set(ctx, s.keyPrefix+key, 1, 0)
		return 1, nil
	}
	return int(val), nil
}

// Get returns the current offset without incrementing.
func (s *redisOffsetStore) Get(ctx context.Context, key string) (int, error) {
	val, err := s.client.Get(ctx, s.keyPrefix+key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int(val), nil
}

// Reset sets the offset back to 0.
func (s *redisOffsetStore) Reset(ctx context.Context, key string) error {
	return s.client.Del(ctx, s.keyPrefix+key).Err()
}
