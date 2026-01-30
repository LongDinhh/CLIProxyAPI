// Package state provides a Redis initialization helper for horizontal scaling.
package state

import (
	"context"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

// RedisStateStoreConfig holds Redis connection settings.
type RedisStateStoreConfig struct {
	// URL is the Redis connection string (redis://host:port or redis://user:pass@host:port).
	URL string
	// KeyPrefix is prepended to all keys (default: "cliproxy:").
	KeyPrefix string
}

// redisClient is the shared Redis client instance.
var redisClient *redis.Client

// GetRedisClient returns the shared Redis client if configured.
func GetRedisClient() *redis.Client {
	return redisClient
}

// InitRedisStateStores initializes Redis-backed state stores from environment.
// Call this early in main() before any cache/selector operations.
//
// Environment variables:
//   - REDIS_URL: Redis connection string (e.g., redis://localhost:6379)
//   - REDIS_STATE_PREFIX: Optional key prefix (default: "cliproxy:")
//
// Returns true if Redis stores were initialized, false otherwise.
func InitRedisStateStores() bool {
	url := strings.TrimSpace(os.Getenv("REDIS_URL"))
	if url == "" {
		// Also check for common variations
		url = strings.TrimSpace(os.Getenv("REDIS_ADDR"))
		if url != "" && !strings.HasPrefix(url, "redis://") {
			url = "redis://" + url
		}
	}
	if url == "" {
		log.Debug("REDIS_URL not set, using in-memory state stores")
		return false
	}

	prefix := strings.TrimSpace(os.Getenv("REDIS_STATE_PREFIX"))
	if prefix == "" {
		prefix = "cliproxy:"
	}

	opts, err := redis.ParseURL(url)
	if err != nil {
		log.WithError(err).Error("failed to parse REDIS_URL, falling back to in-memory stores")
		return false
	}

	client := redis.NewClient(opts)
	ctx := context.Background()

	// Test connection
	if err := client.Ping(ctx).Err(); err != nil {
		log.WithError(err).Error("failed to connect to Redis, falling back to in-memory stores")
		return false
	}

	// Store client for reuse
	redisClient = client

	// Initialize Redis-backed stores
	// SignatureKey does not have a prefix, so we add "sig:" here
	SetSignatureStore(newRedisSignatureStore(client, prefix+"sig:"))
	// OffsetKey already includes "offset:" prefix
	SetOffsetStore(newRedisOffsetStore(client, prefix))
	// QuotaKey already includes "quota:" prefix
	SetQuotaStore(newRedisQuotaStore(client, prefix))

	log.WithField("url", maskRedisURL(url)).Info("Redis state stores initialized for horizontal scaling")
	return true
}

// maskRedisURL masks password in Redis URL for logging.
func maskRedisURL(url string) string {
	if idx := strings.Index(url, "@"); idx > 0 {
		prefix := url[:strings.Index(url, "://")+3]
		suffix := url[idx:]
		return prefix + "***:***" + suffix
	}
	return url
}
