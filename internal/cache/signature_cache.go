package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/state"
)

const (
	// SignatureCacheTTL is how long signatures are valid
	SignatureCacheTTL = 3 * time.Hour

	// SignatureTextHashLen is the length of the hash key (16 hex chars = 64-bit key space)
	SignatureTextHashLen = 16

	// MinValidSignatureLen is the minimum length for a signature to be considered valid
	MinValidSignatureLen = 50

	// CacheCleanupInterval controls how often stale entries are purged (for in-memory only)
	CacheCleanupInterval = 10 * time.Minute
)

// cacheCleanupOnce ensures the background cleanup goroutine starts only once
var cacheCleanupOnce sync.Once

// hashText creates a stable, Unicode-safe key from text content
func hashText(text string) string {
	h := sha256.Sum256([]byte(text))
	return hex.EncodeToString(h[:])[:SignatureTextHashLen]
}

// startCacheCleanup launches a background goroutine that periodically
// removes expired entries from the SignatureStore (useful for in-memory stores).
func startCacheCleanup() {
	go func() {
		ticker := time.NewTicker(CacheCleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			// For external stores (Redis), TTL is handled by the store itself.
			// This cleanup is primarily for the in-memory default store.
			// The store's Get() automatically cleans expired entries.
		}
	}()
}

// CacheSignature stores a thinking signature for a given model group and text.
// Used for Claude models that require signed thinking blocks in multi-turn conversations.
func CacheSignature(modelName, text, signature string) {
	// Start background cleanup on first access
	cacheCleanupOnce.Do(startCacheCleanup)

	if text == "" || signature == "" {
		return
	}
	if len(signature) < MinValidSignatureLen {
		return
	}

	groupKey := GetModelGroup(modelName)
	textHash := hashText(text)
	key := state.SignatureKey(groupKey, textHash)

	entry := &state.SignatureEntry{
		Signature: signature,
		ModelID:   groupKey,
	}

	ctx := context.Background()
	_ = state.GetSignatureStore().Set(ctx, key, entry, SignatureCacheTTL)
}

// GetCachedSignature retrieves a cached signature for a given model group and text.
// Returns empty string if not found or expired.
func GetCachedSignature(modelName, text string) string {
	groupKey := GetModelGroup(modelName)

	// Special case for Gemini models
	if text == "" {
		if groupKey == "gemini" {
			return "skip_thought_signature_validator"
		}
		return ""
	}

	textHash := hashText(text)
	key := state.SignatureKey(groupKey, textHash)

	ctx := context.Background()
	entry, ok := state.GetSignatureStore().Get(ctx, key)
	if !ok || entry == nil {
		if groupKey == "gemini" {
			return "skip_thought_signature_validator"
		}
		return ""
	}

	// Refresh TTL on access (sliding expiration) by re-setting
	_ = state.GetSignatureStore().Set(ctx, key, entry, SignatureCacheTTL)

	return entry.Signature
}

// ClearSignatureCache clears signature cache for a specific model group or all groups.
func ClearSignatureCache(modelName string) {
	ctx := context.Background()
	store := state.GetSignatureStore()

	if modelName == "" {
		// Clear all
		_ = store.Clear(ctx)
		return
	}

	// For specific model group clearing:
	// The SignatureStore interface doesn't support prefix-based deletion.
	// In the in-memory implementation, entries are keyed by "group:textHash",
	// but we cannot enumerate and selectively delete.
	// This is a known limitation - specific model clearing is currently a no-op.
	// For full clearing functionality with external stores (Redis),
	// a ClearByPrefix method could be added to the interface in the future.
	// Currently, callers should use ClearSignatureCache("") to clear all.
}

// HasValidSignature checks if a signature is valid (non-empty and long enough)
func HasValidSignature(modelName, signature string) bool {
	return (signature != "" && len(signature) >= MinValidSignatureLen) ||
		(signature == "skip_thought_signature_validator" && GetModelGroup(modelName) == "gemini")
}

// GetModelGroup returns the model group for cache bucketing
func GetModelGroup(modelName string) string {
	if strings.Contains(modelName, "gpt") {
		return "gpt"
	} else if strings.Contains(modelName, "claude") {
		return "claude"
	} else if strings.Contains(modelName, "gemini") {
		return "gemini"
	}
	return modelName
}
