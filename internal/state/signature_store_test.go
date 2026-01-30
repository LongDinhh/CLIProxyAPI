package state

import (
	"context"
	"testing"
	"time"
)

func TestSignatureStore_SetAndGet(t *testing.T) {
	store := NewMemorySignatureStore()
	ctx := context.Background()

	entry := &SignatureEntry{
		Signature: "test-signature-123",
		ModelID:   "claude-sonnet",
	}

	// Set entry
	err := store.Set(ctx, "key1", entry, time.Hour)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get entry
	got, ok := store.Get(ctx, "key1")
	if !ok {
		t.Fatal("Get returned false for existing key")
	}
	if got.Signature != entry.Signature {
		t.Errorf("Signature mismatch: got %q, want %q", got.Signature, entry.Signature)
	}
	if got.ModelID != entry.ModelID {
		t.Errorf("ModelID mismatch: got %q, want %q", got.ModelID, entry.ModelID)
	}
}

func TestSignatureStore_GetNotFound(t *testing.T) {
	store := NewMemorySignatureStore()
	ctx := context.Background()

	_, ok := store.Get(ctx, "nonexistent")
	if ok {
		t.Error("Get returned true for nonexistent key")
	}
}

func TestSignatureStore_Expiration(t *testing.T) {
	store := NewMemorySignatureStore()
	ctx := context.Background()

	entry := &SignatureEntry{Signature: "expires-fast"}
	err := store.Set(ctx, "expiring", entry, time.Millisecond)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for expiration
	time.Sleep(5 * time.Millisecond)

	_, ok := store.Get(ctx, "expiring")
	if ok {
		t.Error("Get returned true for expired entry")
	}
}

func TestSignatureStore_Delete(t *testing.T) {
	store := NewMemorySignatureStore()
	ctx := context.Background()

	entry := &SignatureEntry{Signature: "to-delete"}
	_ = store.Set(ctx, "delete-me", entry, time.Hour)

	err := store.Delete(ctx, "delete-me")
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, ok := store.Get(ctx, "delete-me")
	if ok {
		t.Error("Get returned true after Delete")
	}
}

func TestSignatureStore_Clear(t *testing.T) {
	store := NewMemorySignatureStore()
	ctx := context.Background()

	_ = store.Set(ctx, "key1", &SignatureEntry{Signature: "s1"}, time.Hour)
	_ = store.Set(ctx, "key2", &SignatureEntry{Signature: "s2"}, time.Hour)

	err := store.Clear(ctx)
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	_, ok1 := store.Get(ctx, "key1")
	_, ok2 := store.Get(ctx, "key2")
	if ok1 || ok2 {
		t.Error("Clear did not remove all entries")
	}
}

func TestSignatureKey(t *testing.T) {
	key := SignatureKey("claude-sonnet", "abc123")
	expected := "claude-sonnet:abc123"
	if key != expected {
		t.Errorf("SignatureKey mismatch: got %q, want %q", key, expected)
	}
}

func TestSignatureEntry_IsExpired(t *testing.T) {
	// nil entry
	var nilEntry *SignatureEntry
	if !nilEntry.IsExpired() {
		t.Error("nil entry should be expired")
	}

	// Past expiration
	pastEntry := &SignatureEntry{ExpiresAt: time.Now().Add(-time.Hour)}
	if !pastEntry.IsExpired() {
		t.Error("past entry should be expired")
	}

	// Future expiration
	futureEntry := &SignatureEntry{ExpiresAt: time.Now().Add(time.Hour)}
	if futureEntry.IsExpired() {
		t.Error("future entry should not be expired")
	}
}
