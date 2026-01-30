package state

import (
	"context"
	"testing"
	"time"
)

func TestQuotaStore_SetAndIsExceeded(t *testing.T) {
	store := NewMemoryQuotaStore()
	ctx := context.Background()

	until := time.Now().Add(time.Hour)
	err := store.SetExceeded(ctx, "quota1", until, "rate limit")
	if err != nil {
		t.Fatalf("SetExceeded failed: %v", err)
	}

	exceeded, cooldownEnd, err := store.IsExceeded(ctx, "quota1")
	if err != nil {
		t.Fatalf("IsExceeded failed: %v", err)
	}
	if !exceeded {
		t.Error("IsExceeded should return true")
	}
	if cooldownEnd.Before(time.Now()) {
		t.Error("Cooldown end should be in the future")
	}
}

func TestQuotaStore_NotExceeded(t *testing.T) {
	store := NewMemoryQuotaStore()
	ctx := context.Background()

	exceeded, _, err := store.IsExceeded(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("IsExceeded failed: %v", err)
	}
	if exceeded {
		t.Error("Nonexistent key should not be exceeded")
	}
}

func TestQuotaStore_Expiration(t *testing.T) {
	store := NewMemoryQuotaStore()
	ctx := context.Background()

	until := time.Now().Add(time.Millisecond)
	_ = store.SetExceeded(ctx, "expires", until, "test")

	time.Sleep(5 * time.Millisecond)

	exceeded, _, _ := store.IsExceeded(ctx, "expires")
	if exceeded {
		t.Error("Expired quota should not be exceeded")
	}
}

func TestQuotaStore_Clear(t *testing.T) {
	store := NewMemoryQuotaStore()
	ctx := context.Background()

	until := time.Now().Add(time.Hour)
	_ = store.SetExceeded(ctx, "clear-me", until, "test")

	err := store.Clear(ctx, "clear-me")
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	exceeded, _, _ := store.IsExceeded(ctx, "clear-me")
	if exceeded {
		t.Error("Cleared quota should not be exceeded")
	}
}

func TestQuotaKey(t *testing.T) {
	tests := []struct {
		authID   string
		model    string
		expected string
	}{
		{"auth1", "", "quota:auth1"},
		{"auth1", "claude", "quota:auth1:claude"},
	}

	for _, tt := range tests {
		key := QuotaKey(tt.authID, tt.model)
		if key != tt.expected {
			t.Errorf("QuotaKey(%q, %q) = %q, want %q", tt.authID, tt.model, key, tt.expected)
		}
	}
}

func TestQuotaEntry_IsExpired(t *testing.T) {
	// nil entry
	var nilEntry *QuotaEntry
	if !nilEntry.IsExpired() {
		t.Error("nil entry should be expired")
	}

	// Past
	pastEntry := &QuotaEntry{Until: time.Now().Add(-time.Hour)}
	if !pastEntry.IsExpired() {
		t.Error("past entry should be expired")
	}

	// Future
	futureEntry := &QuotaEntry{Until: time.Now().Add(time.Hour)}
	if futureEntry.IsExpired() {
		t.Error("future entry should not be expired")
	}
}
