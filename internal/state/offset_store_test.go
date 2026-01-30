package state

import (
	"context"
	"sync"
	"testing"
)

func TestOffsetStore_Increment(t *testing.T) {
	store := NewMemoryOffsetStore()
	ctx := context.Background()

	// First increment
	val, err := store.Increment(ctx, "model1")
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 1 {
		t.Errorf("First increment should be 1, got %d", val)
	}

	// Second increment
	val, err = store.Increment(ctx, "model1")
	if err != nil {
		t.Fatalf("Increment failed: %v", err)
	}
	if val != 2 {
		t.Errorf("Second increment should be 2, got %d", val)
	}
}

func TestOffsetStore_Get(t *testing.T) {
	store := NewMemoryOffsetStore()
	ctx := context.Background()

	// Get nonexistent
	val, err := store.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != 0 {
		t.Errorf("Nonexistent key should return 0, got %d", val)
	}

	// Set and get
	_, _ = store.Increment(ctx, "model1")
	_, _ = store.Increment(ctx, "model1")
	val, err = store.Get(ctx, "model1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if val != 2 {
		t.Errorf("Get should return 2, got %d", val)
	}
}

func TestOffsetStore_Reset(t *testing.T) {
	store := NewMemoryOffsetStore()
	ctx := context.Background()

	_, _ = store.Increment(ctx, "model1")
	_, _ = store.Increment(ctx, "model1")

	err := store.Reset(ctx, "model1")
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	val, _ := store.Get(ctx, "model1")
	if val != 0 {
		t.Errorf("After reset, Get should return 0, got %d", val)
	}
}

func TestOffsetStore_Concurrent(t *testing.T) {
	store := NewMemoryOffsetStore()
	ctx := context.Background()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, _ = store.Increment(ctx, "concurrent")
		}()
	}

	wg.Wait()

	val, _ := store.Get(ctx, "concurrent")
	if val != goroutines {
		t.Errorf("Concurrent increments: got %d, want %d", val, goroutines)
	}
}

func TestOffsetKey(t *testing.T) {
	key := OffsetKey("claude-sonnet")
	expected := "offset:claude-sonnet"
	if key != expected {
		t.Errorf("OffsetKey mismatch: got %q, want %q", key, expected)
	}
}
