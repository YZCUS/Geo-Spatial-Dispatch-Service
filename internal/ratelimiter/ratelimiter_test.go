package ratelimiter

import (
	"context"
	"sync"
	"testing"

	"github.com/go-redis/redis/v8"
)

func setupTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   1, // Use different DB for tests
	})

	// Clear test DB
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("Failed to flush test DB: %v", err)
	}

	return rdb
}

func TestRateLimiter_SetAndGetBudget(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	err := rl.SetBudget(ctx, "test-key", 100)
	if err != nil {
		t.Fatalf("SetBudget failed: %v", err)
	}

	budget, err := rl.GetBudget(ctx, "test-key")
	if err != nil {
		t.Fatalf("GetBudget failed: %v", err)
	}

	if budget != 100 {
		t.Errorf("Expected budget 100, got %d", budget)
	}
}

func TestRateLimiter_GetBudget_NonExistent(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	budget, err := rl.GetBudget(ctx, "non-existent-key")
	if err != nil {
		t.Fatalf("GetBudget should not error for non-existent key: %v", err)
	}

	if budget != 0 {
		t.Errorf("Expected budget 0 for non-existent key, got %d", budget)
	}
}

func TestRateLimiter_Allow(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	rl.SetBudget(ctx, "campaign1", 50)

	// Test 1: Sufficient budget
	allowed, err := rl.Allow(ctx, "campaign1", 10)
	if err != nil {
		t.Fatalf("Allow failed: %v", err)
	}
	if !allowed {
		t.Error("Expected allowed=true")
	}

	// Check remaining
	budget, _ := rl.GetBudget(ctx, "campaign1")
	if budget != 40 {
		t.Errorf("Expected remaining 40, got %d", budget)
	}

	// Test 2: Insufficient budget
	allowed, err = rl.Allow(ctx, "campaign1", 50)
	if err != nil {
		t.Fatalf("Allow failed: %v", err)
	}
	if allowed {
		t.Error("Expected allowed=false (insufficient budget)")
	}
}

func TestRateLimiter_Allow_ZeroBudget(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	// No budget set, should return false
	allowed, err := rl.Allow(ctx, "no-budget", 10)
	if err != nil {
		t.Fatalf("Allow failed: %v", err)
	}
	if allowed {
		t.Error("Expected allowed=false for zero budget")
	}
}

func TestRateLimiter_InvalidAmount(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	// Negative amount
	err := rl.SetBudget(ctx, "test", -10)
	if err != ErrInvalidAmount {
		t.Errorf("Expected ErrInvalidAmount, got %v", err)
	}

	allowed, err := rl.Allow(ctx, "test", -5)
	if err != ErrInvalidAmount {
		t.Errorf("Expected ErrInvalidAmount, got %v", err)
	}
	if allowed {
		t.Error("Should not allow negative amount")
	}
}

func TestRateLimiter_AllowAtomic(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	rl.SetBudget(ctx, "atomic-test", 100)

	allowed, remaining, err := rl.AllowAtomic(ctx, "atomic-test", 30)
	if err != nil {
		t.Fatalf("AllowAtomic failed: %v", err)
	}

	if !allowed {
		t.Error("Expected allowed=true")
	}

	if remaining != 70 {
		t.Errorf("Expected remaining=70, got %d", remaining)
	}
}

func TestRateLimiter_AllowAtomic_InvalidAmount(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	rl.SetBudget(ctx, "atomic-invalid", 100)

	allowed, remaining, err := rl.AllowAtomic(ctx, "atomic-invalid", -10)
	if err != ErrInvalidAmount {
		t.Errorf("Expected ErrInvalidAmount, got %v", err)
	}
	if allowed {
		t.Error("Should not allow negative amount")
	}
	if remaining != 0 {
		t.Errorf("Expected remaining=0 on error, got %d", remaining)
	}
}

func TestRateLimiter_AllowAtomic_InsufficientBudget(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	rl.SetBudget(ctx, "atomic-insufficient", 50)

	allowed, remaining, err := rl.AllowAtomic(ctx, "atomic-insufficient", 100)
	if err != nil {
		t.Fatalf("AllowAtomic failed: %v", err)
	}
	if allowed {
		t.Error("Expected allowed=false for insufficient budget")
	}
	if remaining != 0 {
		t.Errorf("Expected remaining=0 for denied request, got %d", remaining)
	}
}

func TestRateLimiter_AllowAtomic_Concurrent(t *testing.T) {
	rdb := setupTestRedis(t)
	defer rdb.Close()

	rl := New(rdb)
	ctx := context.Background()

	rl.SetBudget(ctx, "concurrent-test", 100)

	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	// 20 concurrent requests, each costing 10
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed, _, err := rl.AllowAtomic(ctx, "concurrent-test", 10)
			if err != nil {
				return
			}
			if allowed {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Should only allow exactly 10 requests (100 budget / 10 cost each)
	if successCount != 10 {
		t.Errorf("Expected exactly 10 successful requests, got %d", successCount)
	}
}
