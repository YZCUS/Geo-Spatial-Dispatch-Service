package ratelimiter

import (
	"context"
	"errors"

	"github.com/go-redis/redis/v8"
)

var (
	ErrInsufficientBudget = errors.New("insufficient budget")
	ErrInvalidAmount      = errors.New("invalid amount")
)

// RateLimiter implements token bucket algorithm
type RateLimiter struct {
	redis *redis.Client
}

// New creates a new RateLimiter
func New(rdb *redis.Client) *RateLimiter {
	return &RateLimiter{
		redis: rdb,
	}
}

// SetBudget initializes budget for a key
func (rl *RateLimiter) SetBudget(ctx context.Context, key string, amount int64) error {
	if amount < 0 {
		return ErrInvalidAmount
	}
	return rl.redis.Set(ctx, key, amount, 0).Err()
}

// GetBudget retrieves current budget
func (rl *RateLimiter) GetBudget(ctx context.Context, key string) (int64, error) {
	val, err := rl.redis.Get(ctx, key).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// Allow checks if request is allowed (simple version)
func (rl *RateLimiter) Allow(ctx context.Context, key string, cost int64) (bool, error) {
	if cost < 0 {
		return false, ErrInvalidAmount
	}

	current, err := rl.GetBudget(ctx, key)
	if err != nil {
		return false, err
	}

	if current < cost {
		return false, nil
	}

	// Deduct (NOT atomic yet - will fix tomorrow)
	newVal := current - cost
	if err := rl.redis.Set(ctx, key, newVal, 0).Err(); err != nil {
		return false, err
	}

	return true, nil
}

// AllowAtomic uses Lua script for atomic deduction
func (rl *RateLimiter) AllowAtomic(ctx context.Context, key string, cost int64) (bool, int64, error) {
	if cost < 0 {
		return false, 0, ErrInvalidAmount
	}

	result, err := rl.redis.Eval(
		ctx,
		deductBudgetScript,
		[]string{key},
		cost,
	).Int64()

	if err != nil {
		return false, 0, err
	}

	if result == -1 {
		return false, 0, nil
	}

	return true, result, nil
}
