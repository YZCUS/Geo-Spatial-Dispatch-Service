package ratelimiter

import (
	"context"
	"errors"
	"strings"

	"github.com/go-redis/redis/v8"
)

var (
	ErrInsufficientBudget = errors.New("insufficient budget")
	ErrInvalidAmount      = errors.New("invalid amount")
	ErrInvalidKey         = errors.New("invalid key")
)

const budgetKeyPrefix = "ratelimit:budget:"

var deductBudget = redis.NewScript(deductBudgetScript)

// RateLimiter manages fixed per-key request budgets.
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
	redisKey, err := budgetKey(key)
	if err != nil {
		return err
	}
	return rl.redis.Set(ctx, redisKey, amount, 0).Err()
}

// GetBudget retrieves current budget
func (rl *RateLimiter) GetBudget(ctx context.Context, key string) (int64, error) {
	redisKey, err := budgetKey(key)
	if err != nil {
		return 0, err
	}
	val, err := rl.redis.Get(ctx, redisKey).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return val, err
}

// Allow checks if request is allowed (simple version)
func (rl *RateLimiter) Allow(ctx context.Context, key string, cost int64) (bool, error) {
	allowed, _, err := rl.AllowAtomic(ctx, key, cost)
	return allowed, err
}

// AllowAtomic uses Lua script for atomic deduction
func (rl *RateLimiter) AllowAtomic(ctx context.Context, key string, cost int64) (bool, int64, error) {
	if cost < 0 {
		return false, 0, ErrInvalidAmount
	}
	redisKey, err := budgetKey(key)
	if err != nil {
		return false, 0, err
	}

	result, err := deductBudget.Run(ctx, rl.redis, []string{redisKey}, cost).Int64()

	if err != nil {
		return false, 0, err
	}

	if result == -1 {
		return false, 0, nil
	}

	return true, result, nil
}

func budgetKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", ErrInvalidKey
	}
	return budgetKeyPrefix + key, nil
}
