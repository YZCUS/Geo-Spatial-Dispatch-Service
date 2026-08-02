package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ErrNoDriversAvailable = errors.New("no drivers available")
	ErrLockFailed         = errors.New("failed to acquire lock")
	ErrLockNotHeld        = errors.New("lock not held")
	ErrLockExpired        = errors.New("lock expired")
)

var unlockScript = redis.NewScript(`
	local owner = redis.call('GET', KEYS[1])
	if owner == ARGV[1] then
		return redis.call('DEL', KEYS[1])
	end
	if not owner then
		return -1
	end
	return 0
`)

var extendLockScript = redis.NewScript(`
	if redis.call('GET', KEYS[1]) == ARGV[1] then
		return redis.call('EXPIRE', KEYS[1], ARGV[2])
	end
	return 0
`)

// LockManager handles distributed locking for driver assignments
type LockManager struct {
	redis     *redis.Client
	keyPrefix string
	lockTTL   time.Duration
}

// NewLockManager creates a new lock manager
func NewLockManager(rdb *redis.Client, keyPrefix string, lockTTL time.Duration) *LockManager {
	if keyPrefix == "" {
		keyPrefix = "dispatch:lock"
	}
	if lockTTL == 0 {
		lockTTL = 10 * time.Second
	}
	return &LockManager{
		redis:     rdb,
		keyPrefix: keyPrefix,
		lockTTL:   lockTTL,
	}
}

// lockKey returns the Redis key for a driver lock
func (lm *LockManager) lockKey(driverID string) string {
	return fmt.Sprintf("%s:%s", lm.keyPrefix, driverID)
}

// TryLock attempts to acquire a lock on a driver
// Returns a token if successful, empty string if lock not acquired
func (lm *LockManager) TryLock(ctx context.Context, driverID string, requestID string) (bool, error) {
	key := lm.lockKey(driverID)

	// SETNX with TTL
	success, err := lm.redis.SetNX(ctx, key, requestID, lm.lockTTL).Result()
	if err != nil {
		log.Printf("[LockManager] Error acquiring lock for driver %s: %v", driverID, err)
		return false, err
	}

	if success {
		log.Printf("[LockManager] Lock acquired for driver %s by request %s", driverID, requestID)
	} else {
		log.Printf("[LockManager] Lock contention for driver %s, request %s failed", driverID, requestID)
	}

	return success, nil
}

// Unlock releases the lock on a driver
// Only succeeds if the lock is held by the given requestID
func (lm *LockManager) Unlock(ctx context.Context, driverID string, requestID string) error {
	key := lm.lockKey(driverID)

	result, err := unlockScript.Run(ctx, lm.redis, []string{key}, requestID).Int()
	if err != nil {
		return err
	}

	switch result {
	case 1:
		log.Printf("[LockManager] Lock released for driver %s", driverID)
		return nil
	case -1:
		return ErrLockExpired
	default:
		return ErrLockNotHeld
	}
}

// ExtendLock extends the TTL of an existing lock
func (lm *LockManager) ExtendLock(ctx context.Context, driverID string, requestID string) error {
	key := lm.lockKey(driverID)

	result, err := extendLockScript.Run(
		ctx,
		lm.redis,
		[]string{key},
		requestID,
		int(lm.lockTTL.Seconds()),
	).Int()
	if err != nil {
		return err
	}

	if result == 0 {
		return ErrLockNotHeld
	}

	return nil
}

// IsLocked checks if a driver is currently locked
func (lm *LockManager) IsLocked(ctx context.Context, driverID string) (bool, error) {
	key := lm.lockKey(driverID)
	exists, err := lm.redis.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}
