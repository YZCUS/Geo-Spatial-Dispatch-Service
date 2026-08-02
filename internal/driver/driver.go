package driver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	ErrDriverNotFound = errors.New("driver not found")
	ErrInvalidStatus  = errors.New("invalid status")
	ErrStatusConflict = errors.New("status conflict")
	ErrDriverOffline  = errors.New("driver is offline")
)

var setBusyScript = redis.NewScript(`
	local current = redis.call('GET', KEYS[1])
	if current == 'available' then
		redis.call('SET', KEYS[1], 'busy', 'EX', ARGV[1])
		return 1
	elseif current == nil or current == false then
		return -1
	else
		return 0
	end
`)

// DriverStatus represents the current state of a driver
type DriverStatus string

const (
	StatusAvailable DriverStatus = "available"
	StatusBusy      DriverStatus = "busy"
	StatusOffline   DriverStatus = "offline"
)

// Driver represents a driver in the system
type Driver struct {
	ID        string       `json:"id"`
	Status    DriverStatus `json:"status"`
	Longitude float64      `json:"longitude"`
	Latitude  float64      `json:"latitude"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// DriverService manages driver state
type DriverService struct {
	redis      *redis.Client
	keyPrefix  string
	defaultTTL time.Duration
}

// NewDriverService creates a new driver service
func NewDriverService(rdb *redis.Client, keyPrefix string, ttl time.Duration) *DriverService {
	if keyPrefix == "" {
		keyPrefix = "driver"
	}
	if ttl == 0 {
		ttl = 30 * time.Second // Default: offline if no update for 30 seconds
	}
	log.Printf("[DriverService] Initialized with prefix=%s ttl=%v", keyPrefix, ttl)
	return &DriverService{
		redis:      rdb,
		keyPrefix:  keyPrefix,
		defaultTTL: ttl,
	}
}

// statusKey returns the Redis key for driver status
func (ds *DriverService) statusKey(driverID string) string {
	return fmt.Sprintf("%s:status:%s", ds.keyPrefix, driverID)
}

// SetStatus updates the driver's status
func (ds *DriverService) SetStatus(ctx context.Context, driverID string, status DriverStatus) error {
	if strings.TrimSpace(driverID) == "" {
		return ErrDriverNotFound
	}
	if status != StatusAvailable && status != StatusBusy && status != StatusOffline {
		return ErrInvalidStatus
	}

	key := ds.statusKey(driverID)
	log.Printf("[DriverService] Setting status driver=%s status=%s", driverID, status)

	if status == StatusOffline {
		// Remove the key when offline
		return ds.redis.Del(ctx, key).Err()
	}

	// Set with TTL - if driver doesn't heartbeat, they'll go offline
	err := ds.redis.Set(ctx, key, string(status), ds.defaultTTL).Err()
	if err != nil {
		log.Printf("[DriverService] Error setting status: %v", err)
	}
	return err
}

// GetStatus retrieves the driver's current status
func (ds *DriverService) GetStatus(ctx context.Context, driverID string) (DriverStatus, error) {
	if strings.TrimSpace(driverID) == "" {
		return "", ErrDriverNotFound
	}

	key := ds.statusKey(driverID)
	status, err := ds.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return StatusOffline, nil // No key means offline
	}
	if err != nil {
		return "", err
	}
	return DriverStatus(status), nil
}

// Heartbeat refreshes the driver's TTL and optionally updates location
func (ds *DriverService) Heartbeat(ctx context.Context, driverID string) error {
	if strings.TrimSpace(driverID) == "" {
		return ErrDriverNotFound
	}

	key := ds.statusKey(driverID)

	// Refresh the existing status in one round trip.
	refreshed, err := ds.redis.Expire(ctx, key, ds.defaultTTL).Result()
	if err != nil {
		return err
	}
	if refreshed {
		return nil
	}

	// A heartbeat alone must not revive an expired driver at a stale GEO
	// position. A fresh location update is required to come back online.
	return ErrDriverOffline
}

// IsAvailable checks if a driver is available for assignment
func (ds *DriverService) IsAvailable(ctx context.Context, driverID string) (bool, error) {
	status, err := ds.GetStatus(ctx, driverID)
	if err != nil {
		return false, err
	}
	return status == StatusAvailable, nil
}

// SetBusy atomically transitions a driver from available to busy
// Returns error if driver is not currently available
func (ds *DriverService) SetBusy(ctx context.Context, driverID string) error {
	key := ds.statusKey(driverID)

	result, err := setBusyScript.Run(ctx, ds.redis, []string{key}, int(ds.defaultTTL.Seconds())).Int()
	if err != nil {
		return err
	}

	switch result {
	case 1:
		log.Printf("[DriverService] Driver %s set to busy", driverID)
		return nil
	case 0:
		return ErrStatusConflict
	case -1:
		return ErrDriverOffline
	default:
		return ErrStatusConflict
	}
}

// SetAvailable transitions a driver to available status
func (ds *DriverService) SetAvailable(ctx context.Context, driverID string) error {
	return ds.SetStatus(ctx, driverID, StatusAvailable)
}

// GetAvailableDrivers returns a list of all available driver IDs
func (ds *DriverService) GetAvailableDrivers(ctx context.Context) ([]string, error) {
	var available []string
	err := ds.scanStatuses(ctx, func(driverID string, status DriverStatus) {
		if status == StatusAvailable {
			available = append(available, driverID)
		}
	})
	if err != nil {
		return nil, err
	}

	return available, nil
}

// GetStatuses retrieves driver statuses in one Redis round trip.
func (ds *DriverService) GetStatuses(ctx context.Context, driverIDs []string) (map[string]DriverStatus, error) {
	statuses := make(map[string]DriverStatus, len(driverIDs))
	if len(driverIDs) == 0 {
		return statuses, nil
	}

	keys := make([]string, len(driverIDs))
	for i, driverID := range driverIDs {
		keys[i] = ds.statusKey(driverID)
		statuses[driverID] = StatusOffline
	}

	values, err := ds.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		if value != nil {
			statuses[driverIDs[i]] = DriverStatus(fmt.Sprint(value))
		}
	}
	return statuses, nil
}

// GetDriverStats returns active and available driver counts without using KEYS.
func (ds *DriverService) GetDriverStats(ctx context.Context) (total, available int, err error) {
	err = ds.scanStatuses(ctx, func(_ string, status DriverStatus) {
		total++
		if status == StatusAvailable {
			available++
		}
	})
	return total, available, err
}

func (ds *DriverService) scanStatuses(ctx context.Context, visit func(driverID string, status DriverStatus)) error {
	pattern := fmt.Sprintf("%s:status:*", ds.keyPrefix)
	keyPrefix := fmt.Sprintf("%s:status:", ds.keyPrefix)
	var cursor uint64

	for {
		keys, nextCursor, err := ds.redis.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return err
		}

		if len(keys) > 0 {
			values, err := ds.redis.MGet(ctx, keys...).Result()
			if err != nil {
				return err
			}
			for i, value := range values {
				if value == nil {
					continue
				}
				visit(strings.TrimPrefix(keys[i], keyPrefix), DriverStatus(fmt.Sprint(value)))
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			return nil
		}
	}
}
