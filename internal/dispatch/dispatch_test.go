package dispatch

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
	"github.com/go-redis/redis/v8"
)

func setupTestDispatch(t *testing.T) (*Dispatcher, *redis.Client) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   4, // Different DB for dispatch tests
	})

	ctx := context.Background()
	rdb.FlushDB(ctx)

	geoService := geospatial.New(rdb, "test-geo")
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	lockManager := NewLockManager(rdb, "test-lock", 10*time.Second)

	dispatcher := NewDispatcher(geoService, driverService, lockManager)

	return dispatcher, rdb
}

func TestLockManager_TryLock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   4,
	})
	defer rdb.Close()

	ctx := context.Background()
	rdb.FlushDB(ctx)

	lm := NewLockManager(rdb, "test-lock", 5*time.Second)

	// First lock should succeed
	locked, err := lm.TryLock(ctx, "driver1", "request1")
	if err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}
	if !locked {
		t.Error("Expected lock to succeed")
	}

	// Second lock should fail
	locked, err = lm.TryLock(ctx, "driver1", "request2")
	if err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}
	if locked {
		t.Error("Expected lock to fail (already locked)")
	}
}

func TestLockManager_Unlock(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   4,
	})
	defer rdb.Close()

	ctx := context.Background()
	rdb.FlushDB(ctx)

	lm := NewLockManager(rdb, "test-lock", 5*time.Second)

	// Lock
	lm.TryLock(ctx, "driver1", "request1")

	// Unlock with wrong request ID should fail
	err := lm.Unlock(ctx, "driver1", "wrong-request")
	if err != ErrLockNotHeld {
		t.Errorf("Expected ErrLockNotHeld, got %v", err)
	}

	// Unlock with correct request ID should succeed
	err = lm.Unlock(ctx, "driver1", "request1")
	if err != nil {
		t.Errorf("Unlock failed: %v", err)
	}

	// Should be unlocked now
	locked, _ := lm.IsLocked(ctx, "driver1")
	if locked {
		t.Error("Expected driver to be unlocked")
	}
}

func TestDispatcher_FindAndAssign_Success(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()

	// Setup: Add a driver location and set as available
	geoService := geospatial.New(rdb, "test-geo")
	geoService.AddLocation(ctx, geospatial.Location{
		ID:        "driver1",
		Longitude: 0,
		Latitude:  0,
	})

	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	driverService.SetStatus(ctx, "driver1", driver.StatusAvailable)

	// Make dispatch request
	result := dispatcher.FindAndAssign(ctx, DispatchRequest{
		RequestID: "test-request",
		Longitude: 0,
		Latitude:  0,
		RadiusKm:  10,
	})

	if !result.Success {
		t.Errorf("Expected success, got error: %s", result.Error)
	}
	if result.DriverID != "driver1" {
		t.Errorf("Expected driver1, got %s", result.DriverID)
	}
}

func TestDispatcher_FindAndAssign_NoDrivers(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()

	// No drivers added
	result := dispatcher.FindAndAssign(ctx, DispatchRequest{
		RequestID: "test-request",
		Longitude: 0,
		Latitude:  0,
		RadiusKm:  10,
	})

	if result.Success {
		t.Error("Expected failure with no drivers")
	}
	if result.Error != ErrNoDriversAvailable.Error() {
		t.Errorf("Expected NoDriversAvailable error, got: %s", result.Error)
	}
}

func TestDispatcher_ConcurrentAssignment(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()

	// Setup: Add one driver
	geoService := geospatial.New(rdb, "test-geo")
	geoService.AddLocation(ctx, geospatial.Location{
		ID:        "driver1",
		Longitude: 0,
		Latitude:  0,
	})

	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	driverService.SetStatus(ctx, "driver1", driver.StatusAvailable)

	// Make 10 concurrent requests for the same driver
	var wg sync.WaitGroup
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result := dispatcher.FindAndAssign(ctx, DispatchRequest{
				RequestID: string(rune('A' + i)),
				Longitude: 0,
				Latitude:  0,
				RadiusKm:  10,
			})
			if result.Success {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Only one request should succeed
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful assignment, got %d", successCount)
	}
}

func TestDispatcher_UpdateDriverLocation(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()

	err := dispatcher.UpdateDriverLocation(ctx, "driver1", 10.0, 20.0)
	if err != nil {
		t.Fatalf("UpdateDriverLocation failed: %v", err)
	}

	// Verify location was saved
	geoService := geospatial.New(rdb, "test-geo")
	loc, err := geoService.GetLocation(ctx, "driver1")
	if err != nil {
		t.Fatalf("GetLocation failed: %v", err)
	}

	if loc.Longitude < 9.9 || loc.Longitude > 10.1 {
		t.Errorf("Expected longitude ~10, got %f", loc.Longitude)
	}
}
