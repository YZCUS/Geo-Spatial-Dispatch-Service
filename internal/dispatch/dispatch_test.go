package dispatch

import (
	"context"
	"fmt"
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
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush Redis test DB: %v", err)
	}

	geoService := geospatial.New(rdb, "test-geo")
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	lockManager := NewLockManager(rdb, "test-lock", 10*time.Second)

	dispatcher := NewDispatcher(geoService, driverService, lockManager, LifecycleConfig{
		AssignmentPrefix:   "test-assignment",
		RiderActivePrefix:  "test-rider-active",
		DriverActivePrefix: "test-driver-active",
		DriverStatusPrefix: "test-driver:status",
		AssignmentTTL:      time.Hour,
		DriverStatusTTL:    30 * time.Second,
		ArrivalThresholdKm: 0.05,
	})

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

func TestDispatcher_ConcurrentAssignmentsUseDistinctDrivers(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()
	geoService := geospatial.New(rdb, "test-geo")
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)

	for i := 0; i < 3; i++ {
		driverID := fmt.Sprintf("driver%d", i+1)
		if err := geoService.AddLocation(ctx, geospatial.Location{
			ID:        driverID,
			Longitude: float64(i) * 0.001,
			Latitude:  0,
		}); err != nil {
			t.Fatalf("AddLocation(%s) failed: %v", driverID, err)
		}
		if err := driverService.SetStatus(ctx, driverID, driver.StatusAvailable); err != nil {
			t.Fatalf("SetStatus(%s) failed: %v", driverID, err)
		}
	}

	start := make(chan struct{})
	results := make(chan DispatchResult, 3)
	for i := 0; i < 3; i++ {
		go func(i int) {
			<-start
			results <- dispatcher.FindAndAssign(ctx, DispatchRequest{
				RequestID: fmt.Sprintf("rider-request-%d", i+1),
				Longitude: 0,
				Latitude:  0,
				RadiusKm:  10,
			})
		}(i)
	}
	close(start)

	assignedDrivers := make(map[string]string, 3)
	for i := 0; i < 3; i++ {
		result := <-results
		if !result.Success {
			t.Fatalf("Concurrent request %s failed: %s", result.RequestID, result.Error)
		}
		if previousRequest, duplicate := assignedDrivers[result.DriverID]; duplicate {
			t.Fatalf(
				"Driver %s assigned to both %s and %s",
				result.DriverID,
				previousRequest,
				result.RequestID,
			)
		}
		assignedDrivers[result.DriverID] = result.RequestID
	}

	if len(assignedDrivers) != 3 {
		t.Fatalf("Expected 3 distinct assigned drivers, got %d", len(assignedDrivers))
	}
}

func TestDispatcher_UsesLatestDriverLocationsAtRequestTime(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	for _, driverID := range []string{"initially-near", "newly-near"} {
		if err := driverService.SetStatus(ctx, driverID, driver.StatusAvailable); err != nil {
			t.Fatalf("SetStatus(%s) failed: %v", driverID, err)
		}
	}

	if err := dispatcher.UpdateDriverLocation(ctx, "initially-near", 0.001, 0); err != nil {
		t.Fatalf("Set initial location for initially-near: %v", err)
	}
	if err := dispatcher.UpdateDriverLocation(ctx, "newly-near", 0.010, 0); err != nil {
		t.Fatalf("Set initial location for newly-near: %v", err)
	}

	// Simulate the live fleet moving before the rider requests a car.
	if err := dispatcher.UpdateDriverLocation(ctx, "initially-near", 0.020, 0); err != nil {
		t.Fatalf("Move initially-near: %v", err)
	}
	if err := dispatcher.UpdateDriverLocation(ctx, "newly-near", 0.0001, 0); err != nil {
		t.Fatalf("Move newly-near: %v", err)
	}

	result := dispatcher.FindAndAssign(ctx, DispatchRequest{
		RequestID: "request-after-movement",
		Longitude: 0,
		Latitude:  0,
		RadiusKm:  10,
	})
	if !result.Success || result.DriverID != "newly-near" {
		t.Fatalf("Expected latest position to select newly-near, got %+v", result)
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

	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	status, err := driverService.GetStatus(ctx, "driver1")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status != driver.StatusAvailable {
		t.Fatalf("Expected fresh location to make driver available, got %s", status)
	}
}

func TestDispatcher_SkipsMoreThanFiveUnavailableDrivers(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()
	geoService := geospatial.New(rdb, "test-geo")
	for i := 1; i <= 6; i++ {
		if err := geoService.AddLocation(ctx, geospatial.Location{
			ID:        fmt.Sprintf("driver%d", i),
			Longitude: float64(i) * 0.001,
			Latitude:  0,
		}); err != nil {
			t.Fatalf("AddLocation failed: %v", err)
		}
	}

	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	if err := driverService.SetStatus(ctx, "driver6", driver.StatusAvailable); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}

	result := dispatcher.FindAndAssign(ctx, DispatchRequest{
		RequestID: "sixth-driver",
		Longitude: 0,
		Latitude:  0,
		RadiusKm:  10,
	})
	if !result.Success || result.DriverID != "driver6" {
		t.Fatalf("Expected driver6 assignment, got %+v", result)
	}
}

func TestDispatcher_ReturnsRedisDistance(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()
	geoService := geospatial.New(rdb, "test-geo")
	if err := geoService.AddLocation(ctx, geospatial.Location{
		ID:        "north",
		Longitude: 0,
		Latitude:  1,
	}); err != nil {
		t.Fatalf("AddLocation failed: %v", err)
	}
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	if err := driverService.SetStatus(ctx, "north", driver.StatusAvailable); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}

	result := dispatcher.FindAndAssign(ctx, DispatchRequest{
		RequestID: "distance",
		Longitude: 0,
		Latitude:  0,
		RadiusKm:  120,
	})
	if !result.Success {
		t.Fatalf("Expected assignment, got %+v", result)
	}
	if result.Distance < 110 || result.Distance > 112 {
		t.Errorf("Expected distance around 111km, got %.2f", result.Distance)
	}
}

func TestDispatcher_ReleaseRequiresLockOwnership(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	if err := driverService.SetStatus(ctx, "driver1", driver.StatusBusy); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}
	if locked, err := dispatcher.lockManager.TryLock(ctx, "driver1", "owner"); err != nil || !locked {
		t.Fatalf("TryLock failed: locked=%v err=%v", locked, err)
	}

	if err := dispatcher.ReleaseDriver(ctx, "driver1", "not-owner"); err != ErrLockNotHeld {
		t.Fatalf("Expected ErrLockNotHeld, got %v", err)
	}
	status, err := driverService.GetStatus(ctx, "driver1")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status != driver.StatusBusy {
		t.Fatalf("Expected driver to remain busy, got %s", status)
	}
}

func TestDispatcher_ReleaseAfterLockExpiry(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()

	ctx := context.Background()
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	if err := driverService.SetStatus(ctx, "driver1", driver.StatusBusy); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}
	if locked, err := dispatcher.lockManager.TryLock(ctx, "driver1", "owner"); err != nil || !locked {
		t.Fatalf("TryLock failed: locked=%v err=%v", locked, err)
	}
	if err := rdb.Del(ctx, dispatcher.lockManager.lockKey("driver1")).Err(); err != nil {
		t.Fatalf("Failed to expire lock: %v", err)
	}

	if err := dispatcher.ReleaseDriver(ctx, "driver1", "owner"); err != nil {
		t.Fatalf("ReleaseDriver failed after lock expiry: %v", err)
	}
	status, err := driverService.GetStatus(ctx, "driver1")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status != driver.StatusAvailable {
		t.Fatalf("Expected driver to become available, got %s", status)
	}
}
