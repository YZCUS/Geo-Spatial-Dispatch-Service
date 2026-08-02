package driver

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

func setupTestDriver(t *testing.T) *DriverService {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   3, // Different DB for driver tests
	})

	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush Redis test DB: %v", err)
	}

	return NewDriverService(rdb, "test-driver", 5*time.Second)
}

func TestDriverService_SetAndGetStatus(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Set status
	err := ds.SetStatus(ctx, "driver1", StatusAvailable)
	if err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}

	// Get status
	status, err := ds.GetStatus(ctx, "driver1")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if status != StatusAvailable {
		t.Errorf("Expected status 'available', got '%s'", status)
	}
}

func TestDriverService_GetStatus_Offline(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Non-existent driver should be offline
	status, err := ds.GetStatus(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}

	if status != StatusOffline {
		t.Errorf("Expected status 'offline', got '%s'", status)
	}
}

func TestDriverService_SetBusy_Success(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Set driver as available first
	ds.SetStatus(ctx, "driver1", StatusAvailable)

	// Try to set busy
	err := ds.SetBusy(ctx, "driver1")
	if err != nil {
		t.Fatalf("SetBusy failed: %v", err)
	}

	// Verify status
	status, _ := ds.GetStatus(ctx, "driver1")
	if status != StatusBusy {
		t.Errorf("Expected status 'busy', got '%s'", status)
	}
}

func TestDriverService_SetBusy_Conflict(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Set driver as busy first
	ds.SetStatus(ctx, "driver1", StatusBusy)

	// Try to set busy again - should fail
	err := ds.SetBusy(ctx, "driver1")
	if err != ErrStatusConflict {
		t.Errorf("Expected ErrStatusConflict, got %v", err)
	}
}

func TestDriverService_SetBusy_Offline(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Try to set busy on non-existent driver
	err := ds.SetBusy(ctx, "nonexistent")
	if err != ErrDriverOffline {
		t.Errorf("Expected ErrDriverOffline, got %v", err)
	}
}

func TestDriverService_InvalidStatus(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	err := ds.SetStatus(ctx, "driver1", "invalid")
	if err != ErrInvalidStatus {
		t.Errorf("Expected ErrInvalidStatus, got %v", err)
	}
}

func TestDriverService_Heartbeat(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	if err := ds.SetStatus(ctx, "driver1", StatusAvailable); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}

	err := ds.Heartbeat(ctx, "driver1")
	if err != nil {
		t.Fatalf("Heartbeat failed: %v", err)
	}

	status, _ := ds.GetStatus(ctx, "driver1")
	if status != StatusAvailable {
		t.Errorf("Expected status 'available' after heartbeat, got '%s'", status)
	}
}

func TestDriverService_HeartbeatDoesNotReviveOfflineDriver(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	if err := ds.Heartbeat(ctx, "driver1"); err != ErrDriverOffline {
		t.Fatalf("Expected ErrDriverOffline, got %v", err)
	}

	status, err := ds.GetStatus(ctx, "driver1")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status != StatusOffline {
		t.Fatalf("Expected driver to remain offline, got %s", status)
	}
}

func TestDriverService_IsAvailable(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	ds.SetStatus(ctx, "driver1", StatusAvailable)
	ds.SetStatus(ctx, "driver2", StatusBusy)

	avail1, _ := ds.IsAvailable(ctx, "driver1")
	avail2, _ := ds.IsAvailable(ctx, "driver2")
	avail3, _ := ds.IsAvailable(ctx, "nonexistent")

	if !avail1 {
		t.Error("driver1 should be available")
	}
	if avail2 {
		t.Error("driver2 should not be available")
	}
	if avail3 {
		t.Error("nonexistent driver should not be available")
	}
}

func TestDriverService_SetOffline(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Set driver as available
	ds.SetStatus(ctx, "driver1", StatusAvailable)

	// Set offline
	err := ds.SetStatus(ctx, "driver1", StatusOffline)
	if err != nil {
		t.Fatalf("SetStatus offline failed: %v", err)
	}

	// Should now be offline
	status, _ := ds.GetStatus(ctx, "driver1")
	if status != StatusOffline {
		t.Errorf("Expected status 'offline', got '%s'", status)
	}
}

func TestDriverService_GetAvailableDrivers(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Set up some drivers
	ds.SetStatus(ctx, "driver1", StatusAvailable)
	ds.SetStatus(ctx, "driver2", StatusBusy)
	ds.SetStatus(ctx, "driver3", StatusAvailable)

	available, err := ds.GetAvailableDrivers(ctx)
	if err != nil {
		t.Fatalf("GetAvailableDrivers failed: %v", err)
	}

	if len(available) != 2 {
		t.Errorf("Expected 2 available drivers, got %d", len(available))
	}
}

func TestDriverService_ConcurrentSetBusy(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	// Set driver as available
	ds.SetStatus(ctx, "driver1", StatusAvailable)

	// Try concurrent SetBusy
	successCount := 0
	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func() {
			err := ds.SetBusy(ctx, "driver1")
			if err == nil {
				successCount++
			}
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Only one should succeed
	if successCount != 1 {
		t.Errorf("Expected exactly 1 successful SetBusy, got %d", successCount)
	}
}

func TestDriverService_GetDriverStats(t *testing.T) {
	ds := setupTestDriver(t)
	ctx := context.Background()

	if err := ds.SetStatus(ctx, "driver1", StatusAvailable); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}
	if err := ds.SetStatus(ctx, "driver2", StatusBusy); err != nil {
		t.Fatalf("SetStatus failed: %v", err)
	}

	total, available, err := ds.GetDriverStats(ctx)
	if err != nil {
		t.Fatalf("GetDriverStats failed: %v", err)
	}
	if total != 2 || available != 1 {
		t.Fatalf("Expected total=2 available=1, got total=%d available=%d", total, available)
	}
}
