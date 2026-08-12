package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
)

func seedLifecycleDriver(t *testing.T, dispatcher *Dispatcher, driverID string, lon, lat float64) {
	t.Helper()
	ctx := context.Background()
	if err := dispatcher.geoService.AddLocation(ctx, geospatial.Location{
		ID: driverID, Longitude: lon, Latitude: lat,
	}); err != nil {
		t.Fatalf("AddLocation(%s): %v", driverID, err)
	}
	if err := dispatcher.driverService.SetStatus(ctx, driverID, driver.StatusAvailable); err != nil {
		t.Fatalf("SetStatus(%s): %v", driverID, err)
	}
}

func requestRide(dispatcher *Dispatcher, requestID, riderID string, lon, lat float64) DispatchResult {
	return dispatcher.FindAndAssign(context.Background(), DispatchRequest{
		RequestID: requestID,
		RiderID:   riderID,
		Longitude: lon,
		Latitude:  lat,
		RadiusKm:  2,
	})
}

func TestDispatcher_CancelAllowsRiderToRebook(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()
	ctx := context.Background()
	seedLifecycleDriver(t, dispatcher, "driver-1", 0, 0)

	first := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
	if !first.Success || first.Status != AssignmentEnRoute {
		t.Fatalf("first dispatch = %+v", first)
	}
	assignment, err := dispatcher.GetAssignment(ctx, first.RequestID)
	if err != nil {
		t.Fatalf("GetAssignment: %v", err)
	}
	if assignment.RiderID != "rider-1" || assignment.DriverID != "driver-1" ||
		assignment.PickupLongitude != 0 || assignment.PickupLatitude != 0 ||
		assignment.Status != AssignmentEnRoute {
		t.Fatalf("persisted assignment = %+v", assignment)
	}
	if ttl := rdb.TTL(ctx, dispatcher.lifecycle.assignmentKey(first.RequestID)).Val(); ttl <= 0 {
		t.Fatalf("assignment TTL = %s, want positive", ttl)
	}

	cancelled, err := dispatcher.CancelAssignment(ctx, first.RequestID)
	if err != nil {
		t.Fatalf("CancelAssignment: %v", err)
	}
	if cancelled.Status != AssignmentCancelled {
		t.Fatalf("cancelled status = %q", cancelled.Status)
	}
	status, err := dispatcher.driverService.GetStatus(ctx, "driver-1")
	if err != nil || status != driver.StatusAvailable {
		t.Fatalf("driver status after cancel = %q, err=%v", status, err)
	}
	if owner := rdb.Get(ctx, dispatcher.lockManager.lockKey("driver-1")).Val(); owner != "" {
		t.Fatalf("driver lock owner after cancel = %q", owner)
	}
	if active := rdb.Get(ctx, dispatcher.lifecycle.activeRiderKey("rider-1")).Val(); active != "" {
		t.Fatalf("active request after cancel = %q", active)
	}

	second := requestRide(dispatcher, "request-2", "rider-1", 0, 0)
	if !second.Success || second.Status != AssignmentEnRoute {
		t.Fatalf("rebook dispatch = %+v", second)
	}
}

func TestDispatcher_ArrivalClosesCancellationAndKeepsDriverBusy(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()
	ctx := context.Background()
	seedLifecycleDriver(t, dispatcher, "driver-1", 0.001, 0)
	seedLifecycleDriver(t, dispatcher, "driver-2", 0.002, 0)

	result := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
	if !result.Success {
		t.Fatalf("dispatch = %+v", result)
	}
	if _, err := dispatcher.ArriveAssignment(ctx, result.RequestID); !errors.Is(err, ErrDriverTooFar) {
		t.Fatalf("far arrival error = %v, want ErrDriverTooFar", err)
	}
	assignment, err := dispatcher.GetAssignment(ctx, result.RequestID)
	if err != nil || assignment.Status != AssignmentEnRoute {
		t.Fatalf("assignment after far arrival = %+v, err=%v", assignment, err)
	}

	if err := dispatcher.UpdateDriverLocation(ctx, result.DriverID, 0.0001, 0); err != nil {
		t.Fatalf("move driver to pickup: %v", err)
	}
	arrived, err := dispatcher.ArriveAssignment(ctx, result.RequestID)
	if err != nil || arrived.Status != AssignmentArrived {
		t.Fatalf("arrival = %+v, err=%v", arrived, err)
	}
	if _, err := dispatcher.CancelAssignment(ctx, result.RequestID); !errors.Is(err, ErrAssignmentStateConflict) {
		t.Fatalf("late cancel error = %v, want state conflict", err)
	}
	status, err := dispatcher.driverService.GetStatus(ctx, result.DriverID)
	if err != nil || status != driver.StatusBusy {
		t.Fatalf("driver status after late cancel = %q, err=%v", status, err)
	}

	rebook := requestRide(dispatcher, "request-2", "rider-1", 0, 0)
	if !errors.Is(rebook.Cause, ErrActiveAssignment) {
		t.Fatalf("arrived rider rebook = %+v, want active conflict", rebook)
	}
}

func TestDispatcher_CancelArriveRaceHasSingleWinner(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()
	ctx := context.Background()
	seedLifecycleDriver(t, dispatcher, "driver-1", 0, 0)

	result := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
	if !result.Success {
		t.Fatalf("dispatch = %+v", result)
	}

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, err := dispatcher.CancelAssignment(ctx, result.RequestID)
		errs <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		_, err := dispatcher.ArriveAssignment(ctx, result.RequestID)
		errs <- err
	}()
	close(start)
	wg.Wait()
	close(errs)

	successes := 0
	conflicts := 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrAssignmentStateConflict):
			conflicts++
		default:
			t.Fatalf("race returned unexpected error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("race successes=%d conflicts=%d", successes, conflicts)
	}

	assignment, err := dispatcher.GetAssignment(ctx, result.RequestID)
	if err != nil {
		t.Fatalf("GetAssignment: %v", err)
	}
	status, err := dispatcher.driverService.GetStatus(ctx, result.DriverID)
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	switch assignment.Status {
	case AssignmentCancelled:
		if status != driver.StatusAvailable {
			t.Fatalf("cancel won but driver status = %q", status)
		}
	case AssignmentArrived:
		if status != driver.StatusBusy {
			t.Fatalf("arrive won but driver status = %q", status)
		}
	default:
		t.Fatalf("race final assignment = %+v", assignment)
	}
}

func TestDispatcher_ArrivalRequiresCurrentRiderAndBusyDriver(t *testing.T) {
	t.Run("does not overwrite a newer active request", func(t *testing.T) {
		dispatcher, rdb := setupTestDispatch(t)
		defer rdb.Close()
		ctx := context.Background()
		seedLifecycleDriver(t, dispatcher, "driver-1", 0, 0)
		result := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
		if !result.Success {
			t.Fatalf("dispatch = %+v", result)
		}
		activeKey := dispatcher.lifecycle.activeRiderKey("rider-1")
		if err := rdb.Set(ctx, activeKey, "newer-request", 0).Err(); err != nil {
			t.Fatalf("replace active request: %v", err)
		}
		if _, err := dispatcher.ArriveAssignment(ctx, result.RequestID); !errors.Is(err, ErrAssignmentStateConflict) {
			t.Fatalf("arrival error = %v, want state conflict", err)
		}
		if active := rdb.Get(ctx, activeKey).Val(); active != "newer-request" {
			t.Fatalf("arrival overwrote active request with %q", active)
		}
	})

	t.Run("requires driver to remain busy", func(t *testing.T) {
		dispatcher, rdb := setupTestDispatch(t)
		defer rdb.Close()
		ctx := context.Background()
		seedLifecycleDriver(t, dispatcher, "driver-1", 0, 0)
		result := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
		if !result.Success {
			t.Fatalf("dispatch = %+v", result)
		}
		if err := dispatcher.driverService.SetAvailable(ctx, result.DriverID); err != nil {
			t.Fatalf("make driver available: %v", err)
		}
		if _, err := dispatcher.ArriveAssignment(ctx, result.RequestID); !errors.Is(err, ErrAssignmentStateConflict) {
			t.Fatalf("arrival error = %v, want state conflict", err)
		}
		assignment, err := dispatcher.GetAssignment(ctx, result.RequestID)
		if err != nil || assignment.Status != AssignmentEnRoute {
			t.Fatalf("assignment after rejected arrival = %+v, err=%v", assignment, err)
		}
	})
}

func TestDispatcher_CreateConflictRollsBackOwnedDriver(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()
	ctx := context.Background()
	seedLifecycleDriver(t, dispatcher, "driver-2", 0, 0)

	err := dispatcher.lifecycle.create(ctx, Assignment{
		RequestID:       "active-request",
		RiderID:         "rider-1",
		DriverID:        "driver-1",
		PickupLongitude: 0,
		PickupLatitude:  0,
		Status:          AssignmentEnRoute,
	})
	if err != nil {
		t.Fatalf("create active assignment: %v", err)
	}

	_, err = dispatcher.tryAssignDriver(ctx, DispatchRequest{
		RequestID: "losing-request",
		RiderID:   "rider-1",
		Longitude: 0,
		Latitude:  0,
		RadiusKm:  2,
	}, geospatial.Location{ID: "driver-2"})
	if !errors.Is(err, ErrActiveAssignment) {
		t.Fatalf("tryAssignDriver error = %v, want active conflict", err)
	}
	status, err := dispatcher.driverService.GetStatus(ctx, "driver-2")
	if err != nil || status != driver.StatusAvailable {
		t.Fatalf("rolled-back driver status = %q, err=%v", status, err)
	}
	if owner := rdb.Get(ctx, dispatcher.lockManager.lockKey("driver-2")).Val(); owner != "" {
		t.Fatalf("rolled-back driver lock owner = %q", owner)
	}
}

func TestDispatcher_ActiveDriverLocationRefreshRestoresBusy(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()
	ctx := context.Background()
	seedLifecycleDriver(t, dispatcher, "driver-1", 0, 0)

	ride := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
	if !ride.Success {
		t.Fatalf("dispatch = %+v", ride)
	}
	if err := rdb.Del(ctx, dispatcher.lifecycle.driverStatusKey("driver-1")).Err(); err != nil {
		t.Fatalf("expire driver status: %v", err)
	}
	if err := dispatcher.UpdateDriverLocation(ctx, "driver-1", 0.0001, 0); err != nil {
		t.Fatalf("UpdateDriverLocation: %v", err)
	}
	status, err := dispatcher.driverService.GetStatus(ctx, "driver-1")
	if err != nil || status != driver.StatusBusy {
		t.Fatalf("active driver status after location = %q, err=%v", status, err)
	}

	legacy := dispatcher.FindAndAssign(ctx, DispatchRequest{
		RequestID: "legacy-request", Longitude: 0, Latitude: 0, RadiusKm: 2,
	})
	if legacy.Success {
		t.Fatalf("legacy dispatch reused active driver: %+v", legacy)
	}
}

func TestDispatcher_SkipsDurablyOwnedCandidateAfterLockExpiry(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()
	ctx := context.Background()
	seedLifecycleDriver(t, dispatcher, "owned-driver", 0, 0)
	seedLifecycleDriver(t, dispatcher, "next-driver", 0.001, 0)

	ride := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
	if !ride.Success || ride.DriverID != "owned-driver" {
		t.Fatalf("dispatch = %+v", ride)
	}
	if err := rdb.Del(ctx, dispatcher.lockManager.lockKey("owned-driver")).Err(); err != nil {
		t.Fatalf("expire short lock: %v", err)
	}
	if err := dispatcher.driverService.SetAvailable(ctx, "owned-driver"); err != nil {
		t.Fatalf("simulate status drift: %v", err)
	}

	legacy := dispatcher.FindAndAssign(ctx, DispatchRequest{
		RequestID: "legacy-request", Longitude: 0, Latitude: 0, RadiusKm: 2,
	})
	if !legacy.Success || legacy.DriverID != "next-driver" {
		t.Fatalf("legacy dispatch = %+v, want next-driver", legacy)
	}
	if owner := rdb.Get(ctx, dispatcher.lifecycle.activeDriverKey("owned-driver")).Val(); owner != "request-1" {
		t.Fatalf("owned driver durable owner = %q", owner)
	}
}

func TestDispatcher_ArrivalRefreshesBusyTTL(t *testing.T) {
	dispatcher, rdb := setupTestDispatch(t)
	defer rdb.Close()
	ctx := context.Background()
	seedLifecycleDriver(t, dispatcher, "driver-1", 0, 0)
	ride := requestRide(dispatcher, "request-1", "rider-1", 0, 0)
	if !ride.Success {
		t.Fatalf("dispatch = %+v", ride)
	}
	statusKey := dispatcher.lifecycle.driverStatusKey("driver-1")
	if err := rdb.Expire(ctx, statusKey, time.Second).Err(); err != nil {
		t.Fatalf("shorten driver TTL: %v", err)
	}
	if _, err := dispatcher.ArriveAssignment(ctx, ride.RequestID); err != nil {
		t.Fatalf("ArriveAssignment: %v", err)
	}
	if ttl := rdb.TTL(ctx, statusKey).Val(); ttl < 20*time.Second || ttl > 30*time.Second {
		t.Fatalf("driver busy TTL after arrival = %s", ttl)
	}
}
