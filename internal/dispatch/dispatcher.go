package dispatch

import (
	"context"
	"errors"
	"log"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
	"github.com/google/uuid"
)

// DispatchResult represents the outcome of a dispatch request
type DispatchResult struct {
	Success   bool             `json:"success"`
	DriverID  string           `json:"driver_id,omitempty"`
	RequestID string           `json:"request_id"`
	Distance  float64          `json:"distance_km,omitempty"`
	Status    AssignmentStatus `json:"status,omitempty"`
	Error     string           `json:"error,omitempty"`
	Cause     error            `json:"-"`
}

// Dispatcher coordinates driver assignment
type Dispatcher struct {
	geoService    *geospatial.GeoService
	driverService *driver.DriverService
	lockManager   *LockManager
	lifecycle     *lifecycleManager
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(
	geoService *geospatial.GeoService,
	driverService *driver.DriverService,
	lockManager *LockManager,
	lifecycleConfig ...LifecycleConfig,
) *Dispatcher {
	config := LifecycleConfig{}
	if len(lifecycleConfig) > 0 {
		config = lifecycleConfig[0]
	}
	return &Dispatcher{
		geoService:    geoService,
		driverService: driverService,
		lockManager:   lockManager,
		lifecycle:     newLifecycleManager(lockManager.redis, lockManager, config),
	}
}

// DispatchRequest represents an incoming dispatch request
type DispatchRequest struct {
	RequestID string  `json:"request_id"`
	RiderID   string  `json:"rider_id,omitempty"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	RadiusKm  float64 `json:"radius_km"`
}

// FindAndAssign finds the nearest available driver and assigns them
func (d *Dispatcher) FindAndAssign(ctx context.Context, req DispatchRequest) DispatchResult {
	if req.RequestID == "" {
		req.RequestID = uuid.New().String()
	}
	if req.RadiusKm == 0 {
		req.RadiusKm = 5.0 // Default 5km radius
	}
	if req.RiderID != "" {
		active, err := d.lifecycle.active(ctx, req.RiderID)
		if err != nil {
			return DispatchResult{RequestID: req.RequestID, Error: err.Error(), Cause: err}
		}
		if active != nil {
			return DispatchResult{
				RequestID: req.RequestID,
				Error:     ErrActiveAssignment.Error(),
				Cause:     ErrActiveAssignment,
			}
		}
	}

	log.Printf("[Dispatcher] Processing request %s at (%.4f, %.4f) radius=%.2fkm",
		req.RequestID, req.Longitude, req.Latitude, req.RadiusKm)

	// Find nearby drivers (sorted by distance)
	nearby, err := d.geoService.FindNearby(ctx, req.Longitude, req.Latitude, req.RadiusKm)
	if err != nil {
		log.Printf("[Dispatcher] Error finding nearby drivers: %v", err)
		return DispatchResult{
			Success:   false,
			RequestID: req.RequestID,
			Error:     err.Error(),
		}
	}

	if len(nearby) == 0 {
		log.Printf("[Dispatcher] No nearby drivers found for request %s", req.RequestID)
		return DispatchResult{
			Success:   false,
			RequestID: req.RequestID,
			Error:     ErrNoDriversAvailable.Error(),
		}
	}

	log.Printf("[Dispatcher] Found %d nearby drivers for request %s", len(nearby), req.RequestID)

	driverIDs := make([]string, len(nearby))
	for i, loc := range nearby {
		driverIDs[i] = loc.ID
	}
	statuses, err := d.driverService.GetStatuses(ctx, driverIDs)
	if err != nil {
		log.Printf("[Dispatcher] Error loading driver statuses: %v", err)
		return DispatchResult{
			Success:   false,
			RequestID: req.RequestID,
			Error:     err.Error(),
		}
	}

	// Try to assign each driver in order (nearest first)
	for _, loc := range nearby {
		if statuses[loc.ID] != driver.StatusAvailable {
			continue
		}

		result, err := d.tryAssignDriver(ctx, req, loc)
		if err != nil {
			log.Printf("[Dispatcher] Error assigning driver %s: %v", loc.ID, err)
			return DispatchResult{
				Success:   false,
				RequestID: req.RequestID,
				Error:     err.Error(),
				Cause:     err,
			}
		}
		if result.Success {
			return result
		}
		log.Printf("[Dispatcher] Failed to assign driver %s, trying next", loc.ID)
	}

	return DispatchResult{
		Success:   false,
		RequestID: req.RequestID,
		Error:     ErrNoDriversAvailable.Error(),
	}
}

// tryAssignDriver attempts to assign a specific driver
func (d *Dispatcher) tryAssignDriver(ctx context.Context, req DispatchRequest, loc geospatial.Location) (DispatchResult, error) {
	driverID := loc.ID
	var assignmentStatus AssignmentStatus

	// Step 1: Try to acquire lock
	locked, err := d.lockManager.TryLock(ctx, driverID, req.RequestID)
	if err != nil {
		return DispatchResult{Success: false, RequestID: req.RequestID}, err
	}
	if !locked {
		return DispatchResult{Success: false, RequestID: req.RequestID}, nil
	}
	owner, err := d.lifecycle.driverOwner(ctx, driverID)
	if err != nil {
		_ = d.lockManager.Unlock(ctx, driverID, req.RequestID)
		return DispatchResult{Success: false, RequestID: req.RequestID}, err
	}
	if owner != "" {
		_ = d.lockManager.Unlock(ctx, driverID, req.RequestID)
		return DispatchResult{Success: false, RequestID: req.RequestID}, nil
	}

	// Step 2: Double-check availability and set busy atomically
	err = d.driverService.SetBusy(ctx, driverID)
	if err != nil {
		// Release lock if we couldn't set busy
		_ = d.lockManager.Unlock(ctx, driverID, req.RequestID)
		if errors.Is(err, driver.ErrStatusConflict) || errors.Is(err, driver.ErrDriverOffline) {
			return DispatchResult{Success: false, RequestID: req.RequestID}, nil
		}
		return DispatchResult{Success: false, RequestID: req.RequestID}, err
	}

	if req.RiderID != "" {
		assignment := Assignment{
			RequestID:       req.RequestID,
			RiderID:         req.RiderID,
			DriverID:        driverID,
			PickupLongitude: req.Longitude,
			PickupLatitude:  req.Latitude,
			Status:          AssignmentEnRoute,
		}
		if err := validatePickup(assignment); err != nil {
			_ = d.ReleaseDriver(ctx, driverID, req.RequestID)
			return DispatchResult{Success: false, RequestID: req.RequestID}, err
		}
		if err := d.lifecycle.create(ctx, assignment); err != nil {
			if errors.Is(err, ErrDriverOwnershipConflict) {
				_ = d.lockManager.Unlock(ctx, driverID, req.RequestID)
				active, refreshErr := d.lifecycle.refreshActiveDriver(ctx, driverID)
				if refreshErr != nil {
					return DispatchResult{Success: false, RequestID: req.RequestID}, refreshErr
				}
				if !active {
					_ = d.driverService.SetAvailable(ctx, driverID)
				}
				return DispatchResult{Success: false, RequestID: req.RequestID}, nil
			}
			if releaseErr := d.ReleaseDriver(ctx, driverID, req.RequestID); releaseErr != nil {
				log.Printf("[Dispatcher] Failed to roll back driver %s for request %s: %v", driverID, req.RequestID, releaseErr)
			}
			return DispatchResult{Success: false, RequestID: req.RequestID}, err
		}
		assignmentStatus = AssignmentEnRoute
	}

	log.Printf("[Dispatcher] Successfully assigned driver %s to request %s", driverID, req.RequestID)

	return DispatchResult{
		Success:   true,
		DriverID:  driverID,
		RequestID: req.RequestID,
		Distance:  loc.DistanceKm,
		Status:    assignmentStatus,
	}, nil
}

// CancelAssignment cancels only an en-route assignment. The lifecycle manager
// atomically changes assignment state, releases the driver, clears the owned
// lock, and frees the rider to request another ride.
func (d *Dispatcher) CancelAssignment(ctx context.Context, requestID string) (*Assignment, error) {
	if err := validateLifecycleID(requestID); err != nil {
		return nil, err
	}
	return d.lifecycle.cancel(ctx, requestID)
}

// ArriveAssignment confirms the driver's latest Redis GEO position is close
// enough to the pickup before atomically transitioning en_route to arrived.
func (d *Dispatcher) ArriveAssignment(ctx context.Context, requestID string) (*Assignment, error) {
	if err := validateLifecycleID(requestID); err != nil {
		return nil, err
	}
	assignment, err := d.lifecycle.get(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if assignment.Status != AssignmentEnRoute {
		return nil, ErrAssignmentStateConflict
	}
	position, err := d.geoService.GetLocation(ctx, assignment.DriverID)
	if err != nil {
		return nil, err
	}
	distance := distanceKm(
		position.Longitude,
		position.Latitude,
		assignment.PickupLongitude,
		assignment.PickupLatitude,
	)
	if distance > d.lifecycle.arrivalThresholdKm {
		return nil, ErrDriverTooFar
	}
	return d.lifecycle.arrive(ctx, assignment)
}

func (d *Dispatcher) GetAssignment(ctx context.Context, requestID string) (*Assignment, error) {
	return d.lifecycle.get(ctx, requestID)
}

// ResetRiderAssignment is used only by the scoped interview-demo reset. It
// cancels an en-route assignment and otherwise clears that rider's active key.
func (d *Dispatcher) ResetRiderAssignment(ctx context.Context, riderID string) error {
	if err := validateLifecycleID(riderID); err != nil {
		return err
	}
	return d.lifecycle.resetRider(ctx, riderID)
}

// ReleaseDriver releases a driver back to available status
func (d *Dispatcher) ReleaseDriver(ctx context.Context, driverID string, requestID string) error {
	// Unlock first
	if err := d.lockManager.Unlock(ctx, driverID, requestID); err != nil && !errors.Is(err, ErrLockExpired) {
		return err
	}

	// Set back to available
	return d.driverService.SetAvailable(ctx, driverID)
}

// UpdateDriverLocation updates a driver's location in the geo service
func (d *Dispatcher) UpdateDriverLocation(ctx context.Context, driverID string, lon, lat float64) error {
	loc := geospatial.Location{
		ID:        driverID,
		Longitude: lon,
		Latitude:  lat,
	}
	err := d.geoService.AddLocation(ctx, loc)
	if err != nil {
		return err
	}
	active, err := d.lifecycle.refreshActiveDriver(ctx, driverID)
	if err != nil {
		return err
	}
	if active {
		return nil
	}

	// A fresh location can safely bring an expired driver back online. A
	// heartbeat without a fresh location intentionally cannot.
	err = d.driverService.Heartbeat(ctx, driverID)
	if errors.Is(err, driver.ErrDriverOffline) {
		return d.driverService.SetAvailable(ctx, driverID)
	}
	return err
}

// GetStats returns active driver statistics.
type DispatchStats struct {
	TotalDrivers     int `json:"total_drivers"`
	AvailableDrivers int `json:"available_drivers"`
}

func (d *Dispatcher) GetStats(ctx context.Context) (DispatchStats, error) {
	total, available, err := d.driverService.GetDriverStats(ctx)
	if err != nil {
		return DispatchStats{}, err
	}
	return DispatchStats{
		TotalDrivers:     total,
		AvailableDrivers: available,
	}, nil
}
