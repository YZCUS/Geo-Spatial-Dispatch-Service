package dispatch

import (
	"context"
	"log"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
	"github.com/google/uuid"
)

// DispatchResult represents the outcome of a dispatch request
type DispatchResult struct {
	Success   bool    `json:"success"`
	DriverID  string  `json:"driver_id,omitempty"`
	RequestID string  `json:"request_id"`
	Distance  float64 `json:"distance_km,omitempty"`
	Error     string  `json:"error,omitempty"`
}

// Dispatcher coordinates driver assignment
type Dispatcher struct {
	geoService    *geospatial.GeoService
	driverService *driver.DriverService
	lockManager   *LockManager
	maxRetries    int
}

// NewDispatcher creates a new dispatcher
func NewDispatcher(
	geoService *geospatial.GeoService,
	driverService *driver.DriverService,
	lockManager *LockManager,
) *Dispatcher {
	return &Dispatcher{
		geoService:    geoService,
		driverService: driverService,
		lockManager:   lockManager,
		maxRetries:    5,
	}
}

// DispatchRequest represents an incoming dispatch request
type DispatchRequest struct {
	RequestID string  `json:"request_id"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	RadiusKm  float64 `json:"radius_km"`
}

// FindAndAssign finds the nearest available driver and assigns them
func (d *Dispatcher) FindAndAssign(ctx context.Context, req DispatchRequest) DispatchResult {
	if req.RequestID == "" {
		req.RequestID = uuid.New().String()
	}
	if req.RadiusKm <= 0 {
		req.RadiusKm = 5.0 // Default 5km radius
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

	// Try to assign each driver in order (nearest first)
	for i, loc := range nearby {
		if i >= d.maxRetries {
			break
		}

		result := d.tryAssignDriver(ctx, req, loc)
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
func (d *Dispatcher) tryAssignDriver(ctx context.Context, req DispatchRequest, loc geospatial.Location) DispatchResult {
	driverID := loc.ID

	// Step 1: Check if driver is available
	available, err := d.driverService.IsAvailable(ctx, driverID)
	if err != nil || !available {
		return DispatchResult{Success: false, RequestID: req.RequestID}
	}

	// Step 2: Try to acquire lock
	locked, err := d.lockManager.TryLock(ctx, driverID, req.RequestID)
	if err != nil || !locked {
		return DispatchResult{Success: false, RequestID: req.RequestID}
	}

	// Step 3: Double-check availability and set busy atomically
	err = d.driverService.SetBusy(ctx, driverID)
	if err != nil {
		// Release lock if we couldn't set busy
		d.lockManager.Unlock(ctx, driverID, req.RequestID)
		return DispatchResult{Success: false, RequestID: req.RequestID}
	}

	// Calculate distance
	distance, _ := d.geoService.Distance(ctx, driverID, driverID) // Will be 0, need proper calculation
	// For now, estimate based on coordinates
	distance = estimateDistance(req.Longitude, req.Latitude, loc.Longitude, loc.Latitude)

	log.Printf("[Dispatcher] Successfully assigned driver %s to request %s", driverID, req.RequestID)

	return DispatchResult{
		Success:   true,
		DriverID:  driverID,
		RequestID: req.RequestID,
		Distance:  distance,
	}
}

// ReleaseDriver releases a driver back to available status
func (d *Dispatcher) ReleaseDriver(ctx context.Context, driverID string, requestID string) error {
	// Unlock first
	d.lockManager.Unlock(ctx, driverID, requestID)

	// Set back to available
	return d.driverService.SetAvailable(ctx, driverID)
}

// estimateDistance calculates approximate distance in km using Haversine formula
func estimateDistance(lon1, lat1, lon2, lat2 float64) float64 {
	// Simplified distance calculation
	// For production, use proper Haversine formula
	import_math := func(x float64) float64 {
		if x < 0 {
			return -x
		}
		return x
	}

	// Very rough approximation (1 degree ≈ 111km at equator)
	latDiff := import_math(lat2 - lat1)
	lonDiff := import_math(lon2 - lon1)
	return (latDiff + lonDiff) * 111 / 2
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

	// Also heartbeat to maintain status
	return d.driverService.Heartbeat(ctx, driverID)
}

// GetStats returns dispatch statistics (placeholder)
type DispatchStats struct {
	TotalDrivers     int `json:"total_drivers"`
	AvailableDrivers int `json:"available_drivers"`
}

func (d *Dispatcher) GetStats(ctx context.Context) (DispatchStats, error) {
	available, err := d.driverService.GetAvailableDrivers(ctx)
	if err != nil {
		return DispatchStats{}, err
	}
	return DispatchStats{
		AvailableDrivers: len(available),
	}, nil
}
