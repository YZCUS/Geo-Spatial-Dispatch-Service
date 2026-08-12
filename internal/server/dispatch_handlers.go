package server

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/dispatch"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
)

// DispatchRequestDTO is the request body for dispatch
type DispatchRequestDTO struct {
	RequestID string  `json:"request_id,omitempty"`
	RiderID   string  `json:"rider_id,omitempty"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	RadiusKm  float64 `json:"radius_km,omitempty"`
}

type DispatchLifecycleDTO struct {
	RequestID string `json:"request_id"`
}

// DriverStatusDTO is the request body for driver status updates
type DriverStatusDTO struct {
	DriverID string `json:"driver_id"`
	Status   string `json:"status"`
}

// DriverLocationDTO is the request body for driver location updates
type DriverLocationDTO struct {
	DriverID  string  `json:"driver_id"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
}

// HandleDispatchRequest handles incoming dispatch requests
func (s *Server) HandleDispatchRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DispatchRequestDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[Dispatch] Invalid request: %v", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	if err := geospatial.ValidateCoordinates(req.Longitude, req.Latitude); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.RadiusKm < 0 {
		http.Error(w, geospatial.ErrInvalidRadius.Error(), http.StatusBadRequest)
		return
	}
	req.RequestID = strings.TrimSpace(req.RequestID)
	req.RiderID = strings.TrimSpace(req.RiderID)

	log.Printf("[Dispatch] Request received: lon=%.4f lat=%.4f radius=%.2fkm",
		req.Longitude, req.Latitude, req.RadiusKm)

	result := s.dispatcher.FindAndAssign(r.Context(), dispatch.DispatchRequest{
		RequestID: req.RequestID,
		RiderID:   req.RiderID,
		Longitude: req.Longitude,
		Latitude:  req.Latitude,
		RadiusKm:  req.RadiusKm,
	})

	w.Header().Set("Content-Type", "application/json")
	if result.Success {
		w.WriteHeader(http.StatusOK)
	} else if errors.Is(result.Cause, dispatch.ErrActiveAssignment) ||
		errors.Is(result.Cause, dispatch.ErrRequestAlreadyExists) {
		w.WriteHeader(http.StatusConflict)
	} else if result.Error == dispatch.ErrNoDriversAvailable.Error() {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	json.NewEncoder(w).Encode(result)
}

// HandleDispatchCancel atomically cancels an en-route assignment and releases
// its driver back to the available pool.
func (s *Server) HandleDispatchCancel(w http.ResponseWriter, r *http.Request) {
	s.handleDispatchLifecycle(w, r, s.dispatcher.CancelAssignment)
}

// HandleDispatchArrive confirms proximity to pickup before marking a ride as
// arrived. An arrived ride remains active and keeps its driver busy.
func (s *Server) HandleDispatchArrive(w http.ResponseWriter, r *http.Request) {
	s.handleDispatchLifecycle(w, r, s.dispatcher.ArriveAssignment)
}

func (s *Server) handleDispatchLifecycle(
	w http.ResponseWriter,
	r *http.Request,
	transition func(context.Context, string) (*dispatch.Assignment, error),
) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DispatchLifecycleDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	req.RequestID = strings.TrimSpace(req.RequestID)
	if req.RequestID == "" {
		http.Error(w, "request_id is required", http.StatusBadRequest)
		return
	}

	assignment, err := transition(r.Context(), req.RequestID)
	if err != nil {
		switch {
		case errors.Is(err, dispatch.ErrAssignmentNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
		case errors.Is(err, dispatch.ErrAssignmentStateConflict),
			errors.Is(err, dispatch.ErrDriverOwnershipConflict),
			errors.Is(err, dispatch.ErrDriverTooFar),
			errors.Is(err, geospatial.ErrLocationNotFound):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(assignment)
}

// HandleDriverStatus handles driver status get/update
func (s *Server) HandleDriverStatus(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetDriverStatus(w, r)
	case http.MethodPost:
		s.handleSetDriverStatus(w, r)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGetDriverStatus(w http.ResponseWriter, r *http.Request) {
	driverID := r.URL.Query().Get("driver_id")
	if driverID == "" {
		log.Printf("[Driver] GetStatus missing driver_id")
		http.Error(w, "Missing driver_id", http.StatusBadRequest)
		return
	}

	status, err := s.driverService.GetStatus(r.Context(), driverID)
	if err != nil {
		log.Printf("[Driver] Error getting status for %s: %v", driverID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[Driver] Status retrieved: driver=%s status=%s", driverID, status)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"driver_id": driverID,
		"status":    string(status),
	})
}

func (s *Server) handleSetDriverStatus(w http.ResponseWriter, r *http.Request) {
	var req DriverStatusDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[Driver] Invalid status request: %v", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	status := driver.DriverStatus(req.Status)
	log.Printf("[Driver] Setting status: driver=%s status=%s", req.DriverID, status)

	err := s.driverService.SetStatus(r.Context(), req.DriverID, status)
	if err != nil {
		log.Printf("[Driver] Error setting status: %v", err)
		if errors.Is(err, driver.ErrInvalidStatus) || errors.Is(err, driver.ErrDriverNotFound) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

// HandleDriverLocation handles driver location updates
func (s *Server) HandleDriverLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DriverLocationDTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[Driver] Invalid location request: %v", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	log.Printf("[Driver] Updating location: driver=%s lon=%.4f lat=%.4f",
		req.DriverID, req.Longitude, req.Latitude)

	err := s.dispatcher.UpdateDriverLocation(r.Context(), req.DriverID, req.Longitude, req.Latitude)
	if err != nil {
		log.Printf("[Driver] Error updating location: %v", err)
		if errors.Is(err, geospatial.ErrInvalidCoordinates) ||
			errors.Is(err, geospatial.ErrInvalidLocationID) ||
			errors.Is(err, driver.ErrDriverNotFound) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

// HandleDispatchStats returns dispatch statistics
func (s *Server) HandleDispatchStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := s.dispatcher.GetStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
