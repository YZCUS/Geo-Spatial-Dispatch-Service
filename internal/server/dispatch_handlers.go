package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/dispatch"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
)

// DispatchRequestDTO is the request body for dispatch
type DispatchRequestDTO struct {
	RequestID string  `json:"request_id,omitempty"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	RadiusKm  float64 `json:"radius_km,omitempty"`
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

	log.Printf("[Dispatch] Request received: lon=%.4f lat=%.4f radius=%.2fkm",
		req.Longitude, req.Latitude, req.RadiusKm)

	result := s.dispatcher.FindAndAssign(r.Context(), dispatch.DispatchRequest{
		RequestID: req.RequestID,
		Longitude: req.Longitude,
		Latitude:  req.Latitude,
		RadiusKm:  req.RadiusKm,
	})

	w.Header().Set("Content-Type", "application/json")
	if result.Success {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
	json.NewEncoder(w).Encode(result)
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
		if err == driver.ErrInvalidStatus {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

// HandleDispatchStats returns dispatch statistics
func (s *Server) HandleDispatchStats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.dispatcher.GetStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
