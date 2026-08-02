package server

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
)

func (s *Server) HandleAddLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var loc geospatial.Location
	if err := json.NewDecoder(r.Body).Decode(&loc); err != nil {
		log.Printf("[Geo] Invalid AddLocation request: %v", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	log.Printf("[Geo] Adding location id=%s lon=%.4f lat=%.4f", loc.ID, loc.Longitude, loc.Latitude)
	if err := s.geoService.AddLocation(r.Context(), loc); err != nil {
		log.Printf("[Geo] Error adding location id=%s: %v", loc.ID, err)
		if errors.Is(err, geospatial.ErrInvalidCoordinates) || errors.Is(err, geospatial.ErrInvalidLocationID) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	log.Printf("[Geo] Location added successfully id=%s", loc.ID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "created"})
}

func (s *Server) HandleGetLocation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		log.Printf("[Geo] GetLocation missing id parameter")
		http.Error(w, "Missing id parameter", http.StatusBadRequest)
		return
	}

	log.Printf("[Geo] Getting location id=%s", id)
	loc, err := s.geoService.GetLocation(r.Context(), id)
	if err != nil {
		log.Printf("[Geo] Location not found id=%s: %v", id, err)
		switch {
		case errors.Is(err, geospatial.ErrInvalidLocationID):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, geospatial.ErrLocationNotFound):
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	log.Printf("[Geo] Location retrieved id=%s lon=%.4f lat=%.4f", loc.ID, loc.Longitude, loc.Latitude)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(loc)
}

type FindNearbyRequest struct {
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	RadiusKm  float64 `json:"radius_km"`
}

func (s *Server) HandleFindNearby(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FindNearbyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[Geo] Invalid FindNearby request: %v", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	log.Printf("[Geo] Finding nearby lon=%.4f lat=%.4f radius=%.2fkm", req.Longitude, req.Latitude, req.RadiusKm)
	locations, err := s.geoService.FindNearby(
		r.Context(),
		req.Longitude,
		req.Latitude,
		req.RadiusKm,
	)
	if err != nil {
		log.Printf("[Geo] Error in FindNearby: %v", err)
		if errors.Is(err, geospatial.ErrInvalidCoordinates) || errors.Is(err, geospatial.ErrInvalidRadius) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	log.Printf("[Geo] Found %d nearby locations", len(locations))
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count":     len(locations),
		"locations": locations,
	})
}
