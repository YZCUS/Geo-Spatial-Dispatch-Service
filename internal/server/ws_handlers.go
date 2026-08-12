package server

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/realtime"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     checkWebSocketOrigin,
}

func checkWebSocketOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}

	originURL, err := url.Parse(origin)
	if err != nil || originURL.Host == "" {
		return false
	}
	return strings.EqualFold(originURL.Host, r.Host)
}

// HandleDriverWebSocket handles WebSocket connections for drivers
func (s *Server) HandleDriverWebSocket(w http.ResponseWriter, r *http.Request) {
	driverID := r.URL.Query().Get("driver_id")
	if driverID == "" {
		http.Error(w, "Missing driver_id", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Failed to upgrade driver connection: %v", err)
		return
	}

	client := realtime.NewClient(driverID, realtime.ClientTypeDriver, s.hub, conn)
	s.hub.Register(client)

	log.Printf("[WS] Driver %s connected", driverID)

	// Start read/write pumps
	go client.WritePump()
	go client.ReadPump()
}

// HandleRiderWebSocket handles WebSocket connections for riders
func (s *Server) HandleRiderWebSocket(w http.ResponseWriter, r *http.Request) {
	riderID := r.URL.Query().Get("rider_id")
	if riderID == "" {
		http.Error(w, "Missing rider_id", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Failed to upgrade rider connection: %v", err)
		return
	}

	client := realtime.NewClient(riderID, realtime.ClientTypeRider, s.hub, conn)
	s.hub.Register(client)

	log.Printf("[WS] Rider %s connected", riderID)

	// Start read/write pumps
	go client.WritePump()
	go client.ReadPump()
}

// HandleWebSocketStats returns real-time connection statistics
func (s *Server) HandleWebSocketStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.hub == nil {
		http.Error(w, "WebSocket hub not initialized", http.StatusServiceUnavailable)
		return
	}

	stats := s.hub.GetStats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"drivers":          stats.TotalDrivers,
		"riders":           stats.TotalRiders,
		"messages_per_sec": stats.MessagesPerSec,
		"total_messages":   stats.TotalMessages,
	})
}
