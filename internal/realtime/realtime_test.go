package realtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHub_RegisterUnregister(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	time.Sleep(10 * time.Millisecond)

	stats := hub.GetStats()
	if stats.TotalDrivers != 0 {
		t.Errorf("Expected 0 drivers, got %d", stats.TotalDrivers)
	}
}

func TestHub_LocationBroadcast(t *testing.T) {
	var receivedLocations []*LocationPayload
	var mu sync.Mutex

	handler := func(ctx context.Context, loc *LocationPayload) error {
		mu.Lock()
		receivedLocations = append(receivedLocations, loc)
		mu.Unlock()
		return nil
	}

	hub := NewHub(handler)
	go hub.Run()
	defer hub.Stop()

	time.Sleep(10 * time.Millisecond)

	hub.broadcastLocation <- &LocationPayload{
		DriverID:  "driver1",
		Longitude: 10.0,
		Latitude:  20.0,
	}

	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(receivedLocations) != 1 {
		t.Errorf("Expected 1 location update, got %d", len(receivedLocations))
	}

	if len(receivedLocations) > 0 && receivedLocations[0].DriverID != "driver1" {
		t.Errorf("Expected driver1, got %s", receivedLocations[0].DriverID)
	}
}

func TestHub_Stats(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	time.Sleep(10 * time.Millisecond)

	stats := hub.GetStats()
	if stats.TotalDrivers != 0 || stats.TotalRiders != 0 {
		t.Error("Expected zero stats for empty hub")
	}
}

func TestHub_Stop(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()

	time.Sleep(10 * time.Millisecond)

	hub.Stop()
	time.Sleep(50 * time.Millisecond)

	// Hub should be stopped, check it doesn't panic
}

func TestHub_GetDriverCount(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	time.Sleep(10 * time.Millisecond)

	count := hub.GetDriverCount()
	if count != 0 {
		t.Errorf("Expected 0 drivers, got %d", count)
	}
}

func TestHub_GetRiderCount(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	time.Sleep(10 * time.Millisecond)

	count := hub.GetRiderCount()
	if count != 0 {
		t.Errorf("Expected 0 riders, got %d", count)
	}
}

func TestHub_SendToDriver_NotConnected(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	time.Sleep(10 * time.Millisecond)

	err := hub.SendToDriver("nonexistent", &Message{Type: TypeAck})
	if err == nil {
		t.Error("Expected error for non-existent driver")
	}
}

func TestHub_SendToRider_NotConnected(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	time.Sleep(10 * time.Millisecond)

	err := hub.SendToRider("nonexistent", &Message{Type: TypeAck})
	if err == nil {
		t.Error("Expected error for non-existent rider")
	}
}

// Test message types
func TestMessage_Serialization(t *testing.T) {
	payload, _ := json.Marshal(LocationPayload{
		DriverID:  "driver1",
		Longitude: 10.0,
		Latitude:  20.0,
	})

	msg := Message{
		Type:      TypeLocationUpdate,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}

	var decoded Message
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal message: %v", err)
	}

	if decoded.Type != TypeLocationUpdate {
		t.Errorf("Expected type %s, got %s", TypeLocationUpdate, decoded.Type)
	}
}

func TestLocationPayload_Serialization(t *testing.T) {
	payload := LocationPayload{
		DriverID:  "driver1",
		Longitude: -73.9857,
		Latitude:  40.7484,
		Heading:   90.0,
		Speed:     60.5,
		Accuracy:  5.0,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded LocationPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.DriverID != "driver1" {
		t.Errorf("Expected driver1, got %s", decoded.DriverID)
	}
	if decoded.Speed != 60.5 {
		t.Errorf("Expected speed 60.5, got %f", decoded.Speed)
	}
}

func TestDriverLocationPayload_Serialization(t *testing.T) {
	payload := DriverLocationPayload{
		DriverID:  "driver1",
		Longitude: 0,
		Latitude:  0,
		ETA:       5.5,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded DriverLocationPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.ETA != 5.5 {
		t.Errorf("Expected ETA 5.5, got %f", decoded.ETA)
	}
}

func TestOrderUpdatePayload_Serialization(t *testing.T) {
	payload := OrderUpdatePayload{
		OrderID:        "order123",
		RiderID:        "rider1",
		PickupLon:      10.0,
		PickupLat:      20.0,
		DestinationLon: 30.0,
		DestinationLat: 40.0,
		Status:         "pending",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded OrderUpdatePayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.OrderID != "order123" {
		t.Errorf("Expected order123, got %s", decoded.OrderID)
	}
}

func TestErrorPayload_Serialization(t *testing.T) {
	payload := ErrorPayload{
		Code:    "invalid_request",
		Message: "Missing required field",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded ErrorPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.Code != "invalid_request" {
		t.Errorf("Expected invalid_request, got %s", decoded.Code)
	}
}

func TestSubscribePayload_Serialization(t *testing.T) {
	payload := SubscribePayload{
		DriverID: "driver1",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Failed to marshal: %v", err)
	}

	var decoded SubscribePayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal: %v", err)
	}

	if decoded.DriverID != "driver1" {
		t.Errorf("Expected driver1, got %s", decoded.DriverID)
	}
}

// WebSocket integration test
func TestWebSocket_DriverConnection(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("Upgrade error: %v", err)
			return
		}

		client := NewClient("driver1", ClientTypeDriver, hub, conn)
		hub.Register(client)

		go client.WritePump()
		client.ReadPump()
	}))
	defer server.Close()

	// Convert http URL to ws URL
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	if hub.GetDriverCount() != 1 {
		t.Errorf("Expected 1 driver, got %d", hub.GetDriverCount())
	}
}

func TestWebSocket_RiderConnection(t *testing.T) {
	hub := NewHub(nil)
	go hub.Run()
	defer hub.Stop()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		client := NewClient("rider1", ClientTypeRider, hub, conn)
		hub.Register(client)

		go client.WritePump()
		client.ReadPump()
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	if hub.GetRiderCount() != 1 {
		t.Errorf("Expected 1 rider, got %d", hub.GetRiderCount())
	}
}

func TestClient_IsSubscribedTo(t *testing.T) {
	hub := NewHub(nil)
	client := &Client{
		ID:         "rider1",
		Type:       ClientTypeRider,
		hub:        hub,
		send:       make(chan []byte, 256),
		subscribed: make(map[string]bool),
	}

	// Not subscribed initially
	if client.IsSubscribedTo("driver1") {
		t.Error("Should not be subscribed initially")
	}

	// Add subscription
	client.subscribed["driver1"] = true

	if !client.IsSubscribedTo("driver1") {
		t.Error("Should be subscribed to driver1")
	}
}
