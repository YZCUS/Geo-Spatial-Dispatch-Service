package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"
)

var (
	ErrClientBufferFull = errors.New("client send buffer full")
	ErrHubClosed        = errors.New("hub is closed")
)

// LocationUpdateHandler is called when a driver location is updated
type LocationUpdateHandler func(ctx context.Context, loc *LocationPayload) error

// HeartbeatHandler is called when a driver heartbeat is received.
type HeartbeatHandler func(ctx context.Context, driverID string) error

// Hub maintains the set of active clients and broadcasts messages
type Hub struct {
	// Registered clients
	drivers map[string]*Client
	riders  map[string]*Client

	// Channels for client management
	register   chan *Client
	unregister chan *Client

	// Broadcast location updates
	broadcastLocation chan *LocationPayload

	// Location update handler (to update geo service)
	locationHandler  LocationUpdateHandler
	heartbeatHandler HeartbeatHandler

	// Mutex for thread-safe access
	mu sync.RWMutex

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// Stats
	stats HubStats
}

// HubStats tracks hub statistics
type HubStats struct {
	TotalDrivers   int   `json:"total_drivers"`
	TotalRiders    int   `json:"total_riders"`
	MessagesPerSec int64 `json:"messages_per_sec"`
	TotalMessages  int64 `json:"total_messages"`
}

// NewHub creates a new Hub
func NewHub(locationHandler LocationUpdateHandler, heartbeatHandler HeartbeatHandler) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		drivers:           make(map[string]*Client),
		riders:            make(map[string]*Client),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		broadcastLocation: make(chan *LocationPayload, 256),
		locationHandler:   locationHandler,
		heartbeatHandler:  heartbeatHandler,
		ctx:               ctx,
		cancel:            cancel,
	}
}

// Run starts the hub's main loop
func (h *Hub) Run() {
	log.Println("[Hub] Started")

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var messagesThisSecond int64

	for {
		select {
		case <-h.ctx.Done():
			log.Println("[Hub] Shutting down")
			h.closeAllClients()
			return

		case client := <-h.register:
			h.registerClient(client)

		case client := <-h.unregister:
			h.unregisterClient(client)

		case location := <-h.broadcastLocation:
			h.handleLocationBroadcast(location)
			messagesThisSecond++
			h.mu.Lock()
			h.stats.TotalMessages++
			h.mu.Unlock()

		case <-ticker.C:
			h.mu.Lock()
			h.stats.MessagesPerSec = messagesThisSecond
			h.mu.Unlock()
			messagesThisSecond = 0
		}
	}
}

// registerClient adds a client to the hub
func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if client.Type == ClientTypeDriver {
		// Close an existing connection without allowing its later unregister
		// event to remove this replacement.
		if old, exists := h.drivers[client.ID]; exists {
			old.Close()
		}
		h.drivers[client.ID] = client
		h.stats.TotalDrivers = len(h.drivers)
		log.Printf("[Hub] Driver %s connected. Total drivers: %d", client.ID, h.stats.TotalDrivers)
	} else {
		if old, exists := h.riders[client.ID]; exists {
			old.Close()
		}
		h.riders[client.ID] = client
		h.stats.TotalRiders = len(h.riders)
		log.Printf("[Hub] Rider %s connected. Total riders: %d", client.ID, h.stats.TotalRiders)
	}
}

// unregisterClient removes a client from the hub
func (h *Hub) unregisterClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if client.Type == ClientTypeDriver {
		if current, exists := h.drivers[client.ID]; exists && current == client {
			delete(h.drivers, client.ID)
			client.Close()
			h.stats.TotalDrivers = len(h.drivers)
			log.Printf("[Hub] Driver %s disconnected. Total drivers: %d", client.ID, h.stats.TotalDrivers)
		}
	} else {
		if current, exists := h.riders[client.ID]; exists && current == client {
			delete(h.riders, client.ID)
			client.Close()
			h.stats.TotalRiders = len(h.riders)
			log.Printf("[Hub] Rider %s disconnected. Total riders: %d", client.ID, h.stats.TotalRiders)
		}
	}
}

// handleLocationBroadcast broadcasts driver location to subscribed riders
func (h *Hub) handleLocationBroadcast(location *LocationPayload) {
	// Create the message to broadcast
	payload, _ := json.Marshal(DriverLocationPayload{
		DriverID:  location.DriverID,
		Longitude: location.Longitude,
		Latitude:  location.Latitude,
		Heading:   location.Heading,
		Speed:     location.Speed,
	})

	msg := &Message{
		Type:      TypeDriverLocation,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("[Hub] Failed to encode location update: %v", err)
		return
	}

	// Send to subscribed riders
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, rider := range h.riders {
		if rider.IsSubscribedTo(location.DriverID) {
			if err := rider.SendBytes(data); err != nil {
				log.Printf("[Hub] Failed to send to rider %s: %v", rider.ID, err)
			}
		}
	}
}

// closeAllClients closes all client connections
func (h *Hub) closeAllClients() {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, client := range h.drivers {
		client.Close()
	}
	for _, client := range h.riders {
		client.Close()
	}

	h.drivers = make(map[string]*Client)
	h.riders = make(map[string]*Client)
	h.stats.TotalDrivers = 0
	h.stats.TotalRiders = 0
}

// Register adds a client to the hub
func (h *Hub) Register(client *Client) {
	select {
	case h.register <- client:
	case <-h.ctx.Done():
		client.Close()
	}
}

// Unregister removes a client without blocking after the hub has stopped.
func (h *Hub) Unregister(client *Client) {
	select {
	case h.unregister <- client:
	case <-h.ctx.Done():
		client.Close()
	}
}

// PublishLocation persists a location before queueing it for fanout.
// Persistence occurs in the caller's goroutine so one slow Redis request does
// not block all hub registrations and broadcasts.
func (h *Hub) PublishLocation(location *LocationPayload) error {
	if h.locationHandler != nil {
		if err := h.locationHandler(h.ctx, location); err != nil {
			return err
		}
	}

	select {
	case h.broadcastLocation <- location:
		return nil
	case <-h.ctx.Done():
		return ErrHubClosed
	}
}

// HeartbeatDriver refreshes a connected driver's liveness state.
func (h *Hub) HeartbeatDriver(driverID string) error {
	if h.heartbeatHandler == nil {
		return nil
	}
	select {
	case <-h.ctx.Done():
		return ErrHubClosed
	default:
		return h.heartbeatHandler(h.ctx, driverID)
	}
}

// GetStats returns current hub statistics
func (h *Hub) GetStats() HubStats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.stats
}

// Stop gracefully shuts down the hub
func (h *Hub) Stop() {
	h.cancel()
}

// GetDriverCount returns the number of connected drivers
func (h *Hub) GetDriverCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.drivers)
}

// GetRiderCount returns the number of connected riders
func (h *Hub) GetRiderCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.riders)
}

// SendToDriver sends a message to a specific driver
func (h *Hub) SendToDriver(driverID string, msg *Message) error {
	h.mu.RLock()
	driver, exists := h.drivers[driverID]
	h.mu.RUnlock()

	if !exists {
		return errors.New("driver not connected")
	}

	return driver.Send(msg)
}

// SendToRider sends a message to a specific rider
func (h *Hub) SendToRider(riderID string, msg *Message) error {
	h.mu.RLock()
	rider, exists := h.riders[riderID]
	h.mu.RUnlock()

	if !exists {
		return errors.New("rider not connected")
	}

	return rider.Send(msg)
}
