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
	locationHandler LocationUpdateHandler

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
func NewHub(handler LocationUpdateHandler) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		drivers:           make(map[string]*Client),
		riders:            make(map[string]*Client),
		register:          make(chan *Client),
		unregister:        make(chan *Client),
		broadcastLocation: make(chan *LocationPayload, 256),
		locationHandler:   handler,
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
			h.stats.TotalMessages++

		case <-ticker.C:
			h.stats.MessagesPerSec = messagesThisSecond
			messagesThisSecond = 0
		}
	}
}

// registerClient adds a client to the hub
func (h *Hub) registerClient(client *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if client.Type == ClientTypeDriver {
		// Close existing connection if any
		if old, exists := h.drivers[client.ID]; exists {
			close(old.send)
		}
		h.drivers[client.ID] = client
		h.stats.TotalDrivers = len(h.drivers)
		log.Printf("[Hub] Driver %s connected. Total drivers: %d", client.ID, h.stats.TotalDrivers)
	} else {
		if old, exists := h.riders[client.ID]; exists {
			close(old.send)
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
		if _, exists := h.drivers[client.ID]; exists {
			delete(h.drivers, client.ID)
			close(client.send)
			h.stats.TotalDrivers = len(h.drivers)
			log.Printf("[Hub] Driver %s disconnected. Total drivers: %d", client.ID, h.stats.TotalDrivers)
		}
	} else {
		if _, exists := h.riders[client.ID]; exists {
			delete(h.riders, client.ID)
			close(client.send)
			h.stats.TotalRiders = len(h.riders)
			log.Printf("[Hub] Rider %s disconnected. Total riders: %d", client.ID, h.stats.TotalRiders)
		}
	}
}

// handleLocationBroadcast broadcasts driver location to subscribed riders
func (h *Hub) handleLocationBroadcast(location *LocationPayload) {
	// Update geo service if handler is set
	if h.locationHandler != nil {
		if err := h.locationHandler(h.ctx, location); err != nil {
			log.Printf("[Hub] Failed to update location in geo service: %v", err)
		}
	}

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

	// Send to subscribed riders
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, rider := range h.riders {
		if rider.IsSubscribedTo(location.DriverID) {
			if err := rider.Send(msg); err != nil {
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
		close(client.send)
	}
	for _, client := range h.riders {
		close(client.send)
	}

	h.drivers = make(map[string]*Client)
	h.riders = make(map[string]*Client)
}

// Register adds a client to the hub
func (h *Hub) Register(client *Client) {
	h.register <- client
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
