package realtime

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// Time allowed to write a message to the peer
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer
	pongWait = 60 * time.Second

	// Send pings to peer with this period (must be less than pongWait)
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer
	maxMessageSize = 4096
)

// ClientType distinguishes between driver and rider connections
type ClientType string

const (
	ClientTypeDriver ClientType = "driver"
	ClientTypeRider  ClientType = "rider"
)

// Client represents a single WebSocket connection
type Client struct {
	ID         string
	Type       ClientType
	hub        *Hub
	conn       *websocket.Conn
	send       chan []byte
	subscribed map[string]bool // For riders: driver IDs they're tracking
	mu         sync.RWMutex
}

// NewClient creates a new WebSocket client
func NewClient(id string, clientType ClientType, hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		ID:         id,
		Type:       clientType,
		hub:        hub,
		conn:       conn,
		send:       make(chan []byte, 256),
		subscribed: make(map[string]bool),
	}
}

// ReadPump pumps messages from the WebSocket connection to the hub
func (c *Client) ReadPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[Client %s] Read error: %v", c.ID, err)
			}
			break
		}

		// Parse and handle the message
		var msg Message
		if err := json.Unmarshal(message, &msg); err != nil {
			log.Printf("[Client %s] Invalid message format: %v", c.ID, err)
			continue
		}

		c.handleMessage(&msg)
	}
}

// WritePump pumps messages from the hub to the WebSocket connection
func (c *Client) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			// Drain queued messages
			n := len(c.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleMessage processes incoming messages based on type
func (c *Client) handleMessage(msg *Message) {
	switch msg.Type {
	case TypeLocationUpdate:
		c.handleLocationUpdate(msg)
	case TypeSubscribe:
		c.handleSubscribe(msg)
	case TypeUnsubscribe:
		c.handleUnsubscribe(msg)
	case TypeHeartbeat:
		c.sendAck("heartbeat")
	default:
		log.Printf("[Client %s] Unknown message type: %s", c.ID, msg.Type)
	}
}

// handleLocationUpdate processes driver location updates
func (c *Client) handleLocationUpdate(msg *Message) {
	if c.Type != ClientTypeDriver {
		c.sendError("permission_denied", "Only drivers can send location updates")
		return
	}

	var payload LocationPayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		c.sendError("invalid_payload", "Invalid location payload")
		return
	}

	// Override driver ID with client ID for security
	payload.DriverID = c.ID

	log.Printf("[Client %s] Location update: (%.4f, %.4f)", c.ID, payload.Longitude, payload.Latitude)

	// Broadcast to hub
	c.hub.broadcastLocation <- &payload
}

// handleSubscribe handles rider subscribing to driver updates
func (c *Client) handleSubscribe(msg *Message) {
	if c.Type != ClientTypeRider {
		c.sendError("permission_denied", "Only riders can subscribe")
		return
	}

	var payload SubscribePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		c.sendError("invalid_payload", "Invalid subscribe payload")
		return
	}

	c.mu.Lock()
	c.subscribed[payload.DriverID] = true
	c.mu.Unlock()

	log.Printf("[Client %s] Subscribed to driver %s", c.ID, payload.DriverID)
	c.sendAck("subscribed")
}

// handleUnsubscribe handles rider unsubscribing from driver updates
func (c *Client) handleUnsubscribe(msg *Message) {
	var payload SubscribePayload
	if err := json.Unmarshal(msg.Payload, &payload); err != nil {
		c.sendError("invalid_payload", "Invalid unsubscribe payload")
		return
	}

	c.mu.Lock()
	delete(c.subscribed, payload.DriverID)
	c.mu.Unlock()

	log.Printf("[Client %s] Unsubscribed from driver %s", c.ID, payload.DriverID)
	c.sendAck("unsubscribed")
}

// IsSubscribedTo checks if client is subscribed to a driver
func (c *Client) IsSubscribedTo(driverID string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.subscribed[driverID]
}

// Send sends a message to the client
func (c *Client) Send(msg *Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	select {
	case c.send <- data:
		return nil
	default:
		return ErrClientBufferFull
	}
}

// sendError sends an error message to the client
func (c *Client) sendError(code, message string) {
	payload, _ := json.Marshal(ErrorPayload{Code: code, Message: message})
	msg := &Message{
		Type:      TypeError,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
	c.Send(msg)
}

// sendAck sends an acknowledgment to the client
func (c *Client) sendAck(status string) {
	payload, _ := json.Marshal(AckPayload{Status: status})
	msg := &Message{
		Type:      TypeAck,
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
	c.Send(msg)
}
