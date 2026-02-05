package realtime

import "encoding/json"

// MessageType defines the type of WebSocket message
type MessageType string

const (
	// Client -> Server
	TypeLocationUpdate MessageType = "location_update"
	TypeSubscribe      MessageType = "subscribe"
	TypeUnsubscribe    MessageType = "unsubscribe"
	TypeHeartbeat      MessageType = "heartbeat"

	// Server -> Client
	TypeDriverLocation MessageType = "driver_location"
	TypeOrderUpdate    MessageType = "order_update"
	TypeError          MessageType = "error"
	TypeAck            MessageType = "ack"
)

// Message is the base WebSocket message structure
type Message struct {
	Type      MessageType     `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Timestamp int64           `json:"timestamp"`
}

// LocationPayload is sent by drivers to update their position
type LocationPayload struct {
	DriverID  string  `json:"driver_id"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	Heading   float64 `json:"heading,omitempty"`  // Direction in degrees
	Speed     float64 `json:"speed,omitempty"`    // km/h
	Accuracy  float64 `json:"accuracy,omitempty"` // meters
}

// SubscribePayload is sent by riders to track a driver
type SubscribePayload struct {
	DriverID string `json:"driver_id"`
}

// DriverLocationPayload is pushed to riders tracking a driver
type DriverLocationPayload struct {
	DriverID  string  `json:"driver_id"`
	Longitude float64 `json:"longitude"`
	Latitude  float64 `json:"latitude"`
	Heading   float64 `json:"heading,omitempty"`
	Speed     float64 `json:"speed,omitempty"`
	ETA       float64 `json:"eta_minutes,omitempty"`
}

// OrderUpdatePayload is pushed to drivers for new orders
type OrderUpdatePayload struct {
	OrderID        string  `json:"order_id"`
	RiderID        string  `json:"rider_id"`
	PickupLon      float64 `json:"pickup_longitude"`
	PickupLat      float64 `json:"pickup_latitude"`
	DestinationLon float64 `json:"destination_longitude"`
	DestinationLat float64 `json:"destination_latitude"`
	Status         string  `json:"status"`
}

// ErrorPayload is sent when an error occurs
type ErrorPayload struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// AckPayload acknowledges a received message
type AckPayload struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
}
