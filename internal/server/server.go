package server

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/dispatch"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/ratelimiter"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/realtime"
	"github.com/go-redis/redis/v8"
)

type Server struct {
	Redis         *redis.Client
	rateLimiter   *ratelimiter.RateLimiter
	geoService    *geospatial.GeoService
	driverService *driver.DriverService
	dispatcher    *dispatch.Dispatcher
	hub           *realtime.Hub
}

func New(redisAddr string) *Server {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: "",
		DB:       0,
	})

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	log.Println("Connected to Redis")

	geoService := geospatial.New(rdb, "locations")
	driverService := driver.NewDriverService(rdb, "driver", 30*time.Second)
	lockManager := dispatch.NewLockManager(rdb, "dispatch:lock", 10*time.Second)
	dispatcher := dispatch.NewDispatcher(geoService, driverService, lockManager)

	// Create hub with location update handler
	locationHandler := func(ctx context.Context, loc *realtime.LocationPayload) error {
		return dispatcher.UpdateDriverLocation(ctx, loc.DriverID, loc.Longitude, loc.Latitude)
	}
	heartbeatHandler := func(ctx context.Context, driverID string) error {
		return driverService.Heartbeat(ctx, driverID)
	}
	hub := realtime.NewHub(locationHandler, heartbeatHandler)

	return &Server{
		Redis:         rdb,
		rateLimiter:   ratelimiter.New(rdb),
		geoService:    geoService,
		driverService: driverService,
		dispatcher:    dispatcher,
		hub:           hub,
	}
}

// StartHub starts the WebSocket hub in a goroutine
func (s *Server) StartHub() {
	go s.hub.Run()
	log.Println("WebSocket hub started")
}

// StopHub stops the WebSocket hub
func (s *Server) StopHub() {
	s.hub.Stop()
}

func (s *Server) HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "OK")
}

func (s *Server) HandlePing(w http.ResponseWriter, r *http.Request) {
	pong, err := s.Redis.Ping(r.Context()).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"Redis":"%s"}`, pong)
}
