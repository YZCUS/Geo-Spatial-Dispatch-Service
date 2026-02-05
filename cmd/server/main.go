package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/server"
)

func main() {
	srv := server.New("localhost:6379")
	defer srv.Redis.Close()

	// Start WebSocket hub
	srv.StartHub()
	defer srv.StopHub()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", srv.HandleHealthCheck)
	mux.HandleFunc("/ping", srv.HandlePing)
	mux.HandleFunc("/ratelimit/check", srv.HandleRateLimitCheck)
	mux.HandleFunc("/ratelimit/budget/set", srv.HandleSetBudget)
	mux.HandleFunc("/ratelimit/budget/get", srv.HandleGetBudget)
	mux.HandleFunc("/geo/add", srv.HandleAddLocation)
	mux.HandleFunc("/geo/get", srv.HandleGetLocation)
	mux.HandleFunc("/geo/nearby", srv.HandleFindNearby)

	// Dispatch routes
	mux.HandleFunc("/dispatch/request", srv.HandleDispatchRequest)
	mux.HandleFunc("/dispatch/stats", srv.HandleDispatchStats)

	// Driver routes
	mux.HandleFunc("/driver/status", srv.HandleDriverStatus)
	mux.HandleFunc("/driver/location", srv.HandleDriverLocation)

	// WebSocket routes
	mux.HandleFunc("/ws/driver", srv.HandleDriverWebSocket)
	mux.HandleFunc("/ws/rider", srv.HandleRiderWebSocket)
	mux.HandleFunc("/ws/stats", srv.HandleWebSocketStats)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("Server started on http://localhost:8080")
		log.Println("WebSocket endpoints: /ws/driver, /ws/rider")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited properly")
}
