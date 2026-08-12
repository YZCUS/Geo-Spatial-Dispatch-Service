package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/server"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		healthURL := envOrDefault("HEALTHCHECK_URL", "http://127.0.0.1:8080/ping")
		if err := runHealthcheck(healthURL); err != nil {
			log.Printf("Healthcheck failed: %v", err)
			os.Exit(1)
		}
		return
	}

	redisAddr := envOrDefault("REDIS_ADDR", "localhost:6379")
	httpAddr := httpAddrFromEnv()

	srv := server.New(redisAddr)
	defer srv.Redis.Close()

	// Start WebSocket hub
	srv.StartHub()
	defer srv.StopHub()

	mux := newHTTPMux(srv)

	httpServer := &http.Server{
		Addr:              httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("Server started on %s", httpAddr)
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

func newHTTPMux(srv *server.Server) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", srv.HandleDemo)
	mux.HandleFunc("/demo/reset", srv.HandleDemoReset)
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
	mux.HandleFunc("/dispatch/cancel", srv.HandleDispatchCancel)
	mux.HandleFunc("/dispatch/arrive", srv.HandleDispatchArrive)
	mux.HandleFunc("/dispatch/stats", srv.HandleDispatchStats)

	// Driver routes
	mux.HandleFunc("/driver/status", srv.HandleDriverStatus)
	mux.HandleFunc("/driver/location", srv.HandleDriverLocation)

	// WebSocket routes
	mux.HandleFunc("/ws/driver", srv.HandleDriverWebSocket)
	mux.HandleFunc("/ws/rider", srv.HandleRiderWebSocket)
	mux.HandleFunc("/ws/stats", srv.HandleWebSocketStats)
	return mux
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func httpAddrFromEnv() string {
	if addr := os.Getenv("HTTP_ADDR"); addr != "" {
		return addr
	}
	if port := os.Getenv("PORT"); port != "" {
		return ":" + port
	}
	return ":8080"
}

func runHealthcheck(url string) error {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
	return nil
}
