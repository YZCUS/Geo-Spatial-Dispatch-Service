package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/dispatch"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/ratelimiter"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/realtime"
	"github.com/go-redis/redis/v8"
)

func setupTestServer(t *testing.T) *Server {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   2, // Different DB for integration tests
	})

	ctx := context.Background()
	rdb.FlushDB(ctx)

	geoService := geospatial.New(rdb, "test-locations")
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	lockManager := dispatch.NewLockManager(rdb, "test-lock", 10*time.Second)
	dispatcher := dispatch.NewDispatcher(geoService, driverService, lockManager)
	hub := realtime.NewHub(nil)

	return &Server{
		Redis:         rdb,
		rateLimiter:   ratelimiter.New(rdb),
		geoService:    geoService,
		driverService: driverService,
		dispatcher:    dispatcher,
		hub:           hub,
		ctx:           ctx,
	}
}

func TestHealthCheck(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	server.HandleHealthCheck(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
	if w.Body.String() != "OK" {
		t.Errorf("Expected body 'OK', got '%s'", w.Body.String())
	}
}

func TestPing(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	server.HandlePing(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestRateLimitEndToEnd(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	// Set budget
	req := httptest.NewRequest(
		http.MethodPost,
		"/ratelimit/budget/set?key=test&amount=100",
		nil,
	)
	w := httptest.NewRecorder()
	server.HandleSetBudget(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("SetBudget failed with status %d", w.Code)
	}

	// Check rate limit
	reqBody := RateLimitRequest{Key: "test", Cost: 30}
	body, _ := json.Marshal(reqBody)

	req = httptest.NewRequest(
		http.MethodPost,
		"/ratelimit/check",
		bytes.NewReader(body),
	)
	w = httptest.NewRecorder()
	server.HandleRateLimitCheck(w, req)

	var resp RateLimitResponse
	json.NewDecoder(w.Body).Decode(&resp)

	if !resp.Allowed {
		t.Error("Expected allowed=true")
	}

	if resp.Remaining != 70 {
		t.Errorf("Expected remaining=70, got %d", resp.Remaining)
	}
}

func TestRateLimitCheck_MethodNotAllowed(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/ratelimit/check", nil)
	w := httptest.NewRecorder()
	server.HandleRateLimitCheck(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestRateLimitCheck_InvalidJSON(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(
		http.MethodPost,
		"/ratelimit/check",
		bytes.NewReader([]byte("invalid json")),
	)
	w := httptest.NewRecorder()
	server.HandleRateLimitCheck(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestSetBudget_MethodNotAllowed(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/ratelimit/budget/set", nil)
	w := httptest.NewRecorder()
	server.HandleSetBudget(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestSetBudget_InvalidAmount(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(
		http.MethodPost,
		"/ratelimit/budget/set?key=test&amount=invalid",
		nil,
	)
	w := httptest.NewRecorder()
	server.HandleSetBudget(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestGetBudget_MissingKey(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/ratelimit/budget/get", nil)
	w := httptest.NewRecorder()
	server.HandleGetBudget(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestGeospatialEndToEnd(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()
	// Add location
	loc := geospatial.Location{
		ID:        "driver1",
		Longitude: -73.9857,
		Latitude:  40.7484,
	}
	body, _ := json.Marshal(loc)

	req := httptest.NewRequest(
		http.MethodPost,
		"/geo/add",
		bytes.NewReader(body),
	)
	w := httptest.NewRecorder()
	server.HandleAddLocation(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("AddLocation failed with status %d", w.Code)
	}

	// Get location
	req = httptest.NewRequest(
		http.MethodGet,
		"/geo/get?id=driver1",
		nil,
	)
	w = httptest.NewRecorder()
	server.HandleGetLocation(w, req)

	var retrieved geospatial.Location
	json.NewDecoder(w.Body).Decode(&retrieved)

	if retrieved.ID != "driver1" {
		t.Errorf("Expected ID driver1, got %s", retrieved.ID)
	}
}

func TestAddLocation_MethodNotAllowed(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/geo/add", nil)
	w := httptest.NewRecorder()
	server.HandleAddLocation(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestAddLocation_InvalidJSON(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(
		http.MethodPost,
		"/geo/add",
		bytes.NewReader([]byte("invalid")),
	)
	w := httptest.NewRecorder()
	server.HandleAddLocation(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestGetLocation_MissingID(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/geo/get", nil)
	w := httptest.NewRecorder()
	server.HandleGetLocation(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestGetLocation_NotFound(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/geo/get?id=nonexistent", nil)
	w := httptest.NewRecorder()
	server.HandleGetLocation(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestFindNearby_MethodNotAllowed(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/geo/nearby", nil)
	w := httptest.NewRecorder()
	server.HandleFindNearby(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405, got %d", w.Code)
	}
}

func TestFindNearby_InvalidJSON(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(
		http.MethodPost,
		"/geo/nearby",
		bytes.NewReader([]byte("invalid")),
	)
	w := httptest.NewRecorder()
	server.HandleFindNearby(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestFindNearby_Success(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	// Add a location first
	loc := geospatial.Location{ID: "nearby1", Longitude: 0, Latitude: 0}
	body, _ := json.Marshal(loc)
	req := httptest.NewRequest(http.MethodPost, "/geo/add", bytes.NewReader(body))
	w := httptest.NewRecorder()
	server.HandleAddLocation(w, req)

	// Find nearby
	nearbyReq := FindNearbyRequest{Longitude: 0, Latitude: 0, RadiusKm: 10}
	body, _ = json.Marshal(nearbyReq)
	req = httptest.NewRequest(http.MethodPost, "/geo/nearby", bytes.NewReader(body))
	w = httptest.NewRecorder()
	server.HandleFindNearby(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}
