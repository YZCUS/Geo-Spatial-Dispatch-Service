package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/dispatch"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/ratelimiter"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/realtime"
	"github.com/go-redis/redis/v8"
)

func boolPtr(v bool) *bool {
	return &v
}

func newDemoResetRequest(t *testing.T, sessionID string, force bool, clearLocks *bool) *http.Request {
	t.Helper()

	payload := map[string]interface{}{
		"session_id": sessionID,
		"force":      force,
	}
	if clearLocks != nil {
		payload["clear_locks"] = *clearLocks
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal demo reset request: %v", err)
	}

	return httptest.NewRequest(http.MethodPost, "/demo/reset", bytes.NewReader(body))
}

func setupTestServer(t *testing.T) *Server {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   2, // Different DB for integration tests
	})

	ctx := context.Background()
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("Failed to flush Redis test DB: %v", err)
	}

	geoService := geospatial.New(rdb, "test-locations")
	driverService := driver.NewDriverService(rdb, "test-driver", 30*time.Second)
	lockManager := dispatch.NewLockManager(rdb, "test-lock", 10*time.Second)
	dispatcher := dispatch.NewDispatcher(geoService, driverService, lockManager, dispatch.LifecycleConfig{
		AssignmentPrefix:   "test-assignment",
		RiderActivePrefix:  "test-rider-active",
		DriverActivePrefix: "test-driver-active",
		DriverStatusPrefix: "test-driver:status",
		AssignmentTTL:      time.Hour,
		DriverStatusTTL:    30 * time.Second,
		ArrivalThresholdKm: 0.05,
	})
	hub := realtime.NewHub(nil, nil)

	return &Server{
		Redis:         rdb,
		rateLimiter:   ratelimiter.New(rdb),
		geoService:    geoService,
		driverService: driverService,
		dispatcher:    dispatcher,
		hub:           hub,
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

func TestHandleDemo_InterviewConfiguration(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	(&Server{}).HandleDemo(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}
	for _, expected := range []string{
		"demo-driver-08",
		"demo-rider-03",
		"11 sockets",
		"road-constrained",
		"Reset to known state",
		"Redis GEO distance at request time",
		"Live fleet feed",
		"Request all 3 at once",
		`id="assignmentList"`,
		`readonly aria-readonly="true"`,
		"8 moving drivers",
		"3 fixed rider pickups",
		"24 deliveries",
		`id="dispatchButton" type="submit" disabled`,
		"resetInterviewDemo({ scrollToTop: false })",
	} {
		if !strings.Contains(w.Body.String(), expected) {
			t.Errorf("Demo page does not contain %q", expected)
		}
	}

	for _, removed := range []string{
		`id="seedButton"`,
		`id="seedPanelButton"`,
		"dispatch to unlock fleet fan-out",
		"dispatch once to unlock",
	} {
		if strings.Contains(w.Body.String(), removed) {
			t.Errorf("Demo page still contains redundant control %q", removed)
		}
	}
	if count := strings.Count(w.Body.String(), `id="resetDemoButton"`); count != 1 {
		t.Errorf("Demo page contains %d reset controls; want exactly 1", count)
	}
}

func TestHandleDemoReset_ClaimsLeaseAndClearsDemoLocks(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	ctx := context.Background()
	for _, key := range []string{
		"dispatch:lock:demo-driver-01",
		"dispatch:lock:demo-driver-08",
		"dispatch:lock:production-driver",
	} {
		if err := server.Redis.Set(ctx, key, "request", time.Minute).Err(); err != nil {
			t.Fatalf("Set lock %q: %v", key, err)
		}
	}

	req := newDemoResetRequest(t, "tab-a", false, nil)
	w := httptest.NewRecorder()
	server.HandleDemoReset(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var resp demoResetResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if resp.OwnerSessionID != "tab-a" {
		t.Fatalf("Expected owner_session_id tab-a, got %q", resp.OwnerSessionID)
	}
	if resp.ClearedLocks != 2 {
		t.Fatalf("Expected cleared_locks 2, got %d", resp.ClearedLocks)
	}
	for _, key := range []string{
		"dispatch:lock:demo-driver-01",
		"dispatch:lock:demo-driver-08",
	} {
		if exists := server.Redis.Exists(ctx, key).Val(); exists != 0 {
			t.Errorf("Demo lock %q still exists", key)
		}
	}
	if exists := server.Redis.Exists(ctx, "dispatch:lock:production-driver").Val(); exists != 1 {
		t.Error("Demo reset removed a non-demo lock")
	}
	if owner := server.Redis.Get(ctx, demoSessionLeaseKey).Val(); owner != "tab-a" {
		t.Fatalf("Expected lease owner tab-a, got %q", owner)
	}
	if ttl := server.Redis.PTTL(ctx, demoSessionLeaseKey).Val(); ttl <= 0 || ttl > demoSessionLeaseTTL {
		t.Fatalf("Expected lease TTL within (0,%s], got %s", demoSessionLeaseTTL, ttl)
	}
}

func TestHandleDemoReset_LeaseOnlyRefreshSameOwner(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	ctx := context.Background()
	if err := server.Redis.Set(ctx, demoSessionLeaseKey, "tab-a", time.Minute).Err(); err != nil {
		t.Fatalf("Set lease: %v", err)
	}
	if err := server.Redis.Set(ctx, "dispatch:lock:demo-driver-01", "request", time.Minute).Err(); err != nil {
		t.Fatalf("Set demo lock: %v", err)
	}

	req := newDemoResetRequest(t, "tab-a", false, boolPtr(false))
	w := httptest.NewRecorder()
	server.HandleDemoReset(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var resp demoResetResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if resp.OwnerSessionID != "tab-a" {
		t.Fatalf("Expected owner_session_id tab-a, got %q", resp.OwnerSessionID)
	}
	if resp.ClearedLocks != 0 {
		t.Fatalf("Expected cleared_locks 0, got %d", resp.ClearedLocks)
	}
	if exists := server.Redis.Exists(ctx, "dispatch:lock:demo-driver-01").Val(); exists != 1 {
		t.Fatal("Lease-only refresh should not clear demo locks")
	}
	if ttl := server.Redis.PTTL(ctx, demoSessionLeaseKey).Val(); ttl <= time.Minute || ttl > demoSessionLeaseTTL {
		t.Fatalf("Expected refreshed lease TTL within (%s,%s], got %s", time.Minute, demoSessionLeaseTTL, ttl)
	}
}

func TestHandleDemoReset_ConflictPreservesLocks(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	ctx := context.Background()
	if err := server.Redis.Set(ctx, demoSessionLeaseKey, "tab-a", demoSessionLeaseTTL).Err(); err != nil {
		t.Fatalf("Set lease: %v", err)
	}
	for _, key := range []string{
		"dispatch:lock:demo-driver-01",
		"dispatch:lock:production-driver",
	} {
		if err := server.Redis.Set(ctx, key, "request", time.Minute).Err(); err != nil {
			t.Fatalf("Set lock %q: %v", key, err)
		}
	}

	req := newDemoResetRequest(t, "tab-b", false, nil)
	w := httptest.NewRecorder()
	server.HandleDemoReset(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("Expected status 409, got %d", w.Code)
	}

	var resp map[string]string
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if resp["owner_session_id"] != "tab-a" {
		t.Fatalf("Expected owner_session_id tab-a, got %q", resp["owner_session_id"])
	}
	for _, key := range []string{
		"dispatch:lock:demo-driver-01",
		"dispatch:lock:production-driver",
	} {
		if exists := server.Redis.Exists(ctx, key).Val(); exists != 1 {
			t.Fatalf("Conflict should preserve lock %q", key)
		}
	}
	if owner := server.Redis.Get(ctx, demoSessionLeaseKey).Val(); owner != "tab-a" {
		t.Fatalf("Expected lease owner tab-a, got %q", owner)
	}
}

func TestHandleDemoReset_ForceTakeoverClearsOnlyDemoLocks(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	ctx := context.Background()
	if err := server.Redis.Set(ctx, demoSessionLeaseKey, "tab-a", demoSessionLeaseTTL).Err(); err != nil {
		t.Fatalf("Set lease: %v", err)
	}
	for _, key := range []string{
		"dispatch:lock:demo-driver-01",
		"dispatch:lock:demo-driver-08",
		"dispatch:lock:production-driver",
	} {
		if err := server.Redis.Set(ctx, key, "request", time.Minute).Err(); err != nil {
			t.Fatalf("Set lock %q: %v", key, err)
		}
	}

	req := newDemoResetRequest(t, "tab-b", true, nil)
	w := httptest.NewRecorder()
	server.HandleDemoReset(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d", w.Code)
	}

	var resp demoResetResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if resp.OwnerSessionID != "tab-b" {
		t.Fatalf("Expected owner_session_id tab-b, got %q", resp.OwnerSessionID)
	}
	if resp.ClearedLocks != 2 {
		t.Fatalf("Expected cleared_locks 2, got %d", resp.ClearedLocks)
	}
	for _, key := range []string{
		"dispatch:lock:demo-driver-01",
		"dispatch:lock:demo-driver-08",
	} {
		if exists := server.Redis.Exists(ctx, key).Val(); exists != 0 {
			t.Fatalf("Force takeover should clear demo lock %q", key)
		}
	}
	if exists := server.Redis.Exists(ctx, "dispatch:lock:production-driver").Val(); exists != 1 {
		t.Fatal("Force takeover should preserve non-demo locks")
	}
	if owner := server.Redis.Get(ctx, demoSessionLeaseKey).Val(); owner != "tab-b" {
		t.Fatalf("Expected lease owner tab-b, got %q", owner)
	}
}

func TestHandleDemoReset_ClearsOnlyFixedRiderAssignments(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()
	ctx := context.Background()

	seed := func(driverID string, lon float64) {
		t.Helper()
		if err := server.geoService.AddLocation(ctx, geospatial.Location{ID: driverID, Longitude: lon, Latitude: 0}); err != nil {
			t.Fatalf("AddLocation(%s): %v", driverID, err)
		}
		if err := server.driverService.SetStatus(ctx, driverID, driver.StatusAvailable); err != nil {
			t.Fatalf("SetStatus(%s): %v", driverID, err)
		}
	}
	seed("demo-driver-01", 0)
	seed("production-driver", 0.001)

	demoRide := server.dispatcher.FindAndAssign(ctx, dispatch.DispatchRequest{
		RequestID: "demo-request", RiderID: "demo-rider-01", Longitude: 0, Latitude: 0, RadiusKm: 2,
	})
	if !demoRide.Success {
		t.Fatalf("demo dispatch = %+v", demoRide)
	}
	productionRide := server.dispatcher.FindAndAssign(ctx, dispatch.DispatchRequest{
		RequestID: "production-request", RiderID: "production-rider", Longitude: 0, Latitude: 0, RadiusKm: 2,
	})
	if !productionRide.Success {
		t.Fatalf("production dispatch = %+v", productionRide)
	}

	req := newDemoResetRequest(t, "tab-a", false, nil)
	w := httptest.NewRecorder()
	server.HandleDemoReset(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("reset status=%d body=%q", w.Code, w.Body.String())
	}
	if active := server.Redis.Get(ctx, "test-rider-active:demo-rider-01").Val(); active != "" {
		t.Fatalf("demo rider active request = %q", active)
	}
	if owner := server.Redis.Get(ctx, "test-driver-active:demo-driver-01").Val(); owner != "" {
		t.Fatalf("demo driver owner = %q", owner)
	}
	demoAssignment, err := server.dispatcher.GetAssignment(ctx, "demo-request")
	if err != nil || demoAssignment.Status != dispatch.AssignmentCancelled {
		t.Fatalf("demo assignment = %+v, err=%v", demoAssignment, err)
	}
	if active := server.Redis.Get(ctx, "test-rider-active:production-rider").Val(); active != "production-request" {
		t.Fatalf("production rider active request = %q", active)
	}
	if owner := server.Redis.Get(ctx, "test-driver-active:production-driver").Val(); owner != "production-request" {
		t.Fatalf("production driver owner = %q", owner)
	}
}

func TestHandleDemoReset_ClearsArrivedDemoOwnershipAndKeepsHistory(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()
	ctx := context.Background()
	if err := server.geoService.AddLocation(ctx, geospatial.Location{ID: "demo-driver-01", Longitude: 0, Latitude: 0}); err != nil {
		t.Fatalf("AddLocation: %v", err)
	}
	if err := server.driverService.SetStatus(ctx, "demo-driver-01", driver.StatusAvailable); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
	ride := server.dispatcher.FindAndAssign(ctx, dispatch.DispatchRequest{
		RequestID: "demo-arrived", RiderID: "demo-rider-01", Longitude: 0, Latitude: 0, RadiusKm: 2,
	})
	if !ride.Success {
		t.Fatalf("dispatch = %+v", ride)
	}
	if _, err := server.dispatcher.ArriveAssignment(ctx, ride.RequestID); err != nil {
		t.Fatalf("ArriveAssignment: %v", err)
	}

	req := newDemoResetRequest(t, "tab-a", false, nil)
	w := httptest.NewRecorder()
	server.HandleDemoReset(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("reset status=%d body=%q", w.Code, w.Body.String())
	}
	if active := server.Redis.Get(ctx, "test-rider-active:demo-rider-01").Val(); active != "" {
		t.Fatalf("demo rider active request = %q", active)
	}
	if owner := server.Redis.Get(ctx, "test-driver-active:demo-driver-01").Val(); owner != "" {
		t.Fatalf("demo driver owner = %q", owner)
	}
	assignment, err := server.dispatcher.GetAssignment(ctx, ride.RequestID)
	if err != nil || assignment.Status != dispatch.AssignmentArrived {
		t.Fatalf("arrived assignment history = %+v, err=%v", assignment, err)
	}
}

func TestHandleDemoReset_InvalidInput(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	testCases := []struct {
		name string
		body string
	}{
		{name: "invalid json", body: `{"session_id":`},
		{name: "missing session id", body: `{"session_id":" ","force":false}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/demo/reset", bytes.NewReader([]byte(tc.body)))
			w := httptest.NewRecorder()

			server.HandleDemoReset(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("Expected status 400, got %d", w.Code)
			}
		})
	}
}

func TestHandleDemoReset_MethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/demo/reset", nil)
	w := httptest.NewRecorder()

	(&Server{}).HandleDemoReset(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("Expected status 405, got %d", w.Code)
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

func TestGetLocation_RedisFailure(t *testing.T) {
	server := setupTestServer(t)
	if err := server.Redis.Close(); err != nil {
		t.Fatalf("Failed to close Redis client: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/geo/get?id=driver1", nil)
	w := httptest.NewRecorder()
	server.HandleGetLocation(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}
}

func TestGetBudget_WhitespaceKey(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	req := httptest.NewRequest(http.MethodGet, "/ratelimit/budget/get?key=%20%20", nil)
	w := httptest.NewRecorder()
	server.HandleGetBudget(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
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
