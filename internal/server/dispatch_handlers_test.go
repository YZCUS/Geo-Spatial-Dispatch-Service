package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/dispatch"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/driver"
	"github.com/YZCUS/geo-spatial-dispatch-service/internal/geospatial"
)

func postJSON(t *testing.T, path string, payload interface{}, handler http.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal payload: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler(w, req)
	return w
}

func seedServerDriver(t *testing.T, server *Server, driverID string, lon, lat float64) {
	t.Helper()
	ctx := context.Background()
	if err := server.geoService.AddLocation(ctx, geospatial.Location{
		ID: driverID, Longitude: lon, Latitude: lat,
	}); err != nil {
		t.Fatalf("AddLocation: %v", err)
	}
	if err := server.driverService.SetStatus(ctx, driverID, driver.StatusAvailable); err != nil {
		t.Fatalf("SetStatus: %v", err)
	}
}

func dispatchHTTPRide(t *testing.T, server *Server, requestID, riderID string, lon, lat float64) dispatch.DispatchResult {
	t.Helper()
	w := postJSON(t, "/dispatch/request", DispatchRequestDTO{
		RequestID: requestID,
		RiderID:   riderID,
		Longitude: lon,
		Latitude:  lat,
		RadiusKm:  2,
	}, server.HandleDispatchRequest)
	if w.Code != http.StatusOK {
		t.Fatalf("dispatch status=%d body=%q", w.Code, w.Body.String())
	}
	var result dispatch.DispatchResult
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("Decode dispatch response: %v", err)
	}
	return result
}

func TestHandleDispatchRequestWithRiderPersistsEnRouteAssignment(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()
	seedServerDriver(t, server, "driver-1", 0, 0)

	result := dispatchHTTPRide(t, server, "request-1", "rider-1", 0, 0)
	if !result.Success || result.RequestID != "request-1" || result.DriverID != "driver-1" ||
		result.Status != dispatch.AssignmentEnRoute {
		t.Fatalf("dispatch response = %+v", result)
	}
	assignment, err := server.dispatcher.GetAssignment(context.Background(), "request-1")
	if err != nil || assignment.RiderID != "rider-1" || assignment.Status != dispatch.AssignmentEnRoute {
		t.Fatalf("persisted assignment = %+v, err=%v", assignment, err)
	}

	conflict := postJSON(t, "/dispatch/request", DispatchRequestDTO{
		RequestID: "request-2", RiderID: "rider-1", Longitude: 0, Latitude: 0, RadiusKm: 2,
	}, server.HandleDispatchRequest)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("active rider status=%d body=%q", conflict.Code, conflict.Body.String())
	}
}

func TestHandleDispatchRequestWithoutRiderKeepsLegacyResponse(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()
	seedServerDriver(t, server, "driver-1", 0, 0)

	w := postJSON(t, "/dispatch/request", DispatchRequestDTO{
		RequestID: "legacy-request", Longitude: 0, Latitude: 0, RadiusKm: 2,
	}, server.HandleDispatchRequest)
	if w.Code != http.StatusOK {
		t.Fatalf("legacy dispatch status=%d body=%q", w.Code, w.Body.String())
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&raw); err != nil {
		t.Fatalf("Decode response: %v", err)
	}
	if _, exists := raw["status"]; exists {
		t.Fatalf("legacy dispatch unexpectedly advertised lifecycle status: %#v", raw)
	}
}

func TestHandleDispatchCancelAllowsRebook(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()
	seedServerDriver(t, server, "driver-1", 0, 0)
	dispatchHTTPRide(t, server, "request-1", "rider-1", 0, 0)

	w := postJSON(t, "/dispatch/cancel", DispatchLifecycleDTO{RequestID: "request-1"}, server.HandleDispatchCancel)
	if w.Code != http.StatusOK {
		t.Fatalf("cancel status=%d body=%q", w.Code, w.Body.String())
	}
	var cancelled dispatch.Assignment
	if err := json.NewDecoder(w.Body).Decode(&cancelled); err != nil {
		t.Fatalf("Decode cancel response: %v", err)
	}
	if cancelled.Status != dispatch.AssignmentCancelled || cancelled.DriverID != "driver-1" {
		t.Fatalf("cancel response = %+v", cancelled)
	}

	rebook := dispatchHTTPRide(t, server, "request-2", "rider-1", 0, 0)
	if rebook.Status != dispatch.AssignmentEnRoute {
		t.Fatalf("rebook response = %+v", rebook)
	}
}

func TestHandleDispatchArriveAndLateCancel(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()
	seedServerDriver(t, server, "driver-1", 0.001, 0)
	dispatchHTTPRide(t, server, "request-1", "rider-1", 0, 0)

	tooFar := postJSON(t, "/dispatch/arrive", DispatchLifecycleDTO{RequestID: "request-1"}, server.HandleDispatchArrive)
	if tooFar.Code != http.StatusConflict {
		t.Fatalf("far arrive status=%d body=%q", tooFar.Code, tooFar.Body.String())
	}
	if err := server.dispatcher.UpdateDriverLocation(context.Background(), "driver-1", 0.0001, 0); err != nil {
		t.Fatalf("Move driver: %v", err)
	}

	arrive := postJSON(t, "/dispatch/arrive", DispatchLifecycleDTO{RequestID: "request-1"}, server.HandleDispatchArrive)
	if arrive.Code != http.StatusOK {
		t.Fatalf("arrive status=%d body=%q", arrive.Code, arrive.Body.String())
	}
	var arrived dispatch.Assignment
	if err := json.NewDecoder(arrive.Body).Decode(&arrived); err != nil {
		t.Fatalf("Decode arrive response: %v", err)
	}
	if arrived.Status != dispatch.AssignmentArrived {
		t.Fatalf("arrive response = %+v", arrived)
	}

	lateCancel := postJSON(t, "/dispatch/cancel", DispatchLifecycleDTO{RequestID: "request-1"}, server.HandleDispatchCancel)
	if lateCancel.Code != http.StatusConflict {
		t.Fatalf("late cancel status=%d body=%q", lateCancel.Code, lateCancel.Body.String())
	}
	status, err := server.driverService.GetStatus(context.Background(), "driver-1")
	if err != nil || status != driver.StatusBusy {
		t.Fatalf("driver status after late cancel=%q err=%v", status, err)
	}
}

func TestDispatchLifecycleHandlersValidateInputAndMethod(t *testing.T) {
	server := setupTestServer(t)
	defer server.Redis.Close()

	for name, handler := range map[string]http.HandlerFunc{
		"cancel": server.HandleDispatchCancel,
		"arrive": server.HandleDispatchArrive,
	} {
		t.Run(name+" invalid json", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/dispatch/"+name, bytes.NewBufferString("{"))
			w := httptest.NewRecorder()
			handler(w, req)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
			}
		})
		t.Run(name+" missing request", func(t *testing.T) {
			w := postJSON(t, "/dispatch/"+name, DispatchLifecycleDTO{}, handler)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
			}
		})
		t.Run(name+" method", func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/dispatch/"+name, nil)
			w := httptest.NewRecorder()
			handler(w, req)
			if w.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status=%d body=%q", w.Code, w.Body.String())
			}
		})
	}
}
