package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/YZCUS/geo-spatial-dispatch-service/internal/server"
)

func TestHealthCheck(t *testing.T) {
	srv := &server.Server{}

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	srv.HandleHealthCheck(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	if w.Body.String() != "OK" {
		t.Errorf("Expected body %s, got %s", "OK", w.Body.String())
	}
}
