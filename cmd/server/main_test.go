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

func TestRunHealthcheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := runHealthcheck(server.URL); err != nil {
		t.Fatalf("runHealthcheck failed: %v", err)
	}
}

func TestRunHealthcheckRejectsNonOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	if err := runHealthcheck(server.URL); err == nil {
		t.Fatal("Expected non-200 response to fail")
	}
}
