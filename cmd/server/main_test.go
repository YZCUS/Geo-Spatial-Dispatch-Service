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

func TestHTTPAddrFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		httpAddr string
		port     string
		want     string
	}{
		{name: "HTTP_ADDR takes precedence", httpAddr: ":9090", port: "8080", want: ":9090"},
		{name: "Cloud Run PORT", port: "8080", want: ":8080"},
		{name: "local default", want: ":8080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HTTP_ADDR", tt.httpAddr)
			t.Setenv("PORT", tt.port)
			if got := httpAddrFromEnv(); got != tt.want {
				t.Fatalf("httpAddrFromEnv() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHTTPMuxRegistersDispatchLifecycleRoutes(t *testing.T) {
	mux := newHTTPMux(&server.Server{})

	for _, path := range []string{"/dispatch/cancel", "/dispatch/arrive"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status=%d, want 405", path, w.Code)
		}
	}
}
