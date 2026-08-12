package server

import (
	"net/http/httptest"
	"testing"
)

func TestCheckWebSocketOrigin(t *testing.T) {
	tests := []struct {
		name   string
		origin string
		want   bool
	}{
		{name: "same HTTPS origin", origin: "https://demo.example.com", want: true},
		{name: "same HTTP origin", origin: "http://demo.example.com", want: true},
		{name: "non-browser client", want: true},
		{name: "different origin", origin: "https://attacker.example", want: false},
		{name: "malformed origin", origin: "://invalid", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "https://demo.example.com/ws/driver", nil)
			if tt.origin != "" {
				req.Header.Set("Origin", tt.origin)
			}
			if got := checkWebSocketOrigin(req); got != tt.want {
				t.Fatalf("checkWebSocketOrigin() = %t, want %t", got, tt.want)
			}
		})
	}
}
