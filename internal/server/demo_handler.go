package server

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

//go:embed demo/index.html
var demoPage []byte

var demoDriverIDs = []string{
	"demo-driver-01",
	"demo-driver-02",
	"demo-driver-03",
	"demo-driver-04",
	"demo-driver-05",
	"demo-driver-06",
	"demo-driver-07",
	"demo-driver-08",
}

const (
	demoSessionLeaseKey = "demo:session:lease"
	demoSessionLeaseTTL = 15 * time.Minute
)

const demoResetLeaseScript = `
local lease_key = KEYS[1]
local requested_owner = ARGV[1]
local ttl_ms = ARGV[2]
local force = ARGV[3]
local clear_locks = ARGV[4]

local current_owner = redis.call("GET", lease_key)
if current_owner and current_owner ~= requested_owner and force ~= "1" then
  return {0, current_owner, 0}
end

redis.call("SET", lease_key, requested_owner, "PX", ttl_ms)

local cleared = 0
if clear_locks == "1" then
  for i = 2, #KEYS do
    cleared = cleared + redis.call("DEL", KEYS[i])
  end
end

return {1, requested_owner, cleared}
`

type demoResetRequest struct {
	SessionID  string `json:"session_id"`
	Force      bool   `json:"force"`
	ClearLocks *bool  `json:"clear_locks,omitempty"`
}

type demoResetResponse struct {
	ClearedLocks   int64  `json:"cleared_locks"`
	OwnerSessionID string `json:"owner_session_id"`
}

// HandleDemo serves the self-contained interactive demo.
func (s *Server) HandleDemo(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(demoPage)
}

// HandleDemoReset claims or refreshes the demo lease for a browser session and
// optionally clears only the fixed interview fleet locks.
func (s *Server) HandleDemoReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req demoResetRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid demo reset payload", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid demo reset payload", http.StatusBadRequest)
		return
	}
	req.SessionID = strings.TrimSpace(req.SessionID)
	if req.SessionID == "" {
		http.Error(w, "session_id is required", http.StatusBadRequest)
		return
	}

	keys := make([]string, len(demoDriverIDs)+1)
	keys[0] = demoSessionLeaseKey
	for i, driverID := range demoDriverIDs {
		keys[i+1] = "dispatch:lock:" + driverID
	}
	forceFlag := "0"
	if req.Force {
		forceFlag = "1"
	}
	clearLocksFlag := "1"
	if req.ClearLocks != nil && !*req.ClearLocks {
		clearLocksFlag = "0"
	}

	result, err := s.Redis.Eval(
		r.Context(),
		demoResetLeaseScript,
		keys,
		req.SessionID,
		demoSessionLeaseTTL.Milliseconds(),
		forceFlag,
		clearLocksFlag,
	).Result()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	claimed, ownerSessionID, cleared, err := parseDemoResetResult(result)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if !claimed {
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":            "demo session already claimed",
			"owner_session_id": ownerSessionID,
		})
		return
	}

	_ = json.NewEncoder(w).Encode(demoResetResponse{
		ClearedLocks:   cleared,
		OwnerSessionID: ownerSessionID,
	})
}

func parseDemoResetResult(raw interface{}) (bool, string, int64, error) {
	values, ok := raw.([]interface{})
	if !ok || len(values) != 3 {
		return false, "", 0, fmt.Errorf("unexpected demo reset result: %T", raw)
	}

	claimed, ok := values[0].(int64)
	if !ok {
		return false, "", 0, fmt.Errorf("unexpected demo reset claim result: %T", values[0])
	}

	ownerSessionID, ok := values[1].(string)
	if !ok {
		return false, "", 0, fmt.Errorf("unexpected demo reset owner result: %T", values[1])
	}

	cleared, ok := values[2].(int64)
	if !ok {
		return false, "", 0, fmt.Errorf("unexpected demo reset cleared result: %T", values[2])
	}

	return claimed == 1, ownerSessionID, cleared, nil
}
