package server

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
)

type RateLimitRequest struct {
	Key  string `json:"key"`
	Cost int64  `json:"cost"`
}

type RateLimitResponse struct {
	Allowed   bool  `json:"allowed"`
	Remaining int64 `json:"remaining"`
}

func (s *Server) HandleRateLimitCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RateLimitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("[RateLimit] Invalid request: %v", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	log.Printf("[RateLimit] Checking key=%s cost=%d", req.Key, req.Cost)
	allowed, remaining, err := s.rateLimiter.AllowAtomic(r.Context(), req.Key, req.Cost)
	if err != nil {
		log.Printf("[RateLimit] Error checking key=%s: %v", req.Key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[RateLimit] Result key=%s allowed=%v remaining=%d", req.Key, allowed, remaining)
	resp := RateLimitResponse{
		Allowed:   allowed,
		Remaining: remaining,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) HandleSetBudget(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	amountStr := r.URL.Query().Get("amount")

	amount, err := strconv.ParseInt(amountStr, 10, 64)
	if err != nil {
		log.Printf("[RateLimit] Invalid amount for SetBudget: %s", amountStr)
		http.Error(w, "Invalid amount", http.StatusBadRequest)
		return
	}

	log.Printf("[RateLimit] Setting budget key=%s amount=%d", key, amount)
	if err := s.rateLimiter.SetBudget(r.Context(), key, amount); err != nil {
		log.Printf("[RateLimit] Error setting budget key=%s: %v", key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[RateLimit] Budget set successfully key=%s amount=%d", key, amount)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "success"})
}

func (s *Server) HandleGetBudget(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		log.Printf("[RateLimit] GetBudget missing key parameter")
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	log.Printf("[RateLimit] Getting budget key=%s", key)
	budget, err := s.rateLimiter.GetBudget(r.Context(), key)
	if err != nil {
		log.Printf("[RateLimit] Error getting budget key=%s: %v", key, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[RateLimit] Budget retrieved key=%s budget=%d", key, budget)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{"budget": budget})
}
