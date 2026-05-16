package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/time/rate"
)

type TelemetryEvent struct {
	Service    string  `json:"service"`
	Endpoint   string  `json:"endpoint"`
	StatusCode int     `json:"status_code"`
	LatencyMs  float64 `json:"latency_ms"`
	Timestamp  string  `json:"timestamp"`
}

var js jetstream.JetStream

// pendingMessages tracks how many unprocessed messages are in NATS.
// atomic.Int64 allows safe read/write from multiple goroutines without a mutex.
var pendingMessages atomic.Int64

const backpressureThreshold = 5000

// Per-IP rate limiting
const (
	rateLimit       = 100 // requests per second per IP
	cleanupInterval = 10 * time.Minute
)

type ipLimiter struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

var (
	limiters sync.Map // map[string]*ipLimiter
)

// getClientIP extracts the client IP, checking proxy headers first.
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For (may contain multiple IPs: client, proxy1, proxy2)
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// First IP is the original client
		if ip := strings.TrimSpace(strings.Split(xff, ",")[0]); ip != "" {
			return ip
		}
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	// Fall back to RemoteAddr, stripping the port
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// getLimiter returns the rate limiter for a given IP, creating one if needed.
func getLimiter(ip string) *rate.Limiter {
	now := time.Now()
	if v, ok := limiters.Load(ip); ok {
		entry := v.(*ipLimiter)
		entry.lastSeen = now
		return entry.limiter
	}
	limiter := rate.NewLimiter(rate.Limit(rateLimit), rateLimit)
	limiters.Store(ip, &ipLimiter{limiter: limiter, lastSeen: now})
	return limiter
}

// cleanupLimiters removes stale IP entries that haven't been seen recently.
func cleanupLimiters() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		cutoff := time.Now().Add(-cleanupInterval)
		var removed int
		limiters.Range(func(key, value any) bool {
			entry := value.(*ipLimiter)
			if entry.lastSeen.Before(cutoff) {
				limiters.Delete(key)
				removed++
			}
			return true
		})
		if removed > 0 {
			log.Printf("Rate limiter cleanup: removed %d stale entries", removed)
		}
	}
}

func handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	// Rate limit check — per-IP, checked before backpressure
	clientIP := getClientIP(r)
	if !getLimiter(clientIP).Allow() {
		log.Printf("Rate limited: %s", clientIP)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests) // 429
		fmt.Fprintf(w, `{"error": "rate limit exceeded", "limit": "%d req/s"}`, rateLimit)
		return
	}

	var event TelemetryEvent
	err := json.NewDecoder(r.Body).Decode(&event)
	if err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	if event.Timestamp == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	// Backpressure check — reject if worker is too far behind
	pending := pendingMessages.Load()
	if pending > backpressureThreshold {
		log.Printf("Backpressure active: %d pending messages", pending)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusServiceUnavailable) // 503
		fmt.Fprintf(w, `{"error": "backpressure active", "pending": %d}`, pending)
		return
	}

	data, err := json.Marshal(event)
	if err != nil {
		http.Error(w, "failed to marshal event", http.StatusInternalServerError)
		return
	}

	_, err = js.Publish(r.Context(), "TELEMETRY.events", data)
	if err != nil {
		log.Printf("Failed to publish to NATS: %v", err)
		http.Error(w, "failed to queue event", http.StatusInternalServerError)
		return
	}

	log.Printf("Published: service=%s endpoint=%s status=%d latency=%.2fms",
		event.Service, event.Endpoint, event.StatusCode, event.LatencyMs)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, `{"status": "accepted"}`)
}

// monitorLag runs in a background goroutine. Every 3 seconds, it asks NATS
// how many messages the consumer hasn't processed yet and updates the
// shared pendingMessages counter.
func monitorLag(js jetstream.JetStream, stream, consumerName string) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		consumer, err := js.Consumer(context.Background(), stream, consumerName)
		if err != nil {
			// Consumer doesn't exist yet — worker hasn't started.
			// Check stream directly to see if messages are piling up.
			streamInfo, sErr := js.Stream(context.Background(), stream)
			if sErr == nil {
				info, iErr := streamInfo.Info(context.Background())
				if iErr == nil && info.State.Msgs > 0 {
					pendingMessages.Store(int64(info.State.Msgs))
					log.Printf("Lag monitor: consumer not ready, %d messages in stream", info.State.Msgs)
				}
			}
			continue
		}

		info, err := consumer.Info(context.Background())
		if err != nil {
			log.Printf("Lag monitor: failed to get info: %v", err)
			continue
		}

		// NumPending = messages in the stream that this consumer hasn't processed
		pending := int64(info.NumPending)
		pendingMessages.Store(pending)

		if pending > 0 {
			log.Printf("Lag monitor: %d pending messages", pending)
		}
	}
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal("Failed to connect to NATS: ", err)
	}
	defer nc.Close()

	log.Println("Connected to NATS")

	js, err = jetstream.New(nc)
	if err != nil {
		log.Fatal("Failed to create JetStream: ", err)
	}

	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "TELEMETRY",
		Subjects: []string{"TELEMETRY.>"},
	})
	if err != nil {
		log.Fatal("Failed to create stream: ", err)
	}

	log.Println("JetStream stream 'TELEMETRY' ready")

	// Start background lag monitor
	go monitorLag(js, "TELEMETRY", "duckdb-writer")
	log.Printf("Backpressure enabled (threshold: %d pending messages)", backpressureThreshold)

	// Start rate limiter cleanup
	go cleanupLimiters()
	log.Printf("Per-IP rate limiting enabled (%d req/s, cleanup every %v)", rateLimit, cleanupInterval)

	http.HandleFunc("/ingest", handleIngest)

	port := ":8090"
	log.Printf("Telemetry server starting on %s", port)
	err = http.ListenAndServe(port, nil)
	if err != nil {
		log.Fatal(err)
	}
}
