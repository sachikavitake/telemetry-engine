package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"       // NATS client
	"github.com/nats-io/nats.go/jetstream" // JetStream API
)

type TelemetryEvent struct {
	Service    string  `json:"service"`
	Endpoint   string  `json:"endpoint"`
	StatusCode int     `json:"status_code"`
	LatencyMs  float64 `json:"latency_ms"`
	Timestamp  string  `json:"timestamp"`
}

// We store the JetStream instance globally so our handler can use it.
// In Go, variables declared outside functions are "package-level" (like module globals in Python).
var js jetstream.JetStream

func handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
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

	// Convert the event back to JSON bytes to send to NATS.
	// json.Marshal is the opposite of Decode — struct → JSON bytes.
	data, err := json.Marshal(event)
	if err != nil {
		http.Error(w, "failed to marshal event", http.StatusInternalServerError)
		return
	}

	// Publish the event to the "TELEMETRY.events" subject.
	// Think of a "subject" as a topic/channel name.
	// The "r.Context()" passes the request's context — if the request is cancelled,
	// the publish will be cancelled too.
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

func main() {
	// --- Step 1: Connect to NATS ---
	// nats.DefaultURL is "nats://localhost:4222" (the port we exposed from Docker)
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal("Failed to connect to NATS: ", err)
	}
	defer nc.Close() // "defer" = run this when main() exits. Like Python's "with" or "finally".

	log.Println("Connected to NATS")

	// --- Step 2: Create a JetStream instance ---
	js, err = jetstream.New(nc)
	if err != nil {
		log.Fatal("Failed to create JetStream: ", err)
	}

	// --- Step 3: Create a "Stream" ---
	// A Stream is where JetStream stores messages on disk.
	// We tell it: "Capture any message published to subjects matching TELEMETRY.>"
	// The ">" is a wildcard — it matches TELEMETRY.events, TELEMETRY.errors, etc.
	// context.Background() is a "no deadline, no cancellation" context.
	// Used during setup because there's no HTTP request context here.
	_, err = js.CreateOrUpdateStream(context.Background(), jetstream.StreamConfig{
		Name:     "TELEMETRY",          // Name of the stream
		Subjects: []string{"TELEMETRY.>"}, // Which subjects to capture
	})
	if err != nil {
		log.Fatal("Failed to create stream: ", err)
	}

	log.Println("JetStream stream 'TELEMETRY' ready")

	// --- Step 4: Start HTTP server ---
	http.HandleFunc("/ingest", handleIngest)

	port := ":8090"
	log.Printf("Telemetry server starting on %s", port)
	err = http.ListenAndServe(port, nil)
	if err != nil {
		log.Fatal(err)
	}
}
