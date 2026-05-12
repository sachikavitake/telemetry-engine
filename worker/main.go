package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	_ "github.com/marcboeker/go-duckdb"
)

type TelemetryEvent struct {
	Service    string  `json:"service"`
	Endpoint   string  `json:"endpoint"`
	StatusCode int     `json:"status_code"`
	LatencyMs  float64 `json:"latency_ms"`
	Timestamp  string  `json:"timestamp"`
}

// DLQMessage is what we store when a message fails processing 3 times.
type DLQMessage struct {
	OriginalData  string `json:"original_data"`
	Error         string `json:"error"`
	FailedAt      string `json:"failed_at"`
	DeliveryCount int    `json:"delivery_count"`
}

const maxRetries = 3

func main() {
	db, err := sql.Open("duckdb", "telemetry.db")
	if err != nil {
		log.Fatal("Failed to open DuckDB: ", err)
	}
	defer db.Close()

	// Create both tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			service    VARCHAR,
			endpoint   VARCHAR,
			status_code INTEGER,
			latency_ms DOUBLE,
			timestamp  TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatal("Failed to create events table: ", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS dead_letters (
			original_data    VARCHAR,
			error            VARCHAR,
			failed_at        TIMESTAMP,
			delivery_count   INTEGER
		)
	`)
	if err != nil {
		log.Fatal("Failed to create dead_letters table: ", err)
	}
	log.Println("DuckDB ready — tables 'events' and 'dead_letters' exist")

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal("Failed to connect to NATS: ", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("Failed to create JetStream: ", err)
	}
	log.Println("Connected to NATS")

	ctx := context.Background()
	consumer, err := js.CreateOrUpdateConsumer(ctx, "TELEMETRY", jetstream.ConsumerConfig{
		Name:       "duckdb-writer",
		Durable:    "duckdb-writer",
		MaxDeliver: maxRetries, // NATS will deliver each message at most 3 times
		// After 3 Nak()s, NATS stops redelivering automatically.
		// We intercept on the 3rd attempt to save to DLQ before that happens.
		FilterSubject: "TELEMETRY.events", // Only consume events, not deadletter messages
	})
	if err != nil {
		log.Fatal("Failed to create consumer: ", err)
	}
	log.Println("NATS consumer 'duckdb-writer' ready (max retries: 3)")

	go startQueryAPI(db)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	log.Println("Worker started — waiting for events...")

	for {
		batch, err := consumer.Fetch(100,
			jetstream.FetchMaxWait(5*time.Second),
		)
		if err != nil {
			log.Printf("Fetch error: %v", err)
			continue
		}

		var events []TelemetryEvent
		var msgs []jetstream.Msg

		for msg := range batch.Messages() {
			var event TelemetryEvent
			if err := json.Unmarshal(msg.Data(), &event); err != nil {
				handleBadMessage(js, db, msg, err)
				continue
			}
			events = append(events, event)
			msgs = append(msgs, msg)
		}

		if len(events) == 0 {
			select {
			case <-sigCh:
				log.Println("Shutting down...")
				return
			default:
				continue
			}
		}

		if err := writeBatch(db, events); err != nil {
			log.Printf("Failed to write batch: %v", err)
			continue
		}

		for _, msg := range msgs {
			msg.Ack()
		}

		log.Printf("Wrote %d events to DuckDB", len(events))
	}
}

// handleBadMessage decides whether to retry or send to the dead letter queue.
// On attempts 1-2: Nak() tells NATS to redeliver.
// On attempt 3: save to DLQ and Ack() so NATS stops redelivering.
func handleBadMessage(js jetstream.JetStream, db *sql.DB, msg jetstream.Msg, parseErr error) {
	deliveryCount := getDeliveryCount(msg)

	if deliveryCount >= maxRetries {
		// Final attempt — send to dead letter queue
		log.Printf("Dead-lettering message after %d attempts: %v", deliveryCount, parseErr)
		publishToDLQ(js, msg.Data(), parseErr.Error(), deliveryCount)
		saveToDLQ(db, msg.Data(), parseErr.Error(), deliveryCount)
		msg.Ack() // Tell NATS we're done with this message
	} else {
		// Retry — Nak tells NATS to redeliver this message
		log.Printf("Bad message (attempt %d/%d), will retry: %v", deliveryCount, maxRetries, parseErr)
		msg.Nak()
	}
}

// getDeliveryCount reads the delivery count from NATS message metadata.
// JetStream tracks how many times a message has been delivered via msg.Metadata().
func getDeliveryCount(msg jetstream.Msg) int {
	meta, err := msg.Metadata()
	if err != nil {
		return 1
	}
	// NumDelivered starts at 1 for the first delivery
	return int(meta.NumDelivered)
}

// publishToDLQ sends the failed message to the TELEMETRY.deadletter subject.
// This keeps a copy in NATS for potential reprocessing later.
func publishToDLQ(js jetstream.JetStream, rawData []byte, errMsg string, deliveryCount int) {
	dlq := DLQMessage{
		OriginalData:  string(rawData),
		Error:         errMsg,
		FailedAt:      time.Now().UTC().Format(time.RFC3339),
		DeliveryCount: deliveryCount,
	}
	data, err := json.Marshal(dlq)
	if err != nil {
		log.Printf("Failed to marshal DLQ message: %v", err)
		return
	}
	_, err = js.Publish(context.Background(), "TELEMETRY.deadletter", data)
	if err != nil {
		log.Printf("Failed to publish to DLQ: %v", err)
	}
}

// saveToDLQ writes the failed message to the dead_letters table in DuckDB.
func saveToDLQ(db *sql.DB, rawData []byte, errMsg string, deliveryCount int) {
	_, err := db.Exec(
		`INSERT INTO dead_letters (original_data, error, failed_at, delivery_count) VALUES (?, ?, ?, ?)`,
		string(rawData), errMsg, time.Now().UTC(), deliveryCount,
	)
	if err != nil {
		log.Printf("Failed to save to DLQ table: %v", err)
	}
}

func startQueryAPI(db *sql.DB) {
	http.HandleFunc("/query/stats", func(w http.ResponseWriter, r *http.Request) {
		service := r.URL.Query().Get("service")
		window := r.URL.Query().Get("window")

		query := `
			SELECT
				COUNT(*)                                    AS total_events,
				ROUND(AVG(latency_ms), 2)                   AS avg_latency,
				ROUND(MIN(latency_ms), 2)                   AS min_latency,
				ROUND(MAX(latency_ms), 2)                   AS max_latency,
				ROUND(quantile_cont(latency_ms, 0.50), 2)   AS p50_latency,
				ROUND(quantile_cont(latency_ms, 0.95), 2)   AS p95_latency,
				ROUND(quantile_cont(latency_ms, 0.99), 2)   AS p99_latency
			FROM events
			WHERE 1=1
		`
		var args []any

		if service != "" {
			query += " AND service = ?"
			args = append(args, service)
		}
		if window != "" {
			duration, err := time.ParseDuration(window)
			if err != nil {
				http.Error(w, "invalid window format (use 1h, 30m, etc.)", http.StatusBadRequest)
				return
			}
			cutoff := time.Now().UTC().Add(-duration)
			query += " AND timestamp >= ?"
			args = append(args, cutoff)
		}

		var result struct {
			TotalEvents int      `json:"total_events"`
			AvgLatency  *float64 `json:"avg_latency_ms"`
			MinLatency  *float64 `json:"min_latency_ms"`
			MaxLatency  *float64 `json:"max_latency_ms"`
			P50Latency  *float64 `json:"p50_latency_ms"`
			P95Latency  *float64 `json:"p95_latency_ms"`
			P99Latency  *float64 `json:"p99_latency_ms"`
		}

		err := db.QueryRow(query, args...).Scan(
			&result.TotalEvents,
			&result.AvgLatency, &result.MinLatency, &result.MaxLatency,
			&result.P50Latency, &result.P95Latency, &result.P99Latency,
		)
		if err != nil {
			http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	http.HandleFunc("/query/services", func(w http.ResponseWriter, r *http.Request) {
		rows, err := db.Query(`
			SELECT
				service,
				COUNT(*)                                    AS total_events,
				ROUND(AVG(latency_ms), 2)                   AS avg_latency,
				ROUND(quantile_cont(latency_ms, 0.95), 2)   AS p95_latency,
				ROUND(SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
				                                            AS error_rate
			FROM events
			GROUP BY service
			ORDER BY total_events DESC
		`)
		if err != nil {
			http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		type ServiceStats struct {
			Service     string  `json:"service"`
			TotalEvents int     `json:"total_events"`
			AvgLatency  float64 `json:"avg_latency_ms"`
			P95Latency  float64 `json:"p95_latency_ms"`
			ErrorRate   float64 `json:"error_rate_pct"`
		}

		var results []ServiceStats
		for rows.Next() {
			var s ServiceStats
			rows.Scan(&s.Service, &s.TotalEvents, &s.AvgLatency, &s.P95Latency, &s.ErrorRate)
			results = append(results, s)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	// /query/dlq — inspect dead-lettered messages
	http.HandleFunc("/query/dlq", func(w http.ResponseWriter, r *http.Request) {
		limit := r.URL.Query().Get("limit")
		if limit == "" {
			limit = "20"
		}

		rows, err := db.Query(`
			SELECT original_data, error, failed_at, delivery_count
			FROM dead_letters
			ORDER BY failed_at DESC
			LIMIT ?
		`, limit)
		if err != nil {
			http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var results []DLQMessage
		for rows.Next() {
			var d DLQMessage
			rows.Scan(&d.OriginalData, &d.Error, &d.FailedAt, &d.DeliveryCount)
			results = append(results, d)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	log.Println("Query API starting on :8091")
	if err := http.ListenAndServe(":8091", nil); err != nil {
		log.Fatal("Query API failed: ", err)
	}
}

func writeBatch(db *sql.DB, events []TelemetryEvent) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare(`
		INSERT INTO events (service, endpoint, status_code, latency_ms, timestamp)
		VALUES (?, ?, ?, ?, ?)
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()

	for _, e := range events {
		_, err := stmt.Exec(e.Service, e.Endpoint, e.StatusCode, e.LatencyMs, e.Timestamp)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
