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

func main() {
	db, err := sql.Open("duckdb", "telemetry.db")
	if err != nil {
		log.Fatal("Failed to open DuckDB: ", err)
	}
	defer db.Close()

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
		log.Fatal("Failed to create table: ", err)
	}
	log.Println("DuckDB ready — table 'events' exists")

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
		Name:    "duckdb-writer",
		Durable: "duckdb-writer",
	})
	if err != nil {
		log.Fatal("Failed to create consumer: ", err)
	}
	log.Println("NATS consumer 'duckdb-writer' ready")

	// --- NEW: Start the query API in a goroutine ---
	// A goroutine is like a lightweight thread. "go someFunction()" runs it
	// in the background while the rest of main() continues.
	// We need this because:
	//   - The NATS fetch loop below runs forever
	//   - The HTTP server also runs forever
	//   - We need BOTH running at the same time
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
				log.Printf("Bad message, skipping: %v", err)
				msg.Ack()
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

// startQueryAPI runs an HTTP server on port 8091 with analytics endpoints.
func startQueryAPI(db *sql.DB) {
	// /query/stats — overall stats and percentiles, optionally filtered by service
	http.HandleFunc("/query/stats", func(w http.ResponseWriter, r *http.Request) {
		service := r.URL.Query().Get("service") // e.g., /query/stats?service=auth-service
		window := r.URL.Query().Get("window")   // e.g., /query/stats?window=1h

		// Build the WHERE clause dynamically based on filters
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
			// Parse window like "1h", "30m", "24h"
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

	// /query/services — breakdown by service
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
