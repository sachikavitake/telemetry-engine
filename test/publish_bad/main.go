package main

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This simulates a buggy producer that sends bad data directly to NATS,
// bypassing API validation. Used to test the Dead Letter Queue.
func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Use JetStream publish so messages are tracked properly
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// Bad message 1: latency_ms is a string instead of a number
	_, err = js.Publish(ctx, "TELEMETRY.events", []byte(`{"service":"broken-svc","latency_ms":"oops"}`))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Published bad message 1: string instead of float")

	// Bad message 2: not valid JSON at all
	_, err = js.Publish(ctx, "TELEMETRY.events", []byte(`this is not json at all`))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Published bad message 2: garbage data")

	log.Println("Done — both messages published via JetStream")
}
