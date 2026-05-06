package handler

import (
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
)

// Ingest Handler handles incoming telemetry and publishes to NATS JetStream.
type IngestHandler struct { 
	js			jetstream.JetStream
	streamName 	string
	log 		*slog.Logger
}

func NewIngestHanlder(js jetstream.JetStream, strmName string, logger *slog.Logger) *IngestHandler {
	return &IngestHandler{
		js: js,
		streamName: strmName,
		log: logger,
	}
}
