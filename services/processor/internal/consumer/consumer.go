package consumer

import (
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/sandevil23/scryon/services/processor/internal/writer"
)

// Consumer pulls messages from NATS JetStream and routes them
// to the appropriate backend writer.
type Consumer struct {
	jets 		jetstream.JetStream
	victoria	*writer.VictoriaWriter
	log			*slog.Logger
	workers 	int
}