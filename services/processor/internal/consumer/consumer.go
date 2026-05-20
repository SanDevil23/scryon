package consumer

import (
	"context"
	"log/slog"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/sandevil23/scryon/services/processor/internal/writer"
)

// Consumer pulls messages from NATS JetStream and routes them
// to the appropriate backend writer.
type Consumer struct {
	jets 		jetstream.JetStream
	vWriter		*writer.VictoriaWriter
	log			*slog.Logger
	workers 	int
}


func New(js jetstream.JetStream, wr *writer.VictoriaWriter, log *slog.Logger, workers int) *Consumer {
	return &Consumer {
		jets: 			js,
		vWriter:		wr,
		log: 			log,
		workers: 		workers,
	}
}

// process routes a message to the correct writer based on subject.
func (c *Consumer) process(ctx context.Context, msg jetstream.Msg) error {
	sub := msg.Subject()
	c.log.Debug("processing message", "subject", sub)

	switch{
	case len(sub) > 19 && sub[:19] == "telemetry.metrics.":
		return c.handleMetrics(ctx, msg.Data())
	default:
		c.log.Debug("no handler for subject, skipping", "subject", sub)
		return nil
	}
}