package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/sandevil23/scryon/services/processor/internal/writer"
)

// Consumer pulls messages from NATS JetStream and routes them
// to the appropriate backend writer.
type Consumer struct {
	jets    jetstream.JetStream
	vWriter *writer.VictoriaWriter
	log     *slog.Logger
	workers int
}

func New(js jetstream.JetStream, wr *writer.VictoriaWriter, log *slog.Logger, workers int) *Consumer {
	return &Consumer{
		jets:    js,
		vWriter: wr,
		log:     log,
		workers: workers,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	consumer, err := c.jets.CreateOrUpdateConsumer(ctx, "TELEMETRY", jetstream.ConsumerConfig{
		Name:    "processor",
		Durable: "processor",
		FilterSubjects: []string{
			"telemetry.metrics.>",
			"telemetry.logs.>",
			"telemetry.traces.>",
		},
		AckPolicy:  jetstream.AckExplicitPolicy,
		MaxDeliver: 5,
		AckWait:    30 * time.Second,
	})
	if err != nil {
		return err
	}

	msgCh := make(chan jetstream.Msg, c.workers*4)

	// dispatcher
	go func() {
		iter, err := consumer.Messages()
		if err != nil {
			c.log.Error("failed to start message iterator", "err", err)
			return
		}

		defer iter.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := iter.Next()
				if err != nil {
					return
				}
				msgCh <- msg
			}
		}
	}()

	// worker pool
	for i := 0; i < c.workers; i++ {
		go func() {
			for msg := range msgCh {
				if err := c.process(ctx, msg); err != nil {
					c.log.Error("processing failed",
						"err", err,
						"subject", msg.Subject(),
					)

					// if error : negative acknowledge -> re-deliver the message
					err := msg.Nak()
					if err != nil {
						c.log.Error("negative ack failed",
							"err", err,
							"subject", msg.Subject(),
						)
					}
				} else {
					// acknowledge the message
					err := msg.Ack()
					if err != nil {
						c.log.Error("acknowledgement failed",
							"err", err,
							"subject", msg.Subject(),
						)
					}
				}
			}
		}()
	}
	return nil
}

// process routes a message to the correct writer based on subject.
func (c *Consumer) process(ctx context.Context, msg jetstream.Msg) error {
	sub := msg.Subject()
	c.log.Debug("processing message", "subject", sub)

	switch {
	case len(sub) > 19 && sub[:19] == "telemetry.metrics.":
		return c.handleMetrics(ctx, msg.Data())
	default:
		c.log.Debug("no handler for subject, skipping", "subject", sub)
		return nil
	}
}

// --- metric event shape published by the ingest service ---

type metricEvent struct {
	TenantID   string        `json:"tenant_id"`
	Metrics    []metricEntry `json:"metrics"`
	ReceivedAt time.Time     `json:"received_at"`
}

type metricEntry struct {
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels"`
	Value     float64           `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
}

func (c *Consumer) handleMetrics(ctx context.Context, data []byte) error {
	var event metricEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}

	lines := make([]writer.MetricLine, 0, len(event.Metrics))
	for _, m := range event.Metrics {
		lines = append(lines, writer.MetricLine{
			Name:      m.Name,
			Labels:    m.Labels,
			Value:     m.Value,
			Timestamp: m.Timestamp,
		})
	}

	return c.vWriter.Write(ctx, event.TenantID, lines)
}
