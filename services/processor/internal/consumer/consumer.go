package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sync/errgroup"

	"github.com/sandevil23/scryon/services/processor/internal/writer"
)

// Consumer pulls messages from NATS JetStream and routes them
// to the appropriate backend writer.
type Consumer struct {
	jets    jetstream.JetStream
	vWriter *writer.VictoriaWriter
	lw      *writer.LokiWriter
	log     *slog.Logger
	workers int
}

func New(js jetstream.JetStream, wr *writer.VictoriaWriter, loki *writer.LokiWriter, log *slog.Logger, workers int) *Consumer {
	return &Consumer{
		jets:    js,
		vWriter: wr,
		lw:      loki,
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

	iter, err := consumer.Messages()
	if err != nil {
		return err
	}
	defer iter.Stop()

	msgCh := make(chan jetstream.Msg, c.workers*4)

	g, ctx := errgroup.WithContext(ctx)

	// dispatcher

	g.Go(func() error {
		defer close(msgCh)

		for {
			msg, err := iter.Next()
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
			}

			select {
			case <-ctx.Done():
				return nil
			case msgCh <- msg:
			}

		}

		// DEPRECATED WORKFLOW
		// for {
		// 	select {
		// 	case <-ctx.Done():
		// 		return
		// 	default:
		// 		msg, err := iter.Next()
		// 		if err != nil {
		// 			return
		// 		}
		// 		msgCh <- msg
		// 	}
		// }
	})

	// worker pool
	for i := 0; i < c.workers; i++ {
		workerID := i
		g.Go(func() error {
			for {
				select {
				// CASE 1: if a channel is closed
				case <-ctx.Done():
					return nil

				// [CASE 2] if a channel receive msg
				// ok -> tells if the channel is open/closed
				case msg, ok := <-msgCh:
					if !ok {
						return nil
					}

					if err := c.process(ctx, msg); err != nil {
						c.log.Error(
							"processing failed",
							"worker", workerID,
							"subject", msg.Subject(),
							"err", err,
						)
						// negative acknowledgement
						if err := msg.Nak(); err != nil {
							c.log.Error(
								"nak failed",
								"worker", workerID,
								"err", err,
							)
						}

						continue
					}

					// positive acknowledgement
					if err := msg.Ack(); err != nil {
						c.log.Error(
							"ack failed",
							"worker", workerID,
							"err", err,
						)
					}
				}
			}
		})
	}

	c.log.Info("consumer started", "workers", c.workers)

	<-ctx.Done()

	c.log.Info("consumer shutting down")

	return g.Wait()
}

// process routes a message to the correct writer based on subject.
func (c *Consumer) process(ctx context.Context, msg jetstream.Msg) error {
	subj := msg.Subject()
	c.log.Debug("processing message", "subject", subj)

	// adding these so the switch compiles and unknown subjects don't silently swallow future messages:

	switch {
	case strings.HasPrefix(subj, "telemetry.metrics."):
		return c.handleMetrics(ctx, msg.Data())
	case strings.HasPrefix(subj, "telemetry.logs."):
		return c.handleLogs(ctx, msg.Data())
	case strings.HasPrefix(subj, "telemetry.traces."):
		return c.handleTraces(ctx, msg.Data())
	default:
		c.log.Warn("no handler for subject", "subject", subj)
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

type logEvent struct {
	TenantID   string     `json:"tenant_id"`
	Logs       []logEntry `json:"logs"`
	ReceivedAt time.Time  `json:"received_at"`
}

type logEntry struct {
	Timestamp  time.Time         `json:"timestamp"`
	Level      string            `json:"level"`
	Message    string            `json:"message"`
	Attributes map[string]string `json:"attributes"`
	TraceID    string            `json:"trace_id"`
	SpanID     string            `json:"span_id"`
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

func (c *Consumer) handleLogs(ctx context.Context, data []byte) error {
	var event logEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}

	lines := make([]writer.LogLine, 0, len(event.Logs))

	for _, l := range event.Logs {
		lines = append(lines, writer.LogLine{
			Timestamp:  l.Timestamp,
			Level:      l.Level,
			Message:    l.Message,
			Attributes: l.Attributes,
			TraceID:    l.TraceID,
			SpanID:     l.SpanID,
		})
	}

	return c.lw.Write(ctx, event.TenantID, lines)
}

func (c *Consumer) handleTraces(_ context.Context, _ []byte) error {
	c.log.Debug("traces handler not yet implemented")
	return nil
}
