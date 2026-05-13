package handler_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sandevil23/scryon/services/ingest/internal/handler"
	"log/slog"
)

func TestPushMetrics_PublishesToNATS(t *testing.T) {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skip("NATS not available, skipping integration test")
	}
	defer nc.Close()

	js, _ := jetstream.New(nc)
	ctx := context.Background()

	js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TELEMETRY_TEST",
		Subjects: []string{"telemetry.metrics.test.>"},
		MaxAge:   1 * time.Minute,
		Storage:  jetstream.MemoryStorage,
	})

	log := slog.Default()
	h := handler.NewIngestHanlder(js, "TELEMETRY_TEST", log)

	resp, err := h.PushMetrics(ctx, &handler.PushMetricsRequest{
		TenantID: "test-tenant",
		Metrics: []handler.Metric{
			{Name: "cpu_usage", Value: 72.5, Timestamp: time.Now()},
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Accepted != 1 {
		t.Fatalf("expected 1 accepted, got %d", resp.Accepted)
	}
}