package handler_test

import (
	"context"
	"testing"
	"time"

	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	ingestv1 "github.com/sandevil23/scryon/gen/go/proto/ingest/v1"
	"github.com/sandevil23/scryon/services/ingest/internal/handler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPushMetrics_PublishesToNATS(t *testing.T) {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skip("NATS not available, skipping integration test")
	}
	defer nc.Close()

	js, _ := jetstream.New(nc)
	ctx := context.Background()

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TELEMETRY_TEST",
		Subjects: []string{"telemetry.metrics.test.>"},
		MaxAge:   1 * time.Minute,
		Storage:  jetstream.MemoryStorage,
	})

	if err != nil {
		t.Fatalf("failed to create test stream: %v", err)
	}

	log := slog.Default()
	h := handler.NewIngestHanlder(js, "TELEMETRY_TEST", log)

	resp, err := h.PushMetrics(ctx, &ingestv1.PushMetricsRequest{
		TenantId: "test-tenant",
		Metrics: []*ingestv1.Metric{
			{Name: "cpu_usage", Value: 72.5, Timestamp: timestamppb.Now()},
		},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Accepted != 1 {
		t.Fatalf("expected 1 accepted, got %d", resp.Accepted)
	}
}
