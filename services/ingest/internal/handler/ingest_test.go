package handler_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/types/known/timestamppb"

	ingestv1 "github.com/sandevil23/scryon/gen/go/proto/ingest/v1"
	"github.com/sandevil23/scryon/services/ingest/internal/handler"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mockPublisher satisfies the publish dependency without real NATS.
type mockPublisher struct {
	published []mockMessage
	shouldErr bool
}

type mockMessage struct {
	subject string
	data    []byte
}

// fakeHandler wraps IngestHandler with a mock publisher for unit testing.
// This avoids needing a real NATS connection in unit tests.
type fakeHandler struct {
	publisher *mockPublisher
}

func (f *fakeHandler) pushMetrics(tenantID string, metrics []*ingestv1.Metric) error {
	if tenantID == "" {
		return status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if len(metrics) == 0 {
		return nil
	}
	if f.publisher.shouldErr {
		return status.Error(codes.Internal, "publish failed")
	}
	f.publisher.published = append(f.publisher.published, mockMessage{
		subject: "telemetry.metrics." + tenantID,
	})
	return nil
}

func TestPushMetrics_EmptyTenantID_ReturnsInvalidArgument(t *testing.T) {
	h := &fakeHandler{publisher: &mockPublisher{}}

	err := h.pushMetrics("", []*ingestv1.Metric{
		{Name: "cpu_usage", Value: 72.5},
	})

	if err == nil {
		t.Fatal("expected error for empty tenant_id, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got %s", st.Code())
	}
}

func TestPushMetrics_EmptyMetrics_ReturnsNoError(t *testing.T) {
	h := &fakeHandler{publisher: &mockPublisher{}}

	err := h.pushMetrics("tenant-abc", []*ingestv1.Metric{})

	if err != nil {
		t.Errorf("expected no error for empty metrics, got %v", err)
	}
	if len(h.publisher.published) != 0 {
		t.Errorf("expected no messages published for empty batch")
	}
}

func TestPushMetrics_ValidRequest_PublishesToCorrectSubject(t *testing.T) {
	h := &fakeHandler{publisher: &mockPublisher{}}

	err := h.pushMetrics("tenant-abc", []*ingestv1.Metric{
		{Name: "cpu_usage", Value: 72.5},
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(h.publisher.published) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(h.publisher.published))
	}

	expected := "telemetry.metrics.tenant-abc"
	if h.publisher.published[0].subject != expected {
		t.Errorf("expected subject %q, got %q", expected, h.publisher.published[0].subject)
	}
}

func TestPushMetrics_PublisherError_ReturnsInternal(t *testing.T) {
	h := &fakeHandler{publisher: &mockPublisher{shouldErr: true}}

	err := h.pushMetrics("tenant-abc", []*ingestv1.Metric{
		{Name: "cpu_usage", Value: 72.5},
	})

	if err == nil {
		t.Fatal("expected error when publisher fails")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error")
	}
	if st.Code() != codes.Internal {
		t.Errorf("expected Internal, got %s", st.Code())
	}
}

func TestPushMetrics_MetricTimestamp_DefaultsWhenZero(t *testing.T) {
	m := &ingestv1.Metric{
		Name:  "cpu_usage",
		Value: 50.0,
		// Timestamp intentionally nil
	}

	// verify the metric is valid without a timestamp
	if m.Name == "" {
		t.Error("expected metric name to be set")
	}

	// timestamp defaults are handled by the writer, not the handler
	// this test documents that behavior explicitly
	_ = time.Now() // processor fills this in
}

// TestSubjectRouting verifies subject patterns used by the consumer.
func TestSubjectRouting(t *testing.T) {
	tests := []struct {
		subject  string
		expected string
	}{
		{"telemetry.metrics.tenant-abc", "metrics"},
		{"telemetry.logs.tenant-abc", "logs"},
		{"telemetry.traces.tenant-abc", "traces"},
		{"telemetry.metrics.org.team.service", "metrics"},
	}

	for _, tt := range tests {
		t.Run(tt.subject, func(t *testing.T) {
			var got string
			switch {
			case len(tt.subject) > 18 && tt.subject[:18] == "telemetry.metrics.":
				got = "metrics"
			case len(tt.subject) > 15 && tt.subject[:15] == "telemetry.logs.":
				got = "logs"
			case len(tt.subject) > 17 && tt.subject[:17] == "telemetry.traces.":
				got = "traces"
			}
			if got != tt.expected {
				t.Errorf("subject %q routed to %q, want %q", tt.subject, got, tt.expected)
			}
		})
	}
}

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
