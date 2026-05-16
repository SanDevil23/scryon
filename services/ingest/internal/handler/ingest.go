package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	ingestv1 "github.com/sandevil23/scryon/gen/go/proto/ingest/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ingest Handler handles incoming telemetry and publishes to NATS JetStream.
type IngestHandler struct { 
	ingestv1.UnimplementedIngestServiceServer			// for suppressing unimplemented methods and forward compatibility
	jets		jetstream.JetStream
	streamName 	string
	slog 		*slog.Logger
}

func NewIngestHanlder(js jetstream.JetStream, strmName string, logger *slog.Logger) *IngestHandler {
	return &IngestHandler{
		jets: js,
		streamName: strmName,
		slog: logger,
	}
}

// PushMetrics validates and publishes a batch of metrics to NATS.
func (in *IngestHandler) PushMetrics(ctx context.Context, req *ingestv1.PushMetricsRequest)(*ingestv1.PushMetricsResponse, error){
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is empty")
	}

	if len(req.Metrics) == 0 {
		return &ingestv1.PushMetricsResponse{
			Accepted: 0,
			Rejected: 0,
		}, nil
	}

	// creating event to push into jetstream
	event := map[string]any{
		"tenant_id":   req.TenantId,
		"metrics":     req.Metrics,
		"received_at": time.Now().UTC(),
	}

	data, err := json.Marshal(event)
	if err!=nil{
		return nil, status.Error(codes.Internal, "failed to marshal event")
	}

	subject := fmt.Sprintf("telemetry.metrics.%s", req.TenantId)
	if _, err := in.jets.Publish(ctx, subject, data); err!=nil{
		in.slog.Error("failed to publish to NATS", "err", err, "subject", subject)
		return nil, status.Error(codes.Internal, "failed to publish metrics")
	}

	in.slog.Info("metrics published to NATS",
		"tenant", req.TenantId,
		"count", len(req.Metrics),
		"subject", subject,
	)

	return &ingestv1.PushMetricsResponse{Accepted: uint32(len(req.Metrics))}, nil
}
