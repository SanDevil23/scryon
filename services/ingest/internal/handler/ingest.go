package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ingest Handler handles incoming telemetry and publishes to NATS JetStream.
type IngestHandler struct { 
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
// Metric
type Metric struct {
	Name		string				`json:"metric"`
	Labels		map[string]string	`json:"Labels"`
	Value 		float64				`json:"value"`
	Timestamp	time.Time			`json:"time"`
}
// PushMetricRequest
type PushMetricsRequest struct {
	TenantID 	string 		`json:"tenant_id"`
	Metrics		[]Metric	`json:"metrics"`
}
// PushMetricResponse
type PushMetricsResponse struct {
	Accepted uint32 `json:"accepted"`
	Rejected uint32 `json:"rejected"`
}

// PushMetrics validates and publishes a batch of metrics to NATS.
func (in *IngestHandler) PushMetrics(ctx context.Context, req *PushMetricsRequest)(*PushMetricsResponse, error){
	if req.TenantID == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is empty")
	}

	if len(req.Metrics) == 0 {
		return &PushMetricsResponse{
			Accepted: 0,
			Rejected: 0,
		}, nil
	}

	// creating event to push into jetstream
	event := map[string]any{
		"tenant_id":   req.TenantID,
		"metrics":     req.Metrics,
		"received_at": time.Now().UTC(),
	}

	data, err := json.Marshal(event)
	if err!=nil{
		return nil, status.Error(codes.Internal, "failed to marshal event")
	}

	subject := fmt.Sprintf("telemetry.metrics.%s", req.TenantID)
	if _, err := in.jets.Publish(ctx, subject, data); err!=nil{
		in.slog.Error("failed to publish to NATS", "err", err, "subject", subject)
		return nil, status.Error(codes.Internal, "failed to publish metrics")
	}

	in.slog.Info("metrics published to NATS",
		"tenant", req.TenantID,
		"count", len(req.Metrics),
		"subject", subject,
	)

	return &PushMetricsResponse{Accepted: uint32(len(req.Metrics))}, nil
}
