package writer

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

type LokiWriter struct {
	endpoint	string
	httpClient 	*http.Client
	log			*slog.Logger
}

func NewLokiWriter(endpoint string, log *slog.Logger) *LokiWriter {
	return &LokiWriter{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		log: log,
	}
}

type LogLine struct {
	Timestamp	time.Time
	Level 		string
	Message		string
	Attributes	map[string]string
	TraceID		string
	SpanID		string
}

func (lw *LokiWriter) Write(ctx context.Context, tenantID string, lines []LogLine) error {
	// TODO: implement write method
	return nil
}

