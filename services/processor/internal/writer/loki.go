package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
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

// Write pushes a batch of log lines to Loki.
// All lines are grouped under a single stream keyed by tenant + level.
func (lw *LokiWriter) Write(ctx context.Context, tenantID string, lines []LogLine) error {
	if len(lines) == 0 {
		return nil
	}

	// Loki push API payload structure:
	// { "streams": [ { "stream": { labels }, "values": [ [ts, line] ] } ] }
	type lokiValue [2]string // [timestamp_ns, log_line]
	type lokiStream struct {
		Stream map[string]string `json:"stream"`
		Values []lokiValue       `json:"values"`
	}
	type lokiPush struct {
		Streams []lokiStream `json:"streams"`
	}
	
	// Group lines by level so each level is a separate Loki stream.
	// This makes LogQL filtering by level efficient.
	grouped := make(map[string][]lokiValue)
	for _, l := range lines {
		ts := l.Timestamp
		if ts.IsZero(){
			ts = time.Now().UTC()
		}

		msg := l.Message
		if l.TraceID != "" {
			msg = fmt.Sprintf("%s trace_id=%s", msg, l.TraceID)
		}

		lvl := l.Level
		if lvl == ""{
			lvl = "info"
		}

		grouped[lvl] = append(grouped[lvl], lokiValue{
			strconv.FormatInt(ts.UnixNano(), 10),
			msg,
		})
	}

	// slice of loki stream
	streams := make([]lokiStream, 0, len(grouped))
	for lvl, values := range grouped {
		streams = append(streams, lokiStream{
			Stream: map[string]string {
				"tenant": tenantID,
				"level": lvl,
				"source": "observability-platform",
			},
			Values: values,
		})
	}

	payload, err := json.Marshal(lokiPush{Streams: streams})
	if err != nil {
		return fmt.Errorf("marshal loki payload : %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx, 
		http.MethodPost,
		lw.endpoint+"/loki/api/v1/push",
		bytes.NewReader(payload),
	)
	if err != nil {
		return fmt.Errorf("build loki request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := lw.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("loki push: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected loki status: %d", resp.StatusCode)
	}

	lw.log.Info("logs written to Loki",
		"tenant", tenantID,
		"count", len(lines),
	)

	return nil
}

