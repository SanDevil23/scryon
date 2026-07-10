package writer

import (
	"context"
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

	return nil
}

