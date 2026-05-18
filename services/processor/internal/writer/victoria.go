package writer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// VictoriaWriter writes metrics to VictoriaMetrics using the
// Prometheus text exposition format via the /api/v1/import/prometheus endpoint.
type VictoriaWriter struct {
	endpoint   string
	httpClient *http.Client
	log        *slog.Logger
}

func NewVictoriaWriter(endpoint string, log *slog.Logger) *VictoriaWriter {
	return &VictoriaWriter{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		log: log,
	}
}

// MetricLine represents a single metric data point.
type MetricLine struct {
	Name      string
	Labels    map[string]string
	Value     float64
	Timestamp time.Time
}

// Write sends a batch of metrics to VictoriaMetrics
func (w *VictoriaWriter) Writer(ctx context.Context, tenantId string, metrics []MetricLine) error {
	if len(metrics) == 0 {
		return nil
	}

	var buffer bytes.Buffer						// 1 Byte = 8 bits
	for _, m := range metrics {
		buffer.WriteString(formatLine(tenantId, m))
		buffer.WriteByte('\n')
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.endpoint, &buffer)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status from VictoriaMetrics: %d", resp.StatusCode)
	}

	w.log.Info("metrics written to VictoriaMetrics",
		"tenant", tenantId,
		"count", len(metrics),
	)

	return nil
}

// formatLine converts a MetricLine to Prometheus text format.
// Example: cpu_usage{host="web-01",tenant="tenant-abc"} 72.5 1713000000000
func formatLine(tenantId string, metric MetricLine) string {
	labels := make([]string, 0, len(metric.Labels)+1)

	// always inject tenant labels for isolation
	labels = append(labels, fmt.Sprintf(`tenant="%s"`, tenantId))

	for k,v := range metric.Labels {
		labels = append(labels, fmt.Sprintf(`%s="%s"`, k, v))
	}

	ts := metric.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	return fmt.Sprintf("%s{%s} %g %d",
		sanitizeName(metric.Name),
		strings.Join(labels, ","),
		metric.Value,
		ts.UnixMilli(),
	)
}

// sanitizeName replaces characters not allowed in Prometheus metric names.
func sanitizeName(name string) string {
	var b strings.Builder

	// pickup any character between a-z, A-Z, or 0-9
	// special characters allowed can be either `_` or `:`
	// any character other than these will be replace by '_'
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == ':' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}
