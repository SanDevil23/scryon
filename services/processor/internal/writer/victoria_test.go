package writer

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"cpu_usage", "cpu_usage"},
		{"cpu.usage", "cpu_usage"},
		{"cpu-usage", "cpu_usage"},
		{"cpu usage", "cpu_usage"},
		{"cpu/usage", "cpu_usage"},
		{"123metric", "123metric"},
		{"valid:name", "valid:name"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := sanitizeName(tt.input)
			if got != tt.expected {
				t.Errorf("sanitizeName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestFormatLine_Structure(t *testing.T) {
	m := MetricLine{
		Name:      "cpu_usage",
		Labels:    map[string]string{"host": "web-01"},
		Value:     72.5,
		Timestamp: time.UnixMilli(1713000000000).UTC(),
	}

	line := formatLine("tenant-abc", m)
	// Expected entry : cpu_usage{host="web-01",tenant="tenant-abc"} 72.5 1713000000000

	if !strings.HasPrefix(line, "cpu_usage{") {
		t.Errorf("expected line to start with metric name, got: %s", line)
	}

	if !strings.Contains(line, `tenant="tenant-abc"`) {
		t.Errorf("expected line to start with tenant name, got: %s", line)
	}

	if !strings.Contains(line, `host="web-01"`) {
		t.Errorf("expected line to start with host name, got: %s", line)
	}

	if !strings.Contains(line, `72.5`) {
		t.Errorf("expected value 72.5 in line, got: %s", line)
	}

	if !strings.Contains(line, `1713000000000`) {
		t.Errorf("expected timestamp in line, got: %s", line)
	}
}

func TestFormatLine_InvalidMetricName(t *testing.T) {
	m := MetricLine{
		Name:  "cpu.usage.total",
		Value: 10.0,
	}

	line := formatLine("tenant-abc", m)

	if !strings.HasPrefix(line, "cpu_usage_total{") {
		t.Errorf("expected dots replaced with underscores, got: %s", line)
	}
}

func TestFormatLine_ZeroTimeStampUsesNow(t *testing.T) {
	before := time.Now().UnixMilli()

	m := MetricLine{
		Name:  "cpu_usage",
		Value: 1.0,
		// Timestamp intentionally zero
	}

	line := formatLine("tenant-abc", m)
	after := time.Now().UnixMilli()

	// Extract timestamp from line - last space-separated token
	parts := strings.Fields(line)
	if len(parts) != 3 {
		t.Fatalf("expected 3 parts in line, got %d: %s", len(parts), line)
	}

	ts, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil {
		t.Fatalf("failed to parse timestamp: %v", err)
	}

	if ts < before || ts > after {
		t.Errorf("timestamp %d not in expected range [%d, %d]", ts, before, after)
	}

	t.Logf("\nappended timestamp: %d", ts)
}

func TestFormatLine_TenantAlwaysInjected(t *testing.T) {
	m := MetricLine{
		Name:   "mem_usage",
		Value:  55.0,
		Labels: map[string]string{},
	}

	line := formatLine("my-tenant", m)

	if !strings.Contains(line, `tenant="my-tenant"`) {
		t.Errorf("expected tenant label injected even with empty labels, got: %s", line)
	}
}
