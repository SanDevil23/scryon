//go:build integration

package writer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/sandevil23/scryon/services/processor/internal/writer"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupVictoriaMetrics starts a real VictoriaMetrics container and returns
// the writer pointed at it, plus a cleanup function.
func setupVictoriaMetrics(t *testing.T) (*writer.VictoriaWriter, string, func()) {
	t.Helper()
	ctx := context.Background()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "victoriametrics/victoria-metrics:v1.99.0",
			ExposedPorts: []string{"8428/tcp"},
			Cmd:          []string{"--storageDataPath=/storage", "--httpListenAddr=:8428"},
			WaitingFor: wait.ForHTTP("/health").
				WithPort("8428/tcp").
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("failed to start VictoriaMetrics container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "8428")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	baseURL := fmt.Sprintf("http://%s:%s/", host, port.Port())
	endpoint := baseURL + "api/v1/import/prometheus"

	w := writer.NewVictoriaWriter(endpoint, noopLogger())

	cleanup := func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return w, baseURL, cleanup
}

// queryVictoria queries VictoriaMetrics and returns the first value found.
func queryVictoria(t *testing.T, baseURL, query string) (float64, bool) {
	t.Helper()

	url := fmt.Sprintf("%s/api/v1/query?query=%s", baseURL, query)
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("failed to query VictoriaMetrics: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response: %v", err)
	}

	var result struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result.Status != "success" || len(result.Data.Result) == 0 {
		return 0, false
	}

	// value[1] is the metric value as a string
	valStr, ok := result.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, false
	}

	var val float64
	fmt.Sscanf(valStr, "%f", &val)
	return val, true
}

// TestVictoriaWriter_WritesMetric verifies a metric written to VictoriaMetrics
// is immediately queryable via the HTTP API.
func TestVictoriaWriter_WritesMetric(t *testing.T) {
	w, baseURL, cleanup := setupVictoriaMetrics(t)
	defer cleanup()

	ctx := context.Background()

	err := w.Write(ctx, "tenant-abc", []writer.MetricLine{
		{
			Name:      "cpu_usage",
			Value:     72.5,
			Labels:    map[string]string{"host": "web-01"},
			Timestamp: time.Now(),
		},
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// VictoriaMetrics may need a moment to make the metric queryable
	var val float64
	var found bool
	for i := 0; i < 5; i++ {
		val, found = queryVictoria(t, baseURL, "cpu_usage")
		if found {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !found {
		t.Fatal("metric not found in VictoriaMetrics after write")
	}
	if val != 72.5 {
		t.Errorf("expected value 72.5, got %f", val)
	}
}

// TestVictoriaWriter_TenantLabelIsolation verifies that two tenants writing
// the same metric name are isolated by the tenant label.
func TestVictoriaWriter_TenantLabelIsolation(t *testing.T) {
	w, baseURL, cleanup := setupVictoriaMetrics(t)
	defer cleanup()

	ctx := context.Background()

	// tenant-a writes 10.0
	if err := w.Write(ctx, "tenant-a", []writer.MetricLine{
		{Name: "mem_usage", Value: 10.0, Timestamp: time.Now()},
	}); err != nil {
		t.Fatalf("Write tenant-a failed: %v", err)
	}

	// tenant-b writes 99.0
	if err := w.Write(ctx, "tenant-b", []writer.MetricLine{
		{Name: "mem_usage", Value: 99.0, Timestamp: time.Now()},
	}); err != nil {
		t.Fatalf("Write tenant-b failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// query scoped to tenant-a — must not see tenant-b's value
	val, found := queryVictoria(t, baseURL, `mem_usage{tenant="tenant-a"}`)
	if !found {
		t.Fatal("tenant-a metric not found")
	}
	if val != 10.0 {
		t.Errorf("tenant-a: expected 10.0, got %f", val)
	}

	// query scoped to tenant-b
	val, found = queryVictoria(t, baseURL, `mem_usage{tenant="tenant-b"}`)
	if !found {
		t.Fatal("tenant-b metric not found")
	}
	if val != 99.0 {
		t.Errorf("tenant-b: expected 99.0, got %f", val)
	}
}

// TestVictoriaWriter_EmptyBatch verifies no error on empty input.
func TestVictoriaWriter_EmptyBatch(t *testing.T) {
	w, _, cleanup := setupVictoriaMetrics(t)
	defer cleanup()

	err := w.Write(context.Background(), "tenant-abc", []writer.MetricLine{})
	if err != nil {
		t.Errorf("expected no error for empty batch, got %v", err)
	}
}

// TestVictoriaWriter_MultipleBatch verifies a batch of multiple metrics
// are all written correctly.
func TestVictoriaWriter_MultipleBatch(t *testing.T) {
	w, baseURL, cleanup := setupVictoriaMetrics(t)
	defer cleanup()

	ctx := context.Background()

	err := w.Write(ctx, "tenant-abc", []writer.MetricLine{
		{Name: "cpu_usage", Value: 55.0, Timestamp: time.Now()},
		{Name: "mem_usage", Value: 80.0, Timestamp: time.Now()},
		{Name: "disk_usage", Value: 40.0, Timestamp: time.Now()},
	})
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	cases := []struct {
		query    string
		expected float64
	}{
		{"cpu_usage", 55.0},
		{"mem_usage", 80.0},
		{"disk_usage", 40.0},
	}

	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			val, found := queryVictoria(t, baseURL, tc.query)
			if !found {
				t.Fatalf("metric %q not found", tc.query)
			}
			if val != tc.expected {
				t.Errorf("expected %f, got %f", tc.expected, val)
			}
		})
	}
}
