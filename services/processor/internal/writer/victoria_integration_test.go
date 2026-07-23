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

	baseURL := fmt.Sprintf("http://%s:/%s", host, port.Port())
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
