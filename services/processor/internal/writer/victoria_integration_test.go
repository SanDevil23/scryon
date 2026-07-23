//go:build integration

package writer_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sandevil23/scryon/services/processor/internal/writer"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func setupVictoriaMetrics(t *testing.T)(*writer.VictoriaWriter, string, func()){
	t.Helper()
	ctx:=context.Background()

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
	if err!=nil{
		t.Fatalf("failed to start VictoriaMetrics container: %v", err)
	}

	host, err := container.Host(ctx)
	if err!=nil{
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "8428")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	baseURL := fmt.Sprintf("http://%s:/%s", host, port.Port())
	endpoint := baseURL + "api/v1/import/prometheus"

	w:= writer.NewVictoriaWriter(endpoint, noopLogger())

	cleanup := func(){
		if err := container.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}

	return w, baseURL, cleanup
}