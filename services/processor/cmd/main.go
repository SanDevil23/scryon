package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/sandevil23/scryon/pkg/config"
	"github.com/sandevil23/scryon/pkg/logger"
	"github.com/sandevil23/scryon/services/processor/internal/consumer"
	"github.com/sandevil23/scryon/services/processor/internal/writer"
)

func main() {
	cfg, _ := config.LoadBase("processor")
	log := logger.New(cfg.LogLevel)

	lokiURL := getEnv("OBSP_LOKI_URL", "http://localhost:3100")
	victoriaURL := getEnv("OBSP_VICTORIA_URL", "http://localhost:8428/api/v1/import/prometheus")

	log.Info("starting processor service",
		"nats", cfg.NATSUrl,
		"victoria", victoriaURL,)
	
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Connect to NATS
	nc, err := nats.Connect(cfg.NATSUrl,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("NATS disconnected", "err", err)
		}),
	)

	if err!=nil {
		log.Error("failed to connect to NATS", "err", err)
		os.Exit(1)
	}

	defer nc.Drain()
	log.Info("connected to NATS")

	js, err := jetstream.New(nc)
	if err != nil {
		log.Error("failed to init JetStream", "err", err)
		os.Exit(1)
	}

	// wire up dependencies
	victoria := writer.NewVictoriaWriter(victoriaURL, log)
	loki := writer.NewLokiWriter(lokiURL, log)
	c := consumer.New(js, victoria, loki, log, 4)

	log.Info("processor ready, consuming from NATS")
	
	if err := c.Start(ctx); err != nil {
		log.Error("consumer error", "err", err)
		os.Exit(1)
	}

	log.Info("processor stopped")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}