package main

import (
	"context"
	"fmt"
	"log/slog"
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
	if err := run(); err != nil {
		slog.Error("fatal error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, _ := config.LoadBase("processor")
	log := logger.New(cfg.LogLevel)

	lokiURL := getEnv("OBSP_LOKI_URL", "http://localhost:3100")
	victoriaURL := getEnv("OBSP_VICTORIA_URL", "http://localhost:8428/api/v1/import/prometheus")

	log.Info("starting processor service",
		"nats", cfg.NATSUrl,
		"victoria", victoriaURL)

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

	if err != nil {
		return fmt.Errorf("failed to connect to NATS: %w", err)
	}

	defer func() {
		if err := nc.Drain(); err != nil {
			log.Warn("NATS drain error", "err", err)
		}
	}()
	log.Info("connected to NATS")

	js, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("failed to init JetStream: %w", err)
	}

	// wire up dependencies
	victoria := writer.NewVictoriaWriter(victoriaURL, log)
	loki := writer.NewLokiWriter(lokiURL, log)
	c := consumer.New(js, victoria, loki, log, 4)

	log.Info("processor ready, consuming from NATS")

	if err := c.Start(ctx); err != nil {
		return fmt.Errorf("consumer error: %w", err)
	}

	log.Info("processor stopped")

	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
