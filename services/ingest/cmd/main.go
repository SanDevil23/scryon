package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	ingestv1 "github.com/sandevil23/scryon/gen/go/proto/ingest/v1"
	"github.com/sandevil23/scryon/pkg/config"
	"github.com/sandevil23/scryon/pkg/logger"
	"github.com/sandevil23/scryon/services/ingest/internal/handler"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadBase("ingest")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	log := logger.New(cfg.LogLevel)
	log.Info("Starting Ingest Service", "addr", cfg.GRPCAddr())

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Connect to NATS
	nc, err := nats.Connect(cfg.NATSUrl,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(10),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Warn("NATS Disconnected", "err", err)
		}),
	)
	if err != nil {
		return fmt.Errorf("nats connect: %w", err)
	}

	defer func() {
		if err := nc.Drain(); err != nil {
			log.Warn("NATS drain error", "err", err)
		}
	}()

	log.Info("connected to NATS", "url", cfg.NATSUrl)

	// JetStream context
	jets, err := jetstream.New(nc)
	if err != nil {
		return fmt.Errorf("failed to init JetStream: %w", err)
	}

	// ensure stream exists
	_, err = jets.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:     "TELEMETRY",
		Subjects: []string{"telemetry.metrics.>", "telemetry.logs.>", "telemetry.traces.>"},
		MaxAge:   24 * time.Hour,
		Storage:  jetstream.FileStorage,
	})

	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}

	log.Info("NATS stream ready", "stream", "TELEMETRY")

	// gRPC Server
	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			loggingInterceptor(log),
		),
	)

	srv := handler.NewIngestHanlder(jets, "TELEMETRY", log)
	ingestv1.RegisterIngestServiceServer(grpcServer, srv)

	reflection.Register(grpcServer)

	// Start Listening
	lc := net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", cfg.GRPCAddr())
	if err != nil {
		return fmt.Errorf("failed to listen: %s: %w", cfg.GRPCAddr(), err)
	}

	grpcErrCh := make(chan error, 1)
	go func() {
		log.Info("gRPC server listening", "addr", cfg.GRPCAddr())
		grpcErrCh <- grpcServer.Serve(lis)
	}()

	// Block until signal or error
	select {
	case <-ctx.Done():
		log.Info("Shutdown signal received")
	case err := <-grpcErrCh:
		log.Error("gRPC server error", "err", err)
	}

	log.Info("shutting down gRPC server")
	grpcServer.GracefulStop()
	log.Info("ingest service stopped")

	return nil
}

// loggingInterceptor logs every unary RPC call.
func loggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		log.Info("rpc",
			"method", info.FullMethod,
			"duration_ms", time.Since(start).Milliseconds(),
			"err", err,
		)

		return resp, err
	}
}
