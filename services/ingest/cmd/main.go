package cmd

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	ingestv1 "github.com/sandevil23/scryon/gen/go/proto/ingest/v1"
	"github.com/sandevil23/scryon/pkg/config"
	"github.com/sandevil23/scryon/pkg/logger"
	"github.com/sandevil23/scryon/services/ingest/internal/handler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)


func main(){
	cfg, err := config.LoadBase("ingest")
	if err!=nil{
		slog.Error("Failed to load config", "err", err)
		os.Exit(1)
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
		log.Error("failed to connect to NATS", "err", err)
		os.Exit(1)
	}

	defer nc.Drain()
	log.Info("connected to NATS", "url", cfg.NATSUrl)

	// JetStream context
	jets, err := jetstream.New(nc)
	if err!=nil{
		log.Error("failed to init JetStream", "err", err)
		os.Exit(1)
	}

	// ensure stream exists
	_, err = jets.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: "TELEMETRY",
		Subjects: []string{"telemetry.metrics.>", "telemetry.logs.>", "telemetry.traces.>"},
		MaxAge: 24 * time.Hour,
		Storage: jetstream.FileStorage,
	})

	if err!=nil {
		log.Error("failed to create stream", "err", err)
		os.Exit(1)
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
	lis, err := net.Listen("tcp", cfg.GRPCAddr())
	if err!=nil {
		log.Error("failed to listen", "addr", cfg.GRPCAddr(), "err", err)
		os.Exit(1)
	}

	grpcErrCh := make(chan error, 1)
	go func() {
		log.Info("gRPC server listening", "addr", cfg.GRPCAddr())
		grpcErrCh <- grpcServer.Serve(lis)
	}()

	// Block until signal or error
	select {
	case <- ctx.Done():
		log.Info("Shutdown signal received")
	case err := <- grpcErrCh:
		log.Error("gRPC server error", "err", err)
	}

	log.Info("shutting down gRPC server")
	grpcServer.GracefulStop()
	log.Info("ingest service stopped")
}

// loggingInterceptor logs every unary RPC call.
func loggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start:=time.Now()
		resp, err := handler(ctx, req)
		log.Info("rpc",
			"method", info.FullMethod,
			"duration_ms", time.Since(start).Milliseconds(),
			"err", err,
		)

		return resp, err
	}
}