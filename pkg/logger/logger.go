package logger

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const loggerKey contextKey = "logger"

func New(level string) *slog.Logger {
	var logLevel slog.Level
	if err := logLevel.UnmarshalText([]byte(level)); err != nil {
		logLevel = slog.LevelInfo
	}

	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: logLevel == slog.LevelDebug,
	}))
}

func WithContext(ctx context.Context, log *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, log)
}

func FromContext(ctx context.Context) *slog.Logger {
	// safe type assertion
	if log, ok := ctx.Value(loggerKey).(*slog.Logger); ok {
		return log
	}
	return New("info")
}

func With(log *slog.Logger, args ...any) *slog.Logger {
	return log.With(args...)
}
