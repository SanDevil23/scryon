//go:build integration-tests

package writer_test

import (
	"io"
	"log/slog"
)

// noopLogger returns a logger that discards all output.
// Keeps test output clean — only test assertions print.
func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
