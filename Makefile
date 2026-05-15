# =============================================================================
# Buf / Protobuf Commands
# =============================================================================

BUF := buf

PROTO_PATH ?=

# Example:
# make proto-gen
# make proto-gen PROTO_PATH=proto/ingest/v1

# -----------------------------------------------------------------------------
# Generate protobuf + gRPC code
# -----------------------------------------------------------------------------

.PHONY: proto-gen
proto-gen:
ifdef PROTO_PATH
	$(BUF) generate --path $(PROTO_PATH)
else
	$(BUF) generate
endif

# -----------------------------------------------------------------------------
# Lint protobuf definitions
# -----------------------------------------------------------------------------

.PHONY: proto-lint
proto-lint:
ifdef PROTO_PATH
	$(BUF) lint --path $(PROTO_PATH)
else
	$(BUF) lint
endif

# -----------------------------------------------------------------------------
# Format protobuf files
# -----------------------------------------------------------------------------

.PHONY: proto-format
proto-format:
	$(BUF) format -w

# -----------------------------------------------------------------------------
# Breaking change detection
# -----------------------------------------------------------------------------

.PHONY: proto-breaking
proto-breaking:
	$(BUF) breaking --against '.git#branch=main'

# -----------------------------------------------------------------------------
# Push module to Buf registry (optional)
# -----------------------------------------------------------------------------

.PHONY: proto-push
proto-push:
	$(BUF) push

# -----------------------------------------------------------------------------
# Clean generated code
# -----------------------------------------------------------------------------

.PHONY: proto-clean
proto-clean:
	rm -rf gen/