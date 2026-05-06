# Create the stream manually

nats stream add TELEMETRY \
  --subjects "telemetry.metrics.>,telemetry.logs.>,telemetry.traces.>" \
  --storage file \
  --retention limits \
  --max-age 24h \
  --replicas 1 \
  --defaults


## Verify
> nats stream info TELEMETRY


#  Publish and consume a test message

> nats stream view TELEMETRY