# Redis Exporter

Exports traces and/or metrics to redis stream using [OTLP](
https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/protocol/otlp.md)
format.

It creates 3 different client to redis:

- metrics
- traces
- logs

which pub on `$type:$appname` (e.g. `metrics:mail-server`) in the OTLP Protobuf format.

The following settings are required:

- `endpoint`: host:port to which the exporter is going to send traces or
  metrics, using the redis protocol.

Example:

```yaml
exporters:
  redis:
    endpoint: otelcol2:55678
```

The full list of settings exposed for this exporter are documented [here](./config.go)
with detailed sample configurations [here](./testdata/config.yaml).
