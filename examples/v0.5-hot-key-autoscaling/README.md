# v0.5 Hot Key & Autoscaling Example

Demonstrates v0.5 configuration in **observe-only** mode:

- `DefaultHotKeyConfig()` — bounded hot key detection
- Per-key admission **disabled** (detection without mitigation)
- `DefaultShardPressureConfig()` and `DefaultAutoscalingSignalConfig()`
- `DebugSnapshot().HotKeys` — hash-only output
- `ScaleSignal()` — advisory autoscaling signal

Uses synthetic key `tenant-demo-7` (no PII).

## Run

From the repository root:

```bash
go run ./examples/v0.5-hot-key-autoscaling
```

Optional JSON summary:

```bash
KEYLANE_V05_JSON=1 go run ./examples/v0.5-hot-key-autoscaling
```

## Prometheus

To expose metrics, see [examples/prometheus](../prometheus/) and [docs/metrics.md](../../docs/metrics.md).

## Docs

- [v0.5 overview](../../docs/v0.5-hot-key-autoscaling-signals.md)
- [configuration](../../docs/configuration.md)
