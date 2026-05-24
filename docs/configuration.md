# Configuration (v0.5)

v0.5 features are configured on `keylane.Config`. All v0.5 subsystems are **additive** and backward compatible: zero values disable rich diagnostics while core queue behavior continues.

For capacity sizing (workers, shards, queue size), see [production-tuning.md](production-tuning.md).

---

## Quick start (all v0.5 features enabled)

```go
cfg := keylane.Config{
    ShardCount:       8,
    WorkerCount:      4,
    QueueSizePerLane: 1000,
    LaneQuotas:       map[keylane.Lane]int{"default": 2, "payment": 3},

    HotKey:            keylane.DefaultHotKeyConfig(),
    PerKeyAdmission:   keylane.DefaultPerKeyAdmissionConfig(),
    ShardPressure:     keylane.DefaultShardPressureConfig(),
    AutoscalingSignal: keylane.DefaultAutoscalingSignalConfig(),
}
```

---

## HotKeyConfig

```go
type HotKeyConfig struct {
    Enabled                  bool
    MaxTrackedKeysPerShard   int
    DetectionWindow          time.Duration
    HotKeyDepthRatio         float64
    HotKeyWaitRatio          float64
    MaxCandidatesPerSnapshot int
    ExposeRawKey             bool
}
```

| Field | Default (`DefaultHotKeyConfig`) | Notes |
|-------|--------------------------------|-------|
| `Enabled` | `true` | Zero `HotKeyConfig{}` disables |
| `MaxTrackedKeysPerShard` | `64` | `0` with enabled = no-op tracker |
| `DetectionWindow` | `30s` | Approximate decay window |
| `HotKeyDepthRatio` | `0.40` | Depth share threshold |
| `HotKeyWaitRatio` | `0.40` | Wait share threshold |
| `MaxCandidatesPerSnapshot` | `5` | Ranked candidates per shard |
| `ExposeRawKey` | **`false`** | Sensitive; keep off in production |

See [hot-key-detection.md](hot-key-detection.md).

---

## PerKeyAdmissionConfig

Requires `HotKey` with `MaxTrackedKeysPerShard > 0`.

```go
type PerKeyAdmissionConfig struct {
    Enabled                bool
    MinStatus              HotKeyStatus
    DefaultAction          PerKeyMitigationAction
    MaxQueuedPerKey        int
    MaxInflightPerKey      int
    PressureRatioThreshold float64
    RejectRatioThreshold   float64
    Cooldown               time.Duration
    RecoveryWindow         time.Duration
    MaxSnapshotsPerShard   int
    MaxSnapshotsTotal      int
}
```

| Field | Default | Notes |
|-------|---------|-------|
| `Enabled` | `true` | Zero value disables |
| `MinStatus` | `candidate` | Or `dominant` for stricter policy |
| `DefaultAction` | `throttle` | `reject` / `shed` require explicit choice |
| `PressureRatioThreshold` | `0.40` | Minimum concentration to act |
| `RejectRatioThreshold` | `0.20` | Reject rate trigger |
| `Cooldown` | `10s` | Reduce flapping |
| `RecoveryWindow` | `30s` | Recovery after mitigation |

**Observe-only mode:** enable `HotKey`, leave `PerKeyAdmission` disabled or set `DefaultAction: PerKeyMitigationAllow`.

See [per-key-admission-policy.md](per-key-admission-policy.md).

---

## ShardPressureConfig

```go
type ShardPressureConfig struct {
    Enabled                     bool
    Window                      time.Duration
    HotShardPressureRatio       float64
    DominantLaneRatio           float64
    LocalizedHotKeyRatio        float64
    DistributedShardRatio       float64
    WorkerBusyRatio             float64
    MaxHotShards                int
    MaxLaneBreakdownPerShard    int
    MaxHotKeyCandidatesPerShard int
}
```

| Field | Default | Notes |
|-------|---------|-------|
| `Enabled` | `true` | Zero value disables rich diagnostics |
| `Window` | `30s` | Wait normalization window |
| `HotShardPressureRatio` | `0.70` | Shard hot threshold |
| `LocalizedHotKeyRatio` | `0.40` | Hot key dominance threshold |
| `DistributedShardRatio` | `0.50` | Fraction of hot shards for `distributed` |

`Queue.Pressure()` remains available when shard pressure diagnostics are disabled.

See [shard-pressure-diagnostics.md](shard-pressure-diagnostics.md).

---

## AutoscalingSignalConfig

```go
type AutoscalingSignalConfig struct {
    Enabled                       bool
    Window                        time.Duration
    ConsecutiveWindows            int
    QueueDepthRatioThreshold      float64
    QueueWaitMaxThreshold         time.Duration
    AdmissionRejectRateThreshold  float64
    AdmissionShedRateThreshold    float64
    WorkerBusyRatioThreshold      float64
    HotShardRatioThreshold        float64
    ManyHotShardsThreshold        int
    LocalizedHotKeyRatioThreshold float64
}
```

| Field | Default | Notes |
|-------|---------|-------|
| `Enabled` | `true` | Zero value disables scale signal |
| `Window` | `30s` | Observation window |
| `ConsecutiveWindows` | `2` | Sustained unhealthy windows before `Recommended=true` |
| `QueueDepthRatioThreshold` | `0.70` | Conservative scale trigger |
| `QueueWaitMaxThreshold` | `50ms` | Max observed wait trigger |
| `LocalizedHotKeyRatioThreshold` | `0.40` | Guardrail against blind scale-out |

Scale signals are **advisory**. go-keylane does not calculate replica counts or integrate with Kubernetes HPA/KEDA directly.

See [autoscaling-signals.md](autoscaling-signals.md).

---

## Observability flags

v0.5 hooks require explicit enablement:

```go
Observability: keylane.ObservabilityConfig{
    EnableHooks:        true,
    EnableDebugSnapshot: true,
    Hooks: keylane.Hooks{
        OnHotKeyCandidate:         myHotKeyHook,
        OnShardPressureSummary:    myPressureHook,
        OnScaleSignal:             myScaleHook,
        OnPerKeyAdmissionDecision: myPerKeyHook,
    },
},
```

Use `DefaultObservabilityConfig()` for staging and incident response. Use `LowAllocationObservabilityConfig()` on latency-sensitive production hot paths (hooks off).

See [observability.md](observability.md).

---

## Safe defaults summary

| Setting | Safe default | Why |
|---------|--------------|-----|
| `ExposeRawKey` | `false` | Privacy |
| Per-key `DefaultAction` | `throttle` | Avoids hard reject storms |
| Scale thresholds | Conservative (0.70 depth, 2 consecutive windows) | Reduces false scale recommendations |
| Feature disable | Zero config struct | Backward compatible opt-out |
| External autoscaler | Not included | Platform wires metrics/APIs |

---

## Disabling all v0.5 features

```go
cfg.HotKey = keylane.HotKeyConfig{}
cfg.PerKeyAdmission = keylane.PerKeyAdmissionConfig{}
cfg.ShardPressure = keylane.ShardPressureConfig{}
cfg.AutoscalingSignal = keylane.AutoscalingSignalConfig{}
```

Core submit, admission, and overload behavior from v0.3/v0.4 is unchanged.

---

## Related docs

- [v0.5-hot-key-autoscaling-signals.md](v0.5-hot-key-autoscaling-signals.md) — overview
- [metrics.md](metrics.md) — exported metric names
- [examples/v0.5-hot-key-autoscaling](../examples/v0.5-hot-key-autoscaling/) — runnable sample
