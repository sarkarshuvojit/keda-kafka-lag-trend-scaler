# Local Test: Trend Scaling in Action

This documents the local run behind [`docs/test-results-annotated.png`](./test-results-annotated.png). It shows the scaler ignoring a burst the consumer can absorb, reacting to one it can't, and scaling back down only once the produce rate says it's safe.

## Setup

Deployed on minikube with [`start.sh`](../start.sh): Kafka with `test-topic` at **10 partitions**, a single consumer (~10 msg/s: `ReadMessage` + 100ms sleep), the lag-scaler, and the `ScaledObject` in [`examples/k8s/scalers/persistent/scaledobject.yaml`](../examples/k8s/scalers/persistent/scaledobject.yaml) (`pollingInterval: 10`, `minReplicaCount: 1`, `maxReplicaCount: 10`).

Scaler settings, from [`examples/k8s/deploy/lag-scaler.yaml`](../examples/k8s/deploy/lag-scaler.yaml):

| Env var                | Value | Meaning in this run                                                    |
|------------------------|-------|------------------------------------------------------------------------|
| `MAX_DRAIN_SECONDS`    | 120   | Backlog must be projected to clear within 2 minutes                     |
| `TREND_WINDOW_SECONDS` | 60    | Slope and rates are fitted over the last minute                         |
| `MIN_LAG`              | 100   | Lag under 100 is noise                                                  |
| `HEADROOM`             | 1.2   | Size for 20% more throughput than strictly needed                       |
| `SAMPLING_INTERVAL`    | 10    | One scrape every 10s                                                    |
| `MIN_CONSUMERS` / `MAX_CONSUMERS` | 1 / 10 | Clamp (10 is also the partition count)                    |

Per-consumer capacity was **learned**, not configured.

## Load pattern

[`load-test.sh`](../load-test.sh) with defaults:

1. **Phase 1**: 1000 messages at once.
2. **60s wait.**
3. **Phase 2**: 8000 messages at once.

## What the graph shows

Red dashed = desired consumers reported to KEDA. Green = actual consumer-group size. Blue = total lag.

1. **23:38:35, 1000-msg burst.** Lag jumps to ~1000 and falls at ~10/s, a ~100s drain inside the 120s SLO. The scaler stays at 1 consumer.
2. **23:39:35, 8000-msg burst** (on top of ~500 left from phase 1). Within one poll the scaler logs:
   `stalled=true lag=8343 consume=9.9/s drainETA=+Inf capacity=9.9/s consumers=1 desired=10`.
   It learned ~9.9 msg/s per consumer from the saturated consumer and asked for 10, the partition cap.
3. **23:39:40–23:40:10.** HPA steps 1 → 4 → 8 → 10 (its default scale-up rate). With 10 consumers draining at ~99 msg/s, the backlog is gone by **23:41:31**, about 2 minutes after the burst.
4. **23:41:11.** Lag is falling well inside the SLO and the produce rate is 0, so desired drops to 1. It doesn't oscillate. The target comes from `produce rate / capacity`, not from "is lag high right now".
5. **23:46:10.** HPA's default 5-minute scale-down stabilization expires and the consumer returns to 1.

The **Consumers Stalled** panel marks the stretch where the scaler judged that consumers couldn't catch up.

## Known rough edge seen in this run

Phase 2 landed before phase 1's backlog dropped below `MIN_LAG`, so both bursts were one lag episode and the 8000-message jump went into the produce-rate fit (logged as `produce=114/s`). That inflated the computed need (≈22 before the clamp, versus ≈9 from the backlog alone). The partition cap hid it here. A robust slope estimator (e.g. Theil–Sen) for the produce rate would make one-off jumps like this count as backlog rather than sustained rate.

## Reproduce

```bash
./start.sh
./load-test.sh
kubectl logs -l app=lag-scaler -f | grep GetMetrics
```

Grafana: `kubectl port-forward svc/grafana 3300:3000`, then open `http://localhost:3300` (the dashboard is the home page).
