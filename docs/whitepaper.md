# **Whitepaper: Trend-Based Kafka Consumer Scaler for KEDA**

## **Abstract**

Kubernetes Event-Driven Autoscaling (KEDA) currently supports event-source based scaling for many message systems, including Apache Kafka. Traditionally, KEDA’s Kafka scaler uses static thresholds (e.g., message counts per partition) to scale consumers. However, static thresholds are brittle and often lead to under-scaling, over-scaling, or oscillations.

This whitepaper proposes a **trend-based KEDA scaler**. It scales when consumers **cannot drain their backlog within an SLO-derived time**, not when lag crosses a size threshold or stays above one for a while. It projects drain time from the lag trend, learns per-consumer throughput from the consumer group itself, and reports the consumer count needed to meet the SLO. Scaling decisions are tied directly to the question operators care about: *are we keeping up?*

---

## **Table of Contents**

1. Background
2. Problem Statement
3. Why Static Thresholds Are Insufficient
4. Lag Trend as the Scaling Signal
5. Scaler Design
6. Architecture and Flow
7. Example Implementation Logic
8. Evaluation and Benefits
9. Conclusion

---

## **1. Background**

Apache Kafka is a high-throughput distributed streaming platform. Consumer groups read events from topics and partitions. The lag between the **latest offset** and the **consumer’s committed offset** is a key metric describing consumer health.

Kubernetes Event-Driven Autoscaling (KEDA) enables event-driven scale-to-zero or scale-based workloads in Kubernetes. KEDA’s built-in scalers typically use static metrics (e.g., number of messages, age of messages) to decide scaling events.

Static threshold-based policies are simple but often don’t reflect real consumer pressure, especially under variable throughput and non-uniform workloads.

---

## **2. Problem Statement**

In traditional KEDA scaling:

* Static thresholds (e.g., 1000 unprocessed messages) trigger scale-up
* Even if lag is temporary and catches up quickly, autoscaler reacts
* Under heavy throughput spikes, static thresholds can under-react until lag grows dangerously high
* Scaling frequently oscillates (flapping)

Example static policy:

```yaml
spec:
  triggers:
  - type: kafka
    metadata:
      topic: my-topic
      lagThreshold: "1000"
```

But **snapshot-based lag is an instantaneous metric**. It does not reflect:

* how long lag has existed
* whether the cluster can catch up
* whether lag is trending up or down

Thus, scaling based on “current lag” can lead to wasteful or insufficient scaling.

---

## **3. Why Static Thresholds Are Insufficient**

### **3.1 Snapshot Metrics Are Volatile**

Kafka lag can fluctuate rapidly within seconds:

* Temporary surge in messages → lag spikes
* Consumer catches up → lag drops back
* Static threshold triggers scale-up even if no sustained backlog

This causes **unnecessary scaling events**.

### **3.2 Workload Diversity**

Some workloads have high variance:

| Time  | Throughput | Lag     |
| ----- | ---------- | ------- |
| 12:00 | 10k msg/s  | 15k lag |
| 12:05 | 50 msg/s   | 500 lag |

A static threshold of 1k would scale up at both times—even though only one represents actual lag pressure.

### **3.3 Metrics Aren’t SLO-Aligned**

Kafka lag itself is a proxy for delivering real service goals — e.g., latency from publish to processing.

A one-time snapshot doesn’t map directly to a **real user-impacting measure**. For many systems, customers care about **persistent lag** (e.g., sustained delays) more than brief spikes.

---

## **4. Lag Trend as the Scaling Signal**

Neither the size of lag nor how long it has been high answers the operational question. The question is:

> *Can the current consumers drain the backlog within an acceptable time?*

That depends on three things, all of which can be read from offsets alone:

* **Produce rate `P`**: slope of Σ partition end offsets
* **Consume rate `C`**: slope of Σ committed offsets
* **Backlog `L`**: current total lag (the lag slope is `P − C`)

If lag is falling, the projected time to drain is `L / (C − P)`. If it isn't falling, the backlog never drains. Comparing that projection to an SLO-derived `maxDrain` gives a binary, workload-independent signal:

> **Stalled** ⇔ `L ≥ minLag` and `drainETA > maxDrain`

This one rule covers every case a threshold or a persistence rule gets wrong:

| Situation | Threshold | "Above X for N min" | Drain-time trend |
| --- | --- | --- | --- |
| Large burst, drains quickly | scales | scales if N < drain time | ignores |
| Small lag that never drains | ignores | ignores | scales |
| Huge lag, falling at 1 msg/s | scales | scales | scales (ETA ≫ SLO) |
| Lag rising from zero | scales late | scales late | scales once the trend is measurable |

The knob is a **time**, not a message count. It can be read straight off an SLO such as "messages processed within 5 minutes of publish", and it doesn't need retuning as message sizes or throughput change.

---

## **5. Scaler Design**

### **5.1 Collection**

Every sampling interval (default 10s) the scraper records one **batch**: total lag, Σ committed offsets, Σ end offsets, partition count, and consumer-group size. Batches older than the trend window are evicted.

### **5.2 Trend fit**

Over the last `trendWindow` (default 120s), least-squares slopes give the lag slope, `P` and `C`. Least squares is used over endpoint differences because it is robust to scrape jitter, uneven spacing, and consumer commit batching.

The fit only uses the trailing run of batches that:

* share the latest batch's **partition count** and **consumer count**, so a rebalance or repartition restarts the fit, and
* while lag is ≥ `minLag`, belong to the **current lag episode** (since lag last rose above `minLag`). The vertical jump of a burst is never fitted as a rising trend.

Until that segment spans ~80% of the window, the scaler is **warming up** and holds the current consumer count.

### **5.3 Capacity learning**

When lag stayed above `minLag` for the whole segment, consumers had work queued the entire time, so they were saturated. Then `C / consumers` is their true per-consumer throughput. It is folded into an EWMA and remembered across polls. Learning is skipped on offset resets (committed offsets moving backwards). Operators can pin a fixed `perConsumerRate` instead.

### **5.4 Sizing**

```
need = ceil( headroom × (P + L / maxDrain) / capacity )

stalled  → desired = max(need, consumers + 1)
healthy  → desired = min(need, consumers)
desired  ∈ [minConsumers, min(maxConsumers, partitions)]
```

* `P / capacity` is the steady-state requirement. `L / maxDrain` is the extra throughput needed to clear the backlog within the SLO.
* Stalled always steps up at least one, even with no capacity estimate yet.
* Healthy can only hold or scale down.

### **5.5 Hysteresis without timers**

Trend signals are prone to flapping: scale up → lag falls → signal clears → scale down → lag returns. The sizing rule avoids this structurally. A healthy trend blocks *scale-up* but doesn't reset the target to the minimum. The target stays at `P / capacity`, so replicas hold after catching up and fall only when the produce rate does. HPA's own scale-down stabilization adds a time-based buffer on top.

---

## **6. Architecture & Flow**

### **6.1 Components**

1. **Scraper**: pulls end offsets, committed offsets, and group membership from Kafka
2. **Sliding Window**: holds recent batches
3. **Trend Evaluator**: slope fit → drain ETA → stalled → capacity → desired
4. **KEDA External Scaler**: `IsActive` = lag ≥ `minLag`; `GetMetrics` = desired consumer count with `TargetSize: 1`, so HPA's desired replicas equal it directly

---

## **7. Example Implementation Logic**

```go
seg := stableSegment(batches, trendWindow, minLag)   // same n, same partitions, current episode
lagSlope := linearSlope(t, lag)
P, C     := linearSlope(t, endOffsets), linearSlope(t, committed)

drainETA := +Inf
if lagSlope < 0 { drainETA = L / -lagSlope }
stalled := L >= minLag && drainETA > maxDrain

if saturatedWholeSegment && consumerCountStable {
    capacity = ewma(capacity, C / n, alpha)
}

need := ceil(headroom * (P + L/maxDrain) / capacity)
if stalled { desired = max(need, n+1) } else { desired = min(need, n) }
desired = clamp(desired, minConsumers, min(maxConsumers, partitions))
```

Applied in KEDA:

```yaml
spec:
  triggers:
    - type: external
      metadata:
        scalerAddress: lag-scaler.default.svc.cluster.local:50051
        topic: my-topic
        consumerGroup: my-group
```

with `MAX_DRAIN_SECONDS`, `TREND_WINDOW_SECONDS`, etc. set on the scaler Deployment.

---

## **8. Evaluation & Benefits**

### **8.1 Higher Stability**

By scaling on drain time instead of lag size:

* Bursts that the current consumers can absorb never trigger scaling
* After a scale-up, replicas hold at the produce-rate requirement instead of oscillating

### **8.2 Better SLO Alignment**

Lag directly maps to user-visible delay:

* `maxDrain` *is* the latency budget for clearing a backlog
* Stalled = the budget will be missed at the current consumer count

### **8.3 Cost Efficiency**

Reduces:

* Unnecessary pods for bursts that would drain on their own
* Over-provisioning, because replica counts come from measured throughput rather than `lag / threshold`

---

## **9. Conclusion**

Static thresholds for Kafka consumer scaling in KEDA are simple but misaligned with real workload dynamics. Adding a persistence requirement makes them slower, not smarter. A **trend-based scaler** offers:

✔ A signal that directly answers "can consumers catch up?"
✔ A tuning knob expressed as time, readable from an SLO
✔ Replica counts derived from measured throughput
✔ Structural resistance to flapping

The **trend of lag relative to a drain-time budget** is the true indicator of insufficient consumer capacity. Scaling on it aligns autoscaling with operational goals, improving both efficiency and reliability.

