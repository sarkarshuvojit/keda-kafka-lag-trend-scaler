# Kafka Lag Trend Scaler

A standalone gRPC service that implements [KEDA's External Scaler protocol](https://keda.sh/docs/latest/concepts/external-scalers/) to scale Kafka consumers on one question: **can the current consumers drain the backlog in time?** It doesn't scale on how big lag is, or how long it has been high. It fits a trend to lag over a short window, projects when the backlog will drain, and scales only when that projection misses an SLO. It then reports how many consumers are needed, using per-consumer throughput it learns from the group itself.

## How it decides

Every `SAMPLING_INTERVAL` the scraper records one batch per topic: total lag, Σ committed offsets, Σ end offsets, partition count, and consumer-group size. On each KEDA poll, over the last `TREND_WINDOW_SECONDS`:

1. **Fit the trend.** Least-squares slopes give `lagSlope`, the produce rate `P` (slope of end offsets) and the consume rate `C` (slope of committed offsets).
2. **Project drain time.** `drainETA = lag / -lagSlope`. If lag isn't falling, it's infinite.
3. **Stalled** = `lag >= MIN_LAG` and `drainETA > MAX_DRAIN_SECONDS`. This covers lag that is flat, rising, or falling too slowly.
4. **Learn capacity.** If lag stayed above `MIN_LAG` for the whole window and the consumer count didn't change, the consumers were saturated, so `C / consumers` is their real throughput. It's smoothed with an EWMA (`CAPACITY_SMOOTHING`). `PER_CONSUMER_RATE` overrides this.
5. **Size the group.** `need = ceil(HEADROOM × (P + lag / MAX_DRAIN_SECONDS) / capacity)`.
   - stalled → `max(need, consumers + 1)`: always step up.
   - healthy → `min(need, consumers)`: never scale up, but scale down when the produce rate drops.
   - clamped to `[MIN_CONSUMERS, min(MAX_CONSUMERS, partitions)]`.

The fit only covers batches with the same partition and consumer count as the latest one. While lag is above `MIN_LAG`, it also only covers the current lag episode. So a rebalance, a partition change, or the instant jump of a burst never gets mistaken for a trend. Until the segment spans about 80% of the window (and has at least `MIN_SAMPLES` batches), the scaler is **warming up** and reports the current consumer count.

Why it doesn't flap: after scaling up, lag falls and *stalled* clears. But the desired count is still `P / capacity`, the real steady-state requirement, so replicas hold instead of dropping back.

## Prerequisites

- Go 1.25+
- Docker (or minikube with `eval $(minikube docker-env)`)
- A running Kubernetes cluster with [KEDA](https://keda.sh) installed
- Kafka cluster accessible from the cluster (see `examples/k8s/infra/kafka.yaml`)

## Configuration

The running scaler reads its configuration from **environment variables** on the `lag-scaler` Deployment (`examples/k8s/deploy/lag-scaler.yaml`). The metadata keys are what `config.ParseFromMetadata` accepts, but trigger metadata from the ScaledObject isn't wired into the running server yet.

| Environment Variable | Metadata Key | Description | Default |
|---|---|---|---|
| `KAFKA_BROKERS` | `bootstrapServers` | Kafka broker addresses | `localhost:9092` |
| `KAFKA_TOPIC` | `topic` | Topic to monitor | *(required)* |
| `KAFKA_GROUP_ID` | `consumerGroup` | Consumer group to track | *(required)* |
| `SAMPLING_INTERVAL` | `samplingInterval` | Seconds between each lag poll | `10` |
| `MAX_DRAIN_SECONDS` | `maxDrainSeconds` | The SLO: backlog must be projected to drain within this. Lower = more aggressive | `300` |
| `TREND_WINDOW_SECONDS` | `trendWindowSeconds` | History used for the slope/rate fit. Longer = steadier but slower to react. Must be ≥ 2× sampling interval | `120` |
| `MIN_LAG` | `minLag` | Lag below this is ignored (never stalled, scaler inactive). Set above your normal in-flight/commit-batch lag | `100` |
| `HEADROOM` | `headroom` | Multiplier on required throughput when sizing | `1.2` |
| `PER_CONSUMER_RATE` | `perConsumerRate` | Fixed msgs/sec per consumer. Unset = learn it while consumers are saturated | *(learned)* |
| `CAPACITY_SMOOTHING` | `capacitySmoothing` | EWMA alpha for learned capacity, `(0, 1]`. Higher = trusts the latest window more | `0.3` |
| `MIN_CONSUMERS` | `minConsumers` | Lower bound on reported consumers | `1` |
| `MAX_CONSUMERS` | `maxConsumers` | Upper bound on reported consumers. `0` = partition count (never above it either way) | `0` |
| `MIN_SAMPLES` | `minSamples` | Minimum batches in the window before deciding | `6` |
| `GRPC_PORT` | — | Port for the gRPC server | `50051` |
| `METRICS_PORT` | — | Port for the Prometheus `/metrics` endpoint | `9090` |

## Step 1: Run Tests

```bash
cd scaler
make test
```

All packages should report `ok`. `pkg/lag/trend_test.go` covers the core cases: rising, flat, fast-drain, slow-drain, burst jumps, capacity learning, scale-down, clamps, rebalances, partition changes, and offset resets.

## Step 2: Start the Environment

From the repo root, run:

```bash
./start.sh [--partitions N]
```

This builds the sample-app and scaler images against minikube's Docker daemon, applies the infra manifests (Kafka, kafbat UI, Prometheus, Grafana), explicitly creates `test-topic` with `N` partitions (default `3`, useful for testing scaling behavior across more partitions than Kafka's single-partition auto-create default), deploys the consumer/producer/lag-scaler, and applies the persistent `ScaledObject`.

To do the same steps by hand instead:

```bash
eval $(minikube docker-env)
docker build -t kpkls:latest examples/sample-app
docker build -t kpkls-scaler:latest scaler

kubectl apply -f examples/k8s/infra/
kubectl exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic test-topic --partitions 3 --replication-factor 1

kubectl apply -f examples/k8s/deploy/deployment.yaml
kubectl apply -f examples/k8s/deploy/producer.yaml
kubectl apply -f examples/k8s/deploy/lag-scaler.yaml
```

The `lag-scaler` Deployment (1 replica) exposes a ClusterIP Service with the gRPC port (`50051`) and a Prometheus metrics port (`9090`).

### Verify the scaler is running

```bash
kubectl get pods -l app=lag-scaler
```

```
NAME                          READY   STATUS    RESTARTS   AGE
lag-scaler-5d4f8b7c6f-x9k2p  1/1     Running   0          10s
```

Check the logs to confirm it's polling Kafka:

```bash
kubectl logs -l app=lag-scaler -f
```

You should see output like:

```
Starting Kafka lag trend scaler
  Brokers:            kafka.default.svc.cluster.local:9092
  Topic:              test-topic
  Consumer Group:     sample-consumer-group
  Sampling Interval:  10s
  Trend Window:       1m0s
  Max Drain (SLO):    2m0s
  Min Lag:            100
  ...
gRPC server listening on :50051
Collected batch: partitions=3 consumers=1 (window: 1 batches)
Collected batch: partitions=3 consumers=1 (window: 2 batches)
```

### Verify the service is reachable

```bash
kubectl get svc lag-scaler
```

```
NAME         TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)     AGE
lag-scaler   ClusterIP   10.96.xxx.xxx   <none>        50051/TCP   30s
```

## Step 3: Verify the ScaledObject

`start.sh` already removes the basic (threshold-based) ScaledObject if present and applies the persistent one (`examples/k8s/scalers/persistent/scaledobject.yaml`). To check KEDA picked it up:

```bash
kubectl get scaledobject kafka-consumer-scaler
```

```
NAME                     SCALETARGETREF   MIN   MAX   TRIGGERS   ...   READY   ACTIVE
kafka-consumer-scaler    kafka-consumer   1     5     external   ...   True    False
```

`READY=True` means KEDA is successfully talking to the scaler. `ACTIVE=False` means lag is below `MIN_LAG` (expected at rest).

You can also check KEDA's operator logs:

```bash
kubectl logs -l app=keda-operator -n keda --tail=20
```

Look for lines mentioning `kafka-consumer-scaler` without errors.

## Step 4: Test It

### Generate load

Send a burst of messages via the producer:

```bash
kubectl run curl-load --rm -it --restart=Never --image=curlimages/curl -- \
  curl -s -X POST http://kafka-producer/produce \
  -H 'Content-Type: application/json' \
  -d '{"count":5000,"messageSize":64}'
```

### Watch the scaler logs

```bash
kubectl logs -l app=lag-scaler -f
```

You should see:

1. **`warmingUp lag=... desired=1`** — a new lag episode started; the scaler holds the current count until the window has enough of it
2. **`stalled=true ... drainETA=+Infs`** (or an ETA above `MAX_DRAIN_SECONDS`) — the consumers can't drain in time
3. **`capacity=10.0/s`** — learned per-consumer throughput while saturated
4. **`GetMetrics: desired=N`** — the consumer count KEDA/HPA scales to

### Watch the consumer scale

```bash
kubectl get pods -l app=kafka-consumer -w
```

A burst that the current consumers drain within the SLO never scales. One they can't drain scales straight to the computed count, up to the partition count. Compare this with the threshold-based scaler (`examples/k8s/scalers/basic/scaledobject.yaml`), which scales on any lag over 10.

### After lag clears

`stalled` goes back to `false`, but the desired count tracks `HEADROOM × produce rate / capacity`. Replicas only come down when the produce rate actually drops (subject to HPA's scale-down stabilization).

## Observability

`start.sh` deploys Prometheus and Grafana alongside the rest of the infra. Grafana comes pre-provisioned with a "Kafka Lag Trend Scaler" dashboard:

- **Consumer Count vs. Lag** — `kafka_lag_scaler_total_lag`, `kafka_lag_scaler_consumer_count`, and `kafka_lag_scaler_desired_consumers` (what the scaler asked for)
- **Consumers Stalled** — a bar each time `kafka_lag_scaler_activations_total` ticks (a healthy → stalled transition)

The scaler also exports what's behind each decision: `kafka_lag_scaler_lag_slope`, `_produce_rate`, `_consume_rate`, `_drain_eta_seconds`, `_consumer_capacity`, `_stalled`, `_warming_up`, `_active`, and per-partition lag.

Open it with:

```bash
minikube service grafana --url
```

All of these are served at `/metrics` on port `9090`.

## Running Locally (outside Kubernetes)

For development/debugging, you can run the scaler directly:

```bash
KAFKA_BROKERS="localhost:9092" \
KAFKA_TOPIC="test-topic" \
KAFKA_GROUP_ID="sample-consumer-group" \
MAX_DRAIN_SECONDS="300" \
TREND_WINDOW_SECONDS="120" \
go run main.go
```

The gRPC server will start on `:50051`. You can test it with [grpcurl](https://github.com/fullstorydev/grpcurl):

```bash
grpcurl -plaintext -d '{"name":"test","namespace":"default"}' \
  localhost:50051 externalscaler.ExternalScaler/IsActive
```

```json
{
  "result": false
}
```

## Project Structure

```
scaler/
  main.go                       # Entry point: wires everything, gRPC server, signal handling
  Makefile                      # build, proto-gen, test
  Dockerfile                    # Multi-stage alpine build
  proto/
    externalscaler.proto        # KEDA External Scaler proto (vendored)
  pkg/
    externalscaler/             # Generated protobuf + gRPC Go code
    config/config.go            # ScalerConfig: parse from metadata or env vars
    kafka/client.go             # LagFetcher: per-partition lag + consumer group size via kafka-go Client API
    lag/
      sample.go                 # LagSample (per partition) and Batch (per scrape) types
      window.go                 # SlidingWindow of batches: thread-safe, time-based eviction
      trend.go                  # EvaluateTrend: slope fit, drain ETA, capacity learning, sizing
      trend_test.go             # Unit tests
    metrics/metrics.go          # Prometheus gauges + /metrics HTTP handler
    scraper/scraper.go          # Background goroutine: periodic lag + consumer count collection
    server/server.go            # gRPC ExternalScalerServer; holds learned capacity across polls
```
