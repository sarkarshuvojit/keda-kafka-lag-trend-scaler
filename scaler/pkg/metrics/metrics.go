package metrics

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/lag"
)

var (
	registry = prometheus.NewRegistry()

	partitionLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_partition_lag",
		Help: "Current lag for a single topic partition.",
	}, []string{"topic", "partition"})

	totalLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_total_lag",
		Help: "Sum of lag across all partitions of a topic.",
	}, []string{"topic"})

	consumerCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_consumer_count",
		Help: "Number of active members in the consumer group.",
	}, []string{"topic", "consumer_group"})

	active = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_active",
		Help: "Whether lag is at or above minLag (1) or not (0). Drives KEDA activation.",
	}, []string{"topic"})

	stalled = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_stalled",
		Help: "Whether consumers can't drain the backlog within maxDrainSeconds (1) or can (0).",
	}, []string{"topic"})

	warmingUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_warming_up",
		Help: "Whether there isn't enough stable history to judge the trend yet (1) or there is (0).",
	}, []string{"topic"})

	activations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_lag_scaler_activations_total",
		Help: "Count of transitions into the stalled state. Increments once per stall, marking the moment consumers stopped keeping up.",
	}, []string{"topic"})

	lagSlope = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_lag_slope",
		Help: "Least-squares slope of total lag over the trend window, msgs/sec. Negative = draining.",
	}, []string{"topic"})

	produceRate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_produce_rate",
		Help: "Produce rate over the trend window, msgs/sec.",
	}, []string{"topic"})

	consumeRate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_consume_rate",
		Help: "Consume (commit) rate over the trend window, msgs/sec.",
	}, []string{"topic"})

	drainETA = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_drain_eta_seconds",
		Help: "Projected seconds until lag reaches zero. +Inf when lag isn't falling.",
	}, []string{"topic"})

	consumerCapacity = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_consumer_capacity",
		Help: "Per-consumer throughput used for sizing (learned or configured), msgs/sec. 0 = unknown.",
	}, []string{"topic"})

	desiredConsumers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_lag_scaler_desired_consumers",
		Help: "Consumer count reported to KEDA.",
	}, []string{"topic"})

	stateMu   sync.Mutex
	lastStall = make(map[string]bool)
)

func init() {
	registry.MustRegister(
		partitionLag, totalLag, consumerCount, active, stalled, warmingUp, activations,
		lagSlope, produceRate, consumeRate, drainETA, consumerCapacity, desiredConsumers,
	)
}

// UpdateLag records the latest per-partition lag samples and their total.
func UpdateLag(samples []lag.LagSample) {
	if len(samples) == 0 {
		return
	}

	var total int64
	topic := samples[0].Topic
	for _, s := range samples {
		partitionLag.WithLabelValues(s.Topic, strconv.Itoa(s.Partition)).Set(float64(s.Lag))
		total += s.Lag
	}
	totalLag.WithLabelValues(topic).Set(float64(total))
}

// UpdateConsumerCount records the current number of active consumer group members.
func UpdateConsumerCount(topic, consumerGroup string, count int) {
	consumerCount.WithLabelValues(topic, consumerGroup).Set(float64(count))
}

// UpdateTrend records one evaluation. The activations counter goes up exactly
// once per healthy->stalled transition, so a stall can be plotted as a single
// event rather than a band.
func UpdateTrend(topic string, r lag.TrendResult) {
	active.WithLabelValues(topic).Set(boolToFloat(r.Active))
	stalled.WithLabelValues(topic).Set(boolToFloat(r.Stalled))
	warmingUp.WithLabelValues(topic).Set(boolToFloat(r.WarmingUp))
	desiredConsumers.WithLabelValues(topic).Set(float64(r.Desired))
	consumerCapacity.WithLabelValues(topic).Set(r.State.Rate)

	if !r.WarmingUp {
		lagSlope.WithLabelValues(topic).Set(r.LagSlope)
		produceRate.WithLabelValues(topic).Set(r.ProduceRate)
		consumeRate.WithLabelValues(topic).Set(r.ConsumeRate)
		drainETA.WithLabelValues(topic).Set(r.DrainETA)
	}

	stateMu.Lock()
	wasStalled, seen := lastStall[topic]
	if !seen {
		// Create the series at 0 so increase() sees the first stall.
		activations.WithLabelValues(topic).Add(0)
	}
	lastStall[topic] = r.Stalled
	stateMu.Unlock()

	if r.Stalled && seen && !wasStalled {
		activations.WithLabelValues(topic).Inc()
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// Handler returns the HTTP handler that serves the /metrics endpoint.
func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}
