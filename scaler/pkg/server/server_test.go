package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sarkarshuvojit/keda-persistent-kafka-lag-scaler/scaler/pkg/config"
	pb "github.com/sarkarshuvojit/keda-persistent-kafka-lag-scaler/scaler/pkg/externalscaler"
	"github.com/sarkarshuvojit/keda-persistent-kafka-lag-scaler/scaler/pkg/lag"
)

func defaultConfig() *config.ScalerConfig {
	return &config.ScalerConfig{
		BootstrapServers:  "localhost:9092",
		Topic:             "test-topic",
		ConsumerGroup:     "test-group",
		SamplingInterval:  10 * time.Second,
		MinLag:            100,
		MaxDrain:          300 * time.Second,
		TrendWindow:       120 * time.Second,
		Headroom:          1.0,
		CapacitySmoothing: 1.0,
		MinConsumers:      1,
		MinSamples:        6,
	}
}

func ref() *pb.ScaledObjectRef {
	return &pb.ScaledObjectRef{Name: "test", Namespace: "default"}
}

func newServer(cfg *config.ScalerConfig) (*ExternalScalerServer, *lag.SlidingWindow) {
	w := lag.NewSlidingWindow(cfg.WindowDuration())
	return New(w, cfg), w
}

// scrape mimics the real scraper: one scrape every 10s, ending now, with
// `partitions` partitions sharing produce/consume rates evenly.
func scrape(w *lag.SlidingWindow, d time.Duration, partitions, consumers int, initialLag int64, produce, consume float64) {
	start := time.Now().Add(-d)
	for t := time.Duration(0); t <= d; t += 10 * time.Second {
		ts := start.Add(t)
		var samples []lag.LagSample
		for p := range partitions {
			end := 1000 + initialLag/int64(partitions) + int64(produce*t.Seconds())/int64(partitions)
			committed := 1000 + int64(consume*t.Seconds())/int64(partitions)
			committed = min(committed, end)
			samples = append(samples, lag.LagSample{
				Timestamp: ts, Topic: "test-topic", Partition: p,
				Lag: end - committed, Offset: committed, EndOffset: end,
			})
		}
		w.Add(lag.NewBatch(samples, consumers))
	}
}

func desired(t *testing.T, srv *ExternalScalerServer) int64 {
	t.Helper()
	resp, err := srv.GetMetrics(context.Background(), &pb.GetMetricsRequest{ScaledObjectRef: ref(), MetricName: MetricName})
	if err != nil {
		t.Fatalf("GetMetrics: %v", err)
	}
	if len(resp.MetricValues) != 1 || resp.MetricValues[0].MetricName != MetricName {
		t.Fatalf("unexpected metric values: %+v", resp.MetricValues)
	}
	return resp.MetricValues[0].MetricValue
}

func isActive(t *testing.T, srv *ExternalScalerServer) bool {
	t.Helper()
	resp, err := srv.IsActive(context.Background(), ref())
	if err != nil {
		t.Fatalf("IsActive: %v", err)
	}
	return resp.Result
}

func TestGetMetricSpec(t *testing.T) {
	srv, _ := newServer(defaultConfig())
	resp, err := srv.GetMetricSpec(context.Background(), ref())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.MetricSpecs) != 1 || resp.MetricSpecs[0].MetricName != MetricName || resp.MetricSpecs[0].TargetSize != 1 {
		t.Errorf("unexpected spec: %+v", resp.MetricSpecs)
	}
}

func TestGetMetrics_NoDataIsError(t *testing.T) {
	srv, _ := newServer(defaultConfig())
	_, err := srv.GetMetrics(context.Background(), &pb.GetMetricsRequest{ScaledObjectRef: ref(), MetricName: MetricName})
	if !errors.Is(err, ErrNoData) {
		t.Fatalf("expected ErrNoData, got %v", err)
	}
}

func TestIsActive_EmptyWindow(t *testing.T) {
	srv, _ := newServer(defaultConfig())
	if isActive(t, srv) {
		t.Error("expected inactive with empty window")
	}
}

func TestIsActive_FollowsMinLag(t *testing.T) {
	srv, w := newServer(defaultConfig())
	scrape(w, 2*time.Minute, 3, 1, 30, 10, 10)
	if isActive(t, srv) {
		t.Error("expected inactive when lag is below minLag")
	}

	srv, w = newServer(defaultConfig())
	scrape(w, 2*time.Minute, 3, 1, 3000, 10, 10)
	if !isActive(t, srv) {
		t.Error("expected active when lag is above minLag")
	}
}

func TestGetMetrics_WarmingUpHoldsCurrent(t *testing.T) {
	srv, w := newServer(defaultConfig())
	scrape(w, 30*time.Second, 6, 2, 5000, 100, 10)
	if got := desired(t, srv); got != 2 {
		t.Errorf("desired = %d, want 2 (hold during warm-up)", got)
	}
}

func TestGetMetrics_FastDrainDoesNotScale(t *testing.T) {
	// A burst that one consumer drains well within the SLO.
	srv, w := newServer(defaultConfig())
	scrape(w, 2*time.Minute, 6, 1, 3000, 0, 20)
	if got := desired(t, srv); got != 1 {
		t.Errorf("desired = %d, want 1", got)
	}
}

func TestGetMetrics_CantCatchUpScalesToNeed(t *testing.T) {
	// One consumer doing 10 msg/s against 60 msg/s produce, 12 partitions.
	// Learned capacity 10 -> need ≈ (60 + ~7000/300) / 10 ≈ 9.
	srv, w := newServer(defaultConfig())
	scrape(w, 2*time.Minute, 12, 1, 1000, 60, 10)
	got := desired(t, srv)
	if got < 8 || got > 10 {
		t.Errorf("desired = %d, want ~9", got)
	}
	if srv.state.Rate < 9 || srv.state.Rate > 11 {
		t.Errorf("learned capacity = %f, want ~10", srv.state.Rate)
	}
}

func TestGetMetrics_CapacityPersistsAcrossEvaluations(t *testing.T) {
	// Learn capacity while stalled, then after scale-up keep the replica count
	// while draining instead of flapping back down.
	cfg := defaultConfig()
	srv, w := newServer(cfg)
	scrape(w, 2*time.Minute, 12, 1, 1000, 60, 10)
	_ = desired(t, srv)

	w2 := lag.NewSlidingWindow(cfg.WindowDuration())
	srv.window = w2
	scrape(w2, 2*time.Minute, 12, 8, 5000, 60, 80)
	// need = (60 + ~2600/300) / 10 ≈ 6.9 -> 7: trimmed to what the produce
	// rate needs, not collapsed back to 1 just because lag is now falling.
	if got := desired(t, srv); got != 7 {
		t.Errorf("desired = %d, want 7 after scale-up", got)
	}
}

func TestGetMetrics_ClampedToPartitions(t *testing.T) {
	srv, w := newServer(defaultConfig())
	scrape(w, 2*time.Minute, 4, 1, 1000, 500, 10)
	if got := desired(t, srv); got != 4 {
		t.Errorf("desired = %d, want 4 (partition count)", got)
	}
}
