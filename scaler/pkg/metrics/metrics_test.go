package metrics

import (
	"io"
	"math"
	"net/http/httptest"
	"regexp"
	"strconv"
	"testing"

	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/lag"
)

// scrape returns the /metrics body.
func scrape(t *testing.T) string {
	t.Helper()
	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("GET /metrics = %d", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	return string(body)
}

// value returns the sample value of the first series matching name{labels}.
func value(t *testing.T, body, series string) float64 {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(series) + ` (\S+)$`)
	m := re.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("series %s not found in:\n%s", series, body)
	}
	if m[1] == "+Inf" {
		return math.Inf(1)
	}
	v, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		t.Fatalf("parse %s: %v", m[1], err)
	}
	return v
}

func TestUpdateLag_PerPartitionAndTotal(t *testing.T) {
	UpdateLag([]lag.LagSample{
		{Topic: "t-lag", Partition: 0, Lag: 7},
		{Topic: "t-lag", Partition: 1, Lag: 5},
	})
	UpdateLag(nil) // no-op

	body := scrape(t)
	if v := value(t, body, `kafka_lag_scaler_partition_lag{partition="1",topic="t-lag"}`); v != 5 {
		t.Errorf("partition 1 lag = %v, want 5", v)
	}
	if v := value(t, body, `kafka_lag_scaler_total_lag{topic="t-lag"}`); v != 12 {
		t.Errorf("total lag = %v, want 12", v)
	}
}

func TestUpdateConsumerCount(t *testing.T) {
	UpdateConsumerCount("t-cc", "g", 4)
	if v := value(t, scrape(t), `kafka_lag_scaler_consumer_count{consumer_group="g",topic="t-cc"}`); v != 4 {
		t.Errorf("consumer count = %v, want 4", v)
	}
}

func TestUpdateTrend_GaugesAndStallCounter(t *testing.T) {
	const topic = "t-trend"
	healthy := lag.TrendResult{
		Active: true, LagSlope: -12.5, ProduceRate: 30, ConsumeRate: 42.5,
		DrainETA: 80, State: lag.State{Rate: 10.5}, Desired: 4,
	}
	UpdateTrend(topic, healthy)

	body := scrape(t)
	// The counter exists at 0 from the first evaluation, so increase() can
	// see the first stall.
	if v := value(t, body, `kafka_lag_scaler_activations_total{topic="t-trend"}`); v != 0 {
		t.Fatalf("activations = %v, want 0 before any stall", v)
	}
	for series, want := range map[string]float64{
		`kafka_lag_scaler_active{topic="t-trend"}`:            1,
		`kafka_lag_scaler_stalled{topic="t-trend"}`:           0,
		`kafka_lag_scaler_warming_up{topic="t-trend"}`:        0,
		`kafka_lag_scaler_lag_slope{topic="t-trend"}`:         -12.5,
		`kafka_lag_scaler_produce_rate{topic="t-trend"}`:      30,
		`kafka_lag_scaler_consume_rate{topic="t-trend"}`:      42.5,
		`kafka_lag_scaler_drain_eta_seconds{topic="t-trend"}`: 80,
		`kafka_lag_scaler_consumer_capacity{topic="t-trend"}`: 10.5,
		`kafka_lag_scaler_desired_consumers{topic="t-trend"}`: 4,
	} {
		if v := value(t, body, series); v != want {
			t.Errorf("%s = %v, want %v", series, v, want)
		}
	}

	stalled := healthy
	stalled.Stalled = true
	stalled.DrainETA = math.Inf(1)
	UpdateTrend(topic, stalled)
	UpdateTrend(topic, stalled) // still stalled: no second increment
	UpdateTrend(topic, healthy)
	UpdateTrend(topic, stalled) // second stall

	body = scrape(t)
	if v := value(t, body, `kafka_lag_scaler_activations_total{topic="t-trend"}`); v != 2 {
		t.Errorf("activations = %v, want 2 (one per healthy->stalled transition)", v)
	}
	if v := value(t, body, `kafka_lag_scaler_drain_eta_seconds{topic="t-trend"}`); !math.IsInf(v, 1) {
		t.Errorf("drain ETA = %v, want +Inf", v)
	}
}

func TestUpdateTrend_WarmingUpKeepsLastRates(t *testing.T) {
	const topic = "t-warm"
	UpdateTrend(topic, lag.TrendResult{LagSlope: 3, ProduceRate: 9, Desired: 2})
	UpdateTrend(topic, lag.TrendResult{WarmingUp: true, Desired: 2})

	body := scrape(t)
	if v := value(t, body, `kafka_lag_scaler_warming_up{topic="t-warm"}`); v != 1 {
		t.Errorf("warming_up = %v, want 1", v)
	}
	// Warm-up has no fit, so rate gauges keep their last fitted values.
	if v := value(t, body, `kafka_lag_scaler_produce_rate{topic="t-warm"}`); v != 9 {
		t.Errorf("produce rate = %v, want last fitted 9", v)
	}
}
