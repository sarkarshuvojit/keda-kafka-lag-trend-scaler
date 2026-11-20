package lag

import (
	"math"
	"testing"
	"time"
)

func defaultParams() TrendParams {
	return TrendParams{
		MinLag:            100,
		MaxDrain:          300 * time.Second,
		TrendWindow:       120 * time.Second,
		Headroom:          1.0,
		CapacitySmoothing: 1.0,
		MinConsumers:      1,
		MinSamples:        6,
	}
}

// series builds batches at 10s intervals over the given duration. The topic
// starts with initialLag, producers add produce msgs/sec, and consumers commit
// consume msgs/sec (never past the end offset).
type series struct {
	start      time.Time
	duration   time.Duration
	initialLag int64
	produce    float64
	consume    float64
	consumers  int
	partitions int
}

func (s series) build() []Batch {
	if s.start.IsZero() {
		s.start = time.Unix(1_000_000, 0)
	}
	if s.partitions == 0 {
		s.partitions = 10
	}
	var out []Batch
	const committed0 = 1_000_000
	for t := time.Duration(0); t <= s.duration; t += 10 * time.Second {
		sec := t.Seconds()
		end := int64(committed0) + s.initialLag + int64(s.produce*sec)
		committed := int64(committed0) + int64(s.consume*sec)
		if committed > end {
			committed = end
		}
		out = append(out, Batch{
			Timestamp:      s.start.Add(t),
			TotalLag:       end - committed,
			TotalCommitted: committed,
			TotalEnd:       end,
			Partitions:     s.partitions,
			Consumers:      s.consumers,
		})
	}
	return out
}

func TestTrend_Empty(t *testing.T) {
	r := EvaluateTrend(nil, defaultParams(), State{})
	if !r.WarmingUp || r.Stalled || r.Active {
		t.Fatalf("unexpected result for empty input: %+v", r)
	}
	if r.Desired != 1 {
		t.Errorf("desired = %d, want MinConsumers (1)", r.Desired)
	}
}

func TestTrend_WarmingUpHoldsCurrent(t *testing.T) {
	// Only 30s of history; window is 120s.
	b := series{duration: 30 * time.Second, initialLag: 5000, produce: 100, consume: 10, consumers: 3}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.WarmingUp {
		t.Fatal("expected warming up")
	}
	if r.Stalled {
		t.Error("must not report stalled while warming up")
	}
	if r.Desired != 3 {
		t.Errorf("desired = %d, want current consumers 3", r.Desired)
	}
	if !r.Active {
		t.Error("expected active since lag >= minLag")
	}
}

func TestTrend_RisingLagIsStalled(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 1000, produce: 50, consume: 20, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.Stalled {
		t.Fatalf("expected stalled for rising lag: %+v", r)
	}
	if r.LagSlope <= 0 {
		t.Errorf("lag slope = %f, want > 0", r.LagSlope)
	}
	if !math.IsInf(r.DrainETA, 1) {
		t.Errorf("drain ETA = %f, want +Inf", r.DrainETA)
	}
	if math.Abs(r.ProduceRate-50) > 1 || math.Abs(r.ConsumeRate-20) > 1 {
		t.Errorf("rates = produce %f consume %f, want ~50 / ~20", r.ProduceRate, r.ConsumeRate)
	}
}

func TestTrend_FlatLagIsStalled(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 30, consume: 30, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.Stalled {
		t.Fatalf("expected stalled for flat lag: %+v", r)
	}
}

func TestTrend_FastDrainIsHealthy(t *testing.T) {
	// 3000 lag falling at 50/s: ~60s to drain, SLO is 300s.
	b := series{duration: 2 * time.Minute, initialLag: 9000, produce: 10, consume: 60, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.Stalled {
		t.Fatalf("expected healthy for fast drain: %+v", r)
	}
	if r.DrainETA > 300 {
		t.Errorf("drain ETA = %f, want < 300", r.DrainETA)
	}
}

func TestTrend_SlowDrainIsStalled(t *testing.T) {
	// Lag ~100k falling at ~10/s: drains in hours, way past the 300s SLO.
	b := series{duration: 2 * time.Minute, initialLag: 100_000, produce: 40, consume: 50, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.LagSlope >= 0 {
		t.Fatalf("lag slope = %f, want negative", r.LagSlope)
	}
	if !r.Stalled {
		t.Fatalf("expected stalled when drain ETA %f exceeds SLO", r.DrainETA)
	}
}

func TestTrend_SmallFlatLagBelowMinLagIsIgnored(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 5, produce: 10, consume: 10, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.Stalled || r.Active {
		t.Fatalf("lag below minLag must be neither stalled nor active: %+v", r)
	}
}

func TestTrend_LearnsCapacityWhenSaturated(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 100, consume: 40, consumers: 4, partitions: 32}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if math.Abs(r.State.Rate-10) > 0.5 {
		t.Fatalf("capacity = %f, want ~10 msgs/s per consumer", r.State.Rate)
	}
	// need = (P + L/maxDrain) / cap = (100 + ~9200/300) / 10 ≈ 13.1 -> 14
	if r.Desired < 13 || r.Desired > 14 {
		t.Errorf("desired = %d, want ~14", r.Desired)
	}
}

func TestTrend_NoLearningWhenNotSaturated(t *testing.T) {
	// Lag drops to zero inside the window: consumers were idle part of the
	// time, so consume rate isn't their capacity.
	b := series{duration: 2 * time.Minute, initialLag: 500, produce: 10, consume: 30, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.State.Rate != 0 {
		t.Errorf("capacity = %f, want 0 (not learned)", r.State.Rate)
	}
}

func TestTrend_DoesNotRelearnSameWindow(t *testing.T) {
	p := defaultParams()
	p.CapacitySmoothing = 0.5
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 100, consume: 40, consumers: 4}.build()
	prev := State{Rate: 20, LearnedAt: time.Time{}}

	r1 := EvaluateTrend(b, p, prev)
	if math.Abs(r1.State.Rate-15) > 0.5 {
		t.Fatalf("after first learn capacity = %f, want ~15 (EWMA of 20 and 10)", r1.State.Rate)
	}
	r2 := EvaluateTrend(b, p, r1.State)
	if r2.State.Rate != r1.State.Rate {
		t.Errorf("re-evaluating the same window changed capacity: %f -> %f", r1.State.Rate, r2.State.Rate)
	}
}

func TestTrend_ColdStartStalledStepsUpOne(t *testing.T) {
	// Consumers are stuck (committing nothing), so there's no throughput to
	// learn capacity from. Stalled with unknown capacity -> step up by one.
	b := series{duration: 2 * time.Minute, initialLag: 1000, produce: 50, consume: 0, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.Stalled {
		t.Fatalf("expected stalled: %+v", r)
	}
	if r.State.Rate != 0 {
		t.Fatalf("did not expect capacity to be learned: %f", r.State.Rate)
	}
	if r.Desired != 3 {
		t.Errorf("desired = %d, want n+1 = 3", r.Desired)
	}
}

// burst returns quiet batches followed by a sudden jump of size jump that one
// consumer then drains at drain msgs/sec, for `after` seconds post-burst.
func burst(quiet, after time.Duration, jump int64, drain float64) []Batch {
	start := time.Unix(1_000_000, 0)
	var out []Batch
	var end, committed int64 = 5000, 5000
	add := func(ts time.Time) {
		out = append(out, Batch{Timestamp: ts, TotalLag: end - committed, TotalCommitted: committed, TotalEnd: end, Partitions: 6, Consumers: 1})
	}
	ts := start
	for ; ts.Sub(start) <= quiet; ts = ts.Add(10 * time.Second) {
		add(ts)
	}
	end += jump
	for i := time.Duration(0); i <= after; i += 10 * time.Second {
		committed = min(committed+int64(drain*10), end)
		add(ts)
		ts = ts.Add(10 * time.Second)
	}
	return out
}

func TestTrend_BurstJumpIsNotARisingTrend(t *testing.T) {
	// 1000-msg burst drained at 10/s (100s) with a 300s SLO: never stalled,
	// even while the jump is still inside the trend window.
	for _, after := range []time.Duration{0, 30 * time.Second, 60 * time.Second, 90 * time.Second} {
		b := burst(2*time.Minute, after, 1000, 10)
		r := EvaluateTrend(b, defaultParams(), State{})
		if r.Stalled {
			t.Errorf("%s after burst: stalled, want healthy (slope=%f lag=%d)", after, r.LagSlope, r.Lag)
		}
		if r.Desired != 1 {
			t.Errorf("%s after burst: desired = %d, want 1", after, r.Desired)
		}
	}
}

func TestTrend_HugeBurstStallsOnceEpisodeIsJudged(t *testing.T) {
	// 8000-msg burst drained at 10/s = 800s, well past the 300s SLO.
	b := burst(time.Minute, 2*time.Minute, 8000, 10)
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.Stalled {
		t.Fatalf("expected stalled (drain ETA %f > 300): %+v", r.DrainETA, r)
	}
	// Saturated the whole episode, so capacity is learned: ~10/s.
	// need = (0 + ~6800/300) / 10 ≈ 2.3 -> 3
	if r.Desired != 3 {
		t.Errorf("desired = %d, want 3", r.Desired)
	}
}

func TestTrend_StalledNeverBelowNPlusOne(t *testing.T) {
	// Learned capacity is generous, so the formula alone would say 1, but
	// we're stalled, so we must step up anyway.
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 30, consume: 30, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{Rate: 1000, LearnedAt: time.Unix(2_000_000, 0)})
	if !r.Stalled {
		t.Fatal("expected stalled")
	}
	if r.Desired != 3 {
		t.Errorf("desired = %d, want 3", r.Desired)
	}
}

func TestTrend_HealthyHoldsAfterScaleUp(t *testing.T) {
	// After a scale-up: 8 consumers draining fast, produce 70/s, capacity 10/s.
	// need = 70/10 + small backlog term ≈ 8 -> hold, don't flap down to 1.
	b := series{duration: 2 * time.Minute, initialLag: 3000, produce: 70, consume: 80, consumers: 8}.build()
	r := EvaluateTrend(b, defaultParams(), State{Rate: 10, LearnedAt: time.Unix(2_000_000, 0)})
	if r.Stalled {
		t.Fatalf("expected healthy: %+v", r)
	}
	if r.Desired != 8 {
		t.Errorf("desired = %d, want hold at 8", r.Desired)
	}
}

func TestTrend_HealthyScalesDownWhenLoadDrops(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 0, produce: 20, consume: 20, consumers: 8}.build()
	r := EvaluateTrend(b, defaultParams(), State{Rate: 10, LearnedAt: time.Unix(2_000_000, 0)})
	if r.Desired != 2 {
		t.Errorf("desired = %d, want 2 (20 msg/s / 10 per consumer)", r.Desired)
	}
}

func TestTrend_HealthyNeverScalesUp(t *testing.T) {
	// Capacity estimate is stale and low, but lag drains within SLO, so we
	// must not add consumers.
	b := series{duration: 2 * time.Minute, initialLag: 9000, produce: 10, consume: 60, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{Rate: 1, LearnedAt: time.Unix(2_000_000, 0)})
	if r.Stalled {
		t.Fatal("expected healthy")
	}
	if r.Desired > 2 {
		t.Errorf("desired = %d, must not exceed current 2 when healthy", r.Desired)
	}
}

func TestTrend_ClampedToPartitions(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 50_000, produce: 500, consume: 20, consumers: 2, partitions: 6}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.Desired != 6 {
		t.Errorf("desired = %d, want clamp to 6 partitions", r.Desired)
	}
}

func TestTrend_ClampedToMaxConsumers(t *testing.T) {
	p := defaultParams()
	p.MaxConsumers = 4
	b := series{duration: 2 * time.Minute, initialLag: 50_000, produce: 500, consume: 20, consumers: 2}.build()
	r := EvaluateTrend(b, p, State{})
	if r.Desired != 4 {
		t.Errorf("desired = %d, want clamp to maxConsumers 4", r.Desired)
	}
}

func TestTrend_PerConsumerRateOverridesLearning(t *testing.T) {
	p := defaultParams()
	p.PerConsumerRate = 25
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 100, consume: 40, consumers: 4}.build()
	r := EvaluateTrend(b, p, State{})
	if r.State.Rate != 25 {
		t.Errorf("capacity = %f, want configured 25", r.State.Rate)
	}
}

func TestTrend_HeadroomScalesNeed(t *testing.T) {
	p := defaultParams()
	p.Headroom = 2
	b := series{duration: 2 * time.Minute, initialLag: 0, produce: 20, consume: 20, consumers: 8}.build()
	r := EvaluateTrend(b, p, State{Rate: 10, LearnedAt: time.Unix(2_000_000, 0)})
	if r.Desired != 4 {
		t.Errorf("desired = %d, want 4 (2 x 20/10)", r.Desired)
	}
}

func TestTrend_ConsumerCountChangeRestartsWarmup(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 50, consume: 20, consumers: 2}.build()
	// Last two batches after a rebalance to 3 consumers.
	b[len(b)-1].Consumers = 3
	b[len(b)-2].Consumers = 3
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.WarmingUp {
		t.Fatalf("expected warm-up after consumer count change: %+v", r)
	}
	if r.Desired != 3 {
		t.Errorf("desired = %d, want hold at 3", r.Desired)
	}
}

func TestTrend_PartitionChangeRestartsWarmup(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 50, consume: 20, consumers: 2}.build()
	b[len(b)-1].Partitions = 20
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.WarmingUp {
		t.Fatalf("expected warm-up after partition change: %+v", r)
	}
}

func TestTrend_OffsetResetSkipsLearning(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 100, consume: 40, consumers: 4}.build()
	// Consumer group offsets rewound mid-window.
	for i := len(b) / 2; i < len(b); i++ {
		b[i].TotalCommitted -= 500
		b[i].TotalLag += 500
	}
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.State.Rate != 0 {
		t.Errorf("capacity = %f, want 0 after offset reset", r.State.Rate)
	}
}

func TestTrend_OnlyUsesTrendWindow(t *testing.T) {
	// 4 minutes of history: first 2 rising hard, last 2 draining fast. Only the
	// last 2 minutes should count.
	rising := series{duration: 2 * time.Minute, initialLag: 1000, produce: 100, consume: 10, consumers: 2}.build()
	last := rising[len(rising)-1]
	var draining []Batch
	for i := 1; i <= 12; i++ {
		t := float64(i * 10)
		committed := last.TotalCommitted + int64(110*t)
		end := last.TotalEnd + int64(10*t)
		draining = append(draining, Batch{
			Timestamp:      last.Timestamp.Add(time.Duration(i) * 10 * time.Second),
			TotalLag:       end - committed,
			TotalCommitted: committed,
			TotalEnd:       end,
			Partitions:     last.Partitions,
			Consumers:      last.Consumers,
		})
	}
	r := EvaluateTrend(append(rising, draining...), defaultParams(), State{})
	if r.LagSlope >= 0 {
		t.Errorf("lag slope = %f, want negative (only recent window)", r.LagSlope)
	}
}

func TestTrend_NoisyRisingStillStalled(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 2000, produce: 40, consume: 30, consumers: 2}.build()
	// Jitter the lag up and down by +-300 around a rising trend.
	for i := range b {
		if i%2 == 0 {
			b[i].TotalLag += 300
		} else {
			b[i].TotalLag -= 300
		}
	}
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.Stalled {
		t.Errorf("expected stalled despite noise: slope=%f", r.LagSlope)
	}
}

func TestTrend_Unsorted(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 1000, produce: 50, consume: 20, consumers: 2}.build()
	b[0], b[len(b)-1] = b[len(b)-1], b[0]
	r := EvaluateTrend(b, defaultParams(), State{})
	if !r.Stalled {
		t.Errorf("expected stalled regardless of input order: %+v", r)
	}
}

func TestLinearSlope(t *testing.T) {
	if s := linearSlope([]float64{0, 1, 2, 3}, []float64{1, 3, 5, 7}); math.Abs(s-2) > 1e-9 {
		t.Errorf("slope = %f, want 2", s)
	}
	if s := linearSlope([]float64{0}, []float64{5}); s != 0 {
		t.Errorf("single point slope = %f, want 0", s)
	}
	if s := linearSlope([]float64{1, 1}, []float64{1, 5}); s != 0 {
		t.Errorf("vertical slope = %f, want 0", s)
	}
}

func TestNewBatch(t *testing.T) {
	now := time.Now()
	b := NewBatch([]LagSample{
		{Timestamp: now, Partition: 0, Lag: 10, Offset: 90, EndOffset: 100},
		{Timestamp: now, Partition: 1, Lag: 20, Offset: 180, EndOffset: 200},
	}, 3)
	if b.TotalLag != 30 || b.TotalCommitted != 270 || b.TotalEnd != 300 || b.Partitions != 2 || b.Consumers != 3 || !b.Timestamp.Equal(now) {
		t.Errorf("unexpected batch: %+v", b)
	}
}

func TestTrend_IdleScalesDownWithoutLearnedCapacity(t *testing.T) {
	// No traffic at all and nothing learned (e.g. right after startup, or a
	// rollout surge inflated the group): must drop to minConsumers, not hold.
	b := series{duration: 2 * time.Minute, initialLag: 0, produce: 0, consume: 0, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.Desired != 1 {
		t.Errorf("desired = %d, want 1 (minConsumers) when idle", r.Desired)
	}
}

func TestTrend_IdleIgnoresSubMinLagResidue(t *testing.T) {
	// A few messages of lag below minLag that consumers aren't committing
	// must not keep the group pinned at its current size.
	b := series{duration: 2 * time.Minute, initialLag: 5, produce: 0, consume: 0, consumers: 4}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.Desired != 1 {
		t.Errorf("desired = %d, want 1", r.Desired)
	}
}

func TestTrend_UnlearnedCapacityUsesObservedRateConservatively(t *testing.T) {
	// 4 consumers keeping up with 20 msg/s, lag ~0, nothing learned. Observed
	// C/n = 5 is only a lower bound on capacity, so need = 1.0*20/5 = 4: hold,
	// never scale below what the observed rate justifies.
	b := series{duration: 2 * time.Minute, initialLag: 0, produce: 20, consume: 20, consumers: 4}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.Desired != 4 {
		t.Errorf("desired = %d, want hold at 4", r.Desired)
	}
}

func TestTrend_WarmUpHoldsLastDecisionNotGroupSize(t *testing.T) {
	// A rollout surge briefly shows 3 members; the last real decision was 1.
	// Warm-up must keep reporting 1, or HPA ratchets up to the surge size.
	b := series{duration: 20 * time.Second, initialLag: 0, produce: 0, consume: 0, consumers: 3}.build()
	r := EvaluateTrend(b, defaultParams(), State{LastDesired: 1})
	if !r.WarmingUp {
		t.Fatal("expected warm-up")
	}
	if r.Desired != 1 {
		t.Errorf("desired = %d, want last decision 1", r.Desired)
	}
	if r.State.LastDesired != 1 {
		t.Errorf("warm-up must not overwrite LastDesired, got %d", r.State.LastDesired)
	}
}

func TestTrend_WarmUpAfterScaleUpKeepsTarget(t *testing.T) {
	// Decided 7, only 3 consumers have joined so far: keep asking for 7.
	b := series{duration: 20 * time.Second, initialLag: 5000, produce: 50, consume: 30, consumers: 3}.build()
	r := EvaluateTrend(b, defaultParams(), State{Rate: 10, LastDesired: 7})
	if r.Desired != 7 {
		t.Errorf("desired = %d, want 7", r.Desired)
	}
}

func TestTrend_DecisionRecordedInState(t *testing.T) {
	b := series{duration: 2 * time.Minute, initialLag: 1000, produce: 50, consume: 0, consumers: 2}.build()
	r := EvaluateTrend(b, defaultParams(), State{})
	if r.State.LastDesired != r.Desired || r.Desired != 3 {
		t.Errorf("LastDesired = %d, Desired = %d, want both 3", r.State.LastDesired, r.Desired)
	}
}
