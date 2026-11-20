package lag

import (
	"math"
	"sort"
	"time"
)

// TrendParams configures EvaluateTrend.
type TrendParams struct {
	// MinLag: lag below this is ignored. It never counts as stalled, and the
	// scaler reports inactive.
	MinLag int64
	// MaxDrain is the SLO: the backlog must be projected to drain within this.
	MaxDrain time.Duration
	// TrendWindow is how much history the slope and rate fits use.
	TrendWindow time.Duration
	// Headroom multiplies the throughput we need before turning it into a
	// replica count.
	Headroom float64
	// PerConsumerRate (msgs/sec), when > 0, is used as the per-consumer
	// capacity instead of learning it.
	PerConsumerRate float64
	// CapacitySmoothing is the EWMA alpha for learned capacity, in (0, 1].
	CapacitySmoothing float64
	// MinConsumers and MaxConsumers clamp the desired count. MaxConsumers <= 0
	// means "partition count"; it is also never allowed above the partition
	// count, since extra consumers would sit idle.
	MinConsumers int
	MaxConsumers int
	// MinSamples is the minimum number of batches needed before deciding.
	MinSamples int
}

// State is carried between evaluations by the caller.
type State struct {
	// Rate is msgs/sec one consumer processes when saturated. 0 = unknown.
	Rate float64
	// LearnedAt is the timestamp of the newest batch folded into Rate, so the
	// same window isn't learned twice when KEDA polls several times per scrape.
	LearnedAt time.Time
	// LastDesired is the last count decided from a full trend window (0 =
	// none yet). Warm-up keeps reporting it, so transient group membership
	// (rollout surges, a scale-up still joining) can't ratchet the count.
	LastDesired int64
}

// TrendResult is the outcome of one evaluation.
type TrendResult struct {
	// WarmingUp is true when there isn't enough stable history to judge the
	// trend yet (cold start, or right after the consumer or partition count
	// changed). Desired then holds at the last decision, or the current
	// consumer count if there is none.
	WarmingUp bool
	// Active: lag is at or above MinLag. Used for KEDA activation.
	Active bool
	// Stalled: consumers can't drain the backlog within MaxDrain.
	Stalled bool

	Lag         int64
	LagSlope    float64 // msgs/sec; negative means draining
	ProduceRate float64 // msgs/sec
	ConsumeRate float64 // msgs/sec
	// DrainETA is the projected seconds until lag hits zero. +Inf if it isn't
	// falling.
	DrainETA float64

	Consumers  int
	Partitions int
	// State is what to pass to the next evaluation: learned capacity and the
	// last decision.
	State State
	// Desired is the consumer count to report to KEDA.
	Desired int64
}

// EvaluateTrend decides whether consumers are keeping up, and how many there
// should be.
//
// Only the trailing run of batches with the same partition count and consumer
// count as the newest batch is used, so a rebalance or a partition change
// restarts the fit instead of polluting it. While lag is at or above MinLag,
// the fit is further limited to the current episode (since lag last rose
// above MinLag), so the jump from a sudden burst never reads as a rising trend.
func EvaluateTrend(batches []Batch, p TrendParams, prev State) TrendResult {
	res := TrendResult{State: prev, DrainETA: math.Inf(1)}
	if p.PerConsumerRate > 0 {
		res.State = State{Rate: p.PerConsumerRate}
	}
	if len(batches) == 0 {
		res.WarmingUp = true
		res.Desired = int64(clampConsumers(0, 0, p))
		return res
	}

	seg := stableSegment(batches, p.TrendWindow, p.MinLag)
	latest := seg[len(seg)-1]
	n := latest.Consumers

	res.Lag = latest.TotalLag
	res.Consumers = n
	res.Partitions = latest.Partitions
	res.Active = latest.TotalLag > 0 && latest.TotalLag >= p.MinLag
	if latest.TotalLag == 0 {
		res.DrainETA = 0
	}

	span := latest.Timestamp.Sub(seg[0].Timestamp)
	minSamples := max(p.MinSamples, 2)
	if len(seg) < minSamples || float64(span) < 0.8*float64(p.TrendWindow) {
		res.WarmingUp = true
		hold := n
		if prev.LastDesired > 0 {
			hold = int(prev.LastDesired)
		}
		res.Desired = int64(clampConsumers(hold, latest.Partitions, p))
		return res
	}

	xs := make([]float64, len(seg))
	lagY := make([]float64, len(seg))
	endY := make([]float64, len(seg))
	commitY := make([]float64, len(seg))
	offsetReset := false
	saturated := true
	for i, b := range seg {
		xs[i] = b.Timestamp.Sub(seg[0].Timestamp).Seconds()
		lagY[i] = float64(b.TotalLag)
		endY[i] = float64(b.TotalEnd)
		commitY[i] = float64(b.TotalCommitted)
		if i > 0 && b.TotalCommitted < seg[i-1].TotalCommitted {
			offsetReset = true
		}
		if b.TotalLag == 0 || b.TotalLag < p.MinLag {
			saturated = false
		}
	}

	res.LagSlope = linearSlope(xs, lagY)
	res.ProduceRate = math.Max(0, linearSlope(xs, endY))
	res.ConsumeRate = math.Max(0, linearSlope(xs, commitY))

	if res.Lag > 0 && res.LagSlope < 0 {
		res.DrainETA = float64(res.Lag) / -res.LagSlope
	}
	res.Stalled = res.Active && res.DrainETA > p.MaxDrain.Seconds()

	// While there was a backlog for the whole segment, consumers were busy the
	// whole time, so consume rate / consumers is their real capacity.
	if p.PerConsumerRate <= 0 && saturated && !offsetReset && n > 0 &&
		res.ConsumeRate > 0 && latest.Timestamp.After(prev.LearnedAt) {
		observed := res.ConsumeRate / float64(n)
		alpha := p.CapacitySmoothing
		if alpha <= 0 || alpha > 1 {
			alpha = 1
		}
		rate := observed
		if prev.Rate > 0 {
			rate = alpha*observed + (1-alpha)*prev.Rate
		}
		res.State = State{Rate: rate, LearnedAt: latest.Timestamp}
	}

	// Throughput needed: keep up with producers, plus clear the backlog within
	// the SLO. Lag below MinLag is noise and doesn't count as backlog.
	backlog := float64(res.Lag)
	if !res.Active {
		backlog = 0
	}
	required := res.ProduceRate + backlog/p.MaxDrain.Seconds()

	// Capacity to size with. Before anything is learned, fall back to what
	// consumers are doing right now: C/n is never above their true capacity,
	// so the need it gives is never too low (conservative, never under-scales).
	capacity := res.State.Rate
	if capacity <= 0 && n > 0 {
		capacity = res.ConsumeRate / float64(n)
	}

	need := -1 // unknown
	switch {
	case required <= 0:
		// Nothing produced and no backlog: nothing to do, whatever the capacity.
		need = 0
	case capacity > 0:
		need = int(math.Ceil(p.Headroom * required / capacity))
	}

	desired := n
	if res.Stalled {
		desired = n + 1
	}
	if need >= 0 {
		if res.Stalled && need > desired {
			// Can't keep up: jump straight to what the rates say we need.
			desired = need
		} else if !res.Stalled && need < desired {
			// Keeping up: allow scale-down, but never scale up.
			desired = need
		}
	}
	res.Desired = int64(clampConsumers(desired, latest.Partitions, p))
	res.State.LastDesired = res.Desired
	return res
}

// stableSegment returns the trailing batches, sorted by time, that fall within
// window of the newest one and share its partition and consumer counts. If the
// newest batch's lag is at or above minLag, it also stops at the last batch
// whose lag was below minLag (that batch is excluded).
func stableSegment(batches []Batch, window time.Duration, minLag int64) []Batch {
	sorted := make([]Batch, len(batches))
	copy(sorted, batches)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Timestamp.Before(sorted[j].Timestamp)
	})

	latest := sorted[len(sorted)-1]
	cutoff := latest.Timestamp.Add(-window)
	floor := max(minLag, 1)
	inEpisode := latest.TotalLag >= floor
	start := len(sorted) - 1
	for start > 0 {
		b := sorted[start-1]
		if b.Timestamp.Before(cutoff) || b.Partitions != latest.Partitions || b.Consumers != latest.Consumers {
			break
		}
		if inEpisode && b.TotalLag < floor {
			break
		}
		start--
	}
	return sorted[start:]
}

// linearSlope is the least-squares slope of ys over xs. Returns 0 when it is
// undefined (fewer than 2 points, or all xs equal).
func linearSlope(xs, ys []float64) float64 {
	n := float64(len(xs))
	if len(xs) < 2 {
		return 0
	}
	var sx, sy float64
	for i := range xs {
		sx += xs[i]
		sy += ys[i]
	}
	mx, my := sx/n, sy/n
	var num, den float64
	for i := range xs {
		dx := xs[i] - mx
		num += dx * (ys[i] - my)
		den += dx * dx
	}
	if den == 0 {
		return 0
	}
	return num / den
}

func clampConsumers(desired, partitions int, p TrendParams) int {
	upper := p.MaxConsumers
	if partitions > 0 && (upper <= 0 || upper > partitions) {
		upper = partitions
	}
	if upper > 0 && desired > upper {
		desired = upper
	}
	if desired < p.MinConsumers {
		desired = p.MinConsumers
	}
	return desired
}
