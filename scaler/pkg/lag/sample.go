package lag

import "time"

// LagSample is the lag of a single partition at one scrape.
type LagSample struct {
	Timestamp time.Time
	Topic     string
	Partition int
	Lag       int64
	Offset    int64
	EndOffset int64
}

// Batch is one scrape of a topic, summed across all its partitions. The trend
// evaluator works on batches, not on per-partition samples.
type Batch struct {
	Timestamp time.Time
	// TotalLag is the sum of per-partition lag.
	TotalLag int64
	// TotalCommitted is the sum of committed consumer-group offsets. Its slope
	// over time is the consume rate.
	TotalCommitted int64
	// TotalEnd is the sum of partition end offsets (high-water marks). Its
	// slope over time is the produce rate.
	TotalEnd int64
	// Partitions is the number of partitions in the topic at this scrape.
	Partitions int
	// Consumers is the number of members in the consumer group at this scrape.
	Consumers int
}

// NewBatch sums one scrape's per-partition samples into a Batch. The batch
// takes the timestamp of the first sample (all samples in a scrape share it).
func NewBatch(samples []LagSample, consumers int) Batch {
	b := Batch{Partitions: len(samples), Consumers: consumers}
	for i, s := range samples {
		if i == 0 {
			b.Timestamp = s.Timestamp
		}
		b.TotalLag += s.Lag
		b.TotalCommitted += s.Offset
		b.TotalEnd += s.EndOffset
	}
	return b
}
