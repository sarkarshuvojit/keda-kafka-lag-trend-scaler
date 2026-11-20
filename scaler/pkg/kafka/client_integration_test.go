//go:build integration

// Integration tests for LagFetcher against a real broker. They run with
//
//	KAFKA_BROKERS=localhost:9092 go test -tags integration ./pkg/kafka/
//
// CI starts apache/kafka as a service container for this.
package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

const partitions = 3

func brokers(t *testing.T) string {
	t.Helper()
	b := os.Getenv("KAFKA_BROKERS")
	if b == "" {
		t.Skip("KAFKA_BROKERS not set")
	}
	return b
}

// newTopic creates a uniquely named topic with `partitions` partitions and
// produces n messages spread round-robin across them.
func newTopic(t *testing.T, ctx context.Context, addr string, n int) string {
	t.Helper()
	topic := fmt.Sprintf("it-%s-%d", t.Name(), time.Now().UnixNano())

	client := &kafka.Client{Addr: kafka.TCP(addr)}
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := resp.Errors[topic]; err != nil {
		t.Fatalf("create topic: %v", err)
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(addr),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1,
	}
	defer w.Close()
	msgs := make([]kafka.Message, n)
	for i := range msgs {
		msgs[i] = kafka.Message{Value: []byte(fmt.Sprintf("m-%d", i))}
	}
	// Leadership for a brand-new topic can take a moment to settle.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err = w.WriteMessages(ctx, msgs...)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	return topic
}

// consumeAndCommit reads and synchronously commits n messages as group, and
// returns the still-open reader so the caller controls group membership.
func consumeAndCommit(t *testing.T, ctx context.Context, addr, topic, group string, n int) *kafka.Reader {
	t.Helper()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{addr},
		Topic:       topic,
		GroupID:     group,
		StartOffset: kafka.FirstOffset,
		MaxWait:     200 * time.Millisecond,
	})
	for range n {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			r.Close()
			t.Fatalf("fetch: %v", err)
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			r.Close()
			t.Fatalf("commit: %v", err)
		}
	}
	return r
}

func TestFetchLag_CommittedGroup(t *testing.T) {
	addr := brokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const produced, consumed = 30, 12
	topic := newTopic(t, ctx, addr, produced)
	group := topic + "-g"
	r := consumeAndCommit(t, ctx, addr, topic, group, consumed)
	defer r.Close()

	f := NewLagFetcher(addr, topic, group)
	if f.Topic() != topic || f.ConsumerGroup() != group {
		t.Fatalf("accessors = %q/%q", f.Topic(), f.ConsumerGroup())
	}

	samples, err := f.FetchLag(ctx)
	if err != nil {
		t.Fatalf("FetchLag: %v", err)
	}
	if len(samples) != partitions {
		t.Fatalf("got %d samples, want %d", len(samples), partitions)
	}
	var lagSum, committed, end int64
	seen := map[int]bool{}
	for _, s := range samples {
		if s.Topic != topic {
			t.Errorf("sample topic = %q", s.Topic)
		}
		if s.Lag != s.EndOffset-s.Offset {
			t.Errorf("partition %d: lag %d != end %d - committed %d", s.Partition, s.Lag, s.EndOffset, s.Offset)
		}
		seen[s.Partition] = true
		lagSum += s.Lag
		committed += s.Offset
		end += s.EndOffset
	}
	if len(seen) != partitions {
		t.Errorf("partitions seen = %v", seen)
	}
	if end != produced {
		t.Errorf("sum of end offsets = %d, want %d", end, produced)
	}
	if committed != consumed {
		t.Errorf("sum of committed offsets = %d, want %d", committed, consumed)
	}
	if lagSum != produced-consumed {
		t.Errorf("total lag = %d, want %d", lagSum, produced-consumed)
	}

	size, err := f.FetchConsumerGroupSize(ctx)
	if err != nil {
		t.Fatalf("FetchConsumerGroupSize: %v", err)
	}
	if size != 1 {
		t.Errorf("group size = %d, want 1 (reader still open)", size)
	}
}

func TestFetchLag_GroupWithNoCommitsCountsEverythingAsLag(t *testing.T) {
	addr := brokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const produced = 9
	topic := newTopic(t, ctx, addr, produced)
	f := NewLagFetcher(addr, topic, topic+"-never-consumed")

	samples, err := f.FetchLag(ctx)
	if err != nil {
		t.Fatalf("FetchLag: %v", err)
	}
	var lagSum int64
	for _, s := range samples {
		if s.Offset != 0 {
			t.Errorf("partition %d committed = %d, want 0 (no commits clamps -1 to 0)", s.Partition, s.Offset)
		}
		lagSum += s.Lag
	}
	if lagSum != produced {
		t.Errorf("total lag = %d, want %d", lagSum, produced)
	}

	size, err := f.FetchConsumerGroupSize(ctx)
	if err != nil {
		t.Fatalf("FetchConsumerGroupSize: %v", err)
	}
	if size != 0 {
		t.Errorf("group size = %d, want 0", size)
	}
}

func TestFetchLag_UnknownTopic(t *testing.T) {
	addr := brokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	f := NewLagFetcher(addr, fmt.Sprintf("it-missing-%d", time.Now().UnixNano()), "g")
	if _, err := f.FetchLag(ctx); err == nil {
		t.Fatal("expected an error for a topic that doesn't exist")
	}
}

func TestFetch_UnreachableBroker(t *testing.T) {
	brokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	f := NewLagFetcher("127.0.0.1:1", "t", "g")
	if _, err := f.FetchLag(ctx); err == nil {
		t.Error("FetchLag: expected error for unreachable broker")
	}
	if _, err := f.FetchConsumerGroupSize(ctx); err == nil {
		t.Error("FetchConsumerGroupSize: expected error for unreachable broker")
	}
}
