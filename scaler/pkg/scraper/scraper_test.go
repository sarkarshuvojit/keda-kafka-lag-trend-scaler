package scraper

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sarkarshuvojit/keda-kafka-lag-trend-scaler/scaler/pkg/lag"
)

type fakeFetcher struct {
	mu        sync.Mutex
	samples   []lag.LagSample
	lagErr    error
	consumers int
	groupErr  error
	calls     int
}

func (f *fakeFetcher) FetchLag(ctx context.Context) ([]lag.LagSample, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.lagErr != nil {
		return nil, f.lagErr
	}
	// Stamp with now so the window doesn't evict them.
	out := make([]lag.LagSample, len(f.samples))
	now := time.Now()
	for i, s := range f.samples {
		s.Timestamp = now
		out[i] = s
	}
	return out, nil
}

func (f *fakeFetcher) FetchConsumerGroupSize(ctx context.Context) (int, error) {
	return f.consumers, f.groupErr
}

func (f *fakeFetcher) Topic() string         { return "orders" }
func (f *fakeFetcher) ConsumerGroup() string { return "order-workers" }

func (f *fakeFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func twoPartitions() []lag.LagSample {
	return []lag.LagSample{
		{Topic: "orders", Partition: 0, Lag: 10, Offset: 90, EndOffset: 100},
		{Topic: "orders", Partition: 1, Lag: 5, Offset: 195, EndOffset: 200},
	}
}

func TestFetch_AddsOneBatchPerScrape(t *testing.T) {
	f := &fakeFetcher{samples: twoPartitions(), consumers: 3}
	w := lag.NewSlidingWindow(time.Minute)
	s := New(f, w, time.Second)

	s.fetch(context.Background())

	got := w.Snapshot()
	if len(got) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(got))
	}
	b := got[0]
	if b.TotalLag != 15 || b.TotalCommitted != 285 || b.TotalEnd != 300 || b.Partitions != 2 || b.Consumers != 3 {
		t.Errorf("unexpected batch: %+v", b)
	}
}

func TestFetch_LagErrorSkipsBatch(t *testing.T) {
	f := &fakeFetcher{lagErr: errors.New("broker down"), consumers: 1}
	w := lag.NewSlidingWindow(time.Minute)
	New(f, w, time.Second).fetch(context.Background())
	if w.Len() != 0 {
		t.Errorf("expected no batch when lag fetch fails, got %d", w.Len())
	}
}

func TestFetch_GroupErrorSkipsBatch(t *testing.T) {
	// A batch without a consumer count can't be segmented, so it's dropped.
	f := &fakeFetcher{samples: twoPartitions(), groupErr: errors.New("coordinator moved")}
	w := lag.NewSlidingWindow(time.Minute)
	New(f, w, time.Second).fetch(context.Background())
	if w.Len() != 0 {
		t.Errorf("expected no batch when group size fetch fails, got %d", w.Len())
	}
}

func TestRun_ScrapesImmediatelyAndOnTickUntilCancelled(t *testing.T) {
	f := &fakeFetcher{samples: twoPartitions(), consumers: 1}
	w := lag.NewSlidingWindow(time.Minute)
	s := New(f, w, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for f.callCount() < 3 {
		select {
		case <-deadline:
			t.Fatalf("expected at least 3 scrapes, got %d", f.callCount())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
	if w.Len() < 3 {
		t.Errorf("expected at least 3 batches, got %d", w.Len())
	}
}
