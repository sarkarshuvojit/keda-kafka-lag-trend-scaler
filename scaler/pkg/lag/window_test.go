package lag

import (
	"sync"
	"testing"
	"time"
)

func TestSlidingWindow_AddAndSnapshot(t *testing.T) {
	w := NewSlidingWindow(10 * time.Second)

	now := time.Now()
	w.Add(Batch{Timestamp: now, TotalLag: 100})
	w.Add(Batch{Timestamp: now.Add(time.Second), TotalLag: 200})

	snap := w.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(snap))
	}
	if snap[0].TotalLag != 100 || snap[1].TotalLag != 200 {
		t.Errorf("unexpected values: %+v", snap)
	}
}

func TestSlidingWindow_EvictsOldBatches(t *testing.T) {
	w := NewSlidingWindow(5 * time.Second)

	now := time.Now()
	w.Add(
		Batch{Timestamp: now.Add(-10 * time.Second), TotalLag: 100},
		Batch{Timestamp: now.Add(-8 * time.Second), TotalLag: 200},
	)
	if w.Len() != 0 {
		t.Fatalf("expected 0 batches after eviction, got %d", w.Len())
	}

	w.Add(
		Batch{Timestamp: now.Add(-10 * time.Second), TotalLag: 100},
		Batch{Timestamp: now.Add(-3 * time.Second), TotalLag: 200},
		Batch{Timestamp: now, TotalLag: 300},
	)
	snap := w.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("expected 2 batches, got %d", len(snap))
	}
	if snap[0].TotalLag != 200 || snap[1].TotalLag != 300 {
		t.Errorf("unexpected surviving batches: %+v", snap)
	}
}

func TestSlidingWindow_SnapshotIsACopy(t *testing.T) {
	w := NewSlidingWindow(10 * time.Second)
	w.Add(Batch{Timestamp: time.Now(), TotalLag: 100})

	snap := w.Snapshot()
	snap[0].TotalLag = 999

	if w.Snapshot()[0].TotalLag != 100 {
		t.Error("Snapshot should return a copy")
	}
}

func TestSlidingWindow_ConcurrentAccess(t *testing.T) {
	w := NewSlidingWindow(time.Minute)

	now := time.Now()
	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range 100 {
				w.Add(Batch{Timestamp: now.Add(time.Duration(j) * time.Millisecond), TotalLag: int64(id)})
			}
		}(i)
	}
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = w.Snapshot()
				_ = w.Len()
			}
		}()
	}
	wg.Wait()

	if w.Len() != 1000 {
		t.Errorf("expected 1000 batches, got %d", w.Len())
	}
}
