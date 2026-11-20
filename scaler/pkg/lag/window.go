package lag

import (
	"sync"
	"time"
)

// SlidingWindow keeps the batches scraped within the last `duration`.
type SlidingWindow struct {
	mu       sync.RWMutex
	batches  []Batch
	duration time.Duration
}

func NewSlidingWindow(duration time.Duration) *SlidingWindow {
	return &SlidingWindow{duration: duration}
}

func (w *SlidingWindow) Add(batches ...Batch) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.batches = append(w.batches, batches...)
	w.evict()
}

func (w *SlidingWindow) Snapshot() []Batch {
	w.mu.RLock()
	defer w.mu.RUnlock()

	out := make([]Batch, len(w.batches))
	copy(out, w.batches)
	return out
}

func (w *SlidingWindow) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.batches)
}

func (w *SlidingWindow) evict() {
	cutoff := time.Now().Add(-w.duration)
	i := 0
	for i < len(w.batches) && w.batches[i].Timestamp.Before(cutoff) {
		i++
	}
	if i > 0 {
		w.batches = w.batches[i:]
	}
}
