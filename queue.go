package dre

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Queue is a deduplicating work queue with per-item delayed retries.
//
// Contract:
//   - Add while an item is queued or in-flight only sets the dirty flag, so a
//     burst of concurrent submissions collapses into at least one reconcile
//     after the LAST submission (no event is lost).
//   - Get hands an item to a worker; Done must always follow. Done re-queues
//     immediately if the item went dirty while in flight.
//   - Delayed items become ready no earlier than their scheduled time; they
//     are still coalesced with any concurrent Add.
//   - Shutdown stops admission; Get keeps returning the remaining ready items
//     (graceful drain) and then reports ok=false.
type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	ready    []queueEntry
	waiting  []queueEntry
	dirty    map[ResourceID]bool
	inflight map[ResourceID]bool
	closed   bool
	seq      int64
}

type queueEntry struct {
	id      ResourceID
	readyAt time.Time
	seq     int64
}

// NewQueue constructs an empty work queue.
func NewQueue() *Queue {
	q := &Queue{
		dirty:    map[ResourceID]bool{},
		inflight: map[ResourceID]bool{},
	}
	q.cond = &sync.Cond{L: &q.mu}
	return q
}

func (q *Queue) tracked(id ResourceID) bool {
	for _, e := range q.ready {
		if e.id == id {
			return true
		}
	}
	for _, e := range q.waiting {
		if e.id == id {
			return true
		}
	}
	return q.inflight[id]
}

// Add enqueues id for immediate processing.
func (q *Queue) Add(id ResourceID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if q.tracked(id) {
		q.dirty[id] = true
		return
	}
	q.seq++
	q.ready = append(q.ready, queueEntry{id: id, seq: q.seq})
	q.cond.Broadcast()
}

// AddAfter enqueues id after delay (used for retry/backoff). It is normally
// called while id is in-flight (before Done); the future-due entry is parked
// and honored by Done, preserving the delay instead of busy-retrying.
func (q *Queue) AddAfter(id ResourceID, delay time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if delay <= 0 {
		if q.tracked(id) {
			q.dirty[id] = true
		} else {
			q.seq++
			q.ready = append(q.ready, queueEntry{id: id, seq: q.seq})
		}
		q.cond.Broadcast()
		return
	}
	// Park a future-due entry even while in-flight; Done will let it stand.
	q.seq++
	q.waiting = append(q.waiting, queueEntry{
		id: id, readyAt: time.Now().Add(delay), seq: q.seq,
	})
	sort.Slice(q.waiting, func(i, j int) bool { return q.waiting[i].readyAt.Before(q.waiting[j].readyAt) })
	q.cond.Broadcast()
}

// Get blocks until an item is ready; ok=false means shutdown + full drain.
// A context cancellation also returns ok=false WITHOUT consuming the item.
func (q *Queue) Get(ctx context.Context) (ResourceID, bool) {
	done := make(chan struct{})
	defer close(done)
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return "", false
		}
		if len(q.ready) > 0 {
			e := q.ready[0]
			q.ready = q.ready[1:]
			q.inflight[e.id] = true
			delete(q.dirty, e.id)
			return e.id, true
		}
		if len(q.waiting) > 0 {
			now := time.Now()
			next := q.waiting[0].readyAt
			if !next.After(now) {
				e := q.waiting[0]
				q.waiting = q.waiting[1:]
				q.inflight[e.id] = true
				delete(q.dirty, e.id)
				return e.id, true
			}
			// Wake when the earliest delayed item becomes due (or sooner on
			// Add/ctx cancellation/Shutdown broadcasts).
			wake := time.AfterFunc(next.Sub(now), func() {
				q.mu.Lock()
				q.cond.Broadcast()
				q.mu.Unlock()
			})
			q.cond.Wait()
			wake.Stop()
			continue
		}
		if q.closed {
			return "", false
		}
		q.cond.Wait()
	}
}

// Done marks an in-flight item complete.
//   - If an immediate Add happened during flight, the item is re-queued at
//     once and any parked delayed retry for it is discarded (latest signal
//     wins).
//   - Otherwise a parked AddAfter entry stands: the item stays out of the
//     in-flight set but does NOT become ready until its due time, so backoff
//     delays are actually honored.
func (q *Queue) Done(id ResourceID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inflight, id)
	if q.dirty[id] {
		delete(q.dirty, id)
		// Discard parked delayed retries superseded by an immediate signal.
		kept := q.waiting[:0]
		for _, e := range q.waiting {
			if e.id != id {
				kept = append(kept, e)
			}
		}
		q.waiting = kept
		if !q.closed {
			q.seq++
			q.ready = append(q.ready, queueEntry{id: id, seq: q.seq})
		}
	}
	q.cond.Broadcast()
}

// Shutdown prevents new items and unblocks waiters. Items already queued
// remain available to Get for graceful draining.
func (q *Queue) Shutdown() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast()
}

// RemovePending drops all not-yet-in-flight entries for id (ready and
// delayed) and clears its dirty flag. In-flight items are unaffected and
// must be guarded separately by generation tickets. Used when a newer
// admitted generation obsoletes queued old passes (e.g. create after purge).
func (q *Queue) RemovePending(id ResourceID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	keptR := q.ready[:0]
	for _, e := range q.ready {
		if e.id != id {
			keptR = append(keptR, e)
		}
	}
	q.ready = keptR
	keptW := q.waiting[:0]
	for _, e := range q.waiting {
		if e.id != id {
			keptW = append(keptW, e)
		}
	}
	q.waiting = keptW
	delete(q.dirty, id)
}

// Len reports the number of tracked (queued + in-flight) items.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ready) + len(q.waiting) + len(q.inflight)
}
