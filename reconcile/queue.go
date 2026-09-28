package reconcile

import "sync"

// Queue is a deduplicating work queue.
//
// Guarantees:
//   - the same key is queued at most once while waiting and at most once
//     while in flight;
//   - if Add(key) happens while key is being processed, the key is delivered
//     again once Done(key) is called, so no update event is lost;
//   - Close stops new Adds; buffered/in-flight keys (and keys dirtied while
//     in flight) are still drained. Once fully drained Get returns ok=false.
type Queue struct {
	mu         sync.Mutex
	cond       *sync.Cond
	pending    []string
	pendingSet map[string]bool
	in         map[string]bool
	dirty      map[string]bool
	closed     bool
	idle       chan struct{}
}

func NewQueue(_ int) *Queue {
	q := &Queue{
		pendingSet: map[string]bool{},
		in:         map[string]bool{},
		dirty:      map[string]bool{},
	}
	q.cond = sync.NewCond(&q.mu)
	q.idle = make(chan struct{})
	close(q.idle)
	return q
}

// Add schedules id for processing. Returns false when the queue is closed.
func (q *Queue) Add(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return false
	}
	if q.in[id] {
		q.dirty[id] = true
		return true
	}
	if q.pendingSet[id] {
		return true
	}
	if len(q.pending) == 0 && len(q.in) == 0 {
		q.idle = make(chan struct{})
	}
	q.pending = append(q.pending, id)
	q.pendingSet[id] = true
	q.cond.Signal()
	return true
}

// Get blocks until a key is available. ok=false only after Close and full
// drain (including dirty requeues of in-flight keys).
func (q *Queue) Get() (id string, ok bool) {
	q.mu.Lock()
	// After Close we must keep waiting as long as other keys are in flight:
	// their Done may requeue keys that were dirtied during processing.
	for len(q.pending) == 0 && (!q.closed || len(q.in) > 0) {
		q.cond.Wait()
	}
	if len(q.pending) == 0 {
		q.mu.Unlock()
		return "", false
	}
	id = q.pending[0]
	q.pending = q.pending[1:]
	delete(q.pendingSet, id)
	q.in[id] = true
	q.mu.Unlock()
	return id, true
}

// Done marks a key finished. If Add raced the processing it is requeued once;
// if that leaves the queue empty and closed, waiters are unblocked.
func (q *Queue) Done(id string) {
	q.mu.Lock()
	delete(q.in, id)
	requeue := q.dirty[id]
	delete(q.dirty, id)
	if requeue {
		// Even after Close: a key dirtied while in flight must drain once.
		// Get returns false only once it has been consumed.
		q.pending = append(q.pending, id)
		q.pendingSet[id] = true
	}
	empty := len(q.pending) == 0 && len(q.in) == 0
	if empty {
		select {
		case <-q.idle:
		default:
			close(q.idle)
		}
	}
	q.cond.Broadcast()
	q.mu.Unlock()
}

// Close prevents further Adds. Already queued / in-flight keys continue to
// be processed; Get returns ok=false only after full drain.
func (q *Queue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	empty := len(q.pending) == 0 && len(q.in) == 0
	if empty {
		select {
		case <-q.idle:
		default:
			close(q.idle)
		}
	}
	q.cond.Broadcast()
	q.mu.Unlock()
}

// Idle returns a channel closed when nothing is queued or in flight.
func (q *Queue) Idle() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.idle
}

// Len returns the number of queued (not in-flight) keys.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// InFlight returns how many keys are currently being processed.
func (q *Queue) InFlight() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.in)
}
