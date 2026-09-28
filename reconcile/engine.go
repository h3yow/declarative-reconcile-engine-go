package reconcile

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Config controls engine tuning.
type Config struct {
	// Workers is the number of concurrent reconciliation workers. A single
	// resource is never processed concurrently (queue dedup + in-flight key).
	Workers int
	// ReconcileTimeout bounds one reconcile attempt; a timeout is retried.
	ReconcileTimeout time.Duration
	// BaseBackoff/MaxBackoff bound exponential backoff after failures.
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// ResyncInterval is the period of the full resync that enqueues every
	// declared (and every actual) key, repairing drift automatically.
	ResyncInterval time.Duration
	Now            func() time.Time
}

// DefaultConfig returns sane defaults.
func DefaultConfig() Config {
	return Config{
		Workers:          4,
		ReconcileTimeout: 500 * time.Millisecond,
		BaseBackoff:      10 * time.Millisecond,
		MaxBackoff:       2 * time.Second,
		ResyncInterval:   250 * time.Millisecond,
		Now:              time.Now,
	}
}

// Engine drives continuous reconciliation between declared and actual state.
type Engine struct {
	declared   *Declared
	reconciler *Reconciler
	queue      *Queue
	cfg        Config

	mu       sync.Mutex
	started  bool
	stopping bool
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	retries  map[string]int
	timers   map[int64]*time.Timer
	timerSeq int64

	reconciles atomic.Int64
	errs       atomic.Int64
	timeouts   atomic.Int64
	requeues   atomic.Int64
}

func NewEngine(d *Declared, r *Reconciler, cfg Config) *Engine {
	if cfg.Workers <= 0 {
		cfg = DefaultConfig()
	}
	if cfg.ReconcileTimeout <= 0 {
		cfg.ReconcileTimeout = 500 * time.Millisecond
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 10 * time.Millisecond
	}
	if cfg.MaxBackoff < cfg.BaseBackoff {
		cfg.MaxBackoff = 2 * time.Second
	}
	if cfg.ResyncInterval <= 0 {
		cfg.ResyncInterval = 250 * time.Millisecond
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	r.WithLiveDeclarations(
		func(id ResourceID) bool {
			_, ok := d.Get(id)
			return ok
		},
		func(id ResourceID) []ResourceID {
			return d.LiveDependents(id)
		},
	)
	return &Engine{
		declared:   d,
		reconciler: r,
		queue:      NewQueue(0),
		cfg:        cfg,
		retries:    map[string]int{},
		timers:     map[int64]*time.Timer{},
	}
}

// Submit validates via Declared.Submit and enqueues the affected keys:
//   - the resource itself;
//   - on a dependency change while present, the resource's dependents (their
//     readiness may be affected).
//
// Deletion cascade toward former prerequisites is performed by the reconciler
// after the physical delete completes (see ReconcileResult.EnqueueAfter),
// not at submit time, so it cannot race a still-blocked delete.
func (e *Engine) Submit(decl Declaration) (newVersion int64, changed bool, err error) {
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return 0, false, ErrStopped
	}
	e.mu.Unlock()

	v, changed, err := e.declared.Submit(decl)
	if err != nil {
		return v, false, err
	}
	// Enqueue unconditionally: even an idempotent resubmit can be useful
	// after a prior failed attempt, and queue dedup makes it cheap.
	if e.queue.Add(decl.ID) {
		if changed && decl.Phase == PhasePresent {
			// New/changed edges may alter the readiness of dependents.
			_, g := e.declared.Snapshot()
			for _, dep := range g.dependents(decl.ID) {
				e.queue.Add(dep)
			}
		}
	}
	return v, changed, nil
}

// Start launches workers and the resync loop; returns immediately.
func (e *Engine) Start() error {
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return ErrStopped
	}
	if e.started {
		e.mu.Unlock()
		return errors.New("engine already started")
	}
	e.started = true
	e.ctx, e.cancel = context.WithCancel(context.Background())
	ctx := e.ctx
	e.mu.Unlock()

	for i := 0; i < e.cfg.Workers; i++ {
		e.wg.Add(1)
		go e.worker(i, ctx)
	}
	e.wg.Add(1)
	go e.resyncLoop(ctx)
	return nil
}

// Run starts the engine and blocks until ctx is cancelled, then performs a
// graceful shutdown bounded by ctx's effective deadline (or a default grace).
func (e *Engine) Run(ctx context.Context) error {
	if err := e.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	grace := 5 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		if left := time.Until(dl); left > 0 {
			grace = left
		}
	}
	return e.Shutdown(grace)
}

// worker pulls keys until the queue is closed and fully drained.
func (e *Engine) worker(id int, ctx context.Context) {
	defer e.wg.Done()
	for {
		key, ok := e.queue.Get()
		if !ok {
			return
		}
		e.process(ctx, key)
		e.queue.Done(key)
	}
}

// resyncLoop periodically enqueues every declared and every actual key so
// drift and lost transient errors are always picked up again.
func (e *Engine) resyncLoop(ctx context.Context) {
	defer e.wg.Done()
	t := time.NewTicker(e.cfg.ResyncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			decls, _ := e.declared.Snapshot()
			for id := range decls {
				e.queue.Add(id)
			}
			for _, id := range e.reconciler.ActualIDs() {
				e.queue.Add(id)
			}
		}
	}
}

// process runs one bounded reconcile attempt and handles backoff/requeue.
func (e *Engine) process(parent context.Context, id string) {
	if parent.Err() != nil {
		return
	}
	decls, g := e.declared.Snapshot()
	decl, present := decls[id]
	var declPtr *Declaration
	if present {
		d := decl
		declPtr = &d
	}
	known := make(map[ResourceID]bool, len(decls))
	for k := range decls {
		known[k] = true
	}

	ctx, cancel := context.WithTimeout(parent, e.cfg.ReconcileTimeout)
	defer cancel()

	e.reconciles.Add(1)
	res, err := e.reconciler.Reconcile(ctx, id, declPtr, g, known, e.cfg.Now())

	// Converged: clear retry budget.
	if err == nil && !res.Requeue {
		e.mu.Lock()
		delete(e.retries, id)
		e.mu.Unlock()
	}
	for _, other := range res.Enqueue {
		e.queue.Add(other)
	}
	if len(res.EnqueueAfter) > 0 {
		delay := res.EnqueueDelay
		if delay <= 0 {
			delay = 5 * time.Millisecond
		}
		for _, other := range res.EnqueueAfter {
			e.scheduleEnqueue(parent, other, delay)
		}
	}
	if err == nil && !res.Requeue {
		return
	}

	// Decide delay.
	e.requeues.Add(1)
	var delay time.Duration
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		e.timeouts.Add(1)
		e.errs.Add(1)
		delay = e.backoff(id)
	case err != nil:
		e.errs.Add(1)
		delay = e.backoff(id)
	default:
		// Dependency wait / blocked: fixed short delay, no backoff growth.
		delay = res.RequeueDelay
		if delay <= 0 {
			delay = 50 * time.Millisecond
		}
	}
	e.scheduleRequeue(parent, id, delay)
}

// backoff returns the exponential, capped delay for this key and records the
// retry attempt.
func (e *Engine) backoff(id string) time.Duration {
	e.mu.Lock()
	n := e.retries[id]
	e.retries[id] = n + 1
	e.mu.Unlock()
	delay := e.cfg.BaseBackoff << n
	if delay <= 0 || delay > e.cfg.MaxBackoff {
		delay = e.cfg.MaxBackoff
	}
	return delay
}

// scheduleEnqueue adds id once after delay (e.g. a deletion cascade that
// must observe the freshly committed declared state).
func (e *Engine) scheduleEnqueue(ctx context.Context, id string, delay time.Duration) {
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return
	}
	seq := e.timerSeq
	e.timerSeq++
	e.mu.Unlock()

	t := time.AfterFunc(delay, func() {
		e.mu.Lock()
		delete(e.timers, seq)
		stopping := e.stopping
		e.mu.Unlock()
		if stopping || ctx.Err() != nil {
			return
		}
		e.queue.Add(id)
	})
	e.mu.Lock()
	e.timers[seq] = t
	e.mu.Unlock()
}

// scheduleRequeue re-adds id after delay; suppressed during shutdown and
// cancelled attempts do not resurrect keys (the resync loop covers recovery).
func (e *Engine) scheduleRequeue(ctx context.Context, id string, delay time.Duration) {
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return
	}
	seq := e.timerSeq
	e.timerSeq++
	e.mu.Unlock()

	t := time.AfterFunc(delay, func() {
		e.mu.Lock()
		delete(e.timers, seq)
		stopping := e.stopping
		e.mu.Unlock()
		if stopping || ctx.Err() != nil {
			return
		}
		e.queue.Add(id)
	})
	e.mu.Lock()
	e.timers[seq] = t
	e.mu.Unlock()
}

// Shutdown gracefully stops the engine:
//  1. new submits are rejected;
//  2. the reconcile context is cancelled, so no NEW attempt starts;
//  3. already in-flight attempts run to completion (store ops are
//     non-blocking), each producing a definite applied/not-applied result;
//  4. pending/dirty keys are drained, then workers exit;
//  5. pending retry timers are cancelled.
func (e *Engine) Shutdown(grace time.Duration) error {
	e.mu.Lock()
	if !e.started || e.stopping {
		stopping := e.stopping
		e.stopping = true
		e.mu.Unlock()
		if !stopping {
			e.queue.Close()
		}
		return nil
	}
	e.stopping = true
	if e.cancel != nil {
		e.cancel()
	}
	timers := e.timers
	e.timers = map[int64]*time.Timer{}
	e.mu.Unlock()

	for _, t := range timers {
		t.Stop()
	}
	e.queue.Close() // drain pending/in-flight, then unblock Get

	done := make(chan struct{})
	go func() { e.wg.Wait(); close(done) }()
	if grace <= 0 {
		grace = 5 * time.Second
	}
	select {
	case <-done:
		return nil
	case <-time.After(grace):
		return errors.New("shutdown grace period elapsed before workers drained")
	}
}

// WaitFor polls cond until it becomes true or timeout elapses (test helper).
func (e *Engine) WaitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// Stats returns counters useful for assertions and explainability.
type Stats struct {
	Reconciles int64
	Errors     int64
	Timeouts   int64
	Requeues   int64
	InFlight   int
	QueueLen   int
	Stopped    bool
}

func (e *Engine) Stats() Stats {
	e.mu.Lock()
	stopping := e.stopping
	e.mu.Unlock()
	return Stats{
		Reconciles: e.reconciles.Load(),
		Errors:     e.errs.Load(),
		Timeouts:   e.timeouts.Load(),
		Requeues:   e.requeues.Load(),
		InFlight:   e.queue.InFlight(),
		QueueLen:   e.queue.Len(),
		Stopped:    stopping,
	}
}

// Idle exposes the queue's idle signal (no queued/in-flight work).
func (e *Engine) Idle() <-chan struct{} { return e.queue.Idle() }
