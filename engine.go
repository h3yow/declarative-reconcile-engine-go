package dre

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Logger receives engine judgements for explainability and test logs.
type Logger interface {
	LogDecision(d Decision)
	Logf(format string, args ...any)
}

// nopLogger discards all output.
type nopLogger struct{}

func (nopLogger) LogDecision(Decision) {}
func (nopLogger) Logf(string, ...any)  {}

// Config configures an Engine.
type Config struct {
	Workers        int
	OpTimeout      time.Duration
	ResyncInterval time.Duration
	BackoffBase    time.Duration
	BackoffMax     time.Duration
	Logger         Logger
}

func (c *Config) withDefaults() {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.OpTimeout <= 0 {
		c.OpTimeout = 200 * time.Millisecond
	}
	if c.ResyncInterval <= 0 {
		c.ResyncInterval = 100 * time.Millisecond
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 10 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = time.Second
	}
	if c.Logger == nil {
		c.Logger = nopLogger{}
	}
}

// ticket is the newest admitted generation for one resource.
type ticket struct {
	version   int64
	tombstone bool
	purged    bool
}

// Engine runs continuous reconciliation over a Store and a Target.
type Engine struct {
	store  *Store
	target *Target
	rec    *Reconciler
	q      *Queue
	cfg    Config

	mu        sync.Mutex
	statuses  map[ResourceID]*Status
	decisions []Decision
	// tickets records the newest admitted generation per resource so stale
	// queued passes can be detected before touching the target.
	tickets map[ResourceID]ticket

	startOnce sync.Once
	stopOnce  sync.Once
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	started   bool

	// testAdmitGate, if set, is invoked with the op name ("apply"/"delete")
	// while holding no internal lock, just before admission. Tests use it to
	// deterministically interleave contending submissions.
	testAdmitGate func(op string)
}

// NewEngine constructs an engine with default configuration filled in.
func NewEngine(store *Store, target *Target, cfg Config) *Engine {
	cfg.withDefaults()
	return &Engine{
		store:    store,
		target:   target,
		rec:      NewReconciler(store, target, cfg.OpTimeout),
		q:        NewQueue(),
		cfg:      cfg,
		statuses: map[ResourceID]*Status{},
		tickets:  map[ResourceID]ticket{},
	}
}

// Start launches workers and the resync loop. Starting again over the same
// Store/Target replays all declarations (durable in the store), so restart
// never loses tasks.
func (e *Engine) Start(ctx context.Context) {
	e.startOnce.Do(func() {
		runCtx, cancel := context.WithCancel(ctx)
		e.cancel = cancel
		e.started = true
		e.logf("engine starting: workers=%d opTimeout=%s resync=%s",
			e.cfg.Workers, e.cfg.OpTimeout, e.cfg.ResyncInterval)
		for i := 0; i < e.cfg.Workers; i++ {
			e.wg.Add(1)
			go func(n int) {
				defer e.wg.Done()
				e.runWorker(runCtx, n)
			}(i)
		}
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			e.resyncLoop(runCtx)
		}()
		// Seed the queue with every durable declaration and adopt its
		// current generation as the newest ticket.
		for id, r := range e.store.Snapshot() {
			e.setTicket(id, ticket{version: r.version, tombstone: r.deleteAt != 0})
			e.q.Add(id)
		}
	})
}

// Shutdown cancels in-flight reconciliations and drains already-queued work
// bounded by ctx. Durable desired state is untouched: a later Start replays
// it, so shutdown never loses a task.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.stopOnce.Do(func() {
		if e.cancel != nil {
			e.logf("engine shutdown: cancelling in-flight reconciliation")
			e.cancel()
		}
	})
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		e.q.Shutdown()
		return nil
	case <-ctx.Done():
		e.q.Shutdown()
		return ctx.Err()
	}
}

// Apply admits a desired-state upsert after deterministic defaulting and
// enqueues reconciliation of it and its dependents.
func (e *Engine) Apply(d Desired) (int64, error) {
	if e.testAdmitGate != nil {
		e.testAdmitGate("apply")
	}
	in := d
	ApplyDefaults(&in)
	v, changed, err := e.store.Apply(in)
	if err != nil {
		e.recordDecision(Decision{
			Action: "RejectApply", ID: d.ID,
			Reason: reasonOf(err), Detail: err.Error(),
		})
		return 0, err
	}
	e.ensureStatus(d.ID, v)
	if !changed {
		e.recordDecision(Decision{
			Action: "NoopApply", ID: d.ID, Version: v,
			Reason: "Identical",
			Detail: "identical resubmission: no version bump, no reconciliation",
		})
		e.logf("apply no-op id=%s version=%d (identical desired state)", d.ID, v)
		return v, nil
	}
	e.setTicket(d.ID, ticket{version: v, tombstone: false})
	// A freshly admitted generation obsoletes every queued old pass.
	e.q.RemovePending(d.ID)
	e.recordDecision(Decision{
		Action: "AdmitApply", ID: d.ID, Version: v,
		Reason: "Accepted",
		Detail: fmt.Sprintf("kind=%s deps=%d spec=%v", in.Kind, len(in.DependsOn), in.Spec),
	})
	e.logf("apply admitted id=%s version=%d expected=%d", d.ID, v, d.ExpectedVersion)
	e.q.Add(d.ID)
	e.enqueueDependents(d.ID)
	return v, nil
}

// Delete admits a desired-state deletion and enqueues the terminating passes.
func (e *Engine) Delete(id ResourceID, expectedVersion int64) error {
	if e.testAdmitGate != nil {
		e.testAdmitGate("delete")
	}
	if err := e.store.Delete(id, expectedVersion, false); err != nil {
		e.recordDecision(Decision{
			Action: "RejectDelete", ID: id,
			Reason: reasonOf(err), Detail: err.Error(), Version: expectedVersion,
		})
		return err
	}
	if rec, ok := e.store.Get(id); ok {
		e.setTicket(id, ticket{version: rec.version, tombstone: true})
	}
	e.q.RemovePending(id)
	e.recordDecision(Decision{
		Action: "AdmitDelete", ID: id, Version: expectedVersion,
		Reason: "Accepted", Detail: "tombstone recorded",
	})
	e.logf("delete admitted id=%s version=%d", id, expectedVersion)
	e.q.Add(id)
	return nil
}

// hardDelete removes a never-materialized pending record directly.
func (e *Engine) hardDelete(id ResourceID, expectedVersion int64) error {
	if err := e.store.Delete(id, expectedVersion, true); err != nil {
		e.recordDecision(Decision{
			Action: "RejectDelete", ID: id, Reason: reasonOf(err), Detail: err.Error(),
		})
		return err
	}
	e.recordDecision(Decision{Action: "HardDelete", ID: id, Version: expectedVersion,
		Reason: "Accepted", Detail: "pending record removed without target work"})
	return nil
}

func (e *Engine) setTicket(id ResourceID, tk ticket) {
	e.mu.Lock()
	e.tickets[id] = tk
	e.mu.Unlock()
}

// currentTicket reports whether rec matches the newest admitted generation.
func (e *Engine) currentTicket(rec desiredRecord) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	cur, ok := e.tickets[rec.desired.ID]
	if !ok {
		return false
	}
	return cur.version == rec.version && cur.tombstone == (rec.deleteAt != 0) && !cur.purged
}

func (e *Engine) markPurged(id ResourceID) {
	e.mu.Lock()
	if tk, ok := e.tickets[id]; ok {
		tk.purged = true
		e.tickets[id] = tk
	}
	e.mu.Unlock()
}

func (e *Engine) ensureStatus(id ResourceID, version int64) *Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.statuses[id]
	if !ok {
		st = &Status{ID: id, Phase: PhasePending, Version: version}
		e.statuses[id] = st
	}
	st.Version = version
	st.LastError = ""
	return st
}

func (e *Engine) updateStatus(id ResourceID, version int64, fn func(*Status)) Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.statuses[id]
	if !ok {
		st = &Status{ID: id, Phase: PhasePending}
		e.statuses[id] = st
	}
	fn(st)
	return *st
}

// Status returns a copy of the current status for id.
func (e *Engine) Status(id ResourceID) (Status, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.statuses[id]
	if !ok {
		return Status{}, false
	}
	return *st, true
}

// StatusSnapshot returns copies of all resource statuses.
func (e *Engine) StatusSnapshot() map[ResourceID]Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[ResourceID]Status, len(e.statuses))
	for id, st := range e.statuses {
		out[id] = *st
	}
	return out
}

// Decisions returns a copy of all recorded judgement logs.
func (e *Engine) Decisions() []Decision {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Decision, len(e.decisions))
	copy(out, e.decisions)
	return out
}

// WaitFor blocks until predicate is true for a status or ctx ends.
func (e *Engine) WaitFor(ctx context.Context, id ResourceID, pred func(Status) bool) bool {
	t := time.NewTicker(2 * time.Millisecond)
	defer t.Stop()
	for {
		if st, ok := e.Status(id); ok && pred(st) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
		}
	}
}

func (e *Engine) runWorker(ctx context.Context, n int) {
	for {
		id, ok := e.q.Get(ctx)
		if !ok {
			if ctx.Err() != nil {
				return
			}
			return
		}
		e.process(ctx, id)
		e.q.Done(id)
	}
}

func (e *Engine) process(ctx context.Context, id ResourceID) {
	rec, hasRec := e.store.Get(id)
	if !hasRec {
		e.markPurged(id)
		e.updateStatus(id, 0, func(st *Status) {
			st.Phase = PhaseDeleted
			st.LastError = ""
		})
		return
	}
	version := rec.version

	st := e.updateStatus(id, version, func(s *Status) {
		if rec.deleteAt != 0 {
			s.Phase = PhaseDeleting
		} else if s.Phase == "" || s.Phase == PhaseDeleted {
			s.Phase = PhasePending
		}
		s.Version = version
		s.Attempts++
	})

	// Concurrency gate: a pass holds a "ticket" keyed by (version, tombstone
	// generation). A delayed retry/resync pass whose ticket went stale while
	// it was queued (a new Apply/Delete/purge happened in between) performs
	// NO target side effects - the freshest ticket owns convergence, which
	// prevents a zombie pass from re-creating an already deleted resource.
	if !e.currentTicket(rec) {
		e.logf("skip stale ticket id=%s version=%d tombstoned=%v",
			id, rec.version, rec.deleteAt != 0)
		return
	}

	res := e.rec.Reconcile(ctx, id)
	now := time.Now()
	if res.purged {
		e.markPurged(id)
	}

	e.updateStatus(id, version, func(s *Status) {
		s.Phase = res.phase
		if res.purged {
			s.Phase = PhaseDeleted
		}
		s.LastReconciled = now
		s.Conditions = append(s.Conditions, Condition{
			Time: now, Type: string(res.phase),
			Reason: res.reason, Message: res.detail,
		})
		if len(s.Conditions) > 15 {
			s.Conditions = s.Conditions[len(s.Conditions)-15:]
		}
		if res.phase == PhaseHealthy {
			s.Attempts = 0
			s.LastError = ""
		} else if res.failed {
			s.LastError = res.detail
		}
	})

	e.recordDecision(Decision{
		Action: "Reconcile", ID: id, Version: version,
		Reason: res.reason, Detail: res.detail,
	})
	e.logf("reconcile id=%s version=%d -> phase=%s reason=%s detail=%s",
		id, version, res.phase, res.reason, res.detail)

	switch {
	case res.purged:
		// Final state; dependents that may now unblock get a resync anyway.
		return
	case res.phase == PhaseDeleting:
		// Terminalization makes progress across multiple passes (mark then
		// delete): re-drive immediately so purge never waits on resync.
		e.q.Add(id)
	case res.phase == PhaseHealthy:
		e.enqueueDependents(id)
		return
	case res.transient:
		delay := e.backoff(st.Attempts)
		e.logf("transient failure id=%s requeue after %s", id, delay)
		e.q.AddAfter(id, delay)
	case res.requeueAfter > 0:
		e.q.AddAfter(id, res.requeueAfter)
	default:
		// Permanent failure / dependency wait: no busy loop, periodic resync
		// and dependents' state changes are the forward triggers.
		e.q.AddAfter(id, e.cfg.ResyncInterval)
	}
}

func (e *Engine) resyncLoop(ctx context.Context) {
	t := time.NewTicker(e.cfg.ResyncInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Periodic full re-enqueue heals target drift and re-drives any
			// permanently failed or dependency-blocked resource.
			for _, id := range e.store.sortedIDs() {
				e.q.Add(id)
			}
		}
	}
}

func (e *Engine) recordDecision(d Decision) {
	if d.Time.IsZero() {
		d.Time = time.Now()
	}
	e.cfg.Logger.LogDecision(d)
	e.mu.Lock()
	e.decisions = append(e.decisions, d)
	if len(e.decisions) > 500 {
		e.decisions = e.decisions[len(e.decisions)-500:]
	}
	e.mu.Unlock()
}

func (e *Engine) logf(format string, args ...any) {
	e.cfg.Logger.Logf(format, args...)
}

// backoff returns capped exponential backoff with mild jitter-free certainty.
func (e *Engine) backoff(attempts int) time.Duration {
	if attempts <= 1 {
		return e.cfg.BackoffBase
	}
	d := e.cfg.BackoffBase
	for i := 1; i < attempts; i++ {
		d *= 2
		if d >= e.cfg.BackoffMax {
			return e.cfg.BackoffMax
		}
	}
	return d
}

// enqueueDependents re-queues every live declaration depending on id, because
// a dependency state change may unblock it.
func (e *Engine) enqueueDependents(id ResourceID) {
	for otherID, rec := range e.store.Snapshot() {
		if rec.deleteAt != 0 {
			continue
		}
		for _, dep := range rec.desired.DependsOn {
			if dep == id {
				e.q.Add(otherID)
				break
			}
		}
	}
}

func reasonOf(err error) string {
	if ce, ok := AsConflict(err); ok {
		return string(ce.Reason)
	}
	return "Error"
}
