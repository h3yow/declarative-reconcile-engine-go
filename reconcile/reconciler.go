package reconcile

import (
	"context"
	"time"
)

// EventType classifies reconcile events for logs/metrics.
type EventType string

const (
	EventCreate   EventType = "create"
	EventUpdate   EventType = "update"
	EventDelete   EventType = "delete"
	EventNoop     EventType = "noop"
	EventBlocked  EventType = "blocked"
	EventError    EventType = "error"
	EventWaitDeps EventType = "wait_deps"
)

// Event explains one reconcile decision.
type Event struct {
	Time   time.Time
	ID     string
	Type   EventType
	Reason string
	Err    error
}

// Reconciler converges a single resource: it compares desired vs actual and
// issues create/update/delete against the store. Dependency gating and
// retry/backoff decisions are the engine's responsibility; dependency
// *inspection* is done here from the graph snapshot handed in.
type Reconciler struct {
	store  *Store
	events func(Event)
	// liveDecl reports whether id is currently a live declaration, read
	// fresh at decision time (the passed snapshot may already be stale when
	// submissions and processing interleave).
	liveDecl func(ResourceID) bool
	// liveDependents returns resources currently declaring a dependency on
	// id, read fresh (independent of the worker's graph snapshot).
	liveDependents func(ResourceID) []ResourceID
}

func NewReconciler(store *Store) *Reconciler {
	return &Reconciler{
		store:          store,
		events:         func(Event) {},
		liveDecl:       func(ResourceID) bool { return false },
		liveDependents: func(ResourceID) []ResourceID { return nil },
	}
}

// WithLiveDeclarations installs fresh-read predicates over declared state.
func (r *Reconciler) WithLiveDeclarations(
	live func(ResourceID) bool,
	dependents func(ResourceID) []ResourceID,
) *Reconciler {
	if live != nil {
		r.liveDecl = live
	}
	if dependents != nil {
		r.liveDependents = dependents
	}
	return r
}

// WithEventSink installs a callback invoked for every decision (nil resets
// to a no-op sink). Useful for tests to log input and decision basis.
func (r *Reconciler) WithEventSink(sink func(Event)) *Reconciler {
	if sink == nil {
		r.events = func(Event) {}
	} else {
		r.events = sink
	}
	return r
}

func (r *Reconciler) emit(id string, t EventType, reason string, err error, now time.Time) {
	r.events(Event{Time: now, ID: id, Type: t, Reason: reason, Err: err})
}

// ReconcileResult tells the engine what to do next.
type ReconcileResult struct {
	// Requeue requests another pass after RequeueDelay (dependency wait or
	// transient failure).
	Requeue bool
	// RequeueDelay is the backoff the engine should apply.
	RequeueDelay time.Duration
	// Enqueue lists other keys that must be scheduled (dependents that may
	// have just become ready).
	Enqueue []ResourceID
	// EnqueueAfter lists keys to schedule only after EnqueueDelay. This is
	// used for deletion cascade so the re-evaluation happens strictly after
	// the declaring state change is globally visible.
	EnqueueAfter []ResourceID
	// EnqueueDelay bounds the EnqueueAfter delay.
	EnqueueDelay time.Duration
}

// Reconcile performs one convergence attempt for id against the supplied
// declaration snapshot and graph. Missing declaration means "desired
// absent". depsReady is precomputed by the engine from the snapshot.
func (r *Reconciler) Reconcile(
	ctx context.Context,
	id string,
	decl *Declaration,
	g *graph,
	known map[ResourceID]bool,
	now time.Time,
) (ReconcileResult, error) {
	if err := ctx.Err(); err != nil {
		return ReconcileResult{}, err
	}

	// Desired absent: delete, but only when no live dependent still blocks
	// deletion (no orphans, no premature removal of a prerequisite).
	if decl == nil || decl.Phase == PhaseAbsent {
		if _, exists := r.store.Get(id); !exists {
			return ReconcileResult{}, nil // already gone
		}
		// Live blockers are computed from CURRENT declared state (not the
		// worker's potentially stale graph snapshot) intersected with
		// resources that are still physically present.
		var blockers []ResourceID
		for _, dep := range r.liveDependents(id) {
			if _, live := r.store.Get(dep); live {
				blockers = append(blockers, dep)
			}
		}
		if len(blockers) > 0 {
			r.emit(id, EventBlocked, "delete blocked by dependents: "+joinIDs(blockers), nil, now)
			// Dependents will enqueue us again when they disappear.
			return ReconcileResult{Requeue: true, RequeueDelay: 50 * time.Millisecond}, nil
		}
		if err := r.store.Delete(ctx, id, now); err != nil {
			r.emit(id, EventError, "delete failed", err, now)
			return ReconcileResult{Requeue: true, RequeueDelay: 0}, err
		}
		r.emit(id, EventDelete, "actual resource removed", nil, now)
		// Cascade only into prerequisites that are themselves desired
		// absent (deleted declaration); re-evaluate them one tick later so
		// their delete decision reads fresh declared state.
		var cascade []ResourceID
		for _, prereq := range g.prerequisites(id) {
			if !r.liveDecl(prereq) {
				cascade = append(cascade, prereq)
			}
		}
		return ReconcileResult{
			EnqueueAfter: cascade,
			EnqueueDelay: 5 * time.Millisecond,
		}, nil
	}

	// Desired present. First validate the dependency graph references.
	if missing := g.reachableMissing(id, known); len(missing) > 0 {
		r.emit(id, EventWaitDeps, "missing dependencies: "+joinIDs(missing), nil, now)
		return ReconcileResult{Requeue: true, RequeueDelay: 50 * time.Millisecond}, nil
	}

	// Dependency readiness: prerequisites must exist and be Ready in the
	// actual environment before this resource is created/updated.
	var waiting []ResourceID
	for _, dep := range decl.DependsOn {
		st, ok := r.store.Get(dep)
		if !ok || !st.Ready {
			waiting = append(waiting, dep)
		}
	}
	if len(waiting) > 0 {
		r.emit(id, EventWaitDeps, "waiting on not-ready deps: "+joinIDs(waiting), nil, now)
		return ReconcileResult{Requeue: true, RequeueDelay: 20 * time.Millisecond}, nil
	}

	desired := DefaultSpec(decl.Spec)
	actual, exists := r.store.Get(id)

	switch {
	case !exists:
		if _, err := r.store.Create(ctx, id, desired, now); err != nil {
			r.emit(id, EventError, "create failed", err, now)
			return ReconcileResult{Requeue: true}, err
		}
		r.emit(id, EventCreate, "created with image="+desired.Image, nil, now)
		// Our readiness may unblock dependents; let the engine schedule them.
		return ReconcileResult{Enqueue: g.dependents(id)}, nil
	case !specEqual(actual.Spec, desired) || !actual.Ready:
		reason := "spec drift"
		if !actual.Ready {
			reason = "resource not ready (" + actual.Reason + ")"
		}
		if _, err := r.store.Update(ctx, id, desired, now); err != nil {
			r.emit(id, EventError, "update failed", err, now)
			return ReconcileResult{Requeue: true}, err
		}
		r.emit(id, EventUpdate, reason+" repaired", nil, now)
		return ReconcileResult{Enqueue: g.dependents(id)}, nil
	default:
		r.emit(id, EventNoop, "already converged at version-equivalent spec", nil, now)
		return ReconcileResult{}, nil
	}
}

// ActualIDs returns the IDs of resources currently present in the target
// environment, sorted. Used by periodic resync to cover resources whose
// declaration disappeared (orphans) or whose events were missed.
func (r *Reconciler) ActualIDs() []ResourceID {
	list := r.store.List()
	out := make([]ResourceID, 0, len(list))
	for _, st := range list {
		out = append(out, st.ID)
	}
	sortStrings(out)
	return out
}

// Store returns the backing store (primarily for test assertions).
func (r *Reconciler) Store() *Store { return r.store }
