package dre

import (
	"context"
	"time"
)

// reconcileResult is the judgement produced by one reconcile pass.
type reconcileResult struct {
	phase  Phase
	reason string
	detail string
	ready  bool
	// requeueAfter > 0 asks for a delayed retry.
	requeueAfter time.Duration
	// purged means the resource record was fully removed after deletion.
	purged bool
	// transient marks the outcome as a retryable target failure.
	transient bool
	// failed marks any target operation failure (transient or permanent).
	failed bool
}

// Reconciler executes one reconciliation pass for a single resource,
// comparing desired store state against the simulated target.
type Reconciler struct {
	store  *Store
	target *Target
	// opTimeout bounds a single resource's target operations.
	opTimeout time.Duration
}

// NewReconciler constructs a Reconciler.
func NewReconciler(store *Store, target *Target, opTimeout time.Duration) *Reconciler {
	if opTimeout <= 0 {
		opTimeout = 2 * time.Second
	}
	return &Reconciler{store: store, target: target, opTimeout: opTimeout}
}

// Reconcile performs one pass for id. It always reads the FRESH record, so a
// pass interrupted by concurrent submissions naturally converges on the next
// processing of the latest desired state.
func (r *Reconciler) Reconcile(ctx context.Context, id ResourceID) reconcileResult {
	rec, exists := r.store.Get(id)
	if !exists {
		// Declared record vanished (hard delete of a never-materialized
		// resource): nothing to do.
		return reconcileResult{phase: PhasePending, reason: "NoDeclaration",
			detail: "no desired record present"}
	}
	actual, _ := r.target.Get(id)
	if rec.deleteAt != 0 {
		return r.reconcileDelete(ctx, rec, &actual)
	}
	return r.reconcileUpsert(ctx, rec, &actual)
}

// dependenciesReady verifies every dependency exists as a live, healthy,
// non-tombstoned declaration with a healthy actual resource.
func (r *Reconciler) dependenciesReady(rec desiredRecord) (bool, string) {
	for _, dep := range rec.desired.DependsOn {
		drec, ok := r.store.Get(dep)
		if !ok || drec.deleteAt != 0 {
			return false, "dependency missing: " + string(dep)
		}
		obs, found := r.target.Get(dep)
		if !found || !obs.Exists || !obs.Ready || obs.MarkedForDeletion {
			return false, "dependency not ready: " + string(dep)
		}
	}
	return true, ""
}

// reconcileUpsert drives create/update lifecycle for one resource.
func (r *Reconciler) reconcileUpsert(ctx context.Context, rec desiredRecord, actual *Observed) reconcileResult {
	if ok, why := r.dependenciesReady(rec); !ok {
		return reconcileResult{phase: PhasePending, reason: "DependencyNotReady",
			detail: why, requeueAfter: r.opTimeout / 2}
	}

	d := rec.desired
	if !actual.Exists {
		cctx, cancel := context.WithTimeout(ctx, r.opTimeout)
		err := r.target.Create(cctx, Observed{ID: d.ID, Kind: d.Kind, Spec: d.Spec})
		cancel()
		if err != nil {
			return failureResult(PhaseProgressing, "CreateFailed", err)
		}
		return reconcileResult{phase: PhaseProgressing, reason: "Created",
			detail: "resource created; readiness pending"}
	}

	if actual.MarkedForDeletion {
		// Desired state was re-declared while target is terminating: clear
		// the terminating flag by re-creating spec, then re-assert readiness.
		cctx, cancel := context.WithTimeout(ctx, r.opTimeout)
		err := r.target.Update(cctx, d.ID, d.Spec)
		cancel()
		if err != nil {
			return failureResult(PhaseProgressing, "ReassertFailed", err)
		}
		return reconcileResult{phase: PhaseProgressing, reason: "Reasserted",
			detail: "desired re-declared during termination"}
	}

	if !specsEqual(actual.Spec, d.Spec) {
		cctx, cancel := context.WithTimeout(ctx, r.opTimeout)
		err := r.target.Update(cctx, d.ID, d.Spec)
		cancel()
		if err != nil {
			return failureResult(PhaseProgressing, "UpdateFailed", err)
		}
		return reconcileResult{phase: PhaseProgressing, reason: "Updated",
			detail: "spec converged to desired version"}
	}

	if !actual.Ready {
		cctx, cancel := context.WithTimeout(ctx, r.opTimeout)
		err := r.target.SignalReady(cctx, d.ID)
		cancel()
		if err != nil {
			return failureResult(PhaseProgressing, "ReadyFailed", err)
		}
		return reconcileResult{phase: PhaseHealthy, reason: "Ready",
			detail: "resource exists with desired spec and signalled ready", ready: true}
	}

	return reconcileResult{phase: PhaseHealthy, reason: "InSync",
		detail: "actual state matches desired state", ready: true}
}

// reconcileDelete drives deletion: idempotent terminalization then purge.
func (r *Reconciler) reconcileDelete(ctx context.Context, rec desiredRecord, actual *Observed) reconcileResult {
	if !actual.Exists {
		// Target already absent: finalize the tombstone in the store.
		r.store.Purge(rec.desired.ID)
		return reconcileResult{phase: PhasePending, reason: "Purged",
			detail: "absent in target; declaration purged", purged: true}
	}

	if !actual.MarkedForDeletion {
		cctx, cancel := context.WithTimeout(ctx, r.opTimeout)
		err := r.target.MarkDeletion(cctx, rec.desired.ID)
		cancel()
		if err != nil {
			return failureResult(PhaseDeleting, "MarkDeletionFailed", err)
		}
		return reconcileResult{phase: PhaseDeleting, reason: "Terminating",
			detail: "resource marked for deletion"}
	}

	cctx, cancel := context.WithTimeout(ctx, r.opTimeout)
	err := r.target.Delete(cctx, rec.desired.ID)
	cancel()
	if err != nil {
		return failureResult(PhaseDeleting, "DeleteFailed", err)
	}
	r.store.Purge(rec.desired.ID)
	return reconcileResult{phase: PhasePending, reason: "Deleted",
		detail: "resource deleted and declaration purged", purged: true}
}

func failureResult(phase Phase, reason string, err error) reconcileResult {
	rr := reconcileResult{phase: phase, reason: reason, detail: err.Error(), failed: true}
	if IsRetryable(err) {
		rr.transient = true
	}
	return rr
}

// specsEqual compares effective specs deterministically (map key/value equality).
func specsEqual(a, b Spec) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
