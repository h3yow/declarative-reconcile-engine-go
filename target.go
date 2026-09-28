package dre

import (
	"context"
	"sync"
	"time"
)

// FailureRule injects a failure into a target operation.
type FailureRule struct {
	// MatchID, when non-empty, limits the rule to one resource.
	MatchID ResourceID
	// Op selects the operation: "create", "update", "delete", "ready".
	Op string
	// Transient marks the failure as retryable.
	Transient bool
	// Times limits how many times the rule fires; 0 means unlimited.
	Times int
	// Delay is an artificial operation delay (used to simulate timeouts).
	Delay time.Duration
	// Message describes the injected failure.
	Message string
}

// activeRule pairs a rule with its remaining fire count.
type activeRule struct {
	rule      FailureRule
	remaining int
}

// Target simulates the actual environment with an in-memory resource store.
// All operations are concurrency-safe and idempotent at the semantics layer:
// Delete of an absent resource succeeds, while Create/Update against the
// wrong presence state return permanent (non-retryable) errors.
type Target struct {
	mu     sync.Mutex
	actual map[ResourceID]*Observed
	rules  []*activeRule
	// opCounts records successful mutating operation invocations per op kind,
	// supporting deterministic idempotence assertions without sleeping.
	opCounts map[string]int
}

// NewTarget constructs an empty simulated target environment.
func NewTarget() *Target {
	return &Target{actual: map[ResourceID]*Observed{}, opCounts: map[string]int{}}
}

// OpCounts returns a copy of successful mutating-operation counters.
func (t *Target) OpCounts() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]int, len(t.opCounts))
	for k, v := range t.opCounts {
		out[k] = v
	}
	return out
}

// InjectFailure adds a failure-injection rule (test helper). Rules are
// evaluated in insertion order; the first matching rule with remaining
// firings consumes one and fails the operation.
func (t *Target) InjectFailure(rule FailureRule) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rules = append(t.rules, &activeRule{rule: rule, remaining: rule.Times})
}

// ClearFailures removes all failure-injection rules (test helper).
func (t *Target) ClearFailures() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rules = nil
}

// consumeRule finds and consumes a matching rule; caller need not hold t.mu.
func (t *Target) consumeRule(id ResourceID, op string) *FailureRule {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, ar := range t.rules {
		r := ar.rule
		if r.Op != op {
			continue
		}
		if r.MatchID != "" && r.MatchID != id {
			continue
		}
		if r.Times > 0 {
			if ar.remaining <= 0 {
				continue
			}
			ar.remaining--
		}
		fired := r
		return &fired
	}
	return nil
}

// applyRule enforces delay/failure for an operation.
func (t *Target) applyRule(ctx context.Context, id ResourceID, op string) error {
	r := t.consumeRule(id, op)
	if r == nil {
		return nil
	}
	if r.Delay > 0 {
		select {
		case <-time.After(r.Delay):
		case <-ctx.Done():
			return &TargetError{Op: op, ID: id, Transient: true,
				Message: "operation timed out"}
		}
	}
	msg := r.Message
	if msg == "" {
		msg = "injected failure"
	}
	return &TargetError{Op: op, ID: id, Transient: r.Transient, Message: msg}
}

// Create provisions a resource in the target environment. The resource starts
// not-ready; SignalReady marks it ready as a separate target operation.
func (t *Target) Create(ctx context.Context, o Observed) error {
	if err := t.applyRule(ctx, o.ID, "create"); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.actual[o.ID]; exists {
		return &TargetError{Op: "create", ID: o.ID, Transient: false,
			Message: "resource already exists"}
	}
	cp := o
	cp.Exists = true
	cp.Ready = false
	cp.MarkedForDeletion = false
	cp.Spec = cloneSpec(o.Spec)
	t.actual[o.ID] = &cp
	t.opCounts["create"]++
	return nil
}

// Update changes an existing resource's spec in the target environment.
func (t *Target) Update(ctx context.Context, id ResourceID, spec Spec) error {
	if err := t.applyRule(ctx, id, "update"); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, exists := t.actual[id]
	if !exists {
		return &TargetError{Op: "update", ID: id, Transient: false,
			Message: "resource not found"}
	}
	// An update re-asserts desired state and revives a terminating object.
	cur.Spec = cloneSpec(spec)
	cur.MarkedForDeletion = false
	t.opCounts["update"]++
	return nil
}

// SignalReady performs the target-side readiness operation.
func (t *Target) SignalReady(ctx context.Context, id ResourceID) error {
	if err := t.applyRule(ctx, id, "ready"); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, exists := t.actual[id]
	if !exists {
		return &TargetError{Op: "ready", ID: id, Transient: false,
			Message: "resource not found"}
	}
	cur.Ready = true
	t.opCounts["ready"]++
	return nil
}

// MarkDeletion flags a resource as terminating (finalizer-style step).
func (t *Target) MarkDeletion(ctx context.Context, id ResourceID) error {
	if err := t.applyRule(ctx, id, "delete"); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	cur, exists := t.actual[id]
	if !exists {
		return nil
	}
	cur.MarkedForDeletion = true
	t.opCounts["markdelete"]++
	return nil
}

// Delete removes a resource. Absence is success (idempotent delete).
func (t *Target) Delete(ctx context.Context, id ResourceID) error {
	if err := t.applyRule(ctx, id, "delete"); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.actual, id)
	t.opCounts["delete"]++
	return nil
}

// Get reads one actual resource; ok is false if it does not exist.
func (t *Target) Get(id ResourceID) (Observed, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	a, ok := t.actual[id]
	if !ok {
		return Observed{}, false
	}
	cp := *a
	cp.Spec = cloneSpec(a.Spec)
	return cp, true
}

// Snapshot returns a point-in-time copy of all actual resources.
func (t *Target) Snapshot() map[ResourceID]Observed {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[ResourceID]Observed, len(t.actual))
	for id, a := range t.actual {
		cp := *a
		cp.Spec = cloneSpec(a.Spec)
		out[id] = cp
	}
	return out
}

// Drift overwrites a live resource's spec to simulate an external change.
func (t *Target) Drift(id ResourceID, spec Spec) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if a, ok := t.actual[id]; ok {
		a.Spec = cloneSpec(spec)
	}
}

// DriftDelete removes a live resource outside the engine, simulating drift.
func (t *Target) DriftDelete(id ResourceID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.actual, id)
}
