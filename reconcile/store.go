package reconcile

import (
	"context"
	"sync"
	"time"
)

// Operation names accepted by InjectFailures.
const (
	OpCreate = "create"
	OpUpdate = "update"
	OpDelete = "delete"
	OpTrans  = "transition"
)

// Store is the simulated target environment ("actual state"). Every mutating
// operation is linearized under a single mutex and can be made to fail
// transiently for testing, exercising the engine's retry paths.
type Store struct {
	mu       sync.Mutex
	data     map[string]ResourceStatus
	failNext map[string]int // operation -> remaining injected failures
	drift    map[string]Spec
	versions map[string]int64 // versions survive delete/recreate tracking
	order    []string         // global monotonic create order
	seq      int64
	hang     map[string]int // per-id remaining hung mutating attempts
	hangCh   chan struct{}  // closed to release all hung calls
}

func NewStore() *Store {
	return &Store{
		data:     map[string]ResourceStatus{},
		failNext: map[string]int{},
		drift:    map[string]Spec{},
		versions: map[string]int64{},
		hang:     map[string]int{},
		hangCh:   make(chan struct{}),
	}
}

// InjectFailures makes the next n calls of the given operation fail
// transiently. Failures are consumed one per attempt, so once exhausted the
// same logical operation succeeds on retry (simulating recovery).
func (s *Store) InjectFailures(op string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failNext[op] += n
}

// InjectDrift records an out-of-band change the environment should exhibit:
// the next read of id returns this spec instead of the stored one, until a
// reconcile update overwrites it. This models configuration drift.
func (s *Store) InjectDrift(id string, drifted Spec) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drift[id] = DefaultSpec(drifted)
}

// InjectHang makes the next n mutating calls for id block until either the
// caller's context is cancelled or ReleaseHangs is invoked. It simulates a
// hung backend WITHOUT holding the store lock, so other resources stay
// schedulable and the engine's per-attempt timeout can fire.
func (s *Store) InjectHang(id string, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hang[id] += n
}

// ReleaseHangs unblocks every currently hung call.
func (s *Store) ReleaseHangs() {
	s.mu.Lock()
	ch := s.hangCh
	s.hangCh = make(chan struct{})
	s.hang = map[string]int{}
	s.mu.Unlock()
	close(ch)
}

// waitHang blocks (without holding the lock) while injected hangs for id
// remain, returning ctx.Err() on timeout/cancellation.
func (s *Store) waitHang(ctx context.Context, id string) error {
	for {
		s.mu.Lock()
		if s.hang[id] > 0 {
			s.hang[id]--
			ch := s.hangCh
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ch:
				continue
			}
		}
		s.mu.Unlock()
		return nil
	}
}

func (s *Store) consumeFailure(op string) bool {
	if n := s.failNext[op]; n > 0 {
		s.failNext[op] = n - 1
		return true
	}
	return false
}

func (s *Store) addConditionLocked(st *ResourceStatus, now time.Time, typ, status, reason string) {
	st.Conditions = append(st.Conditions, Condition{
		Time: now, Type: typ, Status: status, Reason: reason,
	})
	if len(st.Conditions) > 50 {
		st.Conditions = st.Conditions[len(st.Conditions)-50:]
	}
}

// Create provisions a new resource. It fails transiently while injected
// create-failures remain, and returns ErrAlreadyExists if the resource is
// present.
func (s *Store) Create(ctx context.Context, id string, spec Spec, now time.Time) (ResourceStatus, error) {
	if err := s.waitHang(ctx, id); err != nil {
		return ResourceStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumeFailure(OpCreate) {
		return ResourceStatus{}, transientf("create %s", id)
	}
	if _, ok := s.data[id]; ok {
		return ResourceStatus{}, &ConflictError{ID: id, Reason: ErrAlreadyExists, Current: s.data[id].Version}
	}
	s.versions[id]++
	st := ResourceStatus{
		ID: id, Version: s.versions[id], Phase: PhaseReady,
		Spec: DefaultSpec(spec), Ready: true, Reason: "created",
		ObservedAt: now,
	}
	s.addConditionLocked(&st, now, "Lifecycle", "True", "created")
	s.seq++
	s.order = append(s.order, id)
	s.data[id] = st
	return cloneStatus(st), nil
}

// Update changes an existing resource's spec and bumps its backend version.
func (s *Store) Update(ctx context.Context, id string, spec Spec, now time.Time) (ResourceStatus, error) {
	if err := s.waitHang(ctx, id); err != nil {
		return ResourceStatus{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumeFailure(OpUpdate) {
		return ResourceStatus{}, transientf("update %s", id)
	}
	st, ok := s.data[id]
	if !ok {
		return ResourceStatus{}, &ConflictError{ID: id, Reason: ErrNotFound}
	}
	s.versions[id]++
	st.Version = s.versions[id]
	st.Spec = DefaultSpec(spec)
	st.Phase = PhaseReady
	st.Ready = true
	st.Reason = "updated"
	st.ObservedAt = now
	delete(s.drift, id)
	s.addConditionLocked(&st, now, "Lifecycle", "True", "updated")
	s.data[id] = st
	return cloneStatus(st), nil
}

// Delete removes a resource; deleting an absent one is a successful no-op
// (deletes must be idempotent).
func (s *Store) Delete(ctx context.Context, id string, now time.Time) error {
	if err := s.waitHang(ctx, id); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumeFailure(OpDelete) {
		return transientf("delete %s", id)
	}
	if st, ok := s.data[id]; ok {
		s.addConditionLocked(&st, now, "Lifecycle", "True", "deleted")
	}
	delete(s.data, id)
	delete(s.drift, id)
	return nil
}

// Get reads one resource; ok=false means absent. A pending injected drift is
// reflected so drift repair can be observed/driven.
func (s *Store) Get(id string) (ResourceStatus, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.data[id]
	if !ok {
		return ResourceStatus{}, false
	}
	if drifted, is := s.drift[id]; is {
		st.Spec = drifted
		st.Ready = false
		st.Reason = "drifted"
	}
	return cloneStatus(st), true
}

// List returns a point-in-time copy of all actual resources (drift applied).
func (s *Store) List() []ResourceStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ResourceStatus, 0, len(s.data))
	for _, st := range s.data {
		if drifted, is := s.drift[st.ID]; is {
			st.Spec = drifted
			st.Ready = false
			st.Reason = "drifted"
		}
		out = append(out, cloneStatus(st))
	}
	return out
}

// setPhase records a reconcile-internal phase transition.
func (s *Store) setPhase(id string, phase Phase, reason string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.consumeFailure(OpTrans) {
		return transientf("transition %s", id)
	}
	st, ok := s.data[id]
	if !ok {
		return &ConflictError{ID: id, Reason: ErrNotFound}
	}
	st.Phase = phase
	st.Reason = reason
	st.ObservedAt = now
	s.addConditionLocked(&st, now, "Phase", phase.String(), reason)
	s.data[id] = st
	return nil
}

// CreateOrder returns a copy of the global order in which resources were
// first successfully created (monotonic, deterministic for ordering tests).
func (s *Store) CreateOrder() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.order...)
}

// transientf wraps ErrTransient with operation context.
func transientf(format string, args ...any) error {
	return &transientError{msg: fmtSprintf(format, args...)}
}

type transientError struct{ msg string }

func (e *transientError) Error() string { return ErrTransient.Error() + ": " + e.msg }
func (e *transientError) Unwrap() error { return ErrTransient }
