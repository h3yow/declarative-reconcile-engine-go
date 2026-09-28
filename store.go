package dre

import (
	"sort"
	"sync"
	"time"
)

// desiredRecord is the stored desired declaration plus bookkeeping.
type desiredRecord struct {
	desired Desired
	version int64
	// deleteAt != 0 marks a tombstone: deletion was admitted and the
	// reconciler must remove the resource from the target environment.
	deleteAt int64
}

func (r desiredRecord) clone() desiredRecord {
	d := r.desired
	d.Spec = cloneSpec(r.desired.Spec)
	d.DependsOn = append([]ResourceID(nil), r.desired.DependsOn...)
	return desiredRecord{desired: d, version: r.version, deleteAt: r.deleteAt}
}

func cloneSpec(in Spec) Spec {
	if in == nil {
		return nil
	}
	out := make(Spec, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Store is the concurrency-safe store of desired-state declarations.
// A single mutex serializes admission so conflict decisions are linearizable.
type Store struct {
	mu      sync.Mutex
	records map[ResourceID]*desiredRecord
	counter int64
}

// NewStore constructs an empty desired-state store.
func NewStore() *Store {
	return &Store{records: map[ResourceID]*desiredRecord{}}
}

// Apply admits an upsert after optimistic-concurrency and dependency checks.
// It returns the server-assigned version and whether the stored declaration
// actually changed: an identical resubmission (same kind/spec/dependencies)
// is a no-op that returns the SAME version without any mutation.
// Rejected submissions never mutate stored state.
func (s *Store) Apply(d Desired) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if d.ExpectedVersion == 0 {
		return 0, false, &ConflictError{ID: d.ID, Reason: ReasonInvalidVersion,
			Message: "expected version must be -1 (create) or a positive version (update), got 0"}
	}
	if d.ExpectedVersion < -1 {
		return 0, false, &ConflictError{ID: d.ID, Reason: ReasonInvalidVersion,
			Expected: d.ExpectedVersion, Message: "expected version must be -1 or positive"}
	}

	rec, exists := s.records[d.ID]
	if exists {
		if rec.deleteAt != 0 {
			return 0, false, &ConflictError{ID: d.ID, Reason: ReasonDeleting,
				Actual:  rec.version,
				Message: "resource is terminating; wait for deletion to complete before re-creating"}
		}
		if d.ExpectedVersion == -1 {
			return 0, false, &ConflictError{ID: d.ID, Reason: ReasonMustNotExist,
				Expected: d.ExpectedVersion, Actual: rec.version,
				Message: "create precondition failed: resource already declared"}
		}
		if d.ExpectedVersion != rec.version {
			return 0, false, &ConflictError{ID: d.ID, Reason: ReasonStaleVersion,
				Expected: d.ExpectedVersion, Actual: rec.version,
				Message: "optimistic concurrency check failed: stale version"}
		}
	} else if d.ExpectedVersion > 0 {
		return 0, false, &ConflictError{ID: d.ID, Reason: ReasonMustExist,
			Expected: d.ExpectedVersion, Actual: 0,
			Message: "update precondition failed: resource not declared"}
	}

	d.DependsOn = canonicalDeps(d.DependsOn)
	if exists && sameDeclaration(rec.desired, d) {
		// Idempotent resubmission: no version bump, no mutation, no work.
		return rec.version, false, nil
	}

	if err := s.checkCycleLocked(d); err != nil {
		return 0, false, err
	}

	s.counter++
	v := s.counter
	stored := desiredRecord{version: v, deleteAt: 0}
	stored.desired = d
	stored.desired.Spec = cloneSpec(d.Spec)
	stored.desired.DependsOn = append([]ResourceID(nil), d.DependsOn...)
	stored.desired.ExpectedVersion = v
	s.records[d.ID] = &stored
	return v, true, nil
}

// canonicalDeps sorts dependencies and removes duplicates so that
// semantically identical submissions compare equal regardless of order.
func canonicalDeps(in []ResourceID) []ResourceID {
	if len(in) == 0 {
		return nil
	}
	out := append([]ResourceID(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	w := 0
	for i := range out {
		if i > 0 && out[i] == out[i-1] {
			continue
		}
		out[w] = out[i]
		w++
	}
	return out[:w]
}

// sameDeclaration reports whether two declarations carry the same effective
// content (version precondition excluded).
func sameDeclaration(a, b Desired) bool {
	if a.Kind != b.Kind {
		return false
	}
	if len(a.DependsOn) != len(b.DependsOn) {
		return false
	}
	for i := range a.DependsOn {
		if a.DependsOn[i] != b.DependsOn[i] {
			return false
		}
	}
	return sameSpec(a.Spec, b.Spec)
}

func sameSpec(a, b Spec) bool {
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

// checkCycleLocked treats the prospective declaration d as live and verifies
// the dependency graph reachable from d has no cycle.
func (s *Store) checkCycleLocked(d Desired) error {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[ResourceID]int{}

	var deps func(id ResourceID) []ResourceID
	deps = func(id ResourceID) []ResourceID {
		if id == d.ID {
			return d.DependsOn
		}
		if r, ok := s.records[id]; ok && r.deleteAt == 0 {
			return r.desired.DependsOn
		}
		return nil
	}

	var visit func(id ResourceID) error
	visit = func(id ResourceID) error {
		if color[id] == black {
			return nil
		}
		if color[id] == gray {
			return &ConflictError{ID: d.ID, Reason: ReasonDependencyCycle,
				Message: "dependency cycle detected involving " + string(id)}
		}
		color[id] = gray
		for _, dep := range deps(id) {
			if err := visit(dep); err != nil {
				return err
			}
		}
		color[id] = black
		return nil
	}
	return visit(d.ID)
}

// Delete marks a declaration tombstoned subject to its version precondition.
// hard=true removes a pending (never materialized) record immediately.
// A live resource still depended on by another live declaration is rejected.
func (s *Store) Delete(id ResourceID, expectedVersion int64, hard bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if expectedVersion <= 0 {
		return &ConflictError{ID: id, Reason: ReasonInvalidVersion,
			Expected: expectedVersion,
			Message:  "delete requires the current positive version"}
	}
	rec, exists := s.records[id]
	if !exists {
		return &ConflictError{ID: id, Reason: ReasonMustExist,
			Expected: expectedVersion, Actual: 0,
			Message: "delete precondition failed: resource not declared"}
	}
	if rec.deleteAt != 0 {
		return &ConflictError{ID: id, Reason: ReasonDeleting,
			Actual: rec.version, Message: "resource is already terminating"}
	}
	if expectedVersion != rec.version {
		return &ConflictError{ID: id, Reason: ReasonStaleVersion,
			Expected: expectedVersion, Actual: rec.version,
			Message: "delete rejected: stale version"}
	}
	if !hard {
		for otherID, other := range s.records {
			if otherID == id || other.deleteAt != 0 {
				continue
			}
			for _, dep := range other.desired.DependsOn {
				if dep == id {
					return &ConflictError{ID: id, Reason: ReasonDependencyBlocksDelete,
						Actual:  rec.version,
						Message: "delete blocked: " + string(otherID) + " still depends on it"}
				}
			}
		}
	}

	if hard {
		delete(s.records, id)
		return nil
	}
	rec.deleteAt = time.Now().UnixNano()
	return nil
}

// Get retrieves a copy of a stored record.
func (s *Store) Get(id ResourceID) (desiredRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.records[id]
	if !ok {
		return desiredRecord{}, false
	}
	return r.clone(), true
}

// Purge removes a record entirely (called once the target delete succeeds).
func (s *Store) Purge(id ResourceID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
}

// Snapshot returns copies of all stored records in deterministic ID order.
func (s *Store) Snapshot() map[ResourceID]desiredRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[ResourceID]desiredRecord, len(s.records))
	for id, r := range s.records {
		out[id] = r.clone()
	}
	return out
}

// sortedIDs returns all record IDs in lexicographic order (used by resync).
func (s *Store) sortedIDs() []ResourceID {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]ResourceID, 0, len(s.records))
	for id := range s.records {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
