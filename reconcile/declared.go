package reconcile

import "sync"

// Declared is the thread-safe store of desired state (the "source of truth"
// submitted by clients). Submissions are validated and version-checked here,
// independently from the actual backend.
//
// Versioning rule:
//   - create uses ExpectedVersion == 0 and bumps a fresh resource to 1;
//   - updates/deletes must carry the version returned by the previous
//     successful Submit (0 for a resource that was deleted and recreated);
//   - submitting exactly the same desired state (spec, phase, deps) is a
//     no-op: same version returned, changed == false, no error.
type Declared struct {
	mu      sync.RWMutex
	records map[string]Declaration
	graph   *graph
}

func NewDeclared() *Declared {
	return &Declared{
		records: map[string]Declaration{},
		graph:   newGraph(),
	}
}

// declarationEqual compares the semantic payload (everything except the
// optimistic-concurrency token and submission timestamp).
func declarationEqual(a, b Declaration) bool {
	if a.Phase != b.Phase || !specEqual(a.Spec, b.Spec) {
		return false
	}
	if len(a.DependsOn) != len(b.DependsOn) {
		return false
	}
	// DependsOn is stored sorted/deduped, so positional compare is enough.
	for i := range a.DependsOn {
		if a.DependsOn[i] != b.DependsOn[i] {
			return false
		}
	}
	return true
}

// Submit validates and stores a declaration.
//
// Distinct rejection reasons:
//   - ErrInvalidSpec      image empty on a Present submission
//   - ErrAlreadyExists    ExpectedVersion==0 but resource exists
//   - ErrNotFound         ExpectedVersion!=0 but resource does not exist
//   - ErrVersionConflict  ExpectedVersion differs from the current version
//   - ErrDependencyCycle  new edges would introduce a cycle
func (d *Declared) Submit(decl Declaration) (newVersion int64, changed bool, err error) {
	if decl.ID == "" {
		return 0, false, &DependencyError{ID: decl.ID, Reason: ErrInvalidSpec, Detail: "empty id"}
	}

	// Normalize defaults up front so idempotence comparison is stable even
	// when callers omit Replicas/Labels repeatedly.
	decl.Spec = DefaultSpec(decl.Spec)
	if decl.Phase == PhasePresent {
		if err := ValidateSpec(decl.Spec); err != nil {
			return 0, false, &DependencyError{ID: decl.ID, Reason: err, Detail: "image is required"}
		}
	}
	decl.DependsOn = normalizeDeps(decl.DependsOn, decl.ID)

	d.mu.Lock()
	defer d.mu.Unlock()

	cur, exists := d.records[decl.ID]

	// Deleting something that was never declared (or already deleted) is an
	// idempotent no-op: version 0, changed=false.
	if decl.Phase == PhaseAbsent && !exists {
		return 0, false, nil
	}

	// Optimistic concurrency check.
	if decl.ExpectedVersion == 0 {
		if exists {
			return curVersion(cur, exists), false, &ConflictError{
				ID: decl.ID, Reason: ErrAlreadyExists,
				Expected: 0, Current: curVersion(cur, exists),
			}
		}
	} else {
		if !exists {
			return 0, false, &ConflictError{
				ID: decl.ID, Reason: ErrNotFound,
				Expected: decl.ExpectedVersion, Current: 0,
			}
		}
		if decl.ExpectedVersion != cur.ExpectedVersion {
			// Reuse the declaration's stored version token; cur.ExpectedVersion
			// holds the current version (see version bookkeeping below).
			return cur.ExpectedVersion, false, &ConflictError{
				ID: decl.ID, Reason: ErrVersionConflict,
				Expected: decl.ExpectedVersion, Current: cur.ExpectedVersion,
			}
		}
	}

	// Build the candidate graph and reject cycles before touching state.
	candidate := d.graph
	if decl.Phase == PhasePresent {
		candidate = candidate.withEdges(decl.ID, decl.DependsOn)
	} else {
		// Keep the vertex with both incoming edges (deletion protection)
		// and outgoing edges (deletion cascade).
		candidate = candidate.retainVertex(decl.ID)
	}
	live := make(map[ResourceID]bool, len(d.records)+1)
	for k := range d.records {
		live[k] = true
	}
	if decl.Phase == PhasePresent {
		live[decl.ID] = true
	}
	if candidate.hasCycle(live) {
		return curVersion(cur, exists), false, &DependencyError{
			ID:     decl.ID,
			Reason: ErrDependencyCycle,
			Detail: "cycle: " + joinIDs(candidate.cycleContaining(decl.ID)),
		}
	}

	// Idempotent fast path: identical declaration, no version bump.
	if exists && declarationEqual(cur, decl) {
		return cur.ExpectedVersion, false, nil
	}

	// Apply. The "current version" is carried in ExpectedVersion of the
	// stored record (clients echo it back). A fresh resource starts at 1.
	if decl.Phase == PhaseAbsent {
		// Deletion consumes the record; return the last known version so
		// callers can correlate the request.
		delete(d.records, decl.ID)
		d.graph = candidate
		return cur.ExpectedVersion, true, nil
	}
	var version int64
	if exists {
		version = cur.ExpectedVersion + 1
	} else {
		version = 1
	}
	decl.ExpectedVersion = version
	d.records[decl.ID] = decl
	d.graph = candidate
	return version, true, nil
}

// curVersion returns the current version of an existing record or 0.
func curVersion(cur Declaration, exists bool) int64 {
	if !exists {
		return 0
	}
	return cur.ExpectedVersion
}

// Get returns a stored declaration and its copy of dependencies.
func (d *Declared) Get(id string) (Declaration, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	cur, ok := d.records[id]
	if !ok {
		return Declaration{}, false
	}
	cur.Spec = DefaultSpec(cur.Spec)
	cur.DependsOn = append([]ResourceID(nil), cur.DependsOn...)
	return cur, true
}

// LiveDependents returns the IDs of resources that CURRENTLY declare a
// dependency on id, read under lock (snapshot-independent).
func (d *Declared) LiveDependents(id ResourceID) []ResourceID {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []ResourceID
	for v, rec := range d.records {
		for _, dep := range rec.DependsOn {
			if dep == id {
				out = append(out, v)
				break
			}
		}
	}
	sortStrings(out)
	return out
}

// Snapshot returns point-in-time copies of declarations and the graph.
func (d *Declared) Snapshot() (map[string]Declaration, *graph) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make(map[string]Declaration, len(d.records))
	for k, v := range d.records {
		v.Spec = DefaultSpec(v.Spec)
		v.DependsOn = append([]ResourceID(nil), v.DependsOn...)
		out[k] = v
	}
	return out, d.graph.clone()
}

// normalizeDeps sorts, dedupes and drops self edges.
func normalizeDeps(deps []ResourceID, self ResourceID) []ResourceID {
	if len(deps) == 0 {
		return nil
	}
	set := make(map[ResourceID]struct{}, len(deps))
	for _, dep := range deps {
		if dep != "" && dep != self {
			set[dep] = struct{}{}
		}
	}
	out := make([]ResourceID, 0, len(set))
	for dep := range set {
		out = append(out, dep)
	}
	sortStrings(out)
	return out
}
