// Package dre implements a local, in-memory declarative reconciliation engine.
//
// It simulates a target environment entirely in memory so that desired-state
// declarations can be reconciled against actual state under concurrency,
// version conflicts, dependency ordering and injected failures.
package dre

import "time"

// ResourceID uniquely identifies a resource within the engine.
type ResourceID string

// Spec is the declarative configuration payload of a resource.
type Spec map[string]string

// Desired is a desired-state declaration submitted by a client.
type Desired struct {
	// ID is the resource identifier.
	ID ResourceID
	// Kind is the resource type.
	Kind string
	// ExpectedVersion is the optimistic-concurrency precondition:
	//   -1 means the resource must not currently be declared (create);
	//   any positive value must equal the server-assigned version (update);
	//   0 is invalid and rejected.
	ExpectedVersion int64
	// Spec is the desired configuration; missing keys receive deterministic
	// defaults at admission time.
	Spec Spec
	// DependsOn lists resources that must exist and be ready before this
	// resource is created/updated. Cycles are rejected at admission.
	DependsOn []ResourceID
}

// Observed is the actual state of a resource in the simulated environment.
type Observed struct {
	ID                ResourceID
	Kind              string
	Spec              Spec
	Ready             bool
	Exists            bool
	MarkedForDeletion bool
}

// Phase is the lifecycle phase of a declared resource.
type Phase string

const (
	PhasePending     Phase = "Pending"
	PhaseProgressing Phase = "Progressing"
	PhaseHealthy     Phase = "Healthy"
	PhaseError       Phase = "Error"
	PhaseDeleting    Phase = "Deleting"
	// PhaseDeleted is a terminal bookkeeping phase: the target resource is
	// gone and the declaration has been purged from the desired store.
	PhaseDeleted Phase = "Deleted"
)

// Condition records one reconciliation judgement for explainability.
type Condition struct {
	Time    time.Time
	Type    string
	Reason  string
	Message string
}

// Status is the observable reconciliation status of one resource.
type Status struct {
	ID             ResourceID
	Phase          Phase
	Version        int64
	Attempts       int
	LastError      string
	Conditions     []Condition
	LastReconciled time.Time
}

// Decision is a structured record of an engine judgement, used for logging.
type Decision struct {
	Time    time.Time
	Action  string
	ID      ResourceID
	Reason  string
	Detail  string
	Version int64
}
