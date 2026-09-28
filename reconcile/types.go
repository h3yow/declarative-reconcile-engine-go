// Package reconcile implements an in-memory declarative resource
// reconciliation engine: clients submit desired state, a background set of
// workers converges a simulated (in-memory) target environment to that
// state, with optimistic concurrency control, dependency ordering,
// bounded retries with backoff, drift repair and graceful shutdown.
package reconcile

import "time"

// ResourceID uniquely identifies a resource.
type ResourceID = string

// DefaultReplicas is applied when a submitted Spec omits Replicas (zero
// value). The defaulting rule is fixed and deterministic.
const DefaultReplicas int32 = 1

// Spec describes the desired configuration of a resource.
type Spec struct {
	Image    string            `json:"image"`
	Replicas int32             `json:"replicas"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// DefaultSpec returns a spec with deterministic defaults applied:
//   - Replicas <= 0 is replaced by DefaultReplicas;
//   - a nil Labels map becomes a non-nil empty map;
//   - the labels map is copied so callers cannot mutate stored state.
//
// Image is intentionally not defaulted; it is required and checked by
// ValidateSpec.
func DefaultSpec(s Spec) Spec {
	if s.Replicas <= 0 {
		s.Replicas = DefaultReplicas
	}
	if s.Labels == nil {
		s.Labels = map[string]string{}
	} else {
		copied := make(map[string]string, len(s.Labels))
		for k, v := range s.Labels {
			copied[k] = v
		}
		s.Labels = copied
	}
	return s
}

// ValidateSpec rejects specs that cannot be reconciled. Replicas values are
// left to defaulting; only an empty image is invalid.
func ValidateSpec(s Spec) error {
	if s.Image == "" {
		return ErrInvalidSpec
	}
	return nil
}

// specEqual reports whether two specs are identical after defaulting, so
// repeated identical submissions are detected regardless of map aliasing.
func specEqual(a, b Spec) bool {
	if a.Image != b.Image || a.Replicas != b.Replicas {
		return false
	}
	if len(a.Labels) != len(b.Labels) {
		return false
	}
	for k, v := range a.Labels {
		if b.Labels[k] != v {
			return false
		}
	}
	return true
}

// DesiredPhase is the lifecycle phase requested for a resource.
type DesiredPhase int

const (
	// PhasePresent means the resource should exist and match its spec.
	PhasePresent DesiredPhase = iota
	// PhaseAbsent means the resource should not exist.
	PhaseAbsent
)

func (p DesiredPhase) String() string {
	switch p {
	case PhasePresent:
		return "Present"
	case PhaseAbsent:
		return "Absent"
	default:
		return "Unknown"
	}
}

// Declaration is the full desired state record for one resource.
type Declaration struct {
	ID              string
	Spec            Spec
	Phase           DesiredPhase
	DependsOn       []ResourceID
	ExpectedVersion int64 // 0 means create; otherwise optimistic concurrency token
	SubmittedAt     time.Time
}

// Phase is the observed lifecycle phase of an actual resource.
type Phase int

const (
	PhaseUnknown Phase = iota
	PhaseCreating
	PhaseReady
	PhaseUpdating
	PhaseTerminating
	PhaseGone
)

func (p Phase) String() string {
	switch p {
	case PhaseCreating:
		return "Creating"
	case PhaseReady:
		return "Ready"
	case PhaseUpdating:
		return "Updating"
	case PhaseTerminating:
		return "Terminating"
	case PhaseGone:
		return "Gone"
	default:
		return "Unknown"
	}
}

// Condition is a single explainability entry in a resource's history.
type Condition struct {
	Time   time.Time
	Type   string
	Status string
	Reason string
}

// ResourceStatus is the observable status of an actual resource.
type ResourceStatus struct {
	ID         string
	Version    int64
	Phase      Phase
	Spec       Spec
	Ready      bool
	Reason     string
	ObservedAt time.Time
	Conditions []Condition
}

// cloneStatus returns a deep enough copy for safe reads outside the store.
func cloneStatus(in ResourceStatus) ResourceStatus {
	out := in
	if in.Spec.Labels != nil {
		labels := make(map[string]string, len(in.Spec.Labels))
		for k, v := range in.Spec.Labels {
			labels[k] = v
		}
		out.Spec.Labels = labels
	}
	if len(in.Conditions) > 0 {
		out.Conditions = append([]Condition(nil), in.Conditions...)
	}
	return out
}
