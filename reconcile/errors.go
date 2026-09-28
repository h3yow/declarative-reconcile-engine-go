package reconcile

import (
	"errors"
	"fmt"
)

// Distinct, inspectable failure reasons. Callers can use errors.Is to
// distinguish conflict causes.
var (
	// ErrVersionConflict: an update/delete carried an ExpectedVersion older
	// (or otherwise different) from the current declared version.
	ErrVersionConflict = errors.New("version conflict: expected version does not match current")
	// ErrAlreadyExists: create (ExpectedVersion==0) targeted an existing resource.
	ErrAlreadyExists = errors.New("resource already exists")
	// ErrNotFound: the target resource does not exist.
	ErrNotFound = errors.New("resource not found")
	// ErrInvalidSpec: a submitted spec failed validation.
	ErrInvalidSpec = errors.New("invalid spec")
	// ErrDependencyCycle: the new dependency edges would create a cycle.
	ErrDependencyCycle = errors.New("dependency cycle detected")
	// ErrDependencyMissing: a declared dependency does not exist.
	ErrDependencyMissing = errors.New("dependency missing")
	// ErrTransient marks a temporary backend failure (safe to retry).
	ErrTransient = errors.New("transient backend failure")
	// ErrStopped: the engine is stopping/shut down and rejects new work.
	ErrStopped = errors.New("engine stopped")
)

// ConflictError carries machine-inspectable details for a rejected submit.
// Use errors.Is(err, ErrVersionConflict) / ErrAlreadyExists / ErrNotFound to
// distinguish the reason.
type ConflictError struct {
	ID       string
	Reason   error
	Expected int64
	Current  int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s: id=%q expectedVersion=%d currentVersion=%d",
		e.Reason, e.ID, e.Expected, e.Current)
}

func (e *ConflictError) Unwrap() error { return e.Reason }

// DependencyError describes why dependency validation failed.
type DependencyError struct {
	ID     string
	Reason error
	Detail string
}

func (e *DependencyError) Error() string {
	return fmt.Sprintf("%s: id=%q detail=%s", e.Reason, e.ID, e.Detail)
}

func (e *DependencyError) Unwrap() error { return e.Reason }
