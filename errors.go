package dre

import (
	"errors"
	"fmt"
)

// ConflictReason enumerates the distinguishable rejection reasons.
type ConflictReason string

const (
	// ReasonStaleVersion: submitted expected version differs from stored version.
	ReasonStaleVersion ConflictReason = "StaleVersion"
	// ReasonMustNotExist: create precondition (-1) but resource already declared.
	ReasonMustNotExist ConflictReason = "MustNotExist"
	// ReasonMustExist: update/delete against a resource that is not declared.
	ReasonMustExist ConflictReason = "MustExist"
	// ReasonInvalidVersion: expected version 0 or otherwise malformed.
	ReasonInvalidVersion ConflictReason = "InvalidVersion"
	// ReasonDependencyCycle: dependency graph has a cycle.
	ReasonDependencyCycle ConflictReason = "DependencyCycle"
	// ReasonDependencyBlocksDelete: resource still depended on by others.
	ReasonDependencyBlocksDelete ConflictReason = "DependencyBlocksDelete"
	// ReasonDeleting: operation targets a resource whose deletion was
	// admitted and is being finalized; deletion is terminal - clients must
	// wait for completion before re-creating.
	ReasonDeleting ConflictReason = "Deleting"
)

// ConflictError is returned when a submission is rejected for a reason the
// caller can distinguish programmatically. The submission never mutates state.
type ConflictError struct {
	ID       ResourceID
	Reason   ConflictReason
	Expected int64
	Actual   int64
	Message  string
}

func (e *ConflictError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// AsConflict extracts a *ConflictError from err if present.
func AsConflict(err error) (*ConflictError, bool) {
	var ce *ConflictError
	if errors.As(err, &ce) {
		return ce, true
	}
	return nil, false
}

// TargetError represents a failure reported by the simulated target
// environment. Transient failures are retried by the engine; permanent ones
// mark the resource Error without busy-looping.
type TargetError struct {
	Op        string
	ID        ResourceID
	Transient bool
	Message   string
}

func (e *TargetError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("target %s %s: %s", e.Op, e.ID, e.Message)
}

// IsRetryable reports whether err represents a transient target-side failure.
func IsRetryable(err error) bool {
	var te *TargetError
	return errors.As(err, &te) && te.Transient
}
