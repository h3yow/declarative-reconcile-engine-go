package dre

import (
	"testing"
	"time"
)

// testTimeout returns d enlarged under heavy instrumentation. The race
// detector can slow execution by 5-10x, so waits are scaled via an
// environment-independent heuristic: callers pass the normal budget and we
// multiply uniformly (cheap), keeping waits deterministic per build mode.
func testTimeout(d time.Duration) time.Duration {
	if raceEnabled {
		return d * 10
	}
	return d * 3
}

// raceEnabled is set in race_on_test.go (build tag race) and defaults false.
var raceEnabled = false

// testLogger prints every admission input and reconciliation judgement into
// the test log, making the decision basis observable as required.
type testLogger struct{ t *testing.T }

func (l testLogger) LogDecision(d Decision) {
	l.t.Logf("[DECISION] time=%s action=%s id=%s version=%d reason=%s detail=%s",
		d.Time.Format("15:04:05.000"), d.Action, d.ID, d.Version, d.Reason, d.Detail)
}

func (l testLogger) Logf(format string, args ...any) {
	l.t.Logf("[ENGINE] "+format, args...)
}

func newTestEngine(t *testing.T, workers int) (*Engine, *Store, *Target) {
	t.Helper()
	store := NewStore()
	target := NewTarget()
	eng := NewEngine(store, target, Config{
		Workers:        workers,
		OpTimeout:      500 * time.Millisecond,
		ResyncInterval: 25 * time.Millisecond,
		BackoffBase:    5 * time.Millisecond,
		BackoffMax:     100 * time.Millisecond,
		Logger:         testLogger{t: t},
	})
	return eng, store, target
}

func startEngine(t *testing.T, eng *Engine) func() {
	t.Helper()
	ctx := contextBackground()
	eng.Start(ctx)
	return func() {
		sctx, cancel := contextWithTimeout(2 * time.Second)
		defer cancel()
		if err := eng.Shutdown(sctx); err != nil {
			t.Logf("shutdown returned: %v", err)
		}
	}
}

func applyOK(t *testing.T, eng *Engine, d Desired) int64 {
	t.Helper()
	t.Logf("INPUT apply id=%s expected=%d spec=%v deps=%v",
		d.ID, d.ExpectedVersion, d.Spec, d.DependsOn)
	v, err := eng.Apply(d)
	if err != nil {
		t.Fatalf("apply id=%s failed: %v", d.ID, err)
	}
	return v
}

func expectConflict(t *testing.T, err error, want ConflictReason, id ResourceID) {
	t.Helper()
	ce, ok := AsConflict(err)
	if !ok {
		t.Fatalf("input id=%s: expected conflict %s, got %v", id, want, err)
	}
	if ce.Reason != want {
		t.Fatalf("input id=%s: expected reason %s, got %s (%s)", id, want, ce.Reason, ce.Message)
	}
	t.Logf("input id=%s: correctly rejected reason=%s expected=%d actual=%d -> %s",
		id, want, ce.Expected, ce.Actual, ce.Message)
}

func waitPhase(t *testing.T, eng *Engine, id ResourceID, want Phase, timeout time.Duration) Status {
	t.Helper()
	ctx, cancel := contextWithTimeout(testTimeout(timeout))
	defer cancel()
	if !eng.WaitFor(ctx, id, func(s Status) bool { return s.Phase == want }) {
		s, _ := eng.Status(id)
		t.Fatalf("id=%s never reached phase %s; last=%+v", id, want, s)
	}
	s, _ := eng.Status(id)
	t.Logf("id=%s reached phase=%s attempts=%d lastError=%q", id, s.Phase, s.Attempts, s.LastError)
	return s
}
