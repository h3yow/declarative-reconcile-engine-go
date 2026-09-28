package dre

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConflictReasons verifies each stale/version rejection is distinguished
// by reason instead of silently overwriting desired state.
func TestConflictReasons(t *testing.T) {
	eng, _, _ := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	t.Log("INPUT create id=a expected=-1")
	v := applyOK(t, eng, Desired{ID: "a", ExpectedVersion: -1})
	waitPhase(t, eng, "a", PhaseHealthy, time.Second)

	// create again with -1 -> MustNotExist
	t.Log("INPUT create id=a expected=-1 (duplicate create)")
	_, err := eng.Apply(Desired{ID: "a", ExpectedVersion: -1})
	expectConflict(t, err, ReasonMustNotExist, "a")

	// update with wrong version -> StaleVersion (actual version shown)
	t.Logf("INPUT update id=a expected=%d (stale)", v+99)
	_, err = eng.Apply(Desired{ID: "a", ExpectedVersion: v + 99})
	expectConflict(t, err, ReasonStaleVersion, "a")

	// update unknown resource -> MustExist
	t.Log("INPUT update id=missing expected=5")
	_, err = eng.Apply(Desired{ID: "missing", ExpectedVersion: 5})
	expectConflict(t, err, ReasonMustExist, "missing")

	// version 0 -> InvalidVersion
	t.Log("INPUT update id=a expected=0")
	_, err = eng.Apply(Desired{ID: "a", ExpectedVersion: 0})
	expectConflict(t, err, ReasonInvalidVersion, "a")

	// stale delete -> StaleVersion
	t.Log("INPUT delete id=a expected=999")
	err = eng.Delete("a", 999)
	expectConflict(t, err, ReasonStaleVersion, "a")

	// delete unknown -> MustExist
	t.Log("INPUT delete id=ghost expected=1")
	err = eng.Delete("ghost", 1)
	expectConflict(t, err, ReasonMustExist, "ghost")

	// state unchanged after all rejections
	rec, ok := eng.store.Get("a")
	if !ok || rec.version != v {
		t.Fatalf("state mutated by rejected submissions: %+v ok=%v", rec, ok)
	}
	t.Logf("JUDGEMENT: stored version remains %d after rejections", rec.version)
}

// TestIdempotentSubmission verifies repeated identical desired states cause
// no version bump and no additional reconciliation work.
func TestIdempotentSubmission(t *testing.T) {
	eng, _, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	v := applyOK(t, eng, Desired{ID: "a", ExpectedVersion: -1,
		Spec: Spec{"replicas": "2"}})
	waitPhase(t, eng, "a", PhaseHealthy, time.Second)

	// Wait until the initial convergence settles (1 create + 1 ready).
	waitTargetCounts(t, target, time.Second, func(c map[string]int) bool {
		return c["create"] == 1 && c["ready"] == 1
	})
	before := target.OpCounts()

	for i := 0; i < 10; i++ {
		vv, err := eng.Apply(Desired{ID: "a", ExpectedVersion: v,
			Spec: Spec{"replicas": "2"}})
		if err != nil || vv != v {
			t.Fatalf("identical apply #%d v=%d err=%v", i, vv, err)
		}
	}
	// identical submissions must trigger zero new target mutations;
	// allow the periodic resync to run and prove it performs no writes.
	time.Sleep(120 * time.Millisecond)
	after := target.OpCounts()
	noopCount := countAction(eng.Decisions(), "NoopApply")
	t.Logf("JUDGEMENT: target op counts before=%v after=%v noopApplies=%d version stays=%d",
		before, after, noopCount, v)
	for op, n := range before {
		if after[op] != n {
			t.Fatalf("identical submissions caused extra %q operations: %d -> %d", op, n, after[op])
		}
	}
	if after["update"] != 0 {
		t.Fatalf("identical submissions produced updates: %d", after["update"])
	}
	if noopCount != 10 {
		t.Fatalf("want 10 NoopApply decisions, got %d", noopCount)
	}
}

func waitTargetCounts(t *testing.T, target *Target, timeout time.Duration,
	pred func(map[string]int) bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred(target.OpCounts()) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("target op counts never settled: %v", target.OpCounts())
}

// TestDefaultsDeterministic verifies missing fields receive fixed defaults
// at admission and repeated partial submissions converge.
func TestDefaultsDeterministic(t *testing.T) {
	s := NewStore()
	d := Desired{ID: "a", ExpectedVersion: -1}
	ApplyDefaults(&d)
	if d.Kind != defaultKind || d.Spec["replicas"] != "1" {
		t.Fatalf("defaults not applied: %+v", d)
	}
	v, _, err := s.Apply(d)
	if err != nil {
		t.Fatal(err)
	}
	// resubmitting the original partial declaration defaults identically
	d2 := Desired{ID: "a", ExpectedVersion: v}
	ApplyDefaults(&d2)
	v2, changed, err := s.Apply(d2)
	if err != nil || changed || v2 != v {
		t.Fatalf("partial resubmission must be no-op: v=%d changed=%v err=%v", v2, changed, err)
	}
	t.Logf("JUDGEMENT: defaulted spec=%v version=%d stable", d2.Spec, v2)
}

// TestDependencyCycleRejected verifies cyclic dependencies fail admission.
func TestDependencyCycleRejected(t *testing.T) {
	eng, _, _ := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	applyOK(t, eng, Desired{ID: "a", ExpectedVersion: -1, DependsOn: []ResourceID{"c"}})
	applyOK(t, eng, Desired{ID: "b", ExpectedVersion: -1, DependsOn: []ResourceID{"a"}})
	// c depends on b -> cycle c -> a -> c? a depends on c already: adding c
	// with dep b creates b -> a -> c -> b.
	_, err := eng.Apply(Desired{ID: "c", ExpectedVersion: -1, DependsOn: []ResourceID{"b"}})
	expectConflict(t, err, ReasonDependencyCycle, "c")
}

// TestDeleteBlockedByDependent verifies deleting a depended-on live resource
// is rejected rather than orphaning the dependent.
func TestDeleteBlockedByDependent(t *testing.T) {
	eng, _, _ := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	va := applyOK(t, eng, Desired{ID: "a", ExpectedVersion: -1})
	waitPhase(t, eng, "a", PhaseHealthy, time.Second)
	applyOK(t, eng, Desired{ID: "b", ExpectedVersion: -1, DependsOn: []ResourceID{"a"}})
	waitPhase(t, eng, "b", PhaseHealthy, time.Second)

	t.Log("INPUT delete id=a while b depends on it")
	if err := eng.Delete("a", va); err == nil {
		t.Fatal("expected delete blocked, got nil")
	} else {
		expectConflict(t, err, ReasonDependencyBlocksDelete, "a")
	}
}

// TestConcurrentApplyLinearizability hammers one resource concurrently:
// exactly one create wins; all subsequent stale creates are MustNotExist;
// valid versioned updates serialize and each wins a distinct new version.
func TestConcurrentApplyLinearizability(t *testing.T) {
	eng, _, _ := newTestEngine(t, 4)
	stop := startEngine(t, eng)
	defer stop()

	const n = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	winVersions := map[int64]int{}
	reasons := map[ConflictReason]int{}

	// everyone races a create with -1
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := eng.Apply(Desired{ID: "race", ExpectedVersion: -1})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if ce, ok := AsConflict(err); ok {
					reasons[ce.Reason]++
				}
			} else {
				winVersions[v]++
			}
		}()
	}
	wg.Wait()
	if len(winVersions) != 1 {
		t.Fatalf("expected exactly one winning create, got versions=%v", winVersions)
	}
	if reasons[ReasonMustNotExist] != n-1 {
		t.Fatalf("expected %d MustNotExist, got %v", n-1, reasons)
	}
	t.Logf("JUDGEMENT: 1 winner=%v, %d duplicates rejected MustNotExist", winVersions, reasons[ReasonMustNotExist])

	// Contended optimistic updates: each goroutine CAS-retries using the
	// Actual version reported in the StaleVersion conflict. Every update
	// carries unique content so each successful admission bumps the version.
	var wg2 sync.WaitGroup
	var cur int64
	for v := range winVersions {
		cur = v
	}
	var staleRetries int
	for i := 0; i < n; i++ {
		wg2.Add(1)
		i := i
		go func() {
			defer wg2.Done()
			base := cur // intentionally stale; everyone starts from version 1
			for {
				_, err := eng.Apply(Desired{ID: "race", ExpectedVersion: base,
					Spec: Spec{"replicas": fmt.Sprintf("%d", i+100)}})
				if err == nil {
					return
				}
				ce, ok := AsConflict(err)
				if !ok || ce.Reason != ReasonStaleVersion {
					t.Errorf("unexpected update error: %v", err)
					return
				}
				mu.Lock()
				staleRetries++
				mu.Unlock()
				base = ce.Actual // retry on top of the newest version
			}
		}()
	}
	wg2.Wait()
	waitPhase(t, eng, "race", PhaseHealthy, 2*time.Second)
	rec, _ := eng.store.Get("race")
	t.Logf("JUDGEMENT: final stored version=%d after %d contended updates (stale retries=%d)", rec.version, n, staleRetries)
	if rec.version != cur+int64(n) {
		t.Fatalf("expected final version %d, got %d", cur+int64(n), rec.version)
	}
}

func countAction(ds []Decision, action string) int {
	n := 0
	for _, d := range ds {
		if d.Action == action {
			n++
		}
	}
	return n
}
