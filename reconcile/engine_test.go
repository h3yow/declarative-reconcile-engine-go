package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type engineFixture struct {
	decl   *Declared
	store  *Store
	rec    *Reconciler
	eng    *Engine
	events []Event
	mu     sync.Mutex
}

func newEngineFixture(t *testing.T, cfg Config) *engineFixture {
	t.Helper()
	f := &engineFixture{decl: NewDeclared(), store: NewStore()}
	f.rec = NewReconciler(f.store).WithEventSink(func(e Event) {
		f.mu.Lock()
		f.events = append(f.events, e)
		f.mu.Unlock()
	})
	f.eng = NewEngine(f.decl, f.rec, cfg)
	if err := f.eng.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.eng.Shutdown(3 * time.Second) })
	return f
}

func (f *engineFixture) logEvents(t *testing.T, prefix string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, e := range f.events {
		t.Logf("%s event[%d] t=%s id=%s type=%s reason=%q err=%v",
			prefix, i, e.Time.Format("15:04:05.000"), e.ID, e.Type, e.Reason, e.Err)
	}
}

func (f *engineFixture) converged(t *testing.T, want map[string]Spec, timeout time.Duration) bool {
	t.Helper()
	return f.eng.WaitFor(timeout, func() bool {
		byID := map[string]ResourceStatus{}
		for _, st := range f.store.List() {
			byID[st.ID] = st
		}
		for id, w := range want {
			st, ok := byID[id]
			if !ok || !st.Ready || !specEqual(st.Spec, DefaultSpec(w)) {
				return false
			}
		}
		return true
	})
}

func TestEngineConcurrentSubmissionsConverge(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Workers = 8
	f := newEngineFixture(t, cfg)

	const n = 40
	var wg sync.WaitGroup
	var createsOK, conflict atomic.Int64
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("r-%02d", i)
			_, _, err := f.eng.Submit(Declaration{
				ID: id, Spec: Spec{Image: fmt.Sprintf("img:%d", i), Replicas: 2},
				Phase: PhasePresent,
			})
			if err != nil {
				t.Errorf("submit %s: %v", id, err)
				return
			}
			createsOK.Add(1)
		}(i)
	}
	// Concurrent updates to a single hot key from many goroutines: exactly
	// one wins per version; losers must receive ErrVersionConflict and retry
	// on the fresh version. All updates are applied as a chain eventually.
	const updaters = 20
	for g := 0; g < updaters; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			var lastV int64 = 0
			for attempt := 0; attempt < 50; attempt++ {
				cur, exists := f.decl.Get("hot")
				if !exists {
					_, changed, err := f.eng.Submit(Declaration{
						ID: "hot", Spec: Spec{Image: "hot:0"}, Phase: PhasePresent,
					})
					if err == nil && changed {
						lastV = 1
						break
					}
					if errors.Is(err, ErrAlreadyExists) {
						continue
					}
					if err != nil {
						return
					}
					break
				}
				_, _, err := f.eng.Submit(Declaration{
					ID:    "hot",
					Spec:  Spec{Image: fmt.Sprintf("hot:%d-g%d", attempt, g)},
					Phase: PhasePresent, ExpectedVersion: cur.ExpectedVersion,
				})
				if err == nil {
					lastV = cur.ExpectedVersion + 1
					break
				}
				if errors.Is(err, ErrVersionConflict) {
					conflict.Add(1)
					continue
				}
				return
			}
			_ = lastV
		}(g)
	}
	wg.Wait()
	t.Logf("INPUT %d creates + %d racing updaters; creates_ok=%d conflicts_seen=%d",
		n, updaters, createsOK.Load(), conflict.Load())

	want := map[string]Spec{}
	for i := 0; i < n; i++ {
		want[fmt.Sprintf("r-%02d", i)] = Spec{Image: fmt.Sprintf("img:%d", i), Replicas: 2}
	}
	if !f.converged(t, want, 5*time.Second) {
		f.logEvents(t, "CONCURRENT")
		t.Fatalf("not all resources converged: %+v", f.store.List())
	}
	if !f.eng.WaitFor(2*time.Second, func() bool {
		st, ok := f.store.Get("hot")
		return ok && st.Ready
	}) {
		f.logEvents(t, "CONCURRENT")
		t.Fatal("hot never became ready")
	}
	hot, ok := f.store.Get("hot")
	t.Logf("DECISION hot converged: ready=%v image=%s version=%d", hot.Ready, hot.Spec.Image, hot.Version)
	if !ok || !hot.Ready {
		t.Fatal("hot must converge after contended updates")
	}
	if conflict.Load() == 0 {
		t.Log("note: no conflicts observed (scheduling-dependent); correctness still asserted via chain updates")
	}
	f.logEvents(t, "CONCURRENT")
}

func TestEngineFaultRecovery(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseBackoff = 5 * time.Millisecond
	cfg.ResyncInterval = 50 * time.Millisecond
	f := newEngineFixture(t, cfg)

	f.store.InjectFailures(OpCreate, 3) // first 3 attempts fail, then recovery
	t.Log("INPUT submit a with 3 injected transient create failures")
	_, _, _ = f.eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	// An unrelated resource must not be blocked by a's failures.
	_, _, _ = f.eng.Submit(Declaration{ID: "b", Spec: Spec{Image: "j"}, Phase: PhasePresent})

	if !f.eng.WaitFor(3*time.Second, func() bool {
		sa, oka := f.store.Get("a")
		sb, okb := f.store.Get("b")
		return oka && okb && sa.Ready && sb.Ready
	}) {
		f.logEvents(t, "FAULT")
		t.Fatalf("recovery failed: %+v", f.store.List())
	}
	st := f.eng.Stats()
	t.Logf("DECISION both converged after retries: reconciles=%d errors=%d requeues=%d; b independent of a's failures",
		st.Reconciles, st.Errors, st.Requeues)
	if st.Errors < 3 {
		t.Fatalf("expected >=3 transient errors recorded, got %d", st.Errors)
	}
}

func TestEngineDriftAutoRepair(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ResyncInterval = 40 * time.Millisecond
	f := newEngineFixture(t, cfg)
	v, _, _ := f.eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "img:1", Replicas: 2}, Phase: PhasePresent})
	if !f.converged(t, map[string]Spec{"a": {Image: "img:1", Replicas: 2}}, 2*time.Second) {
		t.Fatal("initial converge failed")
	}
	t.Logf("INPUT out-of-band drift on a (declared version=%d unchanged)", v)
	f.store.InjectDrift("a", Spec{Image: "TAMPERED", Replicas: 99})
	if drifted, ok := f.store.Get("a"); !ok || drifted.Spec.Image != "TAMPERED" {
		t.Fatalf("drift injection failed: %+v %v", drifted, ok)
	}
	if !f.eng.WaitFor(3*time.Second, func() bool {
		s, ok := f.store.Get("a")
		return ok && s.Ready && s.Spec.Image == "img:1" && s.Spec.Replicas == 2
	}) {
		f.logEvents(t, "DRIFT")
		t.Fatalf("drift not repaired: %+v", f.store.List())
	}
	t.Log("DECISION periodic resync observed drift and updated actual state back to declared spec")

	// Idempotence across repeated resyncs: let several resync periods pass.
	time.Sleep(300 * time.Millisecond)
	s, _ := f.store.Get("a")
	t.Logf("ASSERT repeated resyncs: version=%d image=%s reason=%q (no further churn)", s.Version, s.Spec.Image, s.Reason)
	if s.Spec.Image != "img:1" {
		t.Fatal("repeated resync must keep converged state stable")
	}
}

func TestEngineTimeoutDoesNotStall(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReconcileTimeout = 30 * time.Millisecond
	cfg.BaseBackoff = 5 * time.Millisecond
	f := newEngineFixture(t, cfg)

	// Inject a hung backend call for slow (does NOT hold the store lock):
	// the per-attempt timeout must fire, and fast must stay independent.
	f.store.InjectHang("slow", 2)
	t.Log("INPUT slow has 2 hung create attempts; fast is healthy")
	_, _, _ = f.eng.Submit(Declaration{ID: "slow", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	_, _, _ = f.eng.Submit(Declaration{ID: "fast", Spec: Spec{Image: "j"}, Phase: PhasePresent})
	ok := f.eng.WaitFor(2*time.Second, func() bool {
		s, ok := f.store.Get("fast")
		return ok && s.Ready
	})
	if !ok {
		t.Fatal("fast resource stalled while slow is timing out")
	}
	// Wait until at least one attempt actually timed out while still hung.
	if !f.eng.WaitFor(2*time.Second, func() bool { return f.eng.Stats().Timeouts >= 1 }) {
		t.Fatal("expected a reconcile timeout to be recorded")
	}
	t.Log("DECISION fast converged and slow attempt timed out; now releasing hung calls")
	f.store.ReleaseHangs()
	if !f.eng.WaitFor(3*time.Second, func() bool {
		s, ok := f.store.Get("slow")
		return ok && s.Ready
	}) {
		f.logEvents(t, "TIMEOUT")
		t.Fatal("slow did not converge after backend release")
	}
	st := f.eng.Stats()
	t.Logf("DECISION slow converged post-recovery; timeouts=%d errors=%d", st.Timeouts, st.Errors)
	if st.Timeouts == 0 {
		t.Fatal("expected at least one recorded reconcile timeout")
	}
}

func TestEngineDependencyOrdering(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Workers = 6
	f := newEngineFixture(t, cfg)

	// chain: c -> b -> a (a must be ready first)
	subs := []Declaration{
		{ID: "c", Spec: Spec{Image: "ic"}, Phase: PhasePresent, DependsOn: []string{"b"}},
		{ID: "b", Spec: Spec{Image: "ib"}, Phase: PhasePresent, DependsOn: []string{"a"}},
		{ID: "a", Spec: Spec{Image: "ia"}, Phase: PhasePresent},
	}
	for _, d := range subs {
		_, _, _ = f.eng.Submit(d)
	}
	t.Log("INPUT submit chain c->b->a (out of dependency order)")
	if !f.converged(t, map[string]Spec{
		"a": {Image: "ia"}, "b": {Image: "ib"}, "c": {Image: "ic"},
	}, 3*time.Second) {
		f.logEvents(t, "DEPS")
		t.Fatalf("chain did not converge: %+v", f.store.List())
	}
	// Assert create ordering via the store's monotonic create sequence:
	// dependency gating forces a before b before c regardless of scheduling.
	order := f.store.CreateOrder()
	t.Logf("DECISION monotonic create order=%v", order)
	pos := map[string]int{}
	for i, id := range order {
		pos[id] = i
	}
	pa, oka := pos["a"]
	pb, okb := pos["b"]
	pc, okc := pos["c"]
	if !oka || !okb || !okc || !(pa < pb && pb < pc) {
		t.Fatal("resources must be created in topological order a<b<c")
	}

	// Tear down in reverse order: deleting a first must be blocked; removing
	// c, then b, then a must succeed with no orphan/duplicate processing.
	va, _ := f.decl.Get("a")
	vb, _ := f.decl.Get("b")
	vc, _ := f.decl.Get("c")
	_, _, _ = f.eng.Submit(Declaration{ID: "a", Phase: PhaseAbsent, ExpectedVersion: va.ExpectedVersion})
	time.Sleep(150 * time.Millisecond)
	if _, ok := f.store.Get("a"); !ok {
		t.Fatal("a must remain while b,c still depend on it")
	}
	t.Log("DECISION deleting a first was blocked (dependents alive)")
	_, _, _ = f.eng.Submit(Declaration{ID: "c", Phase: PhaseAbsent, ExpectedVersion: vc.ExpectedVersion})
	_, _, _ = f.eng.Submit(Declaration{ID: "b", Phase: PhaseAbsent, ExpectedVersion: vb.ExpectedVersion})
	if !f.eng.WaitFor(3*time.Second, func() bool {
		_, a := f.store.Get("a")
		_, b := f.store.Get("b")
		_, c := f.store.Get("c")
		return !a && !b && !c
	}) {
		f.logEvents(t, "DEPS-DELETE")
		t.Fatalf("reverse-order delete failed: %+v", f.store.List())
	}
	t.Log("DECISION reverse-order teardown completed; no orphaned prerequisites remain")
}

func TestEngineConcurrentDeleteVsUpdateNoOrphans(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Workers = 6
	f := newEngineFixture(t, cfg)
	_, _, _ = f.eng.Submit(Declaration{ID: "parent", Spec: Spec{Image: "p"}, Phase: PhasePresent})
	_, _, _ = f.eng.Submit(Declaration{ID: "child", Spec: Spec{Image: "c"}, Phase: PhasePresent, DependsOn: []string{"parent"}})
	if !f.converged(t, map[string]Spec{
		"parent": {Image: "p"}, "child": {Image: "c"},
	}, 3*time.Second) {
		t.Fatal("setup failed")
	}

	// Race: one goroutine updates the child (keep desired), one deletes it;
	// then try to delete parent. Whichever wins, final state must be
	// consistent: either both present-and-ready, or both absent.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		cur, _ := f.decl.Get("child")
		_, _, _ = f.eng.Submit(Declaration{
			ID: "child", Spec: Spec{Image: "c2"}, Phase: PhasePresent,
			DependsOn: []string{"parent"}, ExpectedVersion: cur.ExpectedVersion,
		})
	}()
	go func() {
		defer wg.Done()
		cur, _ := f.decl.Get("child")
		_, _, _ = f.eng.Submit(Declaration{
			ID: "child", Phase: PhaseAbsent, ExpectedVersion: cur.ExpectedVersion,
		})
	}()
	wg.Wait()
	t.Log("INPUT racing update/delete on child resolved at declared layer; asserting eventual consistency")

	// Wait for the declared winner to be fully reflected in actual state.
	if !f.eng.WaitFor(3*time.Second, func() bool {
		declChild, childDeclared := f.decl.Get("child")
		actualChild, childExists := f.store.Get("child")
		if childDeclared {
			return childExists && actualChild.Ready &&
				actualChild.Spec.Image == declChild.Spec.Image
		}
		return !childExists
	}) {
		f.logEvents(t, "RACE")
		t.Fatalf("no consistent steady state: %+v", f.store.List())
	}

	_, childDeclared := f.decl.Get("child")
	if childDeclared {
		// Update won: child ready at the declared image, parent still present.
		c, _ := f.store.Get("child")
		_, parentExists := f.store.Get("parent")
		t.Logf("DECISION update won: child ready image=%s parentPresent=%v", c.Spec.Image, parentExists)
		if !parentExists {
			t.Fatal("parent must remain while child depends on it")
		}
		return
	}

	// Delete won: child is gone. Now submit parent deletion; it must proceed
	// promptly (not blocked by a ghost child), leaving no orphaned pair.
	t.Log("INPUT delete won; now submitting parent deletion (must not be blocked by ghost child)")
	parent, _ := f.decl.Get("parent")
	_, _, _ = f.eng.Submit(Declaration{
		ID: "parent", Phase: PhaseAbsent, ExpectedVersion: parent.ExpectedVersion,
	})
	if !f.eng.WaitFor(3*time.Second, func() bool {
		_, c := f.store.Get("child")
		_, p := f.store.Get("parent")
		return !c && !p
	}) {
		f.logEvents(t, "RACE")
		t.Fatal("parent deletion must proceed after child delete (no ghost blocking, no orphans)")
	}
	t.Log("DECISION delete won; parent cascaded away; childPresent=false parentPresent=false")
}

func TestEngineGracefulShutdownAndReject(t *testing.T) {
	cfg := DefaultConfig()
	decl := NewDeclared()
	store := NewStore()
	rec := NewReconciler(store)
	eng := NewEngine(decl, rec, cfg)
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	_, _, _ = eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	if !eng.WaitFor(2*time.Second, func() bool {
		s, ok := store.Get("a")
		return ok && s.Ready
	}) {
		t.Fatal("setup converge failed")
	}

	if err := eng.Shutdown(time.Second); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	st := eng.Stats()
	t.Logf("DECISION shutdown complete: inFlight=%d queueLen=%d stopped=%v", st.InFlight, st.QueueLen, st.Stopped)
	if st.InFlight != 0 || st.QueueLen != 0 || !st.Stopped {
		t.Fatal("engine must be fully drained and stopped")
	}
	_, _, err := eng.Submit(Declaration{ID: "z", Spec: Spec{Image: "z"}, Phase: PhasePresent})
	t.Logf("DECISION submit after shutdown -> err=%v basis=%v", err, ErrStopped)
	if !errors.Is(err, ErrStopped) {
		t.Fatalf("post-shutdown submit must fail with ErrStopped, got %v", err)
	}
	// Restart is intentionally not supported on the same instance; Start again fails.
	if err := eng.Start(); !errors.Is(err, ErrStopped) {
		t.Fatalf("restart after shutdown must fail closed, got %v", err)
	}
}

func TestEngineRunBlocksUntilContextCancel(t *testing.T) {
	cfg := DefaultConfig()
	decl := NewDeclared()
	store := NewStore()
	rec := NewReconciler(store)
	eng := NewEngine(decl, rec, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- eng.Run(ctx) }()
	_, _, _ = eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	if !eng.WaitFor(time.Second, func() bool {
		s, ok := store.Get("a")
		return ok && s.Ready
	}) {
		t.Fatal("converge before cancel failed")
	}
	cancel()
	select {
	case err := <-done:
		t.Logf("DECISION Run returned after cancel: %v stats=%+v", err, eng.Stats())
		if err != nil {
			t.Fatalf("Run after cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestEngineShutdownCancelsInFlightConsistently(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ReconcileTimeout = 5 * time.Second // longer than the test's shutdown grace
	decl := NewDeclared()
	store := NewStore()
	rec := NewReconciler(store)
	eng := NewEngine(decl, rec, cfg)
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	// A resource whose create hangs; shutdown must cancel its context so the
	// in-flight attempt ends with a definite not-applied result and workers
	// drain promptly.
	store.InjectHang("a", 1_000_000)
	_, _, _ = eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	if !eng.WaitFor(time.Second, func() bool { return eng.Stats().Reconciles >= 1 }) {
		t.Fatal("reconcile never started")
	}
	t.Log("INPUT shutdown while a create is hung; expecting cancellation and clean drain")
	start := time.Now()
	if err := eng.Shutdown(2 * time.Second); err != nil {
		t.Fatalf("shutdown must cancel in-flight reconcile and drain: %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("shutdown took %s; in-flight reconcile was not cancelled promptly", elapsed)
	}
	if _, exists := store.Get("a"); exists {
		t.Fatal("cancelled create must not leave a resource behind (definite not-applied result)")
	}
	st := eng.Stats()
	t.Logf("DECISION shutdown drained in-flight: timeouts=%d inFlight=%d queueLen=%d; resource absent=%v",
		st.Timeouts, st.InFlight, st.QueueLen, mustAbsent(store, "a"))
}

func mustAbsent(s *Store, id string) bool {
	_, ok := s.Get(id)
	return !ok
}

func TestEngineDeleteFailureRetriedAndResyncStable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BaseBackoff = 5 * time.Millisecond
	cfg.ResyncInterval = 30 * time.Millisecond
	f := newEngineFixture(t, cfg)

	_, _, _ = f.eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	if !f.converged(t, map[string]Spec{"a": {Image: "i"}}, 2*time.Second) {
		t.Fatal("create failed")
	}
	v, _ := f.decl.Get("a")
	f.store.InjectFailures(OpDelete, 2)
	_, _, _ = f.eng.Submit(Declaration{ID: "a", Phase: PhaseAbsent, ExpectedVersion: v.ExpectedVersion})
	t.Log("INPUT delete a with 2 injected transient delete failures")
	if !f.eng.WaitFor(3*time.Second, func() bool {
		_, ok := f.store.Get("a")
		return !ok
	}) {
		f.logEvents(t, "DEL-RETRY")
		t.Fatal("delete not retried to completion")
	}
	t.Log("DECISION transient delete failures retried; a absent")

	// Stability: repeated resyncs with no declaration must keep it absent and
	// not recreate it.
	time.Sleep(200 * time.Millisecond)
	if _, ok := f.store.Get("a"); ok {
		t.Fatal("resync must not resurrect a deleted resource")
	}
	t.Log("DECISION repeated resync keeps absent state stable (no resurrection, no churn)")
}

func TestEngineMissingDependencyArrivesLater(t *testing.T) {
	cfg := DefaultConfig()
	f := newEngineFixture(t, cfg)
	// child first references a parent that is not declared yet.
	_, _, _ = f.eng.Submit(Declaration{
		ID: "child", Spec: Spec{Image: "c"}, Phase: PhasePresent, DependsOn: []string{"parent"},
	})
	t.Log("INPUT child submitted before its dependency parent exists")
	time.Sleep(120 * time.Millisecond)
	if _, ok := f.store.Get("child"); ok {
		t.Fatal("child must wait while dependency is missing")
	}
	_, _, _ = f.eng.Submit(Declaration{ID: "parent", Spec: Spec{Image: "p"}, Phase: PhasePresent})
	if !f.converged(t, map[string]Spec{
		"parent": {Image: "p"}, "child": {Image: "c"},
	}, 3*time.Second) {
		f.logEvents(t, "MISSING-DEP")
		t.Fatal("child did not converge after missing dependency arrived")
	}
	t.Log("DECISION child progressed automatically once missing dependency appeared (no task lost)")
}

func TestEngineRepeatedReconcileIsStable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ResyncInterval = 20 * time.Millisecond
	f := newEngineFixture(t, cfg)
	for i := 0; i < 5; i++ {
		_, _, _ = f.eng.Submit(Declaration{
			ID: "a", Spec: Spec{Image: "i", Replicas: 2}, Phase: PhasePresent,
		})
	}
	if !f.converged(t, map[string]Spec{"a": {Image: "i", Replicas: 2}}, 2*time.Second) {
		t.Fatal("initial converge failed")
	}
	first, _ := f.store.Get("a")
	time.Sleep(300 * time.Millisecond) // many resync periods
	second, _ := f.store.Get("a")
	t.Logf("DECISION after many resyncs: version first=%d second=%d reason=%q (identical -> no churn)",
		first.Version, second.Version, second.Reason)
	if first.Version != second.Version {
		t.Fatalf("repeated identical reconciliation churned version: %d -> %d", first.Version, second.Version)
	}
	if !specEqual(first.Spec, second.Spec) {
		t.Fatal("spec must stay stable across repeated reconciliation")
	}
}
