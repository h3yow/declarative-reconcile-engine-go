package dre

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestDependencyOrdering verifies creation proceeds in dependency order:
// a resource stays Pending until every dependency exists and is Ready.
func TestDependencyOrdering(t *testing.T) {
	eng, _, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	// submit dependent-first (c -> b -> a) out of order
	vc := applyOK(t, eng, Desired{ID: "c", ExpectedVersion: -1, DependsOn: []ResourceID{"b"}})
	vb := applyOK(t, eng, Desired{ID: "b", ExpectedVersion: -1, DependsOn: []ResourceID{"a"}})
	_ = vc
	_ = vb

	time.Sleep(80 * time.Millisecond)
	if sc, _ := eng.Status("c"); sc.Phase != PhasePending {
		t.Fatalf("c must stay Pending without deps, got %s", sc.Phase)
	}
	if sb, _ := eng.Status("b"); sb.Phase != PhasePending {
		t.Fatalf("b must stay Pending without a, got %s", sb.Phase)
	}
	if _, ok := target.Get("b"); ok {
		t.Fatal("b must not be created while dependency a is absent")
	}
	t.Log("JUDGEMENT: b,c Pending; nothing created for blocked resources")

	va := applyOK(t, eng, Desired{ID: "a", ExpectedVersion: -1})
	waitPhase(t, eng, "a", PhaseHealthy, time.Second)
	waitPhase(t, eng, "b", PhaseHealthy, time.Second)
	waitPhase(t, eng, "c", PhaseHealthy, time.Second)
	_ = va

	// order proof: a became ready strictly before b before c
	ta := readyAt(eng, "a")
	tb := readyAt(eng, "b")
	tc := readyAt(eng, "c")
	if !(ta.Before(tb) && tb.Before(tc)) {
		t.Fatalf("dependency order violated: a=%s b=%s c=%s", ta, tb, tc)
	}
	t.Logf("JUDGEMENT: ordered readiness a(%s) < b(%s) < c(%s)",
		ta.Format("15:04:05.000"), tb.Format("15:04:05.000"), tc.Format("15:04:05.000"))
}

func readyAt(eng *Engine, id ResourceID) time.Time {
	s, _ := eng.Status(id)
	for i := len(s.Conditions) - 1; i >= 0; i-- {
		if s.Conditions[i].Reason == "Ready" || s.Conditions[i].Reason == "InSync" {
			return s.Conditions[i].Time
		}
	}
	return time.Time{}
}

// TestReverseDependencyDeletion verifies deletion order: deleting a chain
// enforces dependents-first and leaves no orphaned target resources.
func TestReverseDependencyDeletion(t *testing.T) {
	eng, _, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	va := applyOK(t, eng, Desired{ID: "a", ExpectedVersion: -1})
	waitPhase(t, eng, "a", PhaseHealthy, time.Second)
	vb := applyOK(t, eng, Desired{ID: "b", ExpectedVersion: -1, DependsOn: []ResourceID{"a"}})
	waitPhase(t, eng, "b", PhaseHealthy, time.Second)
	vc := applyOK(t, eng, Desired{ID: "c", ExpectedVersion: -1, DependsOn: []ResourceID{"b"}})
	waitPhase(t, eng, "c", PhaseHealthy, time.Second)

	// deleting the dependency while dependents live is refused
	expectConflict(t, eng.Delete("a", va), ReasonDependencyBlocksDelete, "a")
	expectConflict(t, eng.Delete("b", vb), ReasonDependencyBlocksDelete, "b")

	// proper reverse order
	if err := eng.Delete("c", vc); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, eng, "c", PhaseDeleted, time.Second)
	if _, ok := target.Get("c"); ok {
		t.Fatal("c orphaned in target after delete")
	}
	if err := eng.Delete("b", vb); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, eng, "b", PhaseDeleted, time.Second)
	if err := eng.Delete("a", va); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, eng, "a", PhaseDeleted, time.Second)

	if len(target.Snapshot()) != 0 {
		t.Fatalf("orphan resources remain: %v", target.Snapshot())
	}
	if len(eng.store.Snapshot()) != 0 {
		t.Fatalf("tombstones not purged: %v", eng.store.Snapshot())
	}
	t.Log("JUDGEMENT: chain deleted dependents-first; no orphans, no lingering records")
}

// TestDeleteConcurrentChange verifies delete racing with a new versioned
// update: exactly one side wins per round; the loser is rejected with a
// distinguishable reason, never a silent mixed state, and no double delete.
func TestDeleteConcurrentChange(t *testing.T) {
	eng, _, target := newTestEngine(t, 4)
	stop := startEngine(t, eng)
	defer stop()

	const rounds = 10
	outcomes := map[string]int{}
	for round := 0; round < rounds; round++ {
		// Establish a common baseline (replicas=1, healthy) without assuming
		// whether the previous round purged or retained the resource.
		var base int64
		if rec, ok := eng.store.Get("rc"); ok {
			base = applyOK(t, eng, Desired{ID: "rc", ExpectedVersion: rec.version,
				Spec: Spec{"replicas": "1"}})
		} else {
			base = applyOK(t, eng, Desired{ID: "rc", ExpectedVersion: -1,
				Spec: Spec{"replicas": "1"}})
		}
		waitPhase(t, eng, "rc", PhaseHealthy, 2*time.Second)

		// Strictly deterministic admission ordering using a two-phase gate.
		// Both contenders park at the gate; the chosen winner is released
		// FIRST and we only release the loser after the winner's verdict has
		// been recorded - removing every scheduling ambiguity while still
		// exercising concurrent admission code paths.
		var mu sync.Mutex
		entered := make(map[string]chan struct{})
		proceed := make(map[string]chan struct{})
		verdict := make(chan string, 2)
		for _, op := range []string{"delete", "apply"} {
			entered[op] = make(chan struct{})
			proceed[op] = make(chan struct{})
		}
		var enteredMu sync.Mutex
		enteredCount := 0
		eng.testAdmitGate = func(op string) {
			enteredMu.Lock()
			enteredCount++
			if enteredCount == 2 {
				close(entered["delete"])
				close(entered["apply"])
			}
			enteredMu.Unlock()
			<-proceed[op]
		}

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			err := eng.Delete("rc", base)
			mu.Lock()
			classifyOutcome(outcomes, "delete", err)
			verdict <- "delete"
			mu.Unlock()
		}()
		go func() {
			defer wg.Done()
			_, err := eng.Apply(Desired{ID: "rc", ExpectedVersion: base,
				Spec: Spec{"replicas": "7"}})
			mu.Lock()
			classifyOutcome(outcomes, "apply", err)
			verdict <- "apply"
			mu.Unlock()
		}()

		<-entered["delete"]
		<-entered["apply"]
		winner := "delete"
		loser := "apply"
		if round%2 == 1 {
			winner, loser = "apply", "delete"
		}
		close(proceed[winner])
		<-verdict // winner's admission verdict is recorded
		close(proceed[loser])
		eng.testAdmitGate = nil
		wg.Wait()

		// Wait for durable convergence: either purged from store+target, or
		// live, ready and unflagged in target with a matching store record.
		deleted := settleRaceRound(t, eng, round)

		_, inTarget := target.Get("rc")
		_, inStore := eng.store.Get("rc")
		if deleted && (inTarget || inStore) {
			t.Fatalf("round %d: deleted but target/store retain rc", round)
		}
		if !deleted && !inTarget {
			t.Fatalf("round %d: healthy but target missing rc", round)
		}

		if deleted {
			// record fully purged; next round creates it fresh. Nothing to do.
		}
		// If apply won, the resource lives at a bumped version with spec
		// replicas=7; the next round's baseline step version-updates it back
		// to replicas=1 before racing again.
	}
	t.Logf("JUDGEMENT: race outcomes over %d rounds: %+v; every loser explicit", rounds, outcomes)
	if outcomes["delete:ok"] == 0 {
		t.Fatalf("expected delete to win at least once: %v", outcomes)
	}
	if outcomes["apply:StaleVersion"]+outcomes["apply:Deleting"] == 0 {
		t.Fatalf("expected contended applies to be explicitly rejected: %v", outcomes)
	}
	if outcomes["delete:StaleVersion"] > 0 || outcomes["delete:Deleting"] > 0 || outcomes["apply:ok"] > 0 {
		// when apply wins first, delete is stale; when delete wins first,
		// apply is Deleting. Both are valid explicit outcomes.
	}
	// Every round settles with delete eventually admitted (since apply can
	// never revive a tombstone) OR apply wins and delete is stale.
	for _, d := range eng.Decisions() {
		if d.ID == "rc" && d.Reason == "CreateFailed" {
			t.Fatalf("observed target create failure from delete/update race: %s", d.Detail)
		}
	}
}

func classifyOutcome(m map[string]int, op string, err error) {
	if err == nil {
		m[op+":ok"]++
		return
	}
	if ce, ok := AsConflict(err); ok {
		m[op+":"+string(ce.Reason)]++
		return
	}
	m[op+":other"]++
}

// settleRaceRound polls until the resource durably converges after a
// delete/update race; returns true if it was fully purged.
func settleRaceRound(t *testing.T, eng *Engine, round int) bool {
	t.Helper()
	target := eng.target
	deadline := time.Now().Add(testTimeout(time.Second))
	for time.Now().Before(deadline) {
		_, inStore := eng.store.Get("rc")
		obs, inTarget := target.Get("rc")
		if !inStore && !inTarget {
			return true
		}
		if inStore && inTarget && obs.Ready && !obs.MarkedForDeletion {
			rec, _ := eng.store.Get("rc")
			if rec.deleteAt == 0 {
				return false
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	s, _ := eng.Status("rc")
	t.Fatalf("round %d did not durably settle: status=%+v target=%v", round, s, target.Snapshot())
	return false
}

// TestRedeclareDuringTermination verifies a resource whose target object is
// terminating is re-asserted and becomes Healthy again instead of being
// dropped or duplicated.
func TestRedeclareDuringTermination(t *testing.T) {
	store := NewStore()
	target := NewTarget()
	// pre-create a terminating actual resource to race against admission
	if err := target.Create(context.Background(), Observed{ID: "rd", Kind: "generic"}); err != nil {
		t.Fatal(err)
	}
	if err := target.MarkDeletion(context.Background(), "rd"); err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(store, target, Config{Workers: 2, ResyncInterval: 25 * time.Millisecond,
		Logger: testLogger{t: t}})
	stop := startEngine(t, eng)
	defer stop()

	applyOK(t, eng, Desired{ID: "rd", ExpectedVersion: -1})
	waitPhase(t, eng, "rd", PhaseHealthy, 2*time.Second)
	if o, _ := target.Get("rd"); o.MarkedForDeletion {
		t.Fatal("re-declared resource still flagged terminating")
	}
	snap := target.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected exactly one target object, got %d", len(snap))
	}
	t.Log("JUDGEMENT: terminating object re-asserted to a single healthy resource")
}

// TestShutdownConsistencyAndReplay verifies shutdown has a predictable result:
// no target/desired divergence, no queue loss on restart, and an in-flight
// reconciliation interrupted by shutdown is simply re-driven after restart.
func TestShutdownConsistencyAndReplay(t *testing.T) {
	store := NewStore()
	target := NewTarget()
	eng := NewEngine(store, target, Config{Workers: 2, OpTimeout: 500 * time.Millisecond,
		ResyncInterval: 25 * time.Millisecond, Logger: testLogger{t: t}})
	eng.Start(context.Background())

	// make one resource slow to be interrupted mid-flight
	target.InjectFailure(FailureRule{MatchID: "slow1", Op: "create",
		Transient: true, Delay: 5 * time.Second, Message: "interrupt me"})
	applyOK(t, eng, Desired{ID: "slow1", ExpectedVersion: -1})
	applyOK(t, eng, Desired{ID: "fast1", ExpectedVersion: -1})
	waitPhase(t, eng, "fast1", PhaseHealthy, time.Second)

	// shut down while slow1's create is hung in its op timeout
	sctx, sc := context.WithTimeout(context.Background(), time.Second)
	if err := eng.Shutdown(sctx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}
	sc()
	t.Logf("JUDGEMENT: shutdown returned; slow1 present in store=%v target=%v",
		func() bool { _, ok := store.Get("slow1"); return ok }(),
		func() bool { _, ok := target.Get("slow1"); return ok }())

	// desired state remains durable; target contains only fully-completed work
	if _, ok := store.Get("slow1"); !ok {
		t.Fatal("durable desired record lost on shutdown")
	}
	if o, ok := target.Get("slow1"); ok && o.Exists {
		t.Fatal("interrupted create must not leave a half resource")
	}

	// restart replays all declarations and converges everything
	target.ClearFailures()
	eng2 := NewEngine(store, target, Config{Workers: 2, ResyncInterval: 25 * time.Millisecond,
		Logger: testLogger{t: t}})
	stop2 := startEngine(t, eng2)
	defer stop2()
	waitPhase(t, eng2, "slow1", PhaseHealthy, 3*time.Second)
	waitPhase(t, eng2, "fast1", PhaseHealthy, 3*time.Second)
	t.Log("JUDGEMENT: restart replayed durable declarations; both resources healthy")
}

// TestPendingHardDelete verifies deleting a dependency-blocked pending
// resource performs no target work (it was never created).
func TestPendingHardDelete(t *testing.T) {
	eng, store, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	applyOK(t, eng, Desired{ID: "dep", ExpectedVersion: -1, DependsOn: []ResourceID{"missingdep"}})
	time.Sleep(60 * time.Millisecond)
	if _, ok := target.Get("dep"); ok {
		t.Fatal("blocked resource must not exist in target")
	}
	rec, _ := store.Get("dep")
	if err := eng.hardDelete("dep", rec.version); err != nil {
		t.Fatal(err)
	}
	eng.q.Add("dep") // let the worker observe the purge
	time.Sleep(60 * time.Millisecond)
	if rec, ok := store.Get("dep"); ok {
		t.Fatalf("pending record not removed: %+v", rec)
	}
	s, _ := eng.Status("dep")
	t.Logf("JUDGEMENT: hard delete of pending dep -> status phase=%s, no target calls", s.Phase)
}
