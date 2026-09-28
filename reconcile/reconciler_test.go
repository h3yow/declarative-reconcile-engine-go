package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"
)

func recEngineFixture(t *testing.T) (*Declared, *Store, *Reconciler, *[]Event) {
	t.Helper()
	d := NewDeclared()
	s := NewStore()
	var events []Event
	r := NewReconciler(s).
		WithLiveDeclarations(
			func(id ResourceID) bool {
				_, ok := d.Get(id)
				return ok
			},
			func(id ResourceID) []ResourceID { return d.LiveDependents(id) },
		).
		WithEventSink(func(e Event) {
			events = append(events, e)
		})
	return d, s, r, &events
}

func snap(t *testing.T, d *Declared) (map[string]Declaration, *graph, map[ResourceID]bool) {
	t.Helper()
	decls, g := d.Snapshot()
	known := map[ResourceID]bool{}
	for k := range decls {
		known[k] = true
	}
	return decls, g, known
}

func TestReconcilerCreateUpdateDelete(t *testing.T) {
	d, s, r, events := recEngineFixture(t)
	_, _, _ = d.Submit(Declaration{ID: "a", Spec: Spec{Image: "i:1"}, Phase: PhasePresent})
	decls, g, known := snap(t, d)
	now := time.Unix(1000, 0)
	ctx := context.Background()

	dp := decls["a"]
	res, err := r.Reconcile(ctx, "a", &dp, g, known, now)
	t.Logf("INPUT reconcile create a -> res=%+v err=%v", res, err)
	if err != nil || len(res.Enqueue) != 0 {
		t.Fatalf("create: %+v %v", res, err)
	}
	st, ok := s.Get("a")
	if !ok || st.Phase != PhaseReady || st.Spec.Image != "i:1" {
		t.Fatalf("a not created ready: %+v ok=%v", st, ok)
	}

	// Second identical pass -> noop.
	_, err = r.Reconcile(ctx, "a", &dp, g, known, now)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("DECISION second identical pass expected noop")
	var noops int
	for _, e := range *events {
		if e.Type == EventNoop {
			noops++
		}
	}
	if noops != 1 {
		t.Fatalf("want 1 noop event, got %d: %+v", noops, *events)
	}

	// Delete with no declaration present.
	if _, err := r.Reconcile(ctx, "a", nil, g, known, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("a should be deleted")
	}
	t.Log("DECISION delete absent-desire removed actual a")
}

func TestReconcilerDependencyGatingAndDeleteProtection(t *testing.T) {
	d, s, r, _ := recEngineFixture(t)
	// child depends on parent.
	_, _, _ = d.Submit(Declaration{ID: "parent", Spec: Spec{Image: "p"}, Phase: PhasePresent})
	_, _, _ = d.Submit(Declaration{ID: "child", Spec: Spec{Image: "c"}, Phase: PhasePresent, DependsOn: []string{"parent"}})
	decls, g, known := snap(t, d)
	ctx := context.Background()
	now := time.Now()

	child := decls["child"]
	res, err := r.Reconcile(ctx, "child", &child, g, known, now)
	t.Logf("INPUT child reconcile before parent ready -> requeue=%v err=%v basis=prerequisite not present", res.Requeue, err)
	if err != nil || !res.Requeue {
		t.Fatalf("child must wait: %+v %v", res, err)
	}
	if _, ok := s.Get("child"); ok {
		t.Fatal("child must not be created before parent")
	}

	parent := decls["parent"]
	if _, err := r.Reconcile(ctx, "parent", &parent, g, known, now); err != nil {
		t.Fatal(err)
	}
	res, err = r.Reconcile(ctx, "child", &child, g, known, now)
	t.Logf("INPUT child reconcile after parent ready -> requeue=%v enqueue=%v err=%v", res.Requeue, res.Enqueue, err)
	if err != nil || res.Requeue {
		t.Fatalf("child should proceed: %+v %v", res, err)
	}
	if _, ok := s.Get("child"); !ok {
		t.Fatal("child must exist after parent ready")
	}

	// Deleting parent while child is still desired must be blocked.
	if _, err := r.Reconcile(ctx, "parent", nil, g, known, now); err != nil {
		t.Fatal(err)
	}
	if st, ok := s.Get("parent"); !ok {
		t.Fatal("parent delete must be blocked while live child depends on it (no orphans)")
	} else {
		t.Logf("DECISION parent delete blocked, still present: %s", st.Reason)
	}

	// Once child's declaration is gone and actual child is removed, parent deletes.
	_, _, _ = d.Submit(Declaration{ID: "child", Phase: PhaseAbsent, ExpectedVersion: 1})
	_, g2, known2 := snap(t, d)
	if _, err := r.Reconcile(ctx, "child", nil, g2, known2, now); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(ctx, "parent", nil, g2, known2, now); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Get("parent"); ok {
		t.Fatal("parent must delete after child is gone")
	}
	t.Log("DECISION parent deleted in dependency-safe order after child removal")
}

func TestReconcilerTransientFailureSurfacesAsRetryable(t *testing.T) {
	d, s, r, _ := recEngineFixture(t)
	_, _, _ = d.Submit(Declaration{ID: "a", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	decls, g, known := snap(t, d)
	s.InjectFailures(OpCreate, 1)
	dp := decls["a"]
	_, err := r.Reconcile(context.Background(), "a", &dp, g, known, time.Now())
	t.Logf("INPUT create with 1 injected failure -> err=%v", err)
	if !errors.Is(err, ErrTransient) {
		t.Fatalf("want ErrTransient, got %v", err)
	}
	if _, ok := s.Get("a"); ok {
		t.Fatal("failed create must not leave a resource behind")
	}
	// Immediate retry succeeds (simulated recovery).
	if _, err := r.Reconcile(context.Background(), "a", &dp, g, known, time.Now()); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	if _, ok := s.Get("a"); !ok {
		t.Fatal("a missing after successful retry")
	}
	t.Log("DECISION retry after transient failure converged")
}
