package reconcile

import (
	"errors"
	"testing"
)

func TestDeclaredCRUDIdempotenceAndDefaults(t *testing.T) {
	d := NewDeclared()

	// 1) create with omitted Replicas -> deterministic default.
	in := Declaration{ID: "web", Spec: Spec{Image: "nginx:1"}, Phase: PhasePresent}
	t.Logf("INPUT submit create id=%q spec=%+v expectedVersion=%d", in.ID, in.Spec, in.ExpectedVersion)
	v, changed, err := d.Submit(in)
	if err != nil || !changed || v != 1 {
		t.Fatalf("create: v=%d changed=%v err=%v", v, changed, err)
	}
	got, _ := d.Get("web")
	t.Logf("DECISION stored version=%d replicas=%d (default=%d) labels=%v",
		got.ExpectedVersion, got.Spec.Replicas, DefaultReplicas, got.Spec.Labels)
	if got.Spec.Replicas != DefaultReplicas {
		t.Fatalf("default replicas = %d, want %d", got.Spec.Replicas, DefaultReplicas)
	}

	// 2) identical resubmit (even omitting defaults) -> no change, same v.
	same := Declaration{ID: "web", Spec: Spec{Image: "nginx:1"}, Phase: PhasePresent, ExpectedVersion: 1}
	v2, changed2, err := d.Submit(same)
	t.Logf("INPUT identical resubmit expectedVersion=%d -> v=%d changed=%v err=%v basis=spec/deps/phase equal after defaulting",
		1, v2, changed2, err)
	if err != nil || changed2 || v2 != 1 {
		t.Fatalf("idempotent submit must be no-op: v=%d changed=%v err=%v", v2, changed2, err)
	}

	// 3) real update bumps version.
	upd := Declaration{ID: "web", Spec: Spec{Image: "nginx:2", Replicas: 3}, Phase: PhasePresent, ExpectedVersion: 1}
	v3, changed3, err := d.Submit(upd)
	t.Logf("INPUT update expectedVersion=1 image=nginx:2 -> v=%d changed=%v err=%v basis=image differs", v3, changed3, err)
	if err != nil || !changed3 || v3 != 2 {
		t.Fatalf("update: v=%d changed=%v err=%v", v3, changed3, err)
	}

	// 4) delete consumes record; deleting again absent is idempotent.
	del := Declaration{ID: "web", Phase: PhaseAbsent, ExpectedVersion: 2}
	v4, changed4, err := d.Submit(del)
	t.Logf("INPUT delete expectedVersion=2 -> v=%d changed=%v err=%v", v4, changed4, err)
	if err != nil || !changed4 {
		t.Fatalf("delete: v=%d changed=%v err=%v", v4, changed4, err)
	}
	if _, ok := d.Get("web"); ok {
		t.Fatal("web should be gone")
	}
	v5, changed5, err := d.Submit(Declaration{ID: "web", Phase: PhaseAbsent})
	t.Logf("INPUT delete absent -> v=%d changed=%v err=%v basis=delete of absent is idempotent", v5, changed5, err)
	if err != nil || changed5 || v5 != 0 {
		t.Fatalf("absent delete: v=%d changed=%v err=%v", v5, changed5, err)
	}
}

func TestDeclaredConflictReasonsAreDistinguishable(t *testing.T) {
	d := NewDeclared()
	v, _, err := d.Submit(Declaration{ID: "r", Spec: Spec{Image: "i"}, Phase: PhasePresent})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("setup created r at version %d", v)

	cases := []struct {
		name        string
		decl        Declaration
		wantReason  error
		wantCurrent int64
	}{
		{
			name:       "create-against-existing",
			decl:       Declaration{ID: "r", Spec: Spec{Image: "i"}, Phase: PhasePresent},
			wantReason: ErrAlreadyExists,
		},
		{
			name:       "stale-update",
			decl:       Declaration{ID: "r", Spec: Spec{Image: "i2"}, Phase: PhasePresent, ExpectedVersion: 1},
			wantReason: ErrVersionConflict,
		},
		{
			name:       "update-missing-resource",
			decl:       Declaration{ID: "ghost", Spec: Spec{Image: "i"}, Phase: PhasePresent, ExpectedVersion: 5},
			wantReason: ErrNotFound,
		},
	}
	// First make the stale-update happen on version 2 setup: bump r to 2.
	if _, _, err := d.Submit(Declaration{ID: "r", Spec: Spec{Image: "i2"}, Phase: PhasePresent, ExpectedVersion: 1}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("INPUT %s: id=%q expectedVersion=%d", tc.name, tc.decl.ID, tc.decl.ExpectedVersion)
			_, _, err := d.Submit(tc.decl)
			if err == nil {
				t.Fatal("expected rejection, got nil")
			}
			var ce *ConflictError
			if !errors.As(err, &ce) {
				t.Fatalf("want *ConflictError, got %T: %v", err, err)
			}
			t.Logf("DECISION reject reason=%q currentVersion=%d error=%q", ce.Reason, ce.Current, ce.Error())
			if !errors.Is(err, tc.wantReason) {
				t.Fatalf("reason = %v, want %v", err, tc.wantReason)
			}
		})
	}

	// Current state must be unchanged after all rejected writes.
	r, ok := d.Get("r")
	if !ok {
		t.Fatal("r disappeared")
	}
	t.Logf("ASSERT state after rejects: r version=%d image=%s (must be v2/i2)", r.ExpectedVersion, r.Spec.Image)
	if r.ExpectedVersion != 2 || r.Spec.Image != "i2" {
		t.Fatalf("rejected writes must not mutate state: %+v", r)
	}
}

func TestDeclaredInvalidSpecAndCycles(t *testing.T) {
	d := NewDeclared()

	// Empty image is invalid for Present.
	_, _, err := d.Submit(Declaration{ID: "x", Spec: Spec{}, Phase: PhasePresent})
	t.Logf("INPUT empty image -> err=%v basis=%v", err, ErrInvalidSpec)
	if !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("want ErrInvalidSpec, got %v", err)
	}

	// Build a -> b, then try b -> a (cycle).
	if _, _, err := d.Submit(Declaration{ID: "b", Spec: Spec{Image: "ib"}, Phase: PhasePresent}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Submit(Declaration{ID: "a", Spec: Spec{Image: "ia"}, Phase: PhasePresent, DependsOn: []string{"b"}}); err != nil {
		t.Fatal(err)
	}
	_, _, err = d.Submit(Declaration{
		ID: "b", Spec: Spec{Image: "ib2"}, Phase: PhasePresent,
		DependsOn: []string{"a"}, ExpectedVersion: 1,
	})
	t.Logf("INPUT cycle b->a while a->b -> err=%v basis=%s", err, ErrDependencyCycle)
	if !errors.Is(err, ErrDependencyCycle) {
		t.Fatalf("want ErrDependencyCycle, got %v", err)
	}
	// b remains at version 1 with no edges.
	b, _ := d.Get("b")
	if b.ExpectedVersion != 1 || len(b.DependsOn) != 0 {
		t.Fatalf("rejected cycle mutated b: %+v", b)
	}
}

func TestSpecDefaultingDeterministic(t *testing.T) {
	a := DefaultSpec(Spec{Image: "i"})
	b := DefaultSpec(Spec{Image: "i", Labels: map[string]string{"k": "v"}})
	b.Labels["k"] = "mutated-by-caller"
	c := DefaultSpec(Spec{Image: "i", Labels: map[string]string{"k": "v"}})
	if a.Replicas != DefaultReplicas || a.Labels == nil {
		t.Fatalf("defaulting wrong: %+v", a)
	}
	if c.Labels["k"] != "v" {
		t.Fatal("defaulting must copy the labels map")
	}
}
