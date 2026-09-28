package reconcile

import (
	"testing"
	"time"
)

func TestSmokeConverge(t *testing.T) {
	decl := NewDeclared()
	store := NewStore()
	rec := NewReconciler(store)
	eng := NewEngine(decl, rec, DefaultConfig())
	if err := eng.Start(); err != nil {
		t.Fatal(err)
	}
	v, _, err := eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "img:1"}, Phase: PhasePresent})
	if err != nil {
		t.Fatal(err)
	}
	if !eng.WaitFor(2*time.Second, func() bool {
		st, ok := store.Get("a")
		return ok && st.Ready && st.Spec.Image == "img:1"
	}) {
		t.Fatalf("a not converged: %+v", store.List())
	}
	// idempotent resubmit
	v2, changed, err := eng.Submit(Declaration{ID: "a", Spec: Spec{Image: "img:1"}, Phase: PhasePresent, ExpectedVersion: v})
	if err != nil || changed || v2 != v {
		t.Fatalf("idempotency broken: v=%d v2=%d changed=%v err=%v", v, v2, changed, err)
	}
	if err := eng.Shutdown(time.Second); err != nil {
		t.Fatal(err)
	}
}
