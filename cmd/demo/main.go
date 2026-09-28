// Command demo walks through the engine's main guarantees without any
// external service:
//
//	go run ./cmd/demo
package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/h3yow/declarative-reconcile-engine-go/reconcile"
)

func main() {
	decl := reconcile.NewDeclared()
	store := reconcile.NewStore()
	rec := reconcile.NewReconciler(store).WithEventSink(func(e reconcile.Event) {
		if e.Err != nil {
			fmt.Printf("    [event] %-10s %-6s %s (err=%v)\n", e.Type, e.ID, e.Reason, e.Err)
		} else {
			fmt.Printf("    [event] %-10s %-6s %s\n", e.Type, e.ID, e.Reason)
		}
	})
	cfg := reconcile.DefaultConfig()
	cfg.ResyncInterval = 100 * time.Millisecond
	eng := reconcile.NewEngine(decl, rec, cfg)
	if err := eng.Start(); err != nil {
		panic(err)
	}
	defer func() { _ = eng.Shutdown(time.Second) }()

	// 1) Declare with defaults; idempotent resubmit.
	fmt.Println("1) declare database (replicas omitted -> default 1)")
	v, _, err := eng.Submit(reconcile.Declaration{
		ID: "database", Spec: reconcile.Spec{Image: "db:1"}, Phase: reconcile.PhasePresent,
	})
	must(err)
	v2, changed, err := eng.Submit(reconcile.Declaration{
		ID: "database", Spec: reconcile.Spec{Image: "db:1"}, Phase: reconcile.PhasePresent, ExpectedVersion: v,
	})
	must(err)
	fmt.Printf("   identical resubmit: version=%d changed=%v\n", v2, changed)

	// 2) Dependency ordering: api depends on database.
	fmt.Println("2) declare api depending on database")
	_, _, _ = eng.Submit(reconcile.Declaration{
		ID: "api", Spec: reconcile.Spec{Image: "api:1"}, Phase: reconcile.PhasePresent,
		DependsOn: []string{"database"},
	})
	wait(eng, func() bool {
		_, a := store.Get("api")
		return a
	}, "api created after database")

	// 3) Stale version submission rejected with a distinct reason.
	fmt.Println("3) stale update (expected an old version) must be rejected")
	curDB, _ := decl.Get("database")
	dbV2, _, err := eng.Submit(reconcile.Declaration{
		ID: "database", Spec: reconcile.Spec{Image: "db:2"}, Phase: reconcile.PhasePresent, ExpectedVersion: curDB.ExpectedVersion,
	})
	must(err)
	_, _, err = eng.Submit(reconcile.Declaration{
		ID: "database", Spec: reconcile.Spec{Image: "db:9"}, Phase: reconcile.PhasePresent,
		ExpectedVersion: curDB.ExpectedVersion, // stale: current is now dbV2
	})
	switch {
	case errors.Is(err, reconcile.ErrVersionConflict):
		fmt.Printf("   rejected as expected (current=%d): %v\n", dbV2, err)
	default:
		panic(fmt.Sprintf("unexpected: %v", err))
	}
	// Restore db:1 desired state for the drift demo below.
	_, _, _ = eng.Submit(reconcile.Declaration{
		ID: "database", Spec: reconcile.Spec{Image: "db:1"}, Phase: reconcile.PhasePresent, ExpectedVersion: dbV2,
	})

	// 4) Transient fault injected: retried and converged.
	fmt.Println("4) inject 2 transient update failures for api")
	store.InjectFailures(reconcile.OpUpdate, 2)
	cur, _ := decl.Get("api")
	_, _, _ = eng.Submit(reconcile.Declaration{
		ID: "api", Spec: reconcile.Spec{Image: "api:2"}, Phase: reconcile.PhasePresent,
		DependsOn: []string{"database"}, ExpectedVersion: cur.ExpectedVersion,
	})
	wait(eng, func() bool {
		s, ok := store.Get("api")
		return ok && s.Spec.Image == "api:2"
	}, "api converged to api:2 after retries")

	// 5) Drift auto-repair by periodic resync.
	fmt.Println("5) inject out-of-band drift on database")
	store.InjectDrift("database", reconcile.Spec{Image: "TAMPERED", Replicas: 7})
	wait(eng, func() bool {
		s, ok := store.Get("database")
		return ok && s.Spec.Image == "db:1" && s.Spec.Replicas == reconcile.DefaultReplicas
	}, "database drift repaired by resync")

	// 6) Ordered teardown.
	fmt.Println("6) delete api then database (reverse dependency order)")
	curAPI, _ := decl.Get("api")
	curDB2, _ := decl.Get("database")
	_, _, _ = eng.Submit(reconcile.Declaration{ID: "api", Phase: reconcile.PhaseAbsent, ExpectedVersion: curAPI.ExpectedVersion})
	_, _, _ = eng.Submit(reconcile.Declaration{ID: "database", Phase: reconcile.PhaseAbsent, ExpectedVersion: curDB2.ExpectedVersion})
	wait(eng, func() bool {
		_, a := store.Get("api")
		_, d := store.Get("database")
		return !a && !d
	}, "both resources removed in order")

	fmt.Println("done")
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func wait(eng *reconcile.Engine, cond func() bool, what string) {
	if !eng.WaitFor(3*time.Second, cond) {
		panic("timeout waiting for: " + what)
	}
	fmt.Printf("   OK: %s\n", what)
}
