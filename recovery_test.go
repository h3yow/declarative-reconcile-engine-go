package dre

import (
	"context"
	"testing"
	"time"
)

// TestTransientFailureRetryAndRecovery: one resource suffers a bounded number
// of transient create failures while a healthy neighbour is unaffected;
// after retries with backoff the failing resource converges healthy.
func TestTransientFailureRetryAndRecovery(t *testing.T) {
	eng, _, target := newTestEngine(t, 3)
	stop := startEngine(t, eng)
	defer stop()

	target.InjectFailure(FailureRule{MatchID: "flaky", Op: "create",
		Transient: true, Times: 3, Message: "simulated transient disk error"})

	t.Log("INPUT create id=flaky expected=-1 (3 transient create failures injected)")
	vf := applyOK(t, eng, Desired{ID: "flaky", ExpectedVersion: -1})
	t.Log("INPUT create id=steady expected=-1")
	applyOK(t, eng, Desired{ID: "steady", ExpectedVersion: -1})

	// neighbour must become healthy promptly even while flaky is erroring
	waitPhase(t, eng, "steady", PhaseHealthy, time.Second)
	if got := waitPhase(t, eng, "flaky", PhaseHealthy, 3*time.Second); got.Version != vf {
		t.Fatalf("flaky version changed unexpectedly: %d", got.Version)
	}
	s, _ := eng.Status("flaky")
	if s.LastError != "" {
		t.Fatalf("flaky should have cleared last error, got %q", s.LastError)
	}
	if obs, ok := target.Get("flaky"); !ok || !obs.Ready {
		t.Fatalf("flaky not healthy in target: %+v ok=%v", obs, ok)
	}
	t.Log("JUDGEMENT: flaky recovered via bounded backoff retries; steady was never blocked")
}

// TestPersistentFailureThenRecover: an unlimited permanent-looking failure
// keeps the resource in Progressing with retries driven by resync; clearing
// the fault lets it converge. Proves no resource is stuck forever and no
// busy-loop: attempts are bounded over time.
func TestPersistentFailureThenRecover(t *testing.T) {
	eng, _, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	target.InjectFailure(FailureRule{MatchID: "sick", Op: "create",
		Transient: false, Message: "permanent fault"})
	applyOK(t, eng, Desired{ID: "sick", ExpectedVersion: -1})

	time.Sleep(200 * time.Millisecond)
	s1, _ := eng.Status("sick")
	if s1.Phase != PhaseProgressing {
		t.Fatalf("expected Progressing under fault, got %s", s1.Phase)
	}
	if s1.LastError == "" {
		t.Fatal("expected last error to be recorded for explainability")
	}
	// attempts should be small (resync-paced, 25ms) not a hot spin
	if s1.Attempts > 20 {
		t.Fatalf("busy loop suspected, attempts=%d in 200ms", s1.Attempts)
	}
	t.Logf("JUDGEMENT: sick held Progressing attempts=%d lastError=%q (no hot loop)",
		s1.Attempts, s1.LastError)

	target.ClearFailures()
	waitPhase(t, eng, "sick", PhaseHealthy, 2*time.Second)
	t.Log("JUDGEMENT: sick recovered after fault cleared, without a new submission")
}

// TestSpecDriftHealed: an out-of-band spec change in the target is detected
// by periodic resync and corrected back to the desired spec.
func TestSpecDriftHealed(t *testing.T) {
	eng, _, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	v := applyOK(t, eng, Desired{ID: "dr", ExpectedVersion: -1, Spec: Spec{"replicas": "2"}})
	waitPhase(t, eng, "dr", PhaseHealthy, time.Second)

	target.Drift("dr", Spec{"replicas": "99"})
	obs, _ := target.Get("dr")
	t.Logf("INPUT external drift: actual spec=%v (desired replicas=2, version=%d)", obs.Spec, v)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if o, _ := target.Get("dr"); o.Spec["replicas"] == "2" && o.Ready {
			t.Logf("JUDGEMENT: drift healed, actual=%v", o.Spec)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if o, _ := target.Get("dr"); o.Spec["replicas"] != "2" {
		t.Fatalf("drift not healed: %v", o.Spec)
	}
}

// TestDriftDeleteRecreated: resource deleted out-of-band is recreated.
func TestDriftDeleteRecreated(t *testing.T) {
	eng, _, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	applyOK(t, eng, Desired{ID: "ghost", ExpectedVersion: -1})
	waitPhase(t, eng, "ghost", PhaseHealthy, time.Second)

	t.Log("INPUT external delete of ghost")
	target.DriftDelete("ghost")
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if o, ok := target.Get("ghost"); ok && o.Ready {
			t.Logf("JUDGEMENT: ghost recreated and ready: %+v", o)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if o, ok := target.Get("ghost"); !ok || !o.Ready {
		t.Fatalf("ghost not recreated: %+v ok=%v", o, ok)
	}
}

// TestOpTimeoutIsolation: a rule injecting delay longer than opTimeout makes
// one resource's operations time out transiently, but other resources remain
// healthy; removing the rule allows convergence.
func TestOpTimeoutIsolation(t *testing.T) {
	eng, _, target := newTestEngine(t, 2)
	stop := startEngine(t, eng)
	defer stop()

	target.InjectFailure(FailureRule{MatchID: "slow", Op: "create",
		Transient: true, Delay: 5 * time.Second, Message: "hung backend"})
	applyOK(t, eng, Desired{ID: "slow", ExpectedVersion: -1})
	applyOK(t, eng, Desired{ID: "fast", ExpectedVersion: -1})

	waitPhase(t, eng, "fast", PhaseHealthy, time.Second)
	t.Log("JUDGEMENT: fast healthy while slow operations time out")

	// wait until at least one slow pass has timed out (opTimeout=500ms)
	pctx, pcancel := context.WithTimeout(context.Background(), testTimeout(2*time.Second))
	defer pcancel()
	if !eng.WaitFor(pctx, "slow",
		func(s Status) bool { return s.LastError != "" }) {
		t.Fatal("slow never recorded a timeout error")
	}
	s, _ := eng.Status("slow")
	if s.Phase != PhaseProgressing {
		t.Fatalf("slow should be Progressing under timeout, got %s", s.Phase)
	}
	target.ClearFailures()
	waitPhase(t, eng, "slow", PhaseHealthy, 3*time.Second)
}
