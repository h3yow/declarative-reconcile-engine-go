package reconcile

import (
	"sync"
	"testing"
	"time"
)

func TestQueueDedupAndDirtyRequeue(t *testing.T) {
	q := NewQueue(0)

	q.Add("a")
	q.Add("a")
	q.Add("a")
	t.Logf("INPUT Add(a) x3 -> Len=%d basis=pending set dedup", q.Len())
	if q.Len() != 1 {
		t.Fatalf("Len=%d, want 1", q.Len())
	}

	key, ok := q.Get()
	if !ok || key != "a" {
		t.Fatalf("Get = %q,%v", key, ok)
	}
	t.Log("INPUT processing a; Add(a) twice while in flight")
	q.Add("a")
	q.Add("a") // must collapse into exactly one extra pass
	q.Done("a")
	if q.Len() != 1 {
		t.Fatalf("after Done dirty key must be requeued once, Len=%d", q.Len())
	}
	key, _ = q.Get()
	q.Done(key)
	t.Logf("DECISION second pass key=%q; Len=%d InFlight=%d", key, q.Len(), q.InFlight())
	if q.Len() != 0 || q.InFlight() != 0 {
		t.Fatal("queue must be empty after dirty requeue consumed")
	}
}

func TestQueueCloseDrains(t *testing.T) {
	q := NewQueue(0)
	q.Add("x")
	got, ok := q.Get()
	t.Logf("INPUT Get before close -> %q,%v; now Add during flight then Close", got, ok)
	q.Add("x") // dirty
	q.Close()
	if q.Add("y") {
		t.Fatal("Add after Close must be rejected")
	}
	q.Done("x")
	k, ok := q.Get()
	t.Logf("DECISION drained dirty key after close: %q,%v (must be delivered)", k, ok)
	if !ok || k != "x" {
		t.Fatalf("in-flight-dirtied key must drain: %q,%v", k, ok)
	}
	q.Done("x")
	if _, ok := q.Get(); ok {
		t.Fatal("Get must return false after closed queue fully drained")
	}
}

func TestQueueGetBlocksUntilAdd(t *testing.T) {
	q := NewQueue(0)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(20 * time.Millisecond)
		q.Add("late")
	}()
	key, ok := q.Get()
	wg.Wait()
	t.Logf("DECISION blocked Get woke with %q,%v", key, ok)
	if !ok || key != "late" {
		t.Fatal("Get should block until Add then deliver")
	}
}
