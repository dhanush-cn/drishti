package storage

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// consumerWith builds a consumer like env.consumer, with cfg edits.
func (e *env) consumerWith(lim Limits, edit func(*Config)) *Consumer {
	e.t.Helper()
	cfg := Config{
		Brokers: e.cluster.ListenAddrs(), Topic: topic, Group: "drishti-storage",
		Limits: lim, InsertTimeout: 5 * time.Second, CH: e.ch.ch(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if edit != nil {
		edit(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

func (e *env) rowsOf(p uint16) int {
	n := 0
	for _, r := range e.ch.snapshot() {
		if r.partition == p {
			n++
		}
	}
	return n
}

func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// The point of background inserts: while one partition's insert is stuck,
// the loop keeps polling and other partitions keep getting stored. The
// serial loop (poll → add → wait for every insert → commit) fails this:
// p1's records are only produced after p0's insert started, so they can
// only be polled if the loop is not standing still in that insert.
func TestRun_PollsAndInsertsWhileAnotherInsertIsInFlight(t *testing.T) {
	e := newEnv(t, 2)
	release := make(chan struct{})
	started := make(chan struct{})
	var once, relOnce sync.Once
	unblock := func() { relOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock) // a failed assertion must not leave the fake hung
	e.ch.gate = func(p uint16, first uint64) {
		if p == 0 {
			once.Do(func() { close(started) })
			<-release
		}
	}
	want := e.produce(0, "slow", 3, 5, time.Now())
	c := e.consumer(fast, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() { unblock(); cancel(); <-done }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("p0 insert never started")
	}
	want += e.produce(1, "fast", 3, 5, time.Now())
	waitUntil(t, 5*time.Second, "p1 rows stored while p0's insert is in flight", func() bool { return e.rowsOf(1) == 15 })
	if n := e.rowsOf(0); n != 0 {
		t.Fatalf("p0 rows = %d while its insert is blocked", n)
	}
	unblock()
	waitUntil(t, 5*time.Second, "all rows", func() bool { return len(e.ch.snapshot()) == want })
	if d := dups(e.ch.snapshot()); len(d) > 0 {
		t.Fatalf("duplicates: %v", d)
	}
}

// Per partition: never two inserts at once, and inserts land in offset
// order (the resume reads max(kafka_offset), so a later batch landing first
// would hide an earlier one). Across partitions inserts do overlap.
func TestRun_OneInsertPerPartitionInOffsetOrder(t *testing.T) {
	e := newEnv(t, 3)
	e.ch.gate = func(uint16, uint64) { time.Sleep(15 * time.Millisecond) }
	want := 0
	for p := int32(0); p < 3; p++ {
		want += e.produce(p, "o", 30, 5, time.Now())
	}
	// 10 rows per batch = 2 messages: ~15 batches per partition queue up
	// behind the slowed inserts.
	if err := e.runUntil(e.consumer(Limits{MaxRows: 10, MaxBytes: 1 << 20, MaxWait: 20 * time.Millisecond}, nil), want, 20*time.Second); err != nil {
		t.Fatal(err)
	}
	if rows := e.ch.snapshot(); len(rows) != want || len(dups(rows)) > 0 {
		t.Fatalf("rows = %d (dups %v), want %d", len(rows), dups(rows), want)
	}
	e.ch.tmu.Lock()
	defer e.ch.tmu.Unlock()
	for p, m := range e.ch.maxInflight {
		if m > 1 {
			t.Errorf("partition %d had %d inserts in flight at once", p, m)
		}
	}
	for p, firsts := range e.ch.order {
		for i := 1; i < len(firsts); i++ {
			if firsts[i] <= firsts[i-1] {
				t.Errorf("partition %d inserts out of order: %v", p, firsts)
				break
			}
		}
		if len(firsts) < 5 {
			t.Errorf("partition %d: only %d inserts, the test needs a queue", p, len(firsts))
		}
	}
	if e.ch.maxAll < 2 {
		t.Errorf("inserts of different partitions never overlapped (max %d at once)", e.ch.maxAll)
	}
}

// A batch that cannot be inserted holds back every later batch of its
// partition (inserting them would raise max(kafka_offset) past it: loss on
// resume), while other partitions carry on.
func TestRun_FailingBatchHoldsBackItsPartitionOnly(t *testing.T) {
	e := newEnv(t, 2)
	e.ch.failIf = func(p uint16, first uint64) bool { return p == 0 && first == 0 }
	e.produce(0, "stuck", 10, 5, time.Now())
	want1 := e.produce(1, "free", 10, 5, time.Now())
	c := e.consumer(Limits{MaxRows: 10, MaxBytes: 1 << 20, MaxWait: 20 * time.Millisecond}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	waitUntil(t, 5*time.Second, "partition 1 stored", func() bool { return e.rowsOf(1) == want1 })
	time.Sleep(300 * time.Millisecond) // retries of p0's first batch keep failing
	cancel()
	if err := <-done; err == nil {
		t.Fatal("Run returned nil with a batch that never stored")
	}
	if n := e.rowsOf(0); n != 0 {
		t.Fatalf("partition 0 stored %d rows behind a failed batch: %v", n, e.ch.order[0])
	}
}

// Backpressure: with inserts stuck and pending bytes past the budget, the
// loop stops polling. The budget is checked after each poll, so the first
// wave can overshoot it by one fetch; the point is that later waves are not
// read at all until inserts catch up.
func TestRun_StopsPollingAtPendingBudget(t *testing.T) {
	e := newEnv(t, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock) // a failed assertion must not leave the fake hung
	e.ch.gate = func(uint16, uint64) { <-release }

	const budget = 8 << 10
	lim := Limits{MaxRows: 5, MaxBytes: 2 << 10, MaxWait: 10 * time.Millisecond}
	c := e.consumerWith(lim, func(cfg *Config) { cfg.MaxPendingBytes = budget })
	want := e.produce(0, "wave1", 40, 5, time.Now()) // ~40 KiB encoded: past the budget
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	defer func() { cancel(); <-done }()

	waitUntil(t, 5*time.Second, "wave 1 read", func() bool { return c.Stats.Records.Load() == 40 })
	c.mu.Lock()
	p1 := c.pending
	c.mu.Unlock()
	if p1 <= budget {
		t.Fatalf("pending %d after wave 1: the test needs it past the budget %d", p1, budget)
	}
	for w := 2; w <= 4; w++ {
		want += e.produce(0, "later", 10, 5, time.Now())
	}
	time.Sleep(300 * time.Millisecond) // ~30 poll intervals
	if n := c.Stats.Records.Load(); n != 40 {
		t.Fatalf("loop read %d records while %d bytes were pending (budget %d): no backpressure", n, p1, budget)
	}
	unblock()
	waitUntil(t, 10*time.Second, "all rows after release", func() bool { return len(e.ch.snapshot()) == want })
	if d := dups(e.ch.snapshot()); len(d) > 0 {
		t.Fatalf("duplicates: %v", d)
	}
}

// A rebalance while an insert is in flight: the revoking consumer must let
// the insert land (and commit it) before the partition moves. Otherwise the
// new owner reads max(kafka_offset) before the insert lands, re-reads the
// same records, and they are stored twice.
func TestRun_RevokeWaitsForInFlightInsert(t *testing.T) {
	e := newEnv(t, 2)
	inFlight := make(chan struct{}, 2)
	var slowed [2]atomic.Bool
	e.ch.gate = func(p uint16, first uint64) {
		if first == 0 && slowed[p].CompareAndSwap(false, true) {
			inFlight <- struct{}{}
			time.Sleep(1500 * time.Millisecond)
		}
	}
	want := e.produce(0, "r", 4, 5, time.Now())
	want += e.produce(1, "r", 4, 5, time.Now())

	a := e.consumer(fast, nil)
	actx, acancel := context.WithCancel(context.Background())
	adone := make(chan error, 1)
	go func() { adone <- a.Run(actx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-inFlight:
		case <-time.After(5 * time.Second):
			t.Fatal("first inserts never started")
		}
	}
	// Both partitions have an insert in flight; B joins and takes one.
	b := e.consumer(fast, nil)
	bctx, bcancel := context.WithCancel(context.Background())
	bdone := make(chan error, 1)
	go func() { bdone <- b.Run(bctx) }()
	time.Sleep(300 * time.Millisecond)
	want += e.produce(0, "after", 4, 5, time.Now())
	want += e.produce(1, "after", 4, 5, time.Now())

	waitUntil(t, 15*time.Second, "all rows", func() bool { return len(e.ch.snapshot()) >= want })
	time.Sleep(500 * time.Millisecond) // a late duplicate would land here
	acancel()
	bcancel()
	if err := <-adone; err != nil {
		t.Fatal(err)
	}
	if err := <-bdone; err != nil {
		t.Fatal(err)
	}
	rows := e.ch.snapshot()
	if d := dups(rows); len(d) > 0 {
		t.Fatalf("rebalance during an insert duplicated rows: %v", d)
	}
	if len(rows) != want {
		t.Fatalf("rows = %d, want %d", len(rows), want)
	}
}
