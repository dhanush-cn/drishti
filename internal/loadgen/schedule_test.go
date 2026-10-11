package loadgen

import (
	"context"
	"testing"
	"time"
)

// fakeClock: Sleep advances time instantly, and send can advance it to
// simulate a generator that is slow to hand requests off.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }
func (c *fakeClock) Sleep(_ context.Context, d time.Duration) {
	if d > 0 {
		c.t = c.t.Add(d)
	}
}

func TestSchedule_DueIsArithmeticNotAccumulated(t *testing.T) {
	s := Schedule{Start: time.Unix(0, 0), Rate: 3, Total: 1}
	// 1/3 s is not representable in ns; accumulating would drift. Computed
	// from i, request 3,000,000 is due at exactly 1,000,000 s.
	if got, want := s.Due(3_000_000), time.Unix(1_000_000, 0); !got.Equal(want) {
		t.Fatalf("due = %v, want %v", got, want)
	}
}

func TestSchedule_SendsEveryRequestOnceInOrderAtOrAfterDue(t *testing.T) {
	clk := &fakeClock{t: time.Unix(100, 0)}
	s := Schedule{Start: clk.t, Rate: 1000, Total: 5000}
	next := 0
	sent, lag := s.Run(context.Background(), clk, func(i int, due time.Time) {
		if i != next {
			t.Fatalf("sent %d, want %d (skipped or reordered)", i, next)
		}
		if clk.Now().Before(due) {
			t.Fatalf("request %d sent before its due time", i)
		}
		next++
	})
	if sent != s.Total || next != s.Total {
		t.Fatalf("sent %d of %d", sent, s.Total)
	}
	if lag != 0 {
		t.Fatalf("lag %v on a generator that is never late", lag)
	}
}

// The time.Ticker failure mode: when handing off is slower than the
// interval, a Ticker drops ticks and the run quietly sends fewer requests.
// The arithmetic schedule must instead catch up with bursts, send every
// request, and report the lag so the run can be invalidated.
func TestSchedule_FallingBehindBurstsAndNeverSkips(t *testing.T) {
	clk := &fakeClock{t: time.Unix(100, 0)}
	s := Schedule{Start: clk.t, Rate: 1000, Total: 2000} // 1 ms interval
	seen := make([]bool, s.Total)
	sent, lag := s.Run(context.Background(), clk, func(i int, _ time.Time) {
		if seen[i] {
			t.Fatalf("request %d sent twice", i)
		}
		seen[i] = true
		clk.t = clk.t.Add(3 * time.Millisecond) // 3x slower than the schedule
	})
	if sent != s.Total {
		t.Fatalf("sent %d of %d: requests were skipped", sent, s.Total)
	}
	for i, ok := range seen {
		if !ok {
			t.Fatalf("request %d never sent", i)
		}
	}
	if lag < time.Second {
		t.Fatalf("max lag %v; a generator 3x behind for 2000 requests must report seconds of lag", lag)
	}
}

func TestSchedule_StopsOnCancel(t *testing.T) {
	clk := &fakeClock{t: time.Unix(100, 0)}
	s := Schedule{Start: clk.t, Rate: 1000, Total: 1000}
	ctx, cancel := context.WithCancel(context.Background())
	sent, _ := s.Run(ctx, clk, func(i int, _ time.Time) {
		if i == 9 {
			cancel()
		}
	})
	if sent >= s.Total {
		t.Fatalf("sent all %d after cancel", sent)
	}
}
