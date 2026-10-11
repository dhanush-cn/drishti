// Package loadgen is the open-loop OTLP load generator (docs/loadgen.md).
package loadgen

import (
	"context"
	"time"
)

// Schedule is an open-loop send schedule computed arithmetically
// (docs/loadgen.md §3): request i is due at Start + i/Rate. There is no
// time.Ticker: a Ticker's channel holds one tick and drops the rest when the
// receiver falls behind, so a Ticker-driven generator silently sends fewer
// requests than scheduled. Here, falling behind means sending a burst of
// every overdue request, never skipping one.
type Schedule struct {
	Start time.Time
	Rate  float64 // requests per second
	Total int     // number of requests in the whole run (warm-up + steady)
}

// Due returns request i's due time. It is computed from i directly, not by
// adding an interval repeatedly, so rounding error never accumulates.
func (s Schedule) Due(i int) time.Time {
	return s.Start.Add(time.Duration(float64(i) * float64(time.Second) / s.Rate))
}

// Clock is the time source; tests substitute a fake.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration)
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Sleep(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// RealClock is the wall clock.
var RealClock Clock = realClock{}

// Run calls send(i, due) for every request in order, each once, at or after
// its due time. send must not block on the response (it hands the RPC to a
// goroutine). Run returns the maximum schedule lag observed: at each wake,
// now minus the due time of the next unsent request. Lag that grows while
// the receiver is healthy means the generator itself is the bottleneck and
// the run is invalid (§3, §10).
//
// If ctx ends early, Run stops and returns the number of requests sent.
func (s Schedule) Run(ctx context.Context, clk Clock, send func(i int, due time.Time)) (sent int, maxLag time.Duration) {
	i := 0
	for i < s.Total {
		if ctx.Err() != nil {
			return i, maxLag
		}
		now := clk.Now()
		if lag := now.Sub(s.Due(i)); lag > maxLag {
			maxLag = lag
		}
		// Catch-up burst: everything that is due now goes out now. At high
		// rates this is how sub-millisecond intervals work without a timer
		// per request: wake about once per ms, send what became due.
		for i < s.Total && !s.Due(i).After(now) {
			send(i, s.Due(i))
			i++
		}
		if i < s.Total {
			clk.Sleep(ctx, s.Due(i).Sub(clk.Now()))
		}
	}
	return i, maxLag
}
