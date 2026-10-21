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
	Burst *Burst  // optional: a window at a different rate
}

// Burst is a window [At, At+For) after Start in which requests are due at
// Rate instead of the schedule's base rate (docs/loadgen.md §14).
type Burst struct {
	At, For time.Duration
	Rate    float64 // requests per second during the burst
}

// Due returns request i's due time. It is computed from i directly, not by
// adding an interval repeatedly, so rounding error never accumulates. With
// a burst the schedule is piecewise: base rate, burst rate, base rate,
// continuous at both edges.
func (s Schedule) Due(i int) time.Time {
	if s.Burst == nil {
		return s.Start.Add(time.Duration(float64(i) * float64(time.Second) / s.Rate))
	}
	b := s.Burst
	n1 := b.At.Seconds() * s.Rate // requests due before the burst
	n2 := n1 + b.For.Seconds()*b.Rate
	fi := float64(i)
	var sec float64
	switch {
	case fi < n1:
		sec = fi / s.Rate
	case fi < n2:
		sec = b.At.Seconds() + (fi-n1)/b.Rate
	default:
		sec = (b.At + b.For).Seconds() + (fi-n2)/s.Rate
	}
	return s.Start.Add(time.Duration(sec * float64(time.Second)))
}

// Requests is how many requests fall due in the first d of the schedule.
func (s Schedule) Requests(d time.Duration) int {
	if s.Burst == nil || d <= s.Burst.At {
		return int(d.Seconds() * s.Rate)
	}
	b := s.Burst
	n := b.At.Seconds()*s.Rate + min(d-b.At, b.For).Seconds()*b.Rate
	if d > b.At+b.For {
		n += (d - b.At - b.For).Seconds() * s.Rate
	}
	return int(n)
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
