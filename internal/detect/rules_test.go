package detect

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

const mib = 1 << 20

// warm runs past the warm-up with a steady INFO line from svc (so the
// service is known and nothing about it is new).
func (s *sim) warm(svc string) {
	s.run(11*time.Minute, func(int) { s.log(svc, info, "cart loaded items=3", 1) })
	s.none("steady INFO lines")
}

func TestNewErrorTemplate_FiresOnFirstLineAndResolves(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.warm("cart")
	at := s.now
	s.log("cart", errLv, "failed to charge card id=5", 1)
	s.run(5*time.Second, func(int) { s.log("cart", info, "cart loaded items=3", 1) })
	e := s.opened("first ERROR line of a new template", RuleTemplateNew, "cart")
	if want := at.Add(5 * time.Second); !e.OpenedAt.Equal(want) {
		t.Errorf("opened_at %v, want the end of the line's bucket %v", e.OpenedAt, want)
	}
	if !strings.Contains(e.Template, "failed to charge card") || e.Series["severity"] != "error" {
		t.Errorf("template %q series %v", e.Template, e.Series)
	}
	if e.Source == nil || e.Source.Topic != TopicLogs || e.Source.Offsets[0] > e.Source.Offsets[1] {
		t.Errorf("source %+v: want the logs offsets behind the window", e.Source)
	}
	// The line doesn't come back: updated while it is within the resolve
	// time, then resolved, all under the same ID.
	s.run(3*time.Minute, func(int) { s.log("cart", info, "cart loaded items=3", 1) })
	evs := s.take()
	if len(evs) == 0 || evs[len(evs)-1].Event != "resolved" {
		t.Fatalf("want updates then resolved, got %v", rules(evs))
	}
	for _, x := range evs {
		if x.ID != e.ID || x.Rule != RuleTemplateNew {
			t.Fatalf("lifecycle event %s %s %s: want ID %s", x.Event, x.Rule, x.ID, e.ID)
		}
	}
	if n := len(only(evs, "resolved")); n != 1 {
		t.Errorf("%d resolved events, want 1", n)
	}
}

func TestNewWarnTemplate_NeedsSecondLine(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.warm("cart")
	s.log("cart", warn, "retrying db connection attempt=1", 1)
	s.run(time.Minute, func(int) { s.log("cart", info, "cart loaded items=3", 1) })
	s.none("one WARN line of a new template")
	// Masked number: attempt=2 is the same template.
	s.log("cart", warn, "retrying db connection attempt=2", 1)
	s.run(5*time.Second, nil)
	s.opened("second WARN line within 5 min", RuleTemplateNew, "cart")
}

func TestWarmUp_NothingFires(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(5*time.Minute, func(int) { s.log("cart", info, "cart loaded items=3", 1) })
	s.run(30*time.Second, func(int) { s.log("cart", errLv, "failed to charge card id=5", 5) })
	s.run(10*time.Minute, func(int) { s.log("cart", info, "cart loaded items=3", 1) })
	s.none("errors during warm-up, then quiet")
}

func TestIgnoredService(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.warm("load-generator")
	s.run(time.Minute, func(int) { s.log("load-generator", errLv, "request to /api/cart failed status=500", 5) })
	s.none("the load generator's own errors")
}

func TestTemplateSpike_TaintKeepsTheBaseline(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(30*time.Minute, func(int) { s.log("cart", info, "GET /api/cart", 1) })
	s.none("30 min of a steady line")
	s.run(time.Minute, func(int) { s.log("cart", info, "GET /api/cart", 8) })
	e := s.opened("8x the line's rate", RuleTemplateSpike, "cart")
	// An incident that lasts must not become the new normal: the buckets
	// behind a firing never enter the baseline, so 20 min at 8x is still
	// one open anomaly, updated every 30 s.
	s.run(20*time.Minute, func(int) { s.log("cart", info, "GET /api/cart", 8) })
	evs := s.take()
	if r := only(evs, "resolved"); len(r) > 0 || len(only(evs, "opened")) > 0 {
		t.Fatalf("a sustained spike resolved or reopened: %v", rules(evs))
	}
	up := only(evs, "updated")
	if len(up) < 35 {
		t.Fatalf("%d updates in 20 min, want one per 30 s", len(up))
	}
	for i := 1; i < len(up); i++ {
		if d := up[i].DetectedAt.Sub(up[i-1].DetectedAt); d != 30*time.Second || up[i].ID != e.ID {
			t.Fatalf("update %d: %v after the previous, id %s: want 30 s, id %s", i, d, up[i].ID, e.ID)
		}
	}
	// Back to normal: resolved once the longest window no longer fires.
	s.run(8*time.Minute, func(int) { s.log("cart", info, "GET /api/cart", 1) })
	if r := only(s.take(), "resolved"); len(r) != 1 || r[0].ID != e.ID {
		t.Fatalf("want the spike resolved after it ends, got %v", r)
	}
}

// The bucket folded into the baseline at bucket b is the one leaving the
// longest window, never one of the windows compared with it.
func TestBaseline_FoldsOnlyTheBucketLeavingTheLongestWindow(t *testing.T) {
	cfg := DefaultConfig()
	s := NewShard(cfg, TopicLogs, 0, "t")
	sr := s.get("k", func() *series { return &series{kind: kindTemplate} }, 0)
	sr.count.add(0, 10)
	longest := int64(5 * time.Minute / cfg.Bucket)
	for b := int64(0); b < longest; b++ {
		s.foldBaseline(sr, b)
		if sr.base.w != 0 {
			t.Fatalf("bucket 0 folded at bucket %d, while the %v window still holds it", b, 5*time.Minute)
		}
	}
	s.foldBaseline(sr, longest)
	if m := sr.base.mean(); m != 10 {
		t.Fatalf("baseline %v after bucket 0 left the longest window, want 10", m)
	}
}

func TestSpanErrors(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(15*time.Minute, func(int) { s.calls("frontend", "GET /api/cart", "SERVER", "STATUS_CODE_UNSET", 20) })
	s.none("steady successful calls")
	s.run(time.Minute, func(int) {
		s.calls("frontend", "GET /api/cart", "SERVER", "STATUS_CODE_UNSET", 15)
		s.calls("frontend", "GET /api/cart", "SERVER", "STATUS_CODE_ERROR", 5)
	})
	e := s.opened("25% of calls failing", RuleSpanErrors, "frontend")
	if e.Series["span.name"] != "GET /api/cart" || e.Evidence.Calls == 0 || e.Evidence.Observed == 0 {
		t.Errorf("evidence %+v series %v", e.Evidence, e.Series)
	}
}

func TestSpanSlow(t *testing.T) {
	s := newSim(t, DefaultConfig())
	fast := []uint64{0, 20, 0, 0, 0, 0, 0}
	slow := []uint64{0, 10, 0, 0, 0, 10, 0}
	step := func(h []uint64) func(int) {
		return func(int) {
			s.calls("frontend", "GET /api/products", "SERVER", "STATUS_CODE_UNSET", 20)
			s.hist("frontend", "GET /api/products", "SERVER", h)
		}
	}
	s.run(15*time.Minute, step(fast))
	s.none("steady fast calls")
	s.run(time.Minute, step(slow))
	e := s.opened("half the calls at 500-1000 ms", RuleSpanSlow, "frontend")
	if !strings.Contains(e.Evidence.Detail, "50ms") {
		t.Errorf("detail %q: want the 50 ms edge (baseline p99 bucket)", e.Evidence.Detail)
	}
}

func TestTrafficSurge(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(15*time.Minute, func(int) { s.calls("frontend", "POST /api/checkout", "SERVER", "STATUS_CODE_UNSET", 10) })
	s.none("steady calls")
	s.run(time.Minute, func(int) { s.calls("frontend", "POST /api/checkout", "SERVER", "STATUS_CODE_UNSET", 60) })
	s.opened("6x the calls", RuleTrafficSurge, "frontend")
}

func TestInternalSpansAreSkipped(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(15*time.Minute, func(int) { s.calls("frontend", "executing api route", "INTERNAL", "STATUS_CODE_UNSET", 10) })
	s.run(time.Minute, func(int) { s.calls("frontend", "executing api route", "INTERNAL", "STATUS_CODE_UNSET", 100) })
	s.none("10x INTERNAL spans")
}

func TestTrafficDrop(t *testing.T) {
	steady := func(kind string) *sim {
		s := newSim(t, DefaultConfig())
		s.run(20*time.Minute, func(int) { s.calls("frontend", "GET /", kind, "STATUS_CODE_UNSET", 10) })
		s.none("steady " + kind + " calls")
		return s
	}
	t.Run("server", func(t *testing.T) {
		s := steady("SERVER")
		s.run(3*time.Minute, func(int) { s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", 0) })
		e := s.opened("steady SERVER calls stop", RuleTrafficDrop, "frontend")
		if w, _ := time.ParseDuration(e.Evidence.Window); w < 2*time.Minute {
			t.Errorf("fired on the %v window: drops are judged on 2 min and longer", w)
		}
	})
	t.Run("client", func(t *testing.T) {
		s := steady("CLIENT")
		s.run(3*time.Minute, func(int) { s.calls("frontend", "GET /", "CLIENT", "STATUS_CODE_UNSET", 0) })
		s.none("CLIENT calls stop (the callee's SERVER span says it)")
	})
	t.Run("pause", func(t *testing.T) {
		s := steady("SERVER")
		s.run(time.Minute, func(int) { s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", 0) })
		s.run(5*time.Minute, func(int) { s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", 10) })
		s.none("a 60 s pause")
	})
	t.Run("bursty", func(t *testing.T) {
		s := newSim(t, DefaultConfig())
		s.run(20*time.Minute, func(i int) {
			s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", float64(20*(i%2)))
		})
		s.run(3*time.Minute, func(int) { s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", 0) })
		s.none("a series busy in half its buckets goes quiet")
	})
}

func TestMemoryGrowth(t *testing.T) {
	const limit = 100 * mib
	run := func(s *sim, d time.Duration, usage func(sec float64) float64) {
		s.run(d, func(i int) {
			if i%2 == 0 { // docker_stats every 10 s
				s.mem("email", usage(s.now.Sub(t0).Seconds()), limit)
			}
		})
	}
	t.Run("climb toward the limit", func(t *testing.T) {
		s := newSim(t, DefaultConfig())
		jit := func(sec float64) float64 { return float64(int(sec/10)%2) * 0.2 * mib }
		run(s, 12*time.Minute, func(sec float64) float64 { return 30*mib + jit(sec) })
		s.none("flat usage")
		from := s.now.Sub(t0).Seconds()
		run(s, 5*time.Minute, func(sec float64) float64 { return 30*mib + 5*mib*(sec-from)/60 })
		e := s.opened("+5 MiB/min, limit 9 min away", RuleMemoryGrowth, "email")
		if e.Series["container"] != "email" {
			t.Errorf("series %v", e.Series)
		}
	})
	t.Run("flat near the limit", func(t *testing.T) {
		s := newSim(t, DefaultConfig())
		run(s, 25*time.Minute, func(sec float64) float64 { return 92*mib + float64(int(sec/10)%2)*0.6*mib })
		s.none("a JVM sitting under its heap")
	})
	t.Run("slow climb far from the limit", func(t *testing.T) {
		s := newSim(t, DefaultConfig())
		run(s, 25*time.Minute, func(sec float64) float64 { return 30*mib + 0.5*mib*sec/60 })
		s.none("+0.5 MiB/min with the limit hours away")
	})
	t.Run("climb past 90% of the limit", func(t *testing.T) {
		s := newSim(t, DefaultConfig())
		// +0.5 MiB/min: at 91-92 MiB the limit is 16-18 min away, beyond
		// the time-to-limit test; only "above 90% and rising" fires.
		run(s, 16*time.Minute, func(sec float64) float64 { return 84*mib + 0.5*mib*sec/60 })
		s.opened("still rising above 90%", RuleMemoryGrowth, "email")
	})
}

func TestMaxSeries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxSeries = 2
	s := newSim(t, cfg)
	s.log("cart", info, "a b c", 1)
	s.log("cart", info, "x y", 1)
	s.log("cart", info, "p q r s t", 1)
	sh := s.d.Shard(TopicLogs, 0)
	if sh.Series() != 2 || sh.Stats.Dropped != 1 {
		t.Fatalf("%d series, %d dropped: want 2 kept, 1 dropped", sh.Series(), sh.Stats.Dropped)
	}
}

func spikeEvents(t *testing.T) []Event {
	s := newSim(t, DefaultConfig())
	s.run(30*time.Minute, func(int) {
		s.log("cart", info, "GET /api/cart", 1)
		s.log("checkout", info, "order placed id=1", 1)
	})
	s.run(2*time.Minute, func(int) {
		s.log("cart", info, "GET /api/cart", 8)
		s.log("checkout", info, "order placed id=1", 8)
	})
	s.run(10*time.Minute, func(int) {
		s.log("cart", info, "GET /api/cart", 1)
		s.log("checkout", info, "order placed id=1", 1)
	})
	return s.take()
}

// The same records give the same events, IDs included, in the same order:
// a restarted detector re-emits an anomaly under the ID it had, and replays
// are byte-comparable.
func TestDeterministic(t *testing.T) {
	a, b := spikeEvents(t), spikeEvents(t)
	if len(a) == 0 {
		t.Fatal("no events")
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("two runs on the same records differ:\n%v\n%v", rules(a), rules(b))
	}
	if anomalyID("r", "s", t0) != anomalyID("r", "s", t0) || anomalyID("r", "s", t0) == anomalyID("r", "s2", t0) ||
		anomalyID("r", "s", t0) == anomalyID("r", "s", t0.Add(5*time.Second)) {
		t.Fatal("anomalyID: same inputs must give the same ID, different ones different IDs")
	}
}

// A metrics partition catching up (records arriving in 3-minute batches,
// timestamps behind the logs partition with the same number) must count every
// record in its own bucket: one shard per input partition.
func TestShardPerInputPartition(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.noTick = true // replay semantics: each shard's clock is its own records
	const batch = 36
	s.run(40*time.Minute, func(i int) {
		s.log("frontend", info, "GET /api/cart", 1)
		if i%batch == batch-1 {
			for j := batch - 1; j >= 0; j-- {
				ts := s.ts().Add(-time.Duration(j) * DefaultConfig().Bucket)
				s.feedAt(TopicMetrics, 0, ts, s.callsReq("frontend", "GET /api/cart", "SERVER", "STATUS_CODE_UNSET", 10))
			}
		}
	})
	s.none("steady traffic delivered in batches")
	if n := len(s.d.Shards()); n != 2 {
		t.Fatalf("%d shards, want one per input partition (2)", n)
	}
}

// A host that sleeps for hours: nothing was observed, so nothing fires on
// wake, and nothing is learned or forgotten from the gap.
func TestLongGap(t *testing.T) {
	s := newSim(t, DefaultConfig())
	step := func(int) {
		s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", 10)
		s.log("frontend", info, "GET / 200", 1)
	}
	s.log("frontend", info, "startup complete", 1)
	s.run(20*time.Minute, step)
	s.none("steady traffic")
	sh := s.d.Shard(TopicMetrics, 0)
	span := sh.series["span|frontend|GET /|SERVER"]
	before, nSeries := span.base, s.d.Shard(TopicLogs, 0).Series()

	s.noTick = true
	s.run(2*time.Hour, nil)
	// The services kept counting while nothing was delivered (a stopped
	// stream): the first point after the gap holds 2 h of calls.
	s.cum["frontend|GET /|SERVER|STATUS_CODE_UNSET"] += 1440 * 10
	s.noTick = false
	// Logs come back first, calls a minute later.
	s.run(time.Minute, func(int) { s.log("frontend", info, "GET / 200", 1) })
	// Just after wake (what follows it will be learned 5 min later, as usual).
	if after := span.base; after.w <= before.w || math.Abs(after.mean()-before.mean()) > 1e-9*before.mean()+1e-9 {
		t.Errorf("span baseline %+v (mean %v) after the gap, was %+v (mean %v): the gap must not age or move it, and the observed buckets before it are learned",
			after, after.mean(), before, before.mean())
	}
	if n := s.d.Shard(TopicLogs, 0).Series(); n != nSeries {
		t.Errorf("%d log series after the gap, had %d: the gap is not idle time", n, nSeries)
	}
	s.run(15*time.Minute, step)
	s.none("the same traffic after a 2 h gap")

	// An anomaly open when the host sleeps resolves when it would have,
	// before the wake, not after it.
	s.run(time.Minute, func(int) { s.log("frontend", info, "GET / 200", 8) })
	e := s.opened("spike", RuleTemplateSpike, "frontend")
	s.noTick = true
	s.run(2*time.Hour, nil)
	wake := s.now
	s.noTick = false
	s.run(3*time.Minute, step)
	r := only(s.take(), "resolved")
	if len(r) != 1 || r[0].ID != e.ID || !r[0].DetectedAt.Before(wake) {
		t.Fatalf("want the open spike resolved during the gap (before %v), got %v", wake, r)
	}
}

// Errors below 3x the 0.5% floor don't fire, even on a series that never had
// any (its baseline error ratio is 0).
func TestSpanErrors_FloorRatio(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(15*time.Minute, func(int) { s.calls("frontend", "GET /api/cart", "SERVER", "STATUS_CODE_UNSET", 20) })
	s.run(10*time.Minute, func(i int) {
		s.calls("frontend", "GET /api/cart", "SERVER", "STATUS_CODE_UNSET", 20)
		if i%6 == 0 {
			s.calls("frontend", "GET /api/cart", "SERVER", "STATUS_CODE_ERROR", 1)
		}
	})
	s.none("0.8% of calls failing on a series with no error history")
}

func TestSpanSlow_FloorRatio(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(25*time.Minute, func(i int) {
		s.calls("frontend", "GET /api/products", "SERVER", "STATUS_CODE_UNSET", 20)
		h := []uint64{0, 20, 0, 0, 0, 0, 0}
		if i > 180 && i%6 == 0 {
			h = []uint64{0, 19, 0, 0, 0, 1, 0}
		}
		s.hist("frontend", "GET /api/products", "SERVER", h)
	})
	s.none("0.8% slow calls on a series that never had any")
}

func TestTrafficSurge_MinRatio(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.run(15*time.Minute, func(int) { s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", 1000) })
	s.run(5*time.Minute, func(int) { s.calls("frontend", "GET /", "SERVER", "STATUS_CODE_UNSET", 1100) })
	s.none("10% more calls on a busy series (significant, not meaningful)")
}

// A series with no history still has a finite expectation (the floor), so
// a first burst is judged by a probability, not an infinite one, and the
// event can be encoded (encoding/json refuses -Inf).
func TestFloorRate_FirstBurstIsFinite(t *testing.T) {
	s := newSim(t, DefaultConfig())
	s.warm("cart")
	s.run(30*time.Second, func(int) { s.log("cart", info, "cache warmed region=eu", 2) })
	s.run(5*time.Second, nil)
	e := s.opened("a new INFO template's first burst", RuleTemplateSpike, "cart")
	if _, err := json.Marshal(e); err != nil || math.IsInf(e.Evidence.Log10P, 0) || e.Evidence.Expected <= 0 {
		t.Fatalf("log10p %v expected %v: %v", e.Evidence.Log10P, e.Evidence.Expected, err)
	}
}

func TestMemoryGrowth_Guards(t *testing.T) {
	const limit = 100 * mib
	run := func(s *sim, d time.Duration, usage func(i int) float64) {
		s.run(d, func(i int) {
			if i%2 == 0 {
				s.mem("email", usage(i), limit)
			}
		})
	}
	t.Run("noise near the limit", func(t *testing.T) {
		s := newSim(t, DefaultConfig())
		x := uint32(1)
		run(s, 30*time.Minute, func(int) float64 {
			x = x*1664525 + 1013904223
			return 92*mib + float64(x>>24)/255*6*mib - 3*mib
		})
		s.none("±3 MiB noise around 92% (no trend)")
	})
	t.Run("creeping near the limit", func(t *testing.T) {
		s := newSim(t, DefaultConfig())
		run(s, 30*time.Minute, func(i int) float64 { return 91*mib + 0.2*mib*float64(i)*5/60 })
		s.none("+0.2 MiB/min at 91%: under 2% of the limit in the window")
	})
}

// Calls that come in clumps (an order converts each of its 6 cart items at
// once, one order every 30 s): a burst of three orders in one bucket is
// ordinary for this series, a sustained rise is not. Poisson would call the
// burst 10^-8 surprising (capture B: 11 of 15 false alarms were this).
func TestDispersion_ClumpedCalls(t *testing.T) {
	order := func(s *sim, n float64) {
		s.calls("checkout", "Convert", "CLIENT", "STATUS_CODE_UNSET", 6*n)
	}
	baseline := func() *sim {
		s := newSim(t, DefaultConfig())
		s.run(20*time.Minute, func(i int) {
			if i%6 == 0 {
				order(s, 1)
			} else {
				order(s, 0)
			}
		})
		s.none("one order every 30 s")
		return s
	}
	t.Run("burst of orders", func(t *testing.T) {
		s := baseline()
		s.run(time.Minute, func(i int) {
			if i == 0 {
				order(s, 3)
			} else {
				order(s, 0)
			}
		})
		s.none("three orders at once")
	})
	t.Run("sustained surge", func(t *testing.T) {
		s := baseline()
		s.run(2*time.Minute, func(int) { order(s, 1) })
		e := s.opened("an order every bucket (6x)", RuleTrafficSurge, "checkout")
		if !strings.Contains(e.Evidence.Detail, "dispersion") {
			t.Errorf("detail %q: want the dispersion it was judged with", e.Evidence.Detail)
		}
	})
}

// Failures that come per order (one order in ten fails, all 6 of its calls)
// vary far more than a binomial at 10%: two failed orders at once are
// ordinary, every order failing is not.
func TestDispersion_FailuresPerOrder(t *testing.T) {
	n := 0
	order := func(s *sim, fail bool) {
		if fail {
			s.calls("checkout", "Charge", "CLIENT", "STATUS_CODE_ERROR", 6)
		} else {
			s.calls("checkout", "Charge", "CLIENT", "STATUS_CODE_UNSET", 6)
		}
	}
	baseline := func() *sim {
		s := newSim(t, DefaultConfig())
		s.calls("checkout", "Charge", "CLIENT", "STATUS_CODE_ERROR", 0) // both series exist from the start
		s.run(30*time.Minute, func(i int) {
			if i%6 == 0 {
				n++
				order(s, n%10 == 0)
			}
		})
		s.none("one order in ten failing")
		return s
	}
	t.Run("two failed orders", func(t *testing.T) {
		s := baseline()
		s.run(time.Minute, func(i int) {
			if i == 0 {
				order(s, true)
				order(s, true)
			}
		})
		s.none("two failed orders at once")
	})
	t.Run("every order fails", func(t *testing.T) {
		s := baseline()
		s.run(5*time.Minute, func(i int) {
			if i%6 == 0 {
				order(s, true)
			}
		})
		s.opened("every order failing", RuleSpanErrors, "checkout")
	})
}

// A browser fetches a page's 12 images at once (calls clumped), but in
// normal times they aren't slow together: when they all turn slow it must
// fire on the shortest window, not wait for the call count's dispersion.
func TestDispersion_SlowIsNotCallClumping(t *testing.T) {
	s := newSim(t, DefaultConfig())
	page := func(slow bool) {
		h := []uint64{0, 12, 0, 0, 0, 0, 0}
		if slow {
			h = []uint64{0, 0, 0, 0, 0, 12, 0}
		}
		s.calls("frontend-proxy", "GET", "SERVER", "STATUS_CODE_UNSET", 12)
		s.hist("frontend-proxy", "GET", "SERVER", h)
	}
	s.run(20*time.Minute, func(i int) {
		if i%3 == 0 {
			page(false)
		} else {
			s.calls("frontend-proxy", "GET", "SERVER", "STATUS_CODE_UNSET", 0)
			s.hist("frontend-proxy", "GET", "SERVER", make([]uint64, 7))
		}
	})
	s.none("a page every 15 s")
	s.run(30*time.Second, func(i int) {
		if i%3 == 0 {
			page(true)
		} else {
			s.calls("frontend-proxy", "GET", "SERVER", "STATUS_CODE_UNSET", 0)
			s.hist("frontend-proxy", "GET", "SERVER", make([]uint64, 7))
		}
	})
	e := s.opened("every image slow", RuleSpanSlow, "frontend-proxy")
	if e.Evidence.Window != "30s" {
		t.Errorf("fired on the %s window: want 30s (slowness isn't clumped here, only calls are)", e.Evidence.Window)
	}
}

// The slow-share dispersion is learned from how slow calls arrive: the same
// 0.5% share, all of an order's 6 calls at once vs one call at a time.
func TestDispersion_SlowShareLearned(t *testing.T) {
	learn := func(slowAt func(x int64) float64) float64 {
		cfg := DefaultConfig()
		cfg.HalfLife = 2 * time.Hour // long memory: rare clumps
		s := NewShard(cfg, TopicMetrics, 0, "t")
		sr := s.get("span|checkout|POST|CLIENT", func() *series { return &series{kind: kindSpan} }, 0)
		sr.bounds = testBounds
		for range len(testBounds) + 1 {
			sr.hist = append(sr.hist, newRing(s.ringN))
			sr.baseH = append(sr.baseH, ewma{})
		}
		for x := int64(0); x < 6000; x++ {
			slow := slowAt(x)
			sr.count.add(x, 6)
			sr.hist[1].add(x, 6-slow)
			sr.hist[5].add(x, slow)
			s.foldBaseline(sr, x+int64(s.ringN)-1)
		}
		return s.ratioDispersion(sr.slowRes, sr.slowVar)
	}
	clumped := learn(func(x int64) float64 {
		if x%200 == 7 {
			return 6
		}
		return 0
	})
	spread := learn(func(x int64) float64 {
		if x%33 == 7 {
			return 1
		}
		return 0
	})
	if clumped < 2 || spread > 1.5 { // clumped: ~3 in theory (6 calls vs a 1% floor)
		t.Fatalf("slow-share dispersion: clumped %.2f (want > 2), spread %.2f (want ~1)", clumped, spread)
	}
}
