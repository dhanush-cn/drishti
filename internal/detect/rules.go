package detect

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// firing is one rule's verdict for one series at one bucket boundary.
type firing struct {
	rule string
	ev   Evidence
	from int64 // first bucket of the window behind it
}

// close evaluates every series at the end of bucket b, then folds the data
// leaving the longest window into each baseline.
func (s *Shard) close(b int64) []Event {
	var evs []Event
	for key, sr := range s.series {
		var fired []firing
		// Warm-up is in evaluate: no window is judged before it has passed.
		if !s.ignore[sr.service] {
			fired = s.evaluate(sr, b, b-s.svcFrom[sr.service])
		}
		evs = s.lifecycle(sr, b, fired, evs)
		s.foldBaseline(sr, b)
		if b-sr.lastB > s.idleB && len(sr.open) == 0 {
			delete(s.series, key)
			s.Stats.Evicted++
		}
	}
	if b%int64(time.Minute/s.cfg.Bucket) == 0 {
		// Cumulative-metric memory: a series not seen for IdleEvict starts
		// over (its next point is a first point again).
		s.deltas.Evict(uint64(s.end(b).Add(-s.cfg.IdleEvict).UnixNano()))
	}
	s.Stats.Events += int64(len(evs))
	// Series are a map: without a sort, events of one bucket would come out in
	// a different order on every run, and two replays of the same recording
	// wouldn't be byte-identical.
	slices.SortFunc(evs, eventOrder)
	return evs
}

func eventOrder(a, b Event) int {
	if c := strings.Compare(a.Rule, b.Rule); c != 0 {
		return c
	}
	if c := strings.Compare(a.Service, b.Service); c != 0 {
		return c
	}
	return strings.Compare(a.ID+a.Event, b.ID+b.Event)
}

// floorRate is the smallest per-bucket rate a series is assumed to have:
// half an event over the history seen so far. A series with no events in 30
// minutes then needs a handful in a window to fire, not one.
func (s *Shard) floorRate(age int64) float64 {
	h := min(max(age, 1), s.floorB)
	return 0.5 / float64(h)
}

func (s *Shard) evaluate(sr *series, b, age int64) []firing {
	var out []firing
	keep := func(rule string, ev Evidence, from int64) {
		for i := range out {
			if out[i].rule == rule {
				if ev.Log10P < out[i].ev.Log10P {
					out[i] = firing{rule, ev, from}
				}
				return
			}
		}
		out = append(out, firing{rule, ev, from})
	}
	floor := s.floorRate(age)
	for _, w := range s.cfg.Windows {
		n := int(w / s.cfg.Bucket)
		from := b - int64(n) + 1
		// Only windows wholly after the warm-up: one that still holds
		// warm-up data would judge it against a baseline that had nothing
		// to learn from yet.
		if int64(n) > age-s.warmB+1 {
			continue
		}
		switch sr.kind {
		case kindTemplate, kindErrorLog:
			obs := sr.count.sum(b, n)
			exp := math.Max(sr.base.mean(), floor) * float64(n)
			if ev, ok := s.countTest(obs, exp, s.dispersion(sr), w); ok {
				rule := RuleTemplateSpike
				if sr.kind == kindErrorLog {
					rule = RuleErrorLogSpike
				}
				keep(rule, ev, from)
			}
		case kindSpan:
			calls := sr.count.sum(b, n)
			phi := s.dispersion(sr)
			// Errors, as a share of calls.
			if errs := sr.errors.sum(b, n); errs > 0 {
				p0 := s.cfg.MinErrorRatio
				if m := sr.base.mean(); m > 0 {
					p0 = math.Max(sr.baseErr.mean()/m, p0)
				}
				if ev, ok := s.ratioTest(errs, calls, p0, s.ratioDispersion(sr.errRes, sr.errVar), w); ok {
					keep(RuleSpanErrors, ev, from)
				}
			}
			// Slow calls: those above the baseline's p99 bucket edge.
			if ev, ok := s.slowTest(sr, b, n, w); ok {
				keep(RuleSpanSlow, ev, from)
			}
			// Traffic: a surge by count, a drop as "silent where traffic was due".
			exp := math.Max(sr.base.mean(), floor) * float64(n)
			if ev, ok := s.countTest(calls, exp, phi, w); ok {
				keep(RuleTrafficSurge, ev, from)
			}
			// Only for steady traffic: bursty series (long-lived streams that
			// report when they end, a browser's image fetches) go quiet for
			// minutes as a matter of course (4 false alarms per 0.54 h on the
			// 3 Nov recording without this guard).
			// Not on the shortest window: 30 s of silence happens in healthy
			// traffic (the browser user's image fetches pause between pages).
			// Not over a gap the shard didn't observe (see closeUntil).
			if w >= 2*time.Minute && from >= s.resumeB && sr.labels["span.kind"] == "SERVER" && calls == 0 && sr.active.mean() >= s.cfg.DropMinActive {
				if e := sr.base.mean() * float64(n); log10PoissonZero(e) < s.cfg.Log10Alpha {
					keep(RuleTrafficDrop, Evidence{Window: w.String(), Observed: 0, Expected: round(e), Log10P: round(log10PoissonZero(e))}, from)
				}
			}
		}
	}
	switch sr.kind {
	case kindTemplate:
		if ev, ok := s.newTemplateTest(sr, b); ok {
			keep(RuleTemplateNew, ev, sr.created)
		}
	case kindMemory:
		if int64(s.cfg.MemWindow/s.cfg.Bucket) > age-s.warmB+1 {
			break
		}
		if ev, ok := s.memoryTest(sr); ok {
			keep(RuleMemoryGrowth, ev, b-int64(s.cfg.MemWindow/s.cfg.Bucket)+1)
		}
	}
	return out
}

// countTest: Poisson upper tail of obs against exp, with the minimum count
// and ratio guards (a 1.3x rise in a series that does thousands per window
// is "significant" and meaningless).
func (s *Shard) countTest(obs, exp, phi float64, w time.Duration) (Evidence, bool) {
	if int(math.Round(obs)) < s.cfg.MinCount || obs < s.cfg.MinRatio*exp {
		return Evidence{}, false
	}
	// Quasi-Poisson: a series whose events come in clumps (one order converts
	// every cart item at once) has variance phi times its mean; counting in
	// clumps of phi makes it Poisson again.
	lp := log10PoissonUpper(int(math.Floor(obs/phi+1e-9)), exp/phi)
	if lp >= s.cfg.Log10Alpha {
		return Evidence{}, false
	}
	ev := Evidence{Window: w.String(), Observed: obs, Expected: round(exp), Log10P: round(lp)}
	if phi > 1 {
		ev.Detail = fmt.Sprintf("dispersion %.3g", phi)
	}
	return ev, true
}

// ratioDispersion: how much more a series' error (or slow) counts vary per
// bucket than a binomial at its baseline ratio would (at least 1). Not the
// call count's dispersion: a browser fetches images in clumps, but in
// normal times they aren't slow together; one checkout order's calls are.
func (s *Shard) ratioDispersion(res, v ewma) float64 {
	if !s.cfg.Dispersion || v.w == 0 || v.mean() <= 0 {
		return 1
	}
	return math.Max(1, res.mean()/v.mean())
}

// dispersion is a series' variance-to-mean ratio per bucket, from its
// baseline (at least 1: Poisson). 1 when disabled or not yet known.
func (s *Shard) dispersion(sr *series) float64 {
	if !s.cfg.Dispersion || sr.base.w == 0 {
		return 1
	}
	m := sr.base.mean()
	if m <= 0 {
		return 1
	}
	return math.Max(1, (sr.base2.mean()-m*m)/m)
}

// ratioTest: binomial upper tail of k events in n calls against ratio p0.
// Calls in clumps fail or slow down in clumps too: k and n are counted in
// clumps of phi, as in countTest.
func (s *Shard) ratioTest(k, n, p0, phi float64, w time.Duration) (Evidence, bool) {
	ki, ni := int(math.Round(k)), int(math.Round(n))
	if ki < s.cfg.MinCount || ni <= 0 || k/n < s.cfg.MinRatio*p0 {
		return Evidence{}, false
	}
	kq, nq := int(math.Floor(k/phi+1e-9)), int(math.Ceil(n/phi-1e-9))
	lp := log10BinomialUpper(kq, max(nq, kq), p0)
	if lp >= s.cfg.Log10Alpha {
		return Evidence{}, false
	}
	return Evidence{Window: w.String(), Observed: k, Expected: round(n * p0), Calls: n, Ratio: round(p0), Log10P: round(lp)}, true
}

// slowTest: the baseline histogram's p99 bucket gives the "slow" edge; the
// window's calls above it are tested as a share of its calls against the
// baseline's share above it (at least MinSlowRatio). Coarse span_metrics
// buckets (…, 100, 200, 400, 800 ms, …) are fine for this: it asks "are many
// more calls in the tail than usual", not "what is p99 now".
// slowEdge: the baseline histogram's p99 bucket and the baseline share of
// calls above it (floored). ok is false while there is no usable baseline.
func (s *Shard) slowEdge(sr *series) (edge int, p0 float64, ok bool) {
	if len(sr.baseH) == 0 || sr.seenCalls < s.cfg.MinSlowCalls {
		return 0, 0, false
	}
	total := 0.0
	for _, e := range sr.baseH {
		total += e.mean()
	}
	if total <= 0 {
		return 0, 0, false
	}
	edge, acc := len(sr.baseH)-1, 0.0
	for i, e := range sr.baseH {
		acc += e.mean()
		if acc >= 0.99*total {
			edge = i
			break
		}
	}
	if edge >= len(sr.baseH)-1 {
		return 0, 0, false // p99 already in the overflow bucket: nothing above it
	}
	baseSlow := 0.0
	for _, e := range sr.baseH[edge+1:] {
		baseSlow += e.mean()
	}
	return edge, math.Max(baseSlow/total, s.cfg.MinSlowRatio), true
}

func (s *Shard) slowTest(sr *series, b int64, n int, w time.Duration) (Evidence, bool) {
	edge, p0, ok := s.slowEdge(sr)
	if !ok {
		return Evidence{}, false
	}
	slow, calls := 0.0, 0.0
	for i, r := range sr.hist {
		v := r.sum(b, n)
		calls += v
		if i > edge {
			slow += v
		}
	}
	if calls == 0 {
		return Evidence{}, false
	}
	phi := s.ratioDispersion(sr.slowRes, sr.slowVar)
	ev, ok := s.ratioTest(slow, calls, p0, phi, w)
	if ok {
		ev.Detail = fmt.Sprintf("slow = above %gms (baseline p99 bucket edge)", sr.bounds[edge])
		if phi > 1 {
			ev.Detail += fmt.Sprintf("; dispersion %.3g", phi)
		}
	}
	return ev, ok
}

// newTemplateTest: a template first seen after its service's warm-up, of
// severity WARN or worse, fires on its first ERROR line or its
// NewWarnCount-th WARN line within NewWarnWithin, and keeps firing while it
// keeps appearing in that time.
func (s *Shard) newTemplateTest(sr *series, b int64) (Evidence, bool) {
	if !sr.newTmpl || sr.severity < sevWarn || b-sr.created > s.newWithinB {
		return Evidence{}, false
	}
	need := int64(s.cfg.NewWarnCount)
	if sr.severity == sevError {
		need = 1
	}
	if sr.occurrence < need || sr.count.sum(b, int(s.resolveB)) == 0 {
		return Evidence{}, false
	}
	return Evidence{Window: (time.Duration(b-sr.created+1) * s.cfg.Bucket).String(), Observed: float64(sr.occurrence),
		Detail: "first seen " + s.start(sr.created).Format(time.RFC3339)}, true
}

// memoryTest: least squares over the memory window. A steady climb (R² high,
// slope > 0) that would reach the container's limit within MemTimeToLimit,
// or is already within MemNearLimit of it, is an anomaly; a flat high level
// (a JVM sitting under its fixed heap) or noise around a level is not.
func (s *Shard) memoryTest(sr *series) (Evidence, bool) {
	need := int(0.8 * s.cfg.MemWindow.Seconds() / 10) // docker_stats every 10 s
	if len(sr.mem) < max(need, 3) || sr.limit <= 0 {
		return Evidence{}, false
	}
	slope, r2 := linfit(sr.mem)
	cur := sr.mem[len(sr.mem)-1].v
	grew := cur - sr.mem[0].v
	if slope <= 0 || r2 < s.cfg.MemMinR2 || grew < 0.02*sr.limit {
		return Evidence{}, false
	}
	ttl := time.Duration((sr.limit - cur) / slope * float64(time.Second))
	if ttl > s.cfg.MemTimeToLimit && cur < s.cfg.MemNearLimit*sr.limit {
		return Evidence{}, false
	}
	return Evidence{Window: s.cfg.MemWindow.String(), Observed: round(cur / (1 << 20)), Expected: round(sr.mem[0].v / (1 << 20)),
		Detail: fmt.Sprintf("+%.1f MiB/min, R²=%.2f, limit %.0f MiB, at this rate in %s", slope*60/(1<<20), r2, sr.limit/(1<<20), ttl.Round(time.Second))}, true
}

// linfit returns the least-squares slope (per second) and R² of samples.
func linfit(ms []memSample) (slope, r2 float64) {
	n := float64(len(ms))
	t0 := ms[0].t
	var sx, sy, sxx, sxy, syy float64
	for _, m := range ms {
		x := m.t - t0
		sx += x
		sy += m.v
		sxx += x * x
		sxy += x * m.v
		syy += m.v * m.v
	}
	den := n*sxx - sx*sx
	if den == 0 {
		return 0, 0
	}
	slope = (n*sxy - sx*sy) / den
	vy := n*syy - sy*sy
	if vy <= 0 {
		return slope, 0
	}
	r := (n*sxy - sx*sy) / math.Sqrt(den*vy)
	return slope, r * r
}

// lifecycle turns verdicts into events: opened on the first firing,
// updated every UpdateEvery while it keeps firing, resolved after
// ResolveAfter without one.
func (s *Shard) lifecycle(sr *series, b int64, fired []firing, evs []Event) []Event {
	for _, f := range fired {
		a := sr.open[f.rule]
		if a == nil {
			if sr.open == nil {
				sr.open = map[string]*anomaly{}
			}
			a = &anomaly{id: anomalyID(f.rule, sr.key, s.end(b)), opened: b, lastFire: b, lastUpdate: b, best: f.ev, window: [2]int64{f.from, b}}
			sr.open[f.rule] = a
			sr.taint(f.from, b)
			evs = append(evs, s.event(sr, f.rule, "opened", a, b))
			continue
		}
		a.lastFire, a.best, a.window = b, f.ev, [2]int64{f.from, b}
		sr.taint(f.from, b)
		if b-a.lastUpdate >= s.updateB {
			a.lastUpdate = b
			evs = append(evs, s.event(sr, f.rule, "updated", a, b))
		}
	}
	for rule, a := range sr.open {
		if b-a.lastFire >= s.resolveB && !firedRule(fired, rule) {
			evs = append(evs, s.event(sr, rule, "resolved", a, b))
			delete(sr.open, rule)
		}
	}
	return evs
}

func firedRule(fs []firing, rule string) bool {
	for _, f := range fs {
		if f.rule == rule {
			return true
		}
	}
	return false
}

func (s *Shard) event(sr *series, rule, kind string, a *anomaly, b int64) Event {
	end := s.end(b)
	e := Event{
		Schema: 1, ID: a.id, Event: kind, Rule: rule, Service: sr.service, Series: sr.labels,
		OpenedAt: s.end(a.opened), DetectedAt: s.DetectedAt(end), EvidenceAt: s.lastTS,
		WindowStart: s.start(a.window[0]), WindowEnd: s.end(a.window[1]), Evidence: a.best,
		Detector: DetectorInfo{Version: s.version, Topic: s.topic, Partition: s.partition},
	}
	if s.lastTS.After(end) {
		e.EvidenceAt = end
	}
	if sr.kind == kindTemplate {
		e.Template = sr.template
	}
	if s.Replayed != nil {
		e.Replayed = s.Replayed(end)
	}
	if r := s.offsets[sr.topic]; r != nil {
		if span, ok := r.span(a.window[0], a.window[1]); ok {
			e.Source = &Source{Topic: sr.topic, Partition: s.partition, Offsets: span}
		}
	}
	return e
}

// taint marks buckets that belong to an anomaly: they never enter the
// baseline, so an incident can't become the new normal. Overlapping or
// adjacent ranges merge.
func (sr *series) taint(from, to int64) {
	if n := len(sr.taints); n > 0 && from <= sr.taints[n-1][1]+1 {
		sr.taints[n-1][0] = min(sr.taints[n-1][0], from)
		sr.taints[n-1][1] = max(sr.taints[n-1][1], to)
		return
	}
	sr.taints = append(sr.taints, [2]int64{from, to})
}

func (sr *series) tainted(x int64) bool {
	for len(sr.taints) > 0 && sr.taints[0][1] < x {
		sr.taints = sr.taints[1:] // wholly older than x: never needed again
	}
	return len(sr.taints) > 0 && x >= sr.taints[0][0]
}

// foldBaseline adds the bucket leaving the longest window to the baselines.
// The baseline therefore never contains the windows it is compared with: a
// 5-minute trickle can't raise its own expectation before it reaches
// significance.
func (s *Shard) foldBaseline(sr *series, b int64) {
	x := b - int64(s.ringN) + 1 // the oldest bucket still in the ring
	if x < sr.created || x < s.resumeB || sr.tainted(x) {
		return
	}
	calls := sr.count.get(x)
	sr.base.update(calls, s.decay)
	sr.base2.update(calls*calls, s.decay)
	if sr.kind == kindSpan {
		on := 0.0
		if calls > 0 {
			on = 1
		}
		sr.active.update(on, s.decay)
		// Overdispersion, at the ratios the tests would use now.
		if calls > 0 {
			p := s.cfg.MinErrorRatio
			if m := sr.base.mean(); m > 0 {
				p = math.Max(sr.baseErr.mean()/m, p)
			}
			k := sr.errors.get(x)
			sr.errRes.update((k-p*calls)*(k-p*calls), s.decay)
			sr.errVar.update(calls*p*(1-p), s.decay)
			if edge, p, ok := s.slowEdge(sr); ok {
				k := 0.0
				for i := edge + 1; i < len(sr.hist); i++ {
					k += sr.hist[i].get(x)
				}
				sr.slowRes.update((k-p*calls)*(k-p*calls), s.decay)
				sr.slowVar.update(calls*p*(1-p), s.decay)
			}
		}
		sr.baseErr.update(sr.errors.get(x), s.decay)
		for i := range sr.baseH {
			v := sr.hist[i].get(x)
			sr.baseH[i].update(v, s.decay)
			sr.seenCalls += v
		}
	}
}

func round(x float64) float64 {
	if math.IsInf(x, 0) || math.IsNaN(x) {
		return x
	}
	p := math.Pow(10, 4-math.Ceil(math.Log10(math.Abs(x)+1e-300)))
	return math.Round(x*p) / p
}
