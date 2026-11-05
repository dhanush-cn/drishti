package detect

import (
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/dhanush-cn/drishti/internal/drain"
	"github.com/dhanush-cn/drishti/internal/otlpmetrics"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
)

// Topic names as the shard labels its evidence.
const (
	TopicLogs    = "otlp.logs"
	TopicMetrics = "otlp.metrics"
)

const (
	metricCalls    = "traces.span.metrics.calls"
	metricDuration = "traces.span.metrics.duration"
	metricMemUsage = "container.memory.usage.total"
	metricMemLimit = "container.memory.usage.limit"
)

type sevClass uint8

const (
	sevInfo sevClass = iota
	sevWarn
	sevError
)

func (s sevClass) String() string {
	return [...]string{"info", "warn", "error"}[s]
}

// sevOf maps OTLP severity to three classes. severity_number is the
// standard; some SDKs only fill severity_text, so that is the fallback.
func sevOf(n logsv1.SeverityNumber, text string) sevClass {
	switch {
	case n >= 17:
		return sevError
	case n >= 13:
		return sevWarn
	case n > 0:
		return sevInfo
	}
	switch strings.ToLower(text) {
	case "error", "err", "fatal", "critical", "crit", "alert", "emerg", "emergency":
		return sevError
	case "warn", "warning":
		return sevWarn
	}
	return sevInfo
}

// LiveStat is one service's activity since the last TakeLive, for the
// dashboard's fast path (live.stats).
type LiveStat struct {
	Logs   float64 `json:"logs"`
	Warn   float64 `json:"warn"`
	Calls  float64 `json:"calls"`  // SERVER span calls
	Errors float64 `json:"errors"` // SERVER span calls with status ERROR
	Open   int     `json:"open_anomalies"`
}

// Stats are cumulative counters for the detector's log line.
type Stats struct {
	Records, Lines, Points, Events, Evicted, Dropped, DecodeErrors int64
}

type offRing struct {
	lo, hi []int64
	stamp  []int64
}

func newOffRing(n int) offRing {
	r := offRing{lo: make([]int64, n), hi: make([]int64, n), stamp: make([]int64, n)}
	for i := range r.stamp {
		r.stamp[i] = -1
	}
	return r
}

func (r *offRing) note(b, off int64) {
	i := int(b % int64(len(r.lo)))
	if r.stamp[i] != b {
		r.stamp[i], r.lo[i], r.hi[i] = b, off, off
		return
	}
	r.lo[i], r.hi[i] = min(r.lo[i], off), max(r.hi[i], off)
}

func (r *offRing) span(from, to int64) ([2]int64, bool) {
	out, ok := [2]int64{math.MaxInt64, -1}, false
	for b := from; b <= to; b++ {
		i := int(b % int64(len(r.lo)))
		if r.stamp[i] == b {
			out[0], out[1], ok = min(out[0], r.lo[i]), max(out[1], r.hi[i]), true
		}
	}
	return out, ok
}

// Shard is the detector state of one input partition (one topic's
// partition). Not safe for concurrent use: one goroutine owns it at a time.
type Shard struct {
	cfg       Config
	topic     string
	partition int32
	version   string

	ringN      int     // buckets kept per series: longest window + 1
	decay      float64 // per-bucket EWMA decay
	resolveB   int64
	updateB    int64
	warmB      int64
	floorB     int64
	newWithinB int64
	idleB      int64
	ignore     map[string]bool
	bucketNs   int64

	series  map[string]*series
	parsers map[string]*drain.Parser // service|severity
	deltas  *otlpmetrics.Deltas
	svcFrom map[string]int64 // service -> first bucket seen
	offsets map[string]*offRing
	cur     int64 // the open bucket; -1 before the first record
	resumeB int64 // first bucket after the last long gap: windows reaching before it saw nothing
	lastTS  time.Time

	live map[string]*LiveStat

	lreq collogs.ExportLogsServiceRequest // reused per record
	mreq colmetrics.ExportMetricsServiceRequest

	// DetectedAt sets an event's detected_at from the end of the bucket
	// being evaluated. Replay keeps the default (that bucket end: the
	// shard's notion of now); the live detector uses the wall clock.
	DetectedAt func(bucketEnd time.Time) time.Time
	// Replayed marks events produced while catching up on the start-up
	// rewind. Nil = never.
	Replayed func(bucketEnd time.Time) bool

	Stats Stats
}

func NewShard(cfg Config, topic string, partition int32, version string) *Shard {
	b := cfg.Bucket
	s := &Shard{
		cfg: cfg, topic: topic, partition: partition, version: version,
		ringN:      cfg.ringLen() + 1,
		decay:      decayFor(float64(cfg.HalfLife) / float64(b)),
		resolveB:   int64(cfg.ResolveAfter / b),
		updateB:    int64(cfg.UpdateEvery / b),
		warmB:      int64(cfg.WarmUp / b),
		floorB:     int64(cfg.FloorHistory / b),
		newWithinB: int64(cfg.NewWarnWithin / b),
		idleB:      int64(cfg.IdleEvict / b),
		ignore:     map[string]bool{},
		bucketNs:   int64(b),
		series:     map[string]*series{},
		parsers:    map[string]*drain.Parser{},
		deltas:     otlpmetrics.NewDeltas(),
		svcFrom:    map[string]int64{},
		offsets:    map[string]*offRing{},
		cur:        -1,
		live:       map[string]*LiveStat{},
		DetectedAt: func(t time.Time) time.Time { return t },
	}
	for _, n := range cfg.IgnoreServices {
		s.ignore[n] = true
	}
	return s
}

// Partition is the partition this shard holds, and Topic its topic.
func (s *Shard) Partition() int32 { return s.partition }
func (s *Shard) Topic() string    { return s.topic }

// Series is how many series the shard holds.
func (s *Shard) Series() int { return len(s.series) }

// LastRecord is the newest record timestamp seen.
func (s *Shard) LastRecord() time.Time { return s.lastTS }

func (s *Shard) bucketOf(t time.Time) int64 { return t.UnixNano() / s.bucketNs }
func (s *Shard) end(b int64) time.Time      { return time.Unix(0, (b+1)*s.bucketNs).UTC() }
func (s *Shard) start(b int64) time.Time    { return time.Unix(0, b*s.bucketNs).UTC() }

// Record decodes one Kafka record of the shard's topic and counts it. An
// undecodable record still moves the clock (and is counted): one bad
// message must not stall a partition's windows.
func (s *Shard) Record(r *kgo.Record) []Event {
	switch r.Topic {
	case TopicLogs:
		proto.Reset(&s.lreq)
		if proto.Unmarshal(r.Value, &s.lreq) != nil {
			s.Stats.DecodeErrors++
			return s.observe(r.Timestamp)
		}
		return s.Logs(&s.lreq, r.Timestamp, r.Offset)
	case TopicMetrics:
		proto.Reset(&s.mreq)
		if proto.Unmarshal(r.Value, &s.mreq) != nil {
			s.Stats.DecodeErrors++
			return s.observe(r.Timestamp)
		}
		return s.Metrics(&s.mreq, r.Timestamp, r.Offset)
	}
	return nil
}

// Logs counts one Kafka record of otlp.logs. It returns the events of every
// bucket the record's timestamp closed.
func (s *Shard) Logs(req *collogs.ExportLogsServiceRequest, ts time.Time, offset int64) []Event {
	evs := s.observe(ts)
	b := s.cur
	s.Stats.Records++
	s.offRing(TopicLogs).note(b, offset)
	for _, rl := range req.GetResourceLogs() {
		svc := otlpmetrics.OwnerOf(rl.GetResource().GetAttributes()).Key()
		s.seen(svc, b)
		live := s.liveOf(svc)
		for _, sl := range rl.GetScopeLogs() {
			for _, lr := range sl.GetLogRecords() {
				s.Stats.Lines++
				sev := sevOf(lr.GetSeverityNumber(), lr.GetSeverityText())
				live.Logs++
				if sev >= sevWarn {
					live.Warn++
				}
				s.line(svc, sev, bodyText(lr.GetBody()), b)
			}
		}
	}
	return evs
}

func bodyText(v *commonv1.AnyValue) string {
	if v == nil {
		return ""
	}
	if s, ok := v.GetValue().(*commonv1.AnyValue_StringValue); ok {
		return s.StringValue
	}
	return v.String()
}

func (s *Shard) line(svc string, sev sevClass, body string, b int64) {
	// Parsers per service and severity: an ERROR "API request failed" must not
	// merge into the INFO "API request completed" (2 of 3 tokens equal, above
	// Drain's threshold): found on the demo's frontend logs.
	pk := svc + "|" + sev.String()
	p := s.parsers[pk]
	if p == nil {
		p = drain.New(drain.DefaultConfig())
		s.parsers[pk] = p
	}
	c, isNew := p.Add(body)
	key := "log|" + pk + "|" + strconv.Itoa(c.ID)
	sr := s.get(key, func() *series {
		return &series{kind: kindTemplate, service: svc, topic: TopicLogs, severity: sev,
			labels:  map[string]string{"severity": sev.String(), "template_id": strconv.Itoa(c.ID)},
			newTmpl: isNew && b-s.svcFrom[svc] >= s.warmB}
	}, b)
	if sr == nil {
		return
	}
	sr.template = c.Template()
	sr.count.add(b, 1)
	sr.occurrence++
	sr.lastB = b
	if sev >= sevWarn {
		er := s.get("logsev|"+svc, func() *series {
			return &series{kind: kindErrorLog, service: svc, topic: TopicLogs, labels: map[string]string{"severity": "warn+"}}
		}, b)
		if er != nil {
			er.count.add(b, 1)
			er.lastB = b
		}
	}
}

var wantMetric = func(name string) bool {
	switch name {
	case metricCalls, metricDuration, metricMemUsage, metricMemLimit:
		return true
	}
	return false
}

// Metrics counts one Kafka record of otlp.metrics.
func (s *Shard) Metrics(req *colmetrics.ExportMetricsServiceRequest, ts time.Time, offset int64) []Event {
	evs := s.observe(ts)
	b := s.cur
	s.Stats.Records++
	s.offRing(TopicMetrics).note(b, offset)
	otlpmetrics.Walk(req, s.deltas, wantMetric, func(p otlpmetrics.Point) {
		s.Stats.Points++
		switch p.Metric {
		case metricCalls, metricDuration:
			s.span(p, b)
		case metricMemUsage, metricMemLimit:
			s.memory(p, ts, b)
		}
	})
	return evs
}

func (s *Shard) span(p otlpmetrics.Point, b int64) {
	svc := p.Owner.Key()
	name := otlpmetrics.Attr(p.Attrs, "span.name")
	kind := strings.TrimPrefix(otlpmetrics.Attr(p.Attrs, "span.kind"), "SPAN_KIND_")
	// INTERNAL spans repeat what their SERVER/CLIENT parents already say
	// (the frontend's "executing api route" under "GET /api/…"), and the
	// browser's INTERNAL resourceFetch spans swing with what the page loads.
	if kind == "INTERNAL" {
		return
	}
	s.seen(svc, b)
	key := "span|" + svc + "|" + name + "|" + kind
	sr := s.get(key, func() *series {
		return &series{kind: kindSpan, service: svc, topic: TopicMetrics,
			labels: map[string]string{"span.name": name, "span.kind": kind}}
	}, b)
	if sr == nil {
		return
	}
	sr.lastB = b
	if p.Delta != nil {
		v := *p.Delta
		sr.count.add(b, v)
		isErr := otlpmetrics.Attr(p.Attrs, "status.code") == "STATUS_CODE_ERROR"
		if isErr {
			sr.errors.add(b, v)
		}
		if kind == "SERVER" {
			l := s.liveOf(svc)
			l.Calls += v
			if isErr {
				l.Errors += v
			}
		}
		return
	}
	if p.Buckets == nil {
		return
	}
	if len(sr.hist) != len(p.Buckets) || !equalBounds(sr.bounds, p.Bounds) {
		// New series, or the bucket layout changed: start the latency
		// baseline over.
		sr.hist = make([]ring, len(p.Buckets))
		sr.baseH = make([]ewma, len(p.Buckets))
		for i := range sr.hist {
			sr.hist[i] = newRing(s.ringN)
		}
		sr.bounds = append([]float64(nil), p.Bounds...)
		sr.seenCalls = 0
	}
	for i, c := range p.Buckets {
		if c > 0 {
			sr.hist[i].add(b, float64(c))
		}
	}
}

func equalBounds(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *Shard) memory(p otlpmetrics.Point, ts time.Time, b int64) {
	if p.Gauge == nil || p.Owner.Container == "" {
		return
	}
	name := p.Owner.Container
	sr := s.get("mem|"+name, func() *series {
		return &series{kind: kindMemory, service: name, topic: TopicMetrics, labels: map[string]string{"container": name}}
	}, b)
	if sr == nil {
		return
	}
	s.seen(name, b)
	sr.lastB = b
	if p.Metric == metricMemLimit {
		sr.limit = *p.Gauge
		return
	}
	t := float64(ts.UnixNano()) / 1e9
	sr.mem = append(sr.mem, memSample{t: t, v: *p.Gauge})
	cut := t - s.cfg.MemWindow.Seconds()
	i := 0
	for i < len(sr.mem) && sr.mem[i].t < cut {
		i++
	}
	if i > 0 {
		sr.mem = append(sr.mem[:0], sr.mem[i:]...)
	}
}

func (s *Shard) offRing(topic string) *offRing {
	r := s.offsets[topic]
	if r == nil {
		nr := newOffRing(s.ringN)
		r = &nr
		s.offsets[topic] = r
	}
	return r
}

func (s *Shard) liveOf(svc string) *LiveStat {
	l := s.live[svc]
	if l == nil {
		l = &LiveStat{}
		s.live[svc] = l
	}
	return l
}

// TakeLive returns per-service activity since the last call, and resets it.
func (s *Shard) TakeLive() map[string]LiveStat {
	out := make(map[string]LiveStat, len(s.live))
	open := map[string]int{}
	for _, sr := range s.series {
		open[sr.service] += len(sr.open)
	}
	for svc, l := range s.live {
		v := *l
		v.Open = open[svc]
		out[svc] = v
	}
	for svc, n := range open {
		if _, ok := out[svc]; !ok && n > 0 {
			out[svc] = LiveStat{Open: n}
		}
	}
	clear(s.live)
	return out
}

func (s *Shard) seen(svc string, b int64) {
	if _, ok := s.svcFrom[svc]; !ok {
		s.svcFrom[svc] = b
	}
}

// get returns the series under key, creating it with mk at bucket b. Past
// MaxSeries it returns nil and the data is dropped (counted).
func (s *Shard) get(key string, mk func() *series, b int64) *series {
	sr := s.series[key]
	if sr != nil {
		return sr
	}
	if len(s.series) >= s.cfg.MaxSeries {
		s.Stats.Dropped++
		return nil
	}
	sr = mk()
	sr.key = key
	sr.created = b
	sr.lastB = b
	sr.count = newRing(s.ringN)
	if sr.kind == kindSpan {
		sr.errors = newRing(s.ringN)
	}
	s.series[key] = sr
	return sr
}

// observe moves the shard's clock to ts, closing every bucket before ts's.
// A record older than the open bucket (rare: per-partition timestamps are
// near-monotonic) is counted in the open bucket.
func (s *Shard) observe(ts time.Time) []Event {
	if ts.After(s.lastTS) {
		s.lastTS = ts
	}
	b := s.bucketOf(ts)
	if s.cur < 0 {
		s.cur = b
		return nil
	}
	return s.closeUntil(b)
}

// Advance closes every bucket that ends at or before t. The live detector
// calls it from the wall clock for shards that have gone quiet, so their
// windows still move (silences are detected, anomalies resolve).
func (s *Shard) Advance(t time.Time) []Event {
	if s.cur < 0 {
		return nil
	}
	return s.closeUntil(s.bucketOf(t))
}

func (s *Shard) closeUntil(b int64) []Event {
	if b <= s.cur {
		return nil
	}
	var evs []Event
	if gap := b - s.cur; gap > int64(s.ringN)+s.resolveB {
		return s.skipGap(b)
	}
	for s.cur < b {
		evs = append(evs, s.close(s.cur)...)
		s.cur++
	}
	return evs
}

// skipGap handles a long silence (a sleeping laptop, a stopped stream): the
// open bucket is closed as usual, then the clock jumps to b. Nothing was
// observed in the gap, so nothing is learned, forgotten or judged from it:
//
//   - its buckets are not evaluated (every steady series would read as a
//     traffic drop on wake);
//   - the baselines take in the observed buckets still in the ring and are
//     not decayed over the gap (decaying a 7.5 h gap scaled a baseline's
//     weight to 2^-45, so the first bucket after it became the whole
//     baseline: on the 3 Nov recording every series surged on wake);
//   - open anomalies resolve at the time they would have with no firing;
//   - idle time doesn't include the gap, so series aren't evicted on wake;
//   - cumulative metric state is forgotten: the first delta after a stopped
//     stream would put the whole gap's calls in one bucket.
func (s *Shard) skipGap(b int64) []Event {
	last := s.cur
	// The open bucket is closed if it holds records. If not, the shard was
	// moved into it by the wall clock and the host went to sleep in it: it
	// wasn't observed, so it is neither judged nor learned.
	obs := last - 1
	var evs []Event
	if s.bucketOf(s.lastTS) >= last {
		obs = last
		evs = s.close(last)
	}
	for _, sr := range s.series {
		// Observed buckets still in the ring (last-ringN+2 .. obs) would have
		// been folded by the next closes.
		for x := last - int64(s.ringN) + 2; x <= obs; x++ {
			s.foldBaseline(sr, x+int64(s.ringN)-1)
		}
		for rule, a := range sr.open {
			evs = append(evs, s.event(sr, rule, "resolved", a, a.lastFire+s.resolveB))
			delete(sr.open, rule)
		}
		sr.lastB += b - last
	}
	// Point times are the senders' clocks; a minute of slack for skew.
	s.deltas.Evict(uint64(s.end(last).Add(time.Minute).UnixNano()))
	s.cur, s.resumeB = b, b
	slices.SortFunc(evs, eventOrder)
	return evs
}
