// Package otlpmetrics turns OTLP metric exports into what the detector
// counts: per-series deltas of cumulative sums and histograms, and gauge
// values (docs/detector.md, section 3).
//
// Why deltas: the collector's span_metrics connector sends running totals
// (cumulative temporality, the default) every 5 s. The detector needs "how
// many calls / errors / calls per latency bucket since the last point", so
// it keeps the previous point of every series and subtracts.
//
// Resets: a cumulative series restarts from zero when its producer restarts
// (a new start time) or, without a new start time, shows a decrease. Both
// are treated the way Prometheus treats a counter reset: the new value is
// the count since the reset, so it is the delta. The first point of a
// series has no previous point and yields nothing: the counts since its
// start may cover hours, and counting them as one 5 s bucket would look
// like a spike. That is also what happens after the detector starts.
//
// Delta temporality (an SDK configured with it) passes through unchanged.
package otlpmetrics

import (
	"slices"
	"strings"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// Owner is who a resource's metrics belong to, with the receiver's
// partitioning fallback (docs/receiver.md): service.name, else
// container.name, else host.name.
type Owner struct {
	Service   string // service.name, or "" when absent
	Container string // container.name, or ""
	Host      string // host.name, or ""
}

// Key is the owner's identity in series keys and alerts.
func (o Owner) Key() string {
	switch {
	case o.Service != "":
		return o.Service
	case o.Container != "":
		return "container:" + o.Container
	case o.Host != "":
		return "host:" + o.Host
	}
	return "unknown_service"
}

// OwnerOf reads the owner from resource attributes.
func OwnerOf(attrs []*commonv1.KeyValue) Owner {
	var o Owner
	for _, kv := range attrs {
		switch kv.GetKey() {
		case "service.name":
			o.Service = kv.GetValue().GetStringValue()
		case "container.name":
			o.Container = kv.GetValue().GetStringValue()
		case "host.name":
			o.Host = kv.GetValue().GetStringValue()
		}
	}
	return o
}

// Attr returns attribute k as a string ("" when absent or not a string).
func Attr(attrs []*commonv1.KeyValue, k string) string {
	for _, kv := range attrs {
		if kv.GetKey() == k {
			return kv.GetValue().GetStringValue()
		}
	}
	return ""
}

// SeriesID identifies one data point stream: owner, metric name and every
// data point attribute (sorted), so two streams never share a previous point.
func SeriesID(owner string, metric string, attrs []*commonv1.KeyValue) string {
	parts := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		parts = append(parts, kv.GetKey()+"="+anyString(kv.GetValue()))
	}
	slices.Sort(parts)
	var b strings.Builder
	b.Grow(len(owner) + len(metric) + 16*len(parts))
	b.WriteString(owner)
	b.WriteByte('|')
	b.WriteString(metric)
	for _, p := range parts {
		b.WriteByte('|')
		b.WriteString(p)
	}
	return b.String()
}

func anyString(v *commonv1.AnyValue) string {
	if s, ok := v.GetValue().(*commonv1.AnyValue_StringValue); ok {
		return s.StringValue
	}
	return v.String()
}

type prevSum struct {
	start uint64
	v     float64
	seen  uint64 // time of the last point, for eviction
}

type prevHist struct {
	start  uint64
	counts []uint64
	bounds []float64
	seen   uint64
}

// Deltas keeps the previous point of every cumulative series. Not safe for
// concurrent use: the detector keeps one per Kafka partition.
type Deltas struct {
	sums  map[string]prevSum
	hists map[string]prevHist
}

func NewDeltas() *Deltas {
	return &Deltas{sums: map[string]prevSum{}, hists: map[string]prevHist{}}
}

// Len is the number of series held.
func (d *Deltas) Len() int { return len(d.sums) + len(d.hists) }

// Sum returns the increase of a cumulative sum since the series' previous
// point, and false for the first point of a series.
func (d *Deltas) Sum(id string, start, t uint64, v float64) (float64, bool) {
	p, ok := d.sums[id]
	d.sums[id] = prevSum{start: start, v: v, seen: t}
	if !ok {
		return 0, false
	}
	if start != p.start || v < p.v {
		return v, true // reset: everything since the new start
	}
	return v - p.v, true
}

// Hist returns per-bucket increases of a cumulative histogram since the
// series' previous point, and false for the first point. A change in the
// bucket layout is treated like a reset that yields nothing: the old and
// new buckets can't be subtracted.
func (d *Deltas) Hist(id string, start, t uint64, counts []uint64, bounds []float64) ([]uint64, bool) {
	p, ok := d.hists[id]
	d.hists[id] = prevHist{start: start, counts: slices.Clone(counts), bounds: slices.Clone(bounds), seen: t}
	if !ok || !slices.Equal(p.bounds, bounds) || len(p.counts) != len(counts) {
		return nil, false
	}
	out := make([]uint64, len(counts))
	reset := start != p.start
	if !reset {
		for i := range counts {
			if counts[i] < p.counts[i] {
				reset = true
				break
			}
		}
	}
	for i := range counts {
		if reset {
			out[i] = counts[i]
		} else {
			out[i] = counts[i] - p.counts[i]
		}
	}
	return out, true
}

// Evict drops series whose last point is older than before (unix ns), so a
// series that stopped reporting (a removed container, a renamed span) doesn't
// stay in memory forever. Returns how many were dropped.
func (d *Deltas) Evict(before uint64) int {
	n := 0
	for k, p := range d.sums {
		if p.seen < before {
			delete(d.sums, k)
			n++
		}
	}
	for k, p := range d.hists {
		if p.seen < before {
			delete(d.hists, k)
			n++
		}
	}
	return n
}

// Point is one data point after temporality handling.
type Point struct {
	Owner  Owner
	Metric string
	Attrs  []*commonv1.KeyValue
	Time   uint64 // unix ns

	// Exactly one of the following is set.
	Gauge   *float64 // gauge value, or a sum read as a level (e.g. memory usage)
	Delta   *float64 // increase of a monotonic sum since the previous point
	Buckets []uint64 // histogram bucket increases since the previous point
	Bounds  []float64
}

// Want decides which metrics Walk decodes; the rest are skipped without
// touching their data points (most of a demo export is metrics the detector
// doesn't read).
type Want func(name string) bool

// Walk calls fn for every data point of the wanted metrics in req, with
// cumulative sums and histograms turned into deltas through d. Non-monotonic
// sums (memory usage, limits: levels that go up and down) are reported as
// gauges. Exponential histograms and summaries are skipped (none of the
// signals the detector reads use them).
func Walk(req interface {
	GetResourceMetrics() []*metricsv1.ResourceMetrics
}, d *Deltas, want Want, fn func(Point)) {
	for _, rm := range req.GetResourceMetrics() {
		owner := OwnerOf(rm.GetResource().GetAttributes())
		key := owner.Key()
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if want != nil && !want(m.GetName()) {
					continue
				}
				switch data := m.GetData().(type) {
				case *metricsv1.Metric_Gauge:
					for _, p := range data.Gauge.GetDataPoints() {
						v := number(p)
						fn(Point{Owner: owner, Metric: m.GetName(), Attrs: p.GetAttributes(), Time: p.GetTimeUnixNano(), Gauge: &v})
					}
				case *metricsv1.Metric_Sum:
					cumulative := data.Sum.GetAggregationTemporality() == metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
					for _, p := range data.Sum.GetDataPoints() {
						v := number(p)
						pt := Point{Owner: owner, Metric: m.GetName(), Attrs: p.GetAttributes(), Time: p.GetTimeUnixNano()}
						switch {
						case !data.Sum.GetIsMonotonic():
							pt.Gauge = &v
						case cumulative:
							dv, ok := d.Sum(SeriesID(key, m.GetName(), p.GetAttributes()), p.GetStartTimeUnixNano(), p.GetTimeUnixNano(), v)
							if !ok {
								continue
							}
							pt.Delta = &dv
						default:
							pt.Delta = &v
						}
						fn(pt)
					}
				case *metricsv1.Metric_Histogram:
					cumulative := data.Histogram.GetAggregationTemporality() == metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
					for _, p := range data.Histogram.GetDataPoints() {
						pt := Point{Owner: owner, Metric: m.GetName(), Attrs: p.GetAttributes(), Time: p.GetTimeUnixNano(), Bounds: p.GetExplicitBounds()}
						if cumulative {
							b, ok := d.Hist(SeriesID(key, m.GetName(), p.GetAttributes()), p.GetStartTimeUnixNano(), p.GetTimeUnixNano(), p.GetBucketCounts(), p.GetExplicitBounds())
							if !ok {
								continue
							}
							pt.Buckets = b
						} else {
							pt.Buckets = p.GetBucketCounts()
						}
						fn(pt)
					}
				}
			}
		}
	}
}

func number(p *metricsv1.NumberDataPoint) float64 {
	switch v := p.GetValue().(type) {
	case *metricsv1.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	case *metricsv1.NumberDataPoint_AsDouble:
		return v.AsDouble
	}
	return 0
}
