package otlpmetrics

import (
	"slices"
	"testing"

	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

func kv(k, v string) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: k, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}}
}

const cum = metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE

func callsReq(svc string, start, t uint64, v int64, status string) *colmetrics.ExportMetricsServiceRequest {
	return &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{kv("service.name", svc)}},
		ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{{
			Name: "traces.span.metrics.calls",
			Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: true, AggregationTemporality: cum,
				DataPoints: []*metricsv1.NumberDataPoint{{StartTimeUnixNano: start, TimeUnixNano: t,
					Attributes: []*commonv1.KeyValue{kv("span.name", "Charge"), kv("status.code", status)},
					Value:      &metricsv1.NumberDataPoint_AsInt{AsInt: v}}}}},
		}}}},
	}}}
}

func deltasOf(t *testing.T, d *Deltas, reqs ...*colmetrics.ExportMetricsServiceRequest) []float64 {
	t.Helper()
	var out []float64
	for _, r := range reqs {
		Walk(r, d, nil, func(p Point) {
			if p.Delta == nil {
				t.Fatalf("calls point without a delta: %+v", p)
			}
			out = append(out, *p.Delta)
		})
	}
	return out
}

// First point: nothing (its total may cover hours). Then increases. A new
// start time or a decrease is a reset: the new value is the delta.
func TestWalk_CumulativeSumDeltasAndResets(t *testing.T) {
	d := NewDeltas()
	got := deltasOf(t, d,
		callsReq("payment", 100, 1000, 40, "OK"), // first: skipped
		callsReq("payment", 100, 2000, 47, "OK"), // +7
		callsReq("payment", 100, 3000, 47, "OK"), // +0
		callsReq("payment", 500, 4000, 3, "OK"),  // new start: reset, +3
		callsReq("payment", 500, 5000, 1, "OK"),  // decrease, same start: reset, +1
		callsReq("payment", 500, 6000, 9, "OK"),  // +8
		callsReq("payment", 900, 7000, 60, "OK"), // new start, higher value: still a reset, +60 (not +51)
	)
	if want := []float64{7, 0, 3, 1, 8, 60}; !slices.Equal(got, want) {
		t.Fatalf("deltas = %v, want %v", got, want)
	}
}

// Series with different attributes (here status.code) never share a
// previous point; an ERROR series appearing late starts with nothing.
func TestWalk_SeriesAreSeparatedByAttributes(t *testing.T) {
	d := NewDeltas()
	got := deltasOf(t, d,
		callsReq("payment", 100, 1000, 40, "OK"),
		callsReq("payment", 100, 1000, 5, "ERROR"), // first ERROR point: skipped
		callsReq("payment", 100, 2000, 42, "OK"),   // +2
		callsReq("payment", 100, 2000, 9, "ERROR"), // +4
		callsReq("checkout", 100, 2000, 42, "OK"),  // other service, first: skipped
	)
	if want := []float64{2, 4}; !slices.Equal(got, want) {
		t.Fatalf("deltas = %v, want %v", got, want)
	}
}

func TestSeriesID_IgnoresAttributeOrder(t *testing.T) {
	a := SeriesID("svc", "m", []*commonv1.KeyValue{kv("a", "1"), kv("b", "2")})
	b := SeriesID("svc", "m", []*commonv1.KeyValue{kv("b", "2"), kv("a", "1")})
	if a != b {
		t.Fatalf("%q != %q", a, b)
	}
}

func histReq(start, t uint64, counts []uint64, bounds []float64) *colmetrics.ExportMetricsServiceRequest {
	return &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{kv("service.name", "ad")}},
		ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{{
			Name: "traces.span.metrics.duration",
			Data: &metricsv1.Metric_Histogram{Histogram: &metricsv1.Histogram{AggregationTemporality: cum,
				DataPoints: []*metricsv1.HistogramDataPoint{{StartTimeUnixNano: start, TimeUnixNano: t,
					Attributes: []*commonv1.KeyValue{kv("span.name", "GetAds")}, BucketCounts: counts, ExplicitBounds: bounds}}}},
		}}}},
	}}}
}

func TestWalk_HistogramBucketDeltas(t *testing.T) {
	d := NewDeltas()
	bounds := []float64{10, 100, 1000}
	var got [][]uint64
	for _, r := range []*colmetrics.ExportMetricsServiceRequest{
		histReq(1, 10, []uint64{5, 2, 0, 0}, bounds),          // first: skipped
		histReq(1, 20, []uint64{9, 2, 3, 0}, bounds),          // +4 +0 +3 +0
		histReq(1, 30, []uint64{9, 1, 3, 0}, bounds),          // a bucket went down: reset
		histReq(1, 40, []uint64{9, 4, 1}, []float64{10, 100}), // bucket layout changed: nothing
		histReq(1, 50, []uint64{9, 4, 2}, []float64{10, 100}), // same new layout: +0 +0 +1
		histReq(1, 60, []uint64{9, 5, 3}, []float64{20, 200}), // same bucket count, other bounds: nothing
		histReq(1, 70, []uint64{9, 6, 3}, []float64{20, 200}), // +0 +1 +0
	} {
		Walk(r, d, nil, func(p Point) { got = append(got, p.Buckets) })
	}
	want := [][]uint64{{4, 0, 3, 0}, {9, 1, 3, 0}, {0, 0, 1}, {0, 1, 0}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if !slices.Equal(got[i], want[i]) {
			t.Fatalf("point %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// docker_stats sends memory usage as a non-monotonic sum: a level, not a
// counter. Turning it into deltas would read every drop as a reset.
func TestWalk_NonMonotonicSumIsALevel(t *testing.T) {
	req := &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{kv("container.name", "email")}},
		ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{{
			Name: "container.memory.usage.total",
			Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: false, AggregationTemporality: cum,
				DataPoints: []*metricsv1.NumberDataPoint{{TimeUnixNano: 7, Value: &metricsv1.NumberDataPoint_AsInt{AsInt: 64 << 20}}}}},
		}}}},
	}}}
	n := 0
	Walk(req, NewDeltas(), nil, func(p Point) {
		n++
		if p.Gauge == nil || *p.Gauge != 64<<20 || p.Owner.Key() != "container:email" {
			t.Fatalf("point = %+v (owner %q)", p, p.Owner.Key())
		}
	})
	if n != 1 {
		t.Fatalf("%d points, want 1 (a level has no first-point skip)", n)
	}
}

func TestWalk_DeltaTemporalityPassesThrough(t *testing.T) {
	req := callsReq("payment", 1, 2, 6, "OK")
	req.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetSum().AggregationTemporality = metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	if got := deltasOf(t, NewDeltas(), req); !slices.Equal(got, []float64{6}) {
		t.Fatalf("delta temporality: %v, want [6] (no first-point skip)", got)
	}
}

func TestWalk_WantSkipsOtherMetrics(t *testing.T) {
	d := NewDeltas()
	n := 0
	for i := uint64(0); i < 3; i++ {
		Walk(callsReq("payment", 1, i, int64(i), "OK"), d, func(name string) bool { return name == "other" }, func(Point) { n++ })
	}
	if n != 0 || d.Len() != 0 {
		t.Fatalf("unwanted metric: %d points, %d series kept", n, d.Len())
	}
}

func TestDeltas_EvictDropsStaleSeries(t *testing.T) {
	d := NewDeltas()
	deltasOf(t, d, callsReq("a", 1, 100, 1, "OK"), callsReq("b", 1, 900, 1, "OK"))
	if n := d.Evict(500); n != 1 || d.Len() != 1 {
		t.Fatalf("evicted %d, left %d; want 1 and 1", n, d.Len())
	}
	// "a" starts over: its next point is a first point again.
	if got := deltasOf(t, d, callsReq("a", 1, 1000, 5, "OK")); len(got) != 0 {
		t.Fatalf("after eviction: %v, want no delta", got)
	}
}
