package metricsql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/chtest"
	"github.com/dhanush-cn/drishti/internal/otlpmetrics"
	"github.com/dhanush-cn/drishti/internal/storage"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

var t0 = time.Date(2026, 11, 3, 17, 0, 0, 0, time.UTC)

func res(k, v string) *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{Key: k, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}}}}
}

func one(r *resourcev1.Resource, m *metricsv1.Metric) *colmetrics.ExportMetricsServiceRequest {
	return &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{Resource: r, ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{m}}}}}}
}

// fixture is 4 series, one point per record, every 5 s for 40 steps, with
// every kind of reset the rules name:
//
//	cart  calls      cumulative sum: start time changes at step 15 while the value goes UP
//	                 (only the start-time rule can see that reset), value drops (no new start) at 27
//	late  calls      cumulative sum whose first point is inside the window (yields nothing)
//	quote calls      delta sum with no start time: passes through (the cumulative rule would subtract)
//	payment duration cumulative histogram: a bucket drops at 10, bounds change at 20, start changes
//	                 at 30 with every bucket going up (only the start-time rule sees it)
//	email memory     non-monotonic sum (a level): read as a gauge
//
// Each rule has a point that only that rule explains: the revert-proofs in
// docs/storage.md break one rule at a time.
func fixture() []*colmetrics.ExportMetricsServiceRequest {
	cum := metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
	del := metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA
	var out []*colmetrics.ExportMetricsServiceRequest
	start1 := uint64(t0.Add(-time.Hour).UnixNano())
	start2 := uint64(t0.Add(75 * time.Second).UnixNano())
	sumV := 1000.0
	hist := []uint64{50, 40, 10}
	bounds := []float64{2, 10}
	hstart := start1
	for i := range 40 {
		t := uint64(t0.Add(time.Duration(i) * 5 * time.Second).UnixNano())
		// cart: running total grows by i+1; reset by start time at 15 (counts restart at 3),
		// and a drop without a new start at 27.
		st := start1
		switch {
		case i == 15:
			sumV += 500 // restarted producer that already counted past the old total
		case i == 27:
			sumV = 1
		default:
			sumV += float64(i + 1)
		}
		if i >= 15 {
			st = start2
		}
		out = append(out, one(res("service.name", "cart"), &metricsv1.Metric{Name: "calls", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: true, AggregationTemporality: cum,
			DataPoints: []*metricsv1.NumberDataPoint{{TimeUnixNano: t, StartTimeUnixNano: st, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: sumV}}}}}}))
		out = append(out, one(res("service.name", "quote"), &metricsv1.Metric{Name: "calls", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: true, AggregationTemporality: del,
			DataPoints: []*metricsv1.NumberDataPoint{{TimeUnixNano: t, Value: &metricsv1.NumberDataPoint_AsInt{AsInt: int64(i%4 + 1)}}}}}}))
		if i >= 20 {
			out = append(out, one(res("service.name", "late"), &metricsv1.Metric{Name: "calls", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: true, AggregationTemporality: cum,
				DataPoints: []*metricsv1.NumberDataPoint{{TimeUnixNano: t, StartTimeUnixNano: start1, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: float64(10 * i)}}}}}}))
		}
		switch {
		case i == 10:
			hist = []uint64{1, 1, 1} // a bucket went down: reset
		case i == 20:
			bounds = []float64{2, 10, 50}
			hist = []uint64{hist[0] + 1, hist[1] + 1, hist[2] + 1, 0} // new layout: yields nothing
		case i == 30:
			hstart = start2
			for j := range hist {
				hist[j] += 7 // restarted, but every bucket already above the old counts
			}
		default:
			for j := range hist {
				hist[j] += uint64((i + j) % 3)
			}
		}
		var n uint64
		for _, c := range hist {
			n += c
		}
		out = append(out, one(res("service.name", "payment"), &metricsv1.Metric{Name: "duration", Data: &metricsv1.Metric_Histogram{Histogram: &metricsv1.Histogram{AggregationTemporality: cum,
			DataPoints: []*metricsv1.HistogramDataPoint{{TimeUnixNano: t, StartTimeUnixNano: hstart, Count: n, ExplicitBounds: append([]float64(nil), bounds...), BucketCounts: append([]uint64(nil), hist...)}}}}}))
		out = append(out, one(res("container.name", "email"), &metricsv1.Metric{Name: "memory", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: false, AggregationTemporality: cum,
			DataPoints: []*metricsv1.NumberDataPoint{{TimeUnixNano: t, StartTimeUnixNano: start1, Value: &metricsv1.NumberDataPoint_AsInt{AsInt: int64(1e6 + i*1000)}}}}}}))
	}
	return out
}

func key(service string, t uint64) string { return fmt.Sprintf("%s@%d", service, t) }

// expected runs the fixture through the detector's own delta code.
func expected(reqs []*colmetrics.ExportMetricsServiceRequest, from, until time.Time) (sums, hists, gauges map[string]string) {
	sums, hists, gauges = map[string]string{}, map[string]string{}, map[string]string{}
	d := otlpmetrics.NewDeltas()
	lo, hi := uint64(from.UnixNano()), uint64(until.UnixNano())
	for _, r := range reqs {
		otlpmetrics.Walk(r, d, nil, func(p otlpmetrics.Point) {
			if p.Time <= lo || p.Time > hi {
				return
			}
			k := key(p.Owner.Key(), p.Time)
			switch {
			case p.Delta != nil:
				sums[k] = strconv.FormatFloat(*p.Delta, 'g', -1, 64)
			case p.Buckets != nil:
				s := make([]string, len(p.Buckets))
				for i, b := range p.Buckets {
					s[i] = strconv.FormatUint(b, 10)
				}
				hists[k] = "[" + strings.Join(s, ",") + "]"
			case p.Gauge != nil:
				gauges[k] = strconv.FormatFloat(*p.Gauge, 'g', -1, 64)
			}
		})
	}
	return
}

func query(t *testing.T, s *chtest.Server, sql, metric string, from, until time.Time, valueCol int) map[string]string {
	t.Helper()
	out, err := s.QueryParams("SELECT service, toUnixTimestamp64Nano(ts), "+[]string{"", "", "", "", "delta", "delta", "value"}[valueCol]+" FROM ("+sql+") FORMAT TabSeparated", Params(metric, from, until, nil))
	if err != nil {
		t.Fatalf("%v\n%s", err, sql)
	}
	got := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		f := strings.Split(line, "\t")
		n, _ := strconv.ParseUint(f[1], 10, 64)
		v := f[2]
		if valueCol != 5 { // numbers: compare values, not ClickHouse's vs Go's formatting
			x, err := strconv.ParseFloat(v, 64)
			if err != nil {
				t.Fatalf("value %q: %v", v, err)
			}
			v = strconv.FormatFloat(x, 'g', -1, 64)
		}
		got[key(f[0], n)] = v
	}
	return got
}

func diff(t *testing.T, what string, got, want map[string]string) {
	t.Helper()
	var bad []string
	for k, w := range want {
		if got[k] != w {
			bad = append(bad, fmt.Sprintf("%s: sql %q, go %q", k, got[k], w))
		}
	}
	for k, g := range got {
		if _, ok := want[k]; !ok {
			bad = append(bad, fmt.Sprintf("%s: sql %q, go nothing", k, g))
		}
	}
	sort.Strings(bad)
	if len(bad) > 0 {
		t.Errorf("%s: SQL and otlpmetrics.Deltas disagree on %d of %d points:\n  %s", what, len(bad), len(want), strings.Join(bad, "\n  "))
	}
	if len(want) == 0 {
		t.Errorf("%s: fixture produced no points in the window", what)
	}
}

// The query-time deltas agree with the detector's in-memory deltas point for
// point, through every reset rule, on a real ClickHouse.
func TestDeltas_AgreeWithTheDetectorsDeltas(t *testing.T) {
	s := chtest.New(t, "003_metrics.sql")
	reqs := fixture()
	var vals [][]byte
	var srcs []storage.Source
	for i, r := range reqs {
		v, err := proto.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		vals = append(vals, v)
		srcs = append(srcs, storage.Source{Partition: 0, Offset: int64(i), Received: t0})
	}
	body, _, err := storage.Metrics.Encode(vals, srcs)
	if err != nil {
		t.Fatal(err)
	}
	ch := &storage.CH{URL: s.URL, User: s.User, Password: s.Password, Table: s.DB + ".metrics", Columns: storage.MetricColumns}
	if err := ch.Insert(context.Background(), "fixture", body); err != nil {
		t.Fatal(err)
	}

	// A window that starts mid-fixture: its first points need the lookback.
	from, until := t0.Add(42*time.Second), t0.Add(180*time.Second)
	ws, wh, wg := expected(reqs, from, until)
	table := s.DB + ".metrics"
	diff(t, "sums", query(t, s, SumDeltas(table), "calls", from, until, 4), ws)
	diff(t, "histograms", query(t, s, HistDeltas(table), "duration", from, until, 5), wh)
	diff(t, "gauges", query(t, s, Gauges(table), "memory", from, until, 6), wg)

	// Nothing after until is read, nothing at or before from is returned.
	got := query(t, s, SumDeltas(table), "calls", from, until, 4)
	for k := range got {
		n, _ := strconv.ParseUint(k[strings.Index(k, "@")+1:], 10, 64)
		if n <= uint64(from.UnixNano()) || n > uint64(until.UnixNano()) {
			t.Errorf("point %s outside (from, until]", k)
		}
	}
	// services filter.
	out, err := s.QueryParams("SELECT DISTINCT service FROM ("+SumDeltas(table)+") FORMAT TabSeparated", Params("calls", from, until, []string{"quote"}))
	if err != nil || strings.TrimSpace(out) != "quote" {
		t.Errorf("services filter: %q, %v", out, err)
	}
}

func TestParams_QuotesServices(t *testing.T) {
	p := Params("m", t0, t0.Add(time.Minute), []string{"a", `o'b\x`})
	if p["services"] != `['a','o\'b\\x']` {
		t.Fatalf("services = %s", p["services"])
	}
	if p["from"] != "2026-11-03 17:00:00.000000000" || p["lookback"] != "180" {
		t.Fatalf("params = %v", p)
	}
}
