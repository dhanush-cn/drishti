package storage

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func kvs(pairs ...string) []*commonv1.KeyValue {
	var out []*commonv1.KeyValue
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &commonv1.KeyValue{Key: pairs[i], Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: pairs[i+1]}}})
	}
	return out
}

// demoMetrics is one export shaped like the demo's: a docker_stats resource
// (no service.name) and the kinds the codec stores, plus one it skips.
func demoMetrics() *colmetrics.ExportMetricsServiceRequest {
	cum := metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
	return &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource: &resourcev1.Resource{Attributes: kvs("container.name", "email", "container.id", "c0ffee")},
		ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{
			{Name: "container.cpu.utilization", Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{
				{TimeUnixNano: 2_000, Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: 0.25}}}}}},
			{Name: "container.memory.usage.total", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: false, AggregationTemporality: cum, DataPoints: []*metricsv1.NumberDataPoint{
				{TimeUnixNano: 2_000, StartTimeUnixNano: 1_000, Value: &metricsv1.NumberDataPoint_AsInt{AsInt: 123456}}}}}},
			{Name: "traces.span.metrics.calls", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: true, AggregationTemporality: cum, DataPoints: []*metricsv1.NumberDataPoint{
				// attributes deliberately out of key order
				{TimeUnixNano: 2_000, StartTimeUnixNano: 1_000, Attributes: kvs("span.name", "Charge", "span.kind", "SPAN_KIND_SERVER"), Value: &metricsv1.NumberDataPoint_AsInt{AsInt: 42}}}}}},
			{Name: "traces.span.metrics.duration", Data: &metricsv1.Metric_Histogram{Histogram: &metricsv1.Histogram{AggregationTemporality: cum, DataPoints: []*metricsv1.HistogramDataPoint{
				{TimeUnixNano: 2_000, StartTimeUnixNano: 1_000, Count: 7, Sum: proto.Float64(31.5), ExplicitBounds: []float64{2, 10}, BucketCounts: []uint64{3, 3, 1}}}}}},
			{Name: "skipped.exp", Data: &metricsv1.Metric_ExponentialHistogram{ExponentialHistogram: &metricsv1.ExponentialHistogram{DataPoints: []*metricsv1.ExponentialHistogramDataPoint{{Count: 1}}}}},
		}}},
	}}}
}

// metricRow is the test-side reading of one MetricColumns row.
type metricRow struct {
	ts, start                 int64
	recvMs                    int64
	service, instance, metric string
	kind, mono, temp          uint8
	value                     float64
	count                     uint64
	sum                       float64
	bounds                    []float64
	buckets                   []uint64
	attrs, rattrs             [][2]string
	partition                 uint16
	offset                    uint64
}

type rbReader struct {
	b []byte
	t *testing.T
}

func (r *rbReader) u64() uint64 { v := binary.LittleEndian.Uint64(r.b); r.b = r.b[8:]; return v }
func (r *rbReader) u16() uint16 { v := binary.LittleEndian.Uint16(r.b); r.b = r.b[2:]; return v }
func (r *rbReader) u8() uint8   { v := r.b[0]; r.b = r.b[1:]; return v }
func (r *rbReader) uv() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.t.Fatal("bad uvarint")
	}
	r.b = r.b[n:]
	return v
}
func (r *rbReader) str() string { n := r.uv(); s := string(r.b[:n]); r.b = r.b[n:]; return s }
func (r *rbReader) m() [][2]string {
	n := r.uv()
	out := make([][2]string, n)
	for i := range out {
		out[i] = [2]string{r.str(), r.str()}
	}
	return out
}

func readMetricRows(t *testing.T, b []byte) []metricRow {
	r := &rbReader{b: b, t: t}
	var rows []metricRow
	for len(r.b) > 0 {
		var x metricRow
		x.ts, x.start, x.recvMs = int64(r.u64()), int64(r.u64()), int64(r.u64())
		x.service, x.instance, x.metric = r.str(), r.str(), r.str()
		x.kind, x.mono, x.temp = r.u8(), r.u8(), r.u8()
		x.value = math.Float64frombits(r.u64())
		x.count = r.u64()
		x.sum = math.Float64frombits(r.u64())
		for n := r.uv(); n > 0; n-- {
			x.bounds = append(x.bounds, math.Float64frombits(r.u64()))
		}
		for n := r.uv(); n > 0; n-- {
			x.buckets = append(x.buckets, r.u64())
		}
		x.attrs, x.rattrs = r.m(), r.m()
		x.partition, x.offset = r.u16(), r.u64()
		rows = append(rows, x)
	}
	return rows
}

func TestMetricsCodec_StoresPointsAsReceived(t *testing.T) {
	val, err := proto.Marshal(demoMetrics())
	if err != nil {
		t.Fatal(err)
	}
	d := Metrics.NewDecoder()
	n, err := d.Decode(val)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("Decode counted %d rows, want 4 (the exponential histogram is skipped)", n)
	}
	var w rowBuf
	recv := time.Date(2026, 11, 3, 17, 0, 0, 0, time.UTC)
	if got := d.Append(&w, Source{Partition: 4, Offset: 99, Received: recv}); got != n {
		t.Fatalf("Append wrote %d rows, Decode said %d", got, n)
	}
	rows := readMetricRows(t, w.b)
	if len(rows) != 4 {
		t.Fatalf("read back %d rows, want 4", len(rows))
	}
	for _, r := range rows {
		if r.service != "container:email" || r.instance != "c0ffee" {
			t.Errorf("%s: service %q instance %q, want the receiver's key container:email and the container id", r.metric, r.service, r.instance)
		}
		if r.recvMs != recv.UnixMilli() || r.partition != 4 || r.offset != 99 {
			t.Errorf("%s: source columns %d/%d/%d", r.metric, r.recvMs, r.partition, r.offset)
		}
	}
	g, mem, calls, hist := rows[0], rows[1], rows[2], rows[3]
	if g.kind != 1 || g.value != 0.25 || g.ts != 2000 {
		t.Errorf("gauge row %+v", g)
	}
	if mem.kind != 2 || mem.mono != 0 || mem.value != 123456 || mem.temp != 2 {
		t.Errorf("non-monotonic sum row %+v (want kind sum, monotonic false, cumulative)", mem)
	}
	// Stored as received: the running total, not a delta.
	if calls.kind != 2 || calls.mono != 1 || calls.value != 42 || calls.start != 1000 || calls.temp != 2 {
		t.Errorf("cumulative sum row %+v (want the running total 42 with its start time)", calls)
	}
	if len(calls.attrs) != 2 || calls.attrs[0][0] != "span.kind" || calls.attrs[1][0] != "span.name" {
		t.Errorf("attributes %v not sorted by key (series_id hashes the map, and map hashing is order-sensitive)", calls.attrs)
	}
	if hist.kind != 3 || hist.count != 7 || hist.sum != 31.5 || len(hist.bounds) != 2 || len(hist.buckets) != 3 || hist.buckets[2] != 1 {
		t.Errorf("histogram row %+v", hist)
	}
}

// The codec contract: a record's rows depend on that record alone. Decoding
// other records in between (as a partition's decoder does) must not change
// what a replayed record produces.
func TestMetricsCodec_ReplayedRecordProducesIdenticalRows(t *testing.T) {
	val, _ := proto.Marshal(demoMetrics())
	other := demoMetrics()
	other.ResourceMetrics[0].Resource.Attributes = kvs("service.name", "cart", "service.instance.id", "i-2")
	otherVal, _ := proto.Marshal(other)
	src := Source{Partition: 1, Offset: 5, Received: time.Unix(1793700000, 0)}

	d := Metrics.NewDecoder()
	var first, again rowBuf
	d.Decode(val)
	d.Append(&first, src)
	d.Decode(otherVal)
	var scratch rowBuf
	d.Append(&scratch, Source{Partition: 1, Offset: 6})
	d.Decode(val)
	d.Append(&again, src)
	if string(first.b) != string(again.b) {
		t.Fatal("replaying a record after others produced different bytes: the decoder carries state between records")
	}
	if rows := readMetricRows(t, scratch.b); rows[0].service != "cart" || rows[0].instance != "i-2" {
		t.Fatalf("service/instance %q/%q, want cart/i-2", rows[0].service, rows[0].instance)
	}
}

func TestInstance_Fallbacks(t *testing.T) {
	for _, c := range []struct {
		attrs []*commonv1.KeyValue
		want  string
	}{
		{kvs("service.instance.id", "a", "container.id", "b", "host.name", "h"), "a"},
		{kvs("container.id", "b", "container.name", "n"), "b"},
		{kvs("container.name", "n", "host.name", "h"), "n"},
		{kvs("host.name", "h"), "h"},
		{nil, ""},
	} {
		if got := Instance(c.attrs); got != c.want {
			t.Errorf("Instance(%v) = %q, want %q", c.attrs, got, c.want)
		}
	}
}
