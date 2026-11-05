package detect

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// sim drives a Detector the way the live consumer does: records go through
// Kafka-record decoding, one step is one 5 s bucket, and after each step every
// shard is advanced to the wall clock (the live detector's idle-shard tick).
type sim struct {
	t      *testing.T
	d      *Detector
	now    time.Time
	off    map[Input]int64
	cum    map[string]float64  // cumulative span calls per point series
	hcum   map[string][]uint64 // cumulative histogram buckets
	start  uint64              // start time of every cumulative series
	evs    []Event
	noTick bool // don't advance shards from the wall clock (a sleeping host)
}

var t0 = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func newSim(t *testing.T, cfg Config) *sim {
	return &sim{t: t, d: NewDetector(cfg, "test"), now: t0, off: map[Input]int64{},
		cum: map[string]float64{}, hcum: map[string][]uint64{}, start: uint64(t0.UnixNano())}
}

func kv(k, v string) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: k, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}}
}

// feedAt sends one record of topic/partition with Kafka timestamp ts.
func (s *sim) feedAt(topic string, p int32, ts time.Time, m proto.Message) {
	s.t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		s.t.Fatal(err)
	}
	in := Input{topic, p}
	r := &kgo.Record{Topic: topic, Partition: p, Offset: s.off[in], Timestamp: ts, Value: b}
	s.off[in]++
	s.evs = append(s.evs, s.d.Record(r)...)
}

func (s *sim) ts() time.Time { return s.now.Add(time.Millisecond) }

// log sends n lines of body from svc at severity sev, in one record.
func (s *sim) log(svc string, sev logsv1.SeverityNumber, body string, n int) {
	s.feedAt(TopicLogs, 0, s.ts(), logReq(svc, sev, body, n))
}

func logReq(svc string, sev logsv1.SeverityNumber, body string, n int) *collogs.ExportLogsServiceRequest {
	var lrs []*logsv1.LogRecord
	for range n {
		lrs = append(lrs, &logsv1.LogRecord{SeverityNumber: sev, Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: body}}})
	}
	return &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource:  &resourcev1.Resource{Attributes: []*commonv1.KeyValue{kv("service.name", svc)}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: lrs}},
	}}}
}

const (
	info  = logsv1.SeverityNumber_SEVERITY_NUMBER_INFO
	warn  = logsv1.SeverityNumber_SEVERITY_NUMBER_WARN
	errLv = logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR
)

// calls adds n calls to a span series (span_metrics' cumulative calls sum)
// and sends the new cumulative value.
func (s *sim) calls(svc, name, kind, status string, n float64) {
	s.feedAt(TopicMetrics, 0, s.ts(), s.callsReq(svc, name, kind, status, n))
}

func (s *sim) callsReq(svc, name, kind, status string, n float64) *colmetrics.ExportMetricsServiceRequest {
	k := svc + "|" + name + "|" + kind + "|" + status
	s.cum[k] += n
	dp := &metricsv1.NumberDataPoint{
		Attributes:        []*commonv1.KeyValue{kv("span.name", name), kv("span.kind", "SPAN_KIND_"+kind), kv("status.code", status)},
		StartTimeUnixNano: s.start, TimeUnixNano: uint64(s.now.UnixNano()),
		Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: s.cum[k]},
	}
	m := &metricsv1.Metric{Name: metricCalls, Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{
		AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, IsMonotonic: true,
		DataPoints: []*metricsv1.NumberDataPoint{dp}}}}
	return metricsReq(svc, "", m)
}

var testBounds = []float64{10, 50, 100, 250, 500, 1000}

// hist adds per-bucket call counts (len(testBounds)+1 buckets) to a span's
// duration histogram and sends the cumulative histogram.
func (s *sim) hist(svc, name, kind string, add []uint64) {
	k := svc + "|" + name + "|" + kind
	c := s.hcum[k]
	if c == nil {
		c = make([]uint64, len(testBounds)+1)
	}
	var cnt uint64
	for i, v := range add {
		c[i] += v
	}
	for _, v := range c {
		cnt += v
	}
	s.hcum[k] = c
	dp := &metricsv1.HistogramDataPoint{
		Attributes:        []*commonv1.KeyValue{kv("span.name", name), kv("span.kind", "SPAN_KIND_"+kind), kv("status.code", "STATUS_CODE_UNSET")},
		StartTimeUnixNano: s.start, TimeUnixNano: uint64(s.now.UnixNano()),
		Count: cnt, BucketCounts: append([]uint64(nil), c...), ExplicitBounds: testBounds,
	}
	m := &metricsv1.Metric{Name: metricDuration, Data: &metricsv1.Metric_Histogram{Histogram: &metricsv1.Histogram{
		AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		DataPoints:             []*metricsv1.HistogramDataPoint{dp}}}}
	s.feedAt(TopicMetrics, 0, s.ts(), metricsReq(svc, "", m))
}

// mem sends a container's memory usage and limit (docker_stats gauges).
func (s *sim) mem(container string, usage, limit float64) {
	g := func(name string, v float64) *metricsv1.Metric {
		return &metricsv1.Metric{Name: name, Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
			TimeUnixNano: uint64(s.now.UnixNano()), Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: v}}}}}}
	}
	s.feedAt(TopicMetrics, 0, s.ts(), metricsReq("", container, g(metricMemUsage, usage), g(metricMemLimit, limit)))
}

func metricsReq(svc, container string, ms ...*metricsv1.Metric) *colmetrics.ExportMetricsServiceRequest {
	var attrs []*commonv1.KeyValue
	if svc != "" {
		attrs = append(attrs, kv("service.name", svc))
	}
	if container != "" {
		attrs = append(attrs, kv("container.name", container))
	}
	return &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource:     &resourcev1.Resource{Attributes: attrs},
		ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: ms}},
	}}}
}

// run calls each(i) for n buckets, moving the clock one bucket after each
// and advancing every shard to it.
func (s *sim) run(d time.Duration, each func(i int)) {
	n := int(d / DefaultConfig().Bucket)
	for i := range n {
		if each != nil {
			each(i)
		}
		s.now = s.now.Add(DefaultConfig().Bucket)
		if !s.noTick {
			s.tick()
		}
	}
}

func (s *sim) tick() {
	for _, in := range sortedInputs(s.d) {
		s.evs = append(s.evs, s.d.Shards()[in].Advance(s.now)...)
	}
}

// take returns the events so far and forgets them.
func (s *sim) take() []Event {
	e := s.evs
	s.evs = nil
	return e
}

func only(evs []Event, kind string) []Event {
	var out []Event
	for _, e := range evs {
		if e.Event == kind {
			out = append(out, e)
		}
	}
	return out
}

func rules(evs []Event) []string {
	var out []string
	for _, e := range evs {
		out = append(out, e.Event+" "+e.Rule+" "+e.Service)
	}
	return out
}

// none fails if any event was produced.
func (s *sim) none(what string) {
	s.t.Helper()
	if evs := s.take(); len(evs) > 0 {
		s.t.Fatalf("%s: want no events, got %d: %v", what, len(evs), rules(evs))
	}
}

// opened requires exactly one opened event, of rule on svc, and returns it.
func (s *sim) opened(what, rule, svc string) Event {
	s.t.Helper()
	evs := only(s.take(), "opened")
	if len(evs) != 1 || evs[0].Rule != rule || evs[0].Service != svc {
		s.t.Fatalf("%s: want one opened %s on %s, got %v", what, rule, svc, rules(evs))
	}
	return evs[0]
}
