package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/kdump"
	"github.com/twmb/franz-go/pkg/kgo"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

func sv(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s}}
}

func res(k, v string) *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{Key: k, Value: sv(v)}}}
}

// The summary is the driver's proof that the collector change took effect:
// span metrics points 5 s apart per series must read as 5 s, container
// memory 10 s apart as 10 s, and resources without service.name must be
// attributed by container.name.
func TestSummarize_IntervalsAndCounts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.kd.gz")
	f, _ := os.Create(path)
	w, _ := kdump.NewWriter(f)
	t0 := uint64(time.Unix(1_793_000_000, 0).UnixNano())
	off := int64(0)
	put := func(topic string, m proto.Message) {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		_ = w.Write(&kgo.Record{Topic: topic, Offset: off, Timestamp: time.Unix(1_793_000_000+off, 0), Value: b})
		off++
	}
	put("otlp.logs", &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: res("service.name", "ad"),
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{
			{SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_WARN, Body: sv("GetAds Failed")},
			{SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_INFO, Body: sv("ok")},
			{SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR, Body: sv("boom")},
		}}},
	}}})
	for i := uint64(0); i < 13; i++ {
		calls := &metricsv1.Metric{Name: "traces.span.metrics.calls", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{DataPoints: []*metricsv1.NumberDataPoint{
			{TimeUnixNano: t0 + i*5e9, Attributes: []*commonv1.KeyValue{{Key: "span.name", Value: sv("GetAds")}}},
			{TimeUnixNano: t0 + i*5e9, Attributes: []*commonv1.KeyValue{{Key: "span.name", Value: sv("Other")}}},
		}}}}
		mem := &metricsv1.Metric{Name: "container.memory.usage.total", Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{
			{TimeUnixNano: t0 + i*10e9},
		}}}}
		put("otlp.metrics", &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{
			{Resource: res("service.name", "ad"), ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{calls}}}},
			{Resource: res("container.name", "email"), ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{mem}}}},
		}})
	}
	_ = w.Write(&kgo.Record{Topic: "otlp.metrics", Offset: off, Value: []byte{0xff}}) // undecodable
	_ = w.Close()
	_ = f.Close()

	s, err := summarize(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.SpanMetricsS != 5 || s.ContainerMemS != 10 {
		t.Fatalf("intervals span=%v mem=%v, want 5 and 10", s.SpanMetricsS, s.ContainerMemS)
	}
	if s.DecodeErrors != 1 {
		t.Fatalf("decode errors = %d, want 1", s.DecodeErrors)
	}
	if len(s.Logs) != 1 || s.Logs[0].Rows != 3 || s.Logs[0].BySev["warn"] != 1 || s.Logs[0].BySev["error_fatal"] != 1 || s.Logs[0].BySev["info"] != 1 {
		t.Fatalf("logs = %+v", s.Logs)
	}
	var calls, mem metricSum
	for _, m := range s.Metrics {
		switch m.Name {
		case "traces.span.metrics.calls":
			calls = m
		case "container.memory.usage.total":
			mem = m
		}
	}
	if calls.Points != 26 || calls.Series != 2 || calls.Services != 1 || calls.Type != "sum" {
		t.Fatalf("calls = %+v", calls)
	}
	if mem.Series != 1 || mem.Services != 1 {
		t.Fatalf("container memory = %+v", mem)
	}
	if s.Topics[0].Topic != "otlp.logs" || s.Topics[1].Records != 14 {
		t.Fatalf("topics = %+v", s.Topics)
	}
}
