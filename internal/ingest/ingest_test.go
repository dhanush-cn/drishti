package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// ---- helpers ----

func str(s string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: s}}
}

func resource(service string) *resourcev1.Resource {
	if service == "" {
		return &resourcev1.Resource{}
	}
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{Key: "service.name", Value: str(service)}}}
}

// logsResource builds n records whose bodies are "<i>|" padded to bodySize.
func logsResource(service string, n, bodySize int) *logsv1.ResourceLogs {
	var recs []*logsv1.LogRecord
	for i := 0; i < n; i++ {
		body := fmt.Sprintf("%d|", i)
		if pad := bodySize - len(body); pad > 0 {
			body += strings.Repeat("x", pad)
		}
		recs = append(recs, &logsv1.LogRecord{Body: str(body)})
	}
	return &logsv1.ResourceLogs{Resource: resource(service), ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: recs}}}
}

func decodeLogs(t *testing.T, b []byte) *logsv1.ResourceLogs {
	t.Helper()
	var req collogs.ExportLogsServiceRequest
	if err := proto.Unmarshal(b, &req); err != nil {
		t.Fatalf("value is not a valid ExportLogsServiceRequest: %v", err)
	}
	if len(req.ResourceLogs) != 1 {
		t.Fatalf("want exactly 1 ResourceLogs per message, got %d", len(req.ResourceLogs))
	}
	return req.ResourceLogs[0]
}

type fakeSink struct {
	err   error
	block bool // block until ctx is done
	got   []Message
}

func (f *fakeSink) Publish(ctx context.Context, _ Signal, msgs []Message) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if f.err != nil {
		return f.err
	}
	f.got = append(f.got, msgs...)
	return nil
}

// ---- splitting ----

func TestSplitLogs_OneMessagePerResourceKeyedByService(t *testing.T) {
	req := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{
		logsResource("cart", 5, 10),
		logsResource("checkout", 7, 10),
		logsResource("", 2, 10),
	}}
	msgs, rejected, err := splitLogs(req, 1<<20)
	if err != nil || rejected != 0 {
		t.Fatalf("err=%v rejected=%d", err, rejected)
	}
	wantKeys := []string{"cart", "checkout", unknownService}
	wantCounts := []int{5, 7, 2}
	if len(msgs) != 3 {
		t.Fatalf("want 3 messages (one per resource), got %d", len(msgs))
	}
	for i, m := range msgs {
		if string(m.Key) != wantKeys[i] {
			t.Errorf("msg %d key=%q want %q", i, m.Key, wantKeys[i])
		}
		if n := len(decodeLogs(t, m.Value).ScopeLogs[0].LogRecords); n != wantCounts[i] {
			t.Errorf("msg %d has %d records, want %d", i, n, wantCounts[i])
		}
	}
}

func TestSplitLogs_EmptyResourceProducesNoMessage(t *testing.T) {
	req := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{
		{Resource: resource("idle")},
	}}
	msgs, _, err := splitLogs(req, 1<<20)
	if err != nil || len(msgs) != 0 {
		t.Fatalf("want no messages, got %d (err=%v)", len(msgs), err)
	}
}

func TestSplitLogs_OversizedResourceIsSplitUnderLimitInOrder(t *testing.T) {
	const max = 20 << 10
	rl := logsResource("cart", 200, 1024) // ~200 KiB, must split
	msgs, rejected, err := splitLogs(&collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{rl}}, max)
	if err != nil || rejected != 0 {
		t.Fatalf("err=%v rejected=%d", err, rejected)
	}
	if len(msgs) < 2 {
		t.Fatalf("expected a split, got %d message(s)", len(msgs))
	}
	next := 0
	for _, m := range msgs {
		if len(m.Value) > max {
			t.Fatalf("message of %d bytes exceeds limit %d", len(m.Value), max)
		}
		if !bytes.Equal(m.Key, []byte("cart")) {
			t.Fatalf("split part lost its key: %q", m.Key)
		}
		got := decodeLogs(t, m.Value)
		if got.Resource == nil || len(got.Resource.Attributes) == 0 {
			t.Fatal("split part lost its resource attributes")
		}
		for _, r := range got.ScopeLogs[0].LogRecords {
			if want := fmt.Sprintf("%d|", next); !strings.HasPrefix(r.Body.GetStringValue(), want) {
				t.Fatalf("order broken: want record starting %q", want)
			}
			next++
		}
	}
	if next != 200 {
		t.Fatalf("records lost in split: got %d of 200", next)
	}
}

func TestSplitLogs_SingleTooLargeRecordIsRejectedOthersKept(t *testing.T) {
	const max = 10 << 10
	rl := logsResource("cart", 4, 100)
	rl.ScopeLogs[0].LogRecords[2].Body = str(strings.Repeat("y", 50<<10)) // cannot fit alone
	msgs, rejected, err := splitLogs(&collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{rl}}, max)
	if err != nil {
		t.Fatal(err)
	}
	if rejected != 1 {
		t.Fatalf("rejected=%d, want 1", rejected)
	}
	kept := 0
	for _, m := range msgs {
		kept += len(decodeLogs(t, m.Value).ScopeLogs[0].LogRecords)
	}
	if kept != 3 {
		t.Fatalf("kept %d records, want 3", kept)
	}
}

func TestSplitMetrics_RejectionCountsDataPoints(t *testing.T) {
	big := &metricsv1.Metric{Name: strings.Repeat("m", 50<<10), Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{
		DataPoints: []*metricsv1.NumberDataPoint{{}, {}, {}},
	}}}
	small := &metricsv1.Metric{Name: "ok", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{
		DataPoints: []*metricsv1.NumberDataPoint{{}},
	}}}
	req := &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource:     resource("ad"),
		ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{big, small}}},
	}}}
	msgs, rejected, err := splitMetrics(req, 10<<10)
	if err != nil {
		t.Fatal(err)
	}
	if rejected != 3 {
		t.Fatalf("rejected=%d data points, want 3", rejected)
	}
	if len(msgs) != 1 || string(msgs[0].Key) != "ad" {
		t.Fatalf("want the small metric kept under key ad, got %d msgs", len(msgs))
	}
}

// ---- export ----

func TestExport_SinkErrorIsMarkedRetryable(t *testing.T) {
	ing := New(&fakeSink{err: errors.New("broker down")}, Config{MaxMessageBytes: 1 << 20, PublishTimeout: time.Second})
	_, err := ing.ExportLogs(context.Background(), &collogs.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{logsResource("cart", 1, 10)},
	})
	if !errors.Is(err, ErrPublish) {
		t.Fatalf("want ErrPublish, got %v", err)
	}
}

func TestExport_PublishTimeoutBoundsTheWait(t *testing.T) {
	ing := New(&fakeSink{block: true}, Config{MaxMessageBytes: 1 << 20, PublishTimeout: 200 * time.Millisecond})
	start := time.Now()
	_, err := ing.ExportLogs(context.Background(), &collogs.ExportLogsServiceRequest{
		ResourceLogs: []*logsv1.ResourceLogs{logsResource("cart", 1, 10)},
	})
	if !errors.Is(err, ErrPublish) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want ErrPublish wrapping DeadlineExceeded, got %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("export blocked %v; the publish timeout is not being applied", took)
	}
}

func TestExport_PartialSuccessReported(t *testing.T) {
	sink := &fakeSink{}
	ing := New(sink, Config{MaxMessageBytes: 10 << 10, PublishTimeout: time.Second})
	rl := logsResource("cart", 3, 10)
	rl.ScopeLogs[0].LogRecords[0].Body = str(strings.Repeat("z", 50<<10))
	res, err := ing.ExportLogs(context.Background(), &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{rl}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rejected != 1 || res.RejectReason == "" {
		t.Fatalf("want 1 rejected with a reason, got %+v", res)
	}
	if len(sink.got) == 0 {
		t.Fatal("the records that fit were not published")
	}
}

func TestServiceKey_FallsBackToContainerThenHost(t *testing.T) {
	kv := func(pairs ...string) *resourcev1.Resource {
		r := &resourcev1.Resource{}
		for i := 0; i < len(pairs); i += 2 {
			r.Attributes = append(r.Attributes, &commonv1.KeyValue{Key: pairs[i], Value: str(pairs[i+1])})
		}
		return r
	}
	for _, tc := range []struct {
		want string
		r    *resourcev1.Resource
	}{
		{"cart", kv("host.name", "h1", "container.name", "c1", "service.name", "cart")},
		{"ad-container", kv("host.name", "h1", "container.name", "ad-container")},
		{"h1", kv("host.name", "h1", "service.name", "")},
		{"c2", kv("service.name", "", "container.name", "c2")},
		{unknownService, kv("os.type", "linux")},
		{unknownService, nil},
	} {
		want, r := tc.want, tc.r
		if got := string(serviceKey(r)); got != want {
			t.Errorf("serviceKey(%v) = %q, want %q", r.GetAttributes(), got, want)
		}
	}
}
