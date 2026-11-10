package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/chtest"
	"google.golang.org/protobuf/proto"
)

// Against a real ClickHouse (DRISHTI_TEST_CLICKHOUSE_URL): the metrics codec's
// RowBinary is read back by the server as the values we meant, series_id
// separates instances, and equal attribute sets in any order are one series.
func TestMetricsCodec_RoundTripThroughClickHouse(t *testing.T) {
	s := chtest.New(t, "003_metrics.sql")
	ch := &CH{URL: s.URL, User: s.User, Password: s.Password, Table: s.DB + ".metrics", Columns: MetricColumns}

	a := demoMetrics()
	b := demoMetrics() // same series, attributes in the other order
	attrs := b.ResourceMetrics[0].ScopeMetrics[0].Metrics[2].GetSum().DataPoints[0].Attributes
	attrs[0], attrs[1] = attrs[1], attrs[0]
	b.ResourceMetrics[0].ScopeMetrics[0].Metrics[2].GetSum().DataPoints[0].TimeUnixNano = 3_000
	c := demoMetrics() // another replica of the same container name
	c.ResourceMetrics[0].Resource.Attributes = kvs("container.name", "email", "container.id", "beef")

	var w rowBuf
	d := Metrics.NewDecoder()
	for i, req := range []any{a, b, c} {
		v, _ := proto.Marshal(req.(proto.Message))
		if _, err := d.Decode(v); err != nil {
			t.Fatal(err)
		}
		d.Append(&w, Source{Partition: 0, Offset: int64(i), Received: time.Date(2026, 11, 3, 17, 0, 0, 0, time.UTC)})
	}
	if err := ch.Insert(context.Background(), "t1", w.b); err != nil {
		t.Fatal(err)
	}

	got := strings.TrimSpace(s.MustQuery("SELECT metric, kind, monotonic, temporality, value, count, sum, bounds, buckets, service, instance FROM " + s.DB + ".metrics WHERE kafka_offset = 0 ORDER BY metric FORMAT TabSeparated"))
	want := strings.Join([]string{
		"container.cpu.utilization\tgauge\tfalse\tunspecified\t0.25\t0\t0\t[]\t[]\tcontainer:email\tc0ffee",
		"container.memory.usage.total\tsum\tfalse\tcumulative\t123456\t0\t0\t[]\t[]\tcontainer:email\tc0ffee",
		"traces.span.metrics.calls\tsum\ttrue\tcumulative\t42\t0\t0\t[]\t[]\tcontainer:email\tc0ffee",
		"traces.span.metrics.duration\thistogram\ttrue\tcumulative\t0\t7\t31.5\t[2,10]\t[3,3,1]\tcontainer:email\tc0ffee",
	}, "\n")
	if got != want {
		t.Fatalf("read back:\n%s\nwant:\n%s", got, want)
	}
	// calls: a and b (attributes reordered) are one series; c (other replica) is another.
	n := strings.TrimSpace(s.MustQuery("SELECT uniqExact(series_id), count() FROM " + s.DB + ".metrics WHERE metric = 'traces.span.metrics.calls' FORMAT TabSeparated"))
	if n != "2\t3" {
		t.Fatalf("calls: series/points = %q, want 2 series over 3 points", n)
	}
	// Resume reads the projection.
	p, err := ch.Progress(context.Background())
	if err != nil || p[0] != 2 {
		t.Fatalf("progress = %v, %v; want partition 0 at offset 2", p, err)
	}
}
