package detect

import (
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// BenchmarkShardLogs: one shard, records of 100 log lines (the receiver's
// typical batch), 8 services x 20 templates with varying numbers in them.
// Reports lines/s for one partition; partitions run in parallel.
func BenchmarkShardLogs(b *testing.B) {
	const perRecord = 100
	var recs []*kgo.Record
	start := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for i := range 2000 {
		svc := fmt.Sprintf("svc-%d", i%8)
		var lrs []*logsv1.LogRecord
		for j := range perRecord {
			body := fmt.Sprintf("request %d to /api/item/%d took %dms status=%d", j%20, i*perRecord+j, (i*7+j)%900, 200+(j%3)*100)
			lrs = append(lrs, &logsv1.LogRecord{SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_INFO,
				Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: body}}})
		}
		v, _ := proto.Marshal(&collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
			Resource:  &resourcev1.Resource{Attributes: []*commonv1.KeyValue{kv("service.name", svc)}},
			ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: lrs}},
		}}})
		recs = append(recs, &kgo.Record{Topic: TopicLogs, Value: v})
	}
	b.ReportAllocs()
	b.ResetTimer()
	s := NewShard(DefaultConfig(), TopicLogs, 0, "bench")
	ts := start
	for i := 0; i < b.N; i++ {
		r := recs[i%len(recs)]
		ts = ts.Add(5 * time.Millisecond) // 200 records/s = 20k lines/s of record time
		r.Timestamp = ts
		s.Record(r)
	}
	b.ReportMetric(float64(b.N*perRecord)/b.Elapsed().Seconds(), "lines/s")
}
