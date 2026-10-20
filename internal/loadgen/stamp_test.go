package loadgen

import (
	"testing"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

// fullRecord sets every LogRecord field, so a field Stamp forgets to copy
// shows up as a difference.
func fullRecord() *logsv1.LogRecord {
	return &logsv1.LogRecord{
		TimeUnixNano:           1,
		ObservedTimeUnixNano:   2,
		SeverityNumber:         logsv1.SeverityNumber_SEVERITY_NUMBER_WARN,
		SeverityText:           "WARN",
		Body:                   str("body"),
		Attributes:             []*commonv1.KeyValue{{Key: "k", Value: str("v")}},
		DroppedAttributesCount: 3,
		Flags:                  1,
		TraceId:                []byte("0123456789abcdef"),
		SpanId:                 []byte("01234567"),
		EventName:              "ev",
	}
}

// Guards the field-by-field copy: if Stamp drops a field (or LogRecord
// gains one that Stamp does not copy and the pool sets), proto.Equal fails.
func TestStamp_ChangesOnlyTimestampsAndSeq(t *testing.T) {
	pool, err := LogPool(LogShape{BatchSize: 8, Services: 2, Resources: 2, BodyMin: 10, BodyMax: 20, AttrsMin: 1, AttrsMax: 2}, 1, "run1", 1)
	if err != nil {
		t.Fatal(err)
	}
	req := pool[0]
	req.ResourceLogs[0].ScopeLogs[0].LogRecords[0] = fullRecord()
	orig := proto.Clone(req)

	got := Stamp(req, 42, 7)

	if !proto.Equal(req, orig) {
		t.Fatal("Stamp mutated the pool request")
	}
	// Apply to a clone exactly what Stamp is allowed to change; everything
	// else must come out equal.
	exp := proto.Clone(orig).(*collogs.ExportLogsServiceRequest)
	for _, rl := range exp.GetResourceLogs() {
		rl.Resource.Attributes = append(rl.Resource.Attributes, &commonv1.KeyValue{Key: SeqAttr, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: 7}}})
		for _, sl := range rl.ScopeLogs {
			for _, r := range sl.LogRecords {
				r.TimeUnixNano, r.ObservedTimeUnixNano = 42, 42
			}
		}
	}
	if !proto.Equal(got, exp) {
		t.Fatalf("Stamp changed more than timestamps and seq:\n got %v\nwant %v", got, exp)
	}
}

func TestStamp_SharesUnchangedPayload(t *testing.T) {
	pool, err := LogPool(LogShape{BatchSize: 4, Services: 1, Resources: 1, BodyMin: 10, BodyMax: 20, AttrsMin: 1, AttrsMax: 2}, 1, "run1", 1)
	if err != nil {
		t.Fatal(err)
	}
	got := Stamp(pool[0], 42, 7)
	o := pool[0].ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	n := got.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if o == n {
		t.Fatal("record not copied: the pool would be mutated")
	}
	if o.Body != n.Body || &o.Attributes[0] != &n.Attributes[0] {
		t.Fatal("body/attributes copied: Stamp should share them (cost)")
	}
}

// go test -bench Stamp -benchmem ./internal/loadgen/  (cost per 512-record request)
func BenchmarkStamp(b *testing.B) {
	pool, err := LogPool(LogShape{BatchSize: 512, Services: 20, Resources: 1, BodyMin: 80, BodyMax: 400, AttrsMin: 5, AttrsMax: 8}, 1, "run1", 1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		_ = Stamp(pool[0], uint64(i), uint64(i))
	}
}
