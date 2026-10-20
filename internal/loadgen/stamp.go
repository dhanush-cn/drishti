package loadgen

import (
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// SeqAttr is the per-send identity (docs/loadgen.md §12). Together with
// RunIDAttr it identifies one sent request; with one resource per request
// (the default) it identifies one Kafka message. The storage check counts
// records per (run_id, seq): a missing seq is loss, a seq with more records
// than one request carries is a duplicate. That catches what a per-run count
// cannot (one loss plus one duplicate cancel out in a total).
const SeqAttr = "drishti.loadgen.seq"

// Stamp returns a copy of req for one send: every record's timestamps set to
// ts (unix nanos, normally the send time) and every resource tagged with
// seq. Pool requests are shared by concurrent sends (one may still be
// marshaling when its slot comes round again), so they are never mutated.
//
// The copy is shallow where it can be: bodies, attributes and scopes are
// shared with the pool; only the structs that hold a changed field are new
// (request, resources, resource attribute slice, scope wrappers, records,
// one backing array per scope). BenchmarkStamp: 12 allocations, ~100 KB and
// ~85 µs per 512-record request on the 2-core cloud VM, about 1% of the
// marshal+gzip cost of the same send.
func Stamp(req *collogs.ExportLogsServiceRequest, ts, seq uint64) *collogs.ExportLogsServiceRequest {
	seqKV := &commonv1.KeyValue{Key: SeqAttr, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(seq)}}}
	out := &collogs.ExportLogsServiceRequest{ResourceLogs: make([]*logsv1.ResourceLogs, len(req.GetResourceLogs()))}
	for i, rl := range req.GetResourceLogs() {
		attrs := make([]*commonv1.KeyValue, 0, len(rl.GetResource().GetAttributes())+1)
		attrs = append(attrs, rl.GetResource().GetAttributes()...)
		attrs = append(attrs, seqKV)
		nrl := &logsv1.ResourceLogs{
			Resource: &resourcev1.Resource{
				Attributes:             attrs,
				DroppedAttributesCount: rl.GetResource().GetDroppedAttributesCount(),
			},
			ScopeLogs: make([]*logsv1.ScopeLogs, len(rl.GetScopeLogs())),
			SchemaUrl: rl.GetSchemaUrl(),
		}
		for j, sl := range rl.GetScopeLogs() {
			recs := make([]*logsv1.LogRecord, len(sl.GetLogRecords()))
			// One backing array for the scope's records: one allocation
			// instead of one per record.
			backing := make([]logsv1.LogRecord, len(recs))
			for k, r := range sl.GetLogRecords() {
				nr := &backing[k]
				// Field by field, not *nr = *r: proto messages hold internal
				// state that must not be copied (go vet copylocks). Every
				// field must be listed; TestStamp_ChangesOnlyTimestampsAndSeq
				// fails if one is missed.
				nr.TimeUnixNano = ts
				nr.ObservedTimeUnixNano = ts
				nr.SeverityNumber = r.GetSeverityNumber()
				nr.SeverityText = r.GetSeverityText()
				nr.Body = r.GetBody()
				nr.Attributes = r.GetAttributes()
				nr.DroppedAttributesCount = r.GetDroppedAttributesCount()
				nr.Flags = r.GetFlags()
				nr.TraceId = r.GetTraceId()
				nr.SpanId = r.GetSpanId()
				nr.EventName = r.GetEventName()
				recs[k] = nr
			}
			nrl.ScopeLogs[j] = &logsv1.ScopeLogs{Scope: sl.GetScope(), LogRecords: recs, SchemaUrl: sl.GetSchemaUrl()}
		}
		out.ResourceLogs[i] = nrl
	}
	return out
}
