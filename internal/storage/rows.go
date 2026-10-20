package storage

import (
	"encoding/hex"
	"strconv"
	"strings"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// LogColumns is the INSERT column list, in RowBinary order. inserted_at is
// left out on purpose: its DEFAULT now64(3) is evaluated by the server once
// per inserted block, which is what makes event → queryable measurable.
var LogColumns = []string{
	"ts", "observed_ts", "received_at", "service", "severity_number", "severity_text",
	"body", "trace_id", "span_id", "flags", "event_name", "scope_name",
	"resource_attributes", "log_attributes", "kafka_partition", "kafka_offset",
}

const unknownService = "unknown_service"

// Source is where a Kafka message came from. Every row carries it: the
// offset is how the consumer resumes exactly after a crash (docs/storage.md
// "Exactly-once into ClickHouse"), not just debugging metadata.
type Source struct {
	Partition int32
	Offset    int64
	Received  time.Time // Kafka record timestamp = receiver produce time
}

// AppendLogs flattens one Kafka message (an ExportLogsServiceRequest with,
// by the receiver's contract, one resource) into RowBinary rows appended to
// w, and returns the number of rows.
func AppendLogs(w *rowBuf, req *collogs.ExportLogsServiceRequest, src Source) int {
	recvNs := uint64(src.Received.UnixNano())
	recvMs := src.Received.UnixMilli()
	rows := 0
	for _, rl := range req.GetResourceLogs() {
		rattrs := rl.GetResource().GetAttributes()
		service := unknownService
		for _, kv := range rattrs {
			if kv.GetKey() == "service.name" {
				if s := kv.GetValue().GetStringValue(); s != "" {
					service = s
				}
				break
			}
		}
		for _, sl := range rl.GetScopeLogs() {
			scope := sl.GetScope().GetName()
			for _, r := range sl.GetLogRecords() {
				// ts: event time; OTLP allows it unset, then observed
				// time, then receive time, so a row never lands in 1970.
				ts := r.GetTimeUnixNano()
				obs := r.GetObservedTimeUnixNano()
				if obs == 0 {
					obs = recvNs
				}
				if ts == 0 {
					ts = obs
				}
				w.dt64ns(ts)
				w.dt64ns(obs)
				w.i64(recvMs)
				w.str(service)
				w.u8(uint8(r.GetSeverityNumber()))
				w.str(r.GetSeverityText())
				w.str(anyString(r.GetBody()))
				w.str(hexID(r.GetTraceId()))
				w.str(hexID(r.GetSpanId()))
				w.u32(r.GetFlags())
				w.str(r.GetEventName())
				w.str(scope)
				attrMap(w, rattrs)
				attrMap(w, r.GetAttributes())
				w.u16(uint16(src.Partition))
				w.u64(uint64(src.Offset))
				rows++
			}
		}
	}
	return rows
}

func hexID(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

// attrMap writes OTLP attributes as Map(LowCardinality(String), String).
// Values are flattened to strings: typed attribute columns are a later
// decision (they need query patterns first), and a Map keeps every key.
func attrMap(w *rowBuf, kvs []*commonv1.KeyValue) {
	w.uvarint(uint64(len(kvs)))
	for _, kv := range kvs {
		w.str(kv.GetKey())
		w.str(anyString(kv.GetValue()))
	}
}

// anyString renders an OTLP AnyValue as text: strings as-is, scalars in
// their canonical form, bytes as hex, arrays and maps as compact JSON.
// Hand-written instead of protojson because protojson deliberately varies
// its whitespace between runs, which would make identical values differ.
func anyString(v *commonv1.AnyValue) string {
	switch x := v.GetValue().(type) {
	case nil:
		return ""
	case *commonv1.AnyValue_StringValue:
		return x.StringValue
	default:
		var sb strings.Builder
		writeJSONish(&sb, v, false)
		return sb.String()
	}
}

func writeJSONish(sb *strings.Builder, v *commonv1.AnyValue, quoteStrings bool) {
	switch x := v.GetValue().(type) {
	case nil:
		sb.WriteString("null")
	case *commonv1.AnyValue_StringValue:
		if quoteStrings {
			sb.WriteString(strconv.Quote(x.StringValue))
		} else {
			sb.WriteString(x.StringValue)
		}
	case *commonv1.AnyValue_BoolValue:
		sb.WriteString(strconv.FormatBool(x.BoolValue))
	case *commonv1.AnyValue_IntValue:
		sb.WriteString(strconv.FormatInt(x.IntValue, 10))
	case *commonv1.AnyValue_DoubleValue:
		sb.WriteString(strconv.FormatFloat(x.DoubleValue, 'g', -1, 64))
	case *commonv1.AnyValue_BytesValue:
		if quoteStrings {
			sb.WriteString(`"` + hex.EncodeToString(x.BytesValue) + `"`)
		} else {
			sb.WriteString(hex.EncodeToString(x.BytesValue))
		}
	case *commonv1.AnyValue_ArrayValue:
		sb.WriteByte('[')
		for i, e := range x.ArrayValue.GetValues() {
			if i > 0 {
				sb.WriteByte(',')
			}
			writeJSONish(sb, e, true)
		}
		sb.WriteByte(']')
	case *commonv1.AnyValue_KvlistValue:
		sb.WriteByte('{')
		for i, kv := range x.KvlistValue.GetValues() {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(strconv.Quote(kv.GetKey()))
			sb.WriteByte(':')
			writeJSONish(sb, kv.GetValue(), true)
		}
		sb.WriteByte('}')
	}
}
