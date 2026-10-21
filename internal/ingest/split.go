package ingest

import (
	"fmt"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

// The message unit is one Resource (one service instance's telemetry from
// one export), not the whole request and not one record:
//
//   - Whole request: mixes services, so there is no meaningful key, ordering
//     per service is lost, and size is unbounded.
//   - One record per message: per-message overhead plus the resource
//     attributes copied into every message; ~10x more Kafka messages.
//   - Per resource: keyed by service.name (ordering per service, partition
//     locality for the detector), resource attributes stored once, and
//     consumers still decode a standard OTLP request.
//
// A resource larger than maxBytes is split by halving its items until each
// part fits. A single item (log record / metric) that cannot fit on its own
// is rejected and reported to the sender as a partial success.

const unknownService = "unknown_service"

// ref points at one item: ScopeX index and item index within that scope.
type ref struct{ scope, item int }

// chunk splits refs into groups whose encoded size (via size) fits max.
// Order is preserved, so per-service ordering survives a split.
func chunk(refs []ref, max int, size func([]ref) int) (groups [][]ref, rejected []ref) {
	if len(refs) == 0 {
		return nil, nil
	}
	if size(refs) <= max {
		return [][]ref{refs}, nil
	}
	if len(refs) == 1 {
		return nil, refs
	}
	mid := len(refs) / 2
	g1, r1 := chunk(refs[:mid], max, size)
	g2, r2 := chunk(refs[mid:], max, size)
	return append(g1, g2...), append(r1, r2...)
}

// keyAttributes are tried in order for the Kafka key (docs/receiver.md
// decision 8). service.name first: per-service ordering and partition
// locality are the point of keying. The collector's scraper receivers
// (docker_stats, host metrics, redis, postgresql, nginx, ...) emit
// resources without it; with a single fallback key all of them shared one
// partition (~90% of otlp.metrics on partition 1, 10 Oct). container.name,
// then host.name, spread them by what they describe, and keep each
// container's or host's series in order.
var keyAttributes = []string{"service.name", "container.name", "host.name"}

func serviceKey(r *resourcev1.Resource) []byte {
	attrs := r.GetAttributes()
	for _, k := range keyAttributes {
		for _, kv := range attrs {
			if kv.GetKey() == k {
				if s := kv.GetValue().GetStringValue(); s != "" {
					return []byte(s)
				}
			}
		}
	}
	return []byte(unknownService)
}

func marshal(m proto.Message) ([]byte, error) {
	b, err := proto.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal OTLP message: %w", err)
	}
	return b, nil
}

// ---- logs ----

func hasLogRecords(rl *logsv1.ResourceLogs) bool {
	for _, sl := range rl.GetScopeLogs() {
		if len(sl.GetLogRecords()) > 0 {
			return true
		}
	}
	return false
}

func wrapLogs(rl *logsv1.ResourceLogs) *collogs.ExportLogsServiceRequest {
	return &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{rl}}
}

// subsetLogs rebuilds a ResourceLogs holding only the referenced records.
// Records are shared, not copied; the original request is never mutated.
func subsetLogs(rl *logsv1.ResourceLogs, refs []ref) *logsv1.ResourceLogs {
	out := &logsv1.ResourceLogs{Resource: rl.Resource, SchemaUrl: rl.SchemaUrl}
	var cur *logsv1.ScopeLogs
	last := -1
	for _, r := range refs {
		src := rl.ScopeLogs[r.scope]
		if r.scope != last {
			cur = &logsv1.ScopeLogs{Scope: src.Scope, SchemaUrl: src.SchemaUrl}
			out.ScopeLogs = append(out.ScopeLogs, cur)
			last = r.scope
		}
		cur.LogRecords = append(cur.LogRecords, src.LogRecords[r.item])
	}
	return out
}

func splitLogs(req *collogs.ExportLogsServiceRequest, maxBytes int) ([]Message, int64, error) {
	var msgs []Message
	var rejected int64
	for _, rl := range req.GetResourceLogs() {
		var refs []ref
		for si, sl := range rl.GetScopeLogs() {
			for li := range sl.GetLogRecords() {
				refs = append(refs, ref{si, li})
			}
		}
		if len(refs) == 0 {
			continue // nothing to store
		}
		key := serviceKey(rl.GetResource())

		// Fast path: the whole resource fits (almost always true).
		whole := wrapLogs(rl)
		if proto.Size(whole) <= maxBytes {
			b, err := marshal(whole)
			if err != nil {
				return nil, 0, err
			}
			msgs = append(msgs, Message{Key: key, Value: b})
			continue
		}

		groups, bad := chunk(refs, maxBytes, func(rs []ref) int {
			return proto.Size(wrapLogs(subsetLogs(rl, rs)))
		})
		rejected += int64(len(bad))
		for _, g := range groups {
			b, err := marshal(wrapLogs(subsetLogs(rl, g)))
			if err != nil {
				return nil, 0, err
			}
			msgs = append(msgs, Message{Key: key, Value: b})
		}
	}
	return msgs, rejected, nil
}

// ---- metrics ----
// Metrics split at Metric granularity (a metric with all its data points),
// never inside a metric: splitting data points of one histogram series is
// valid OTLP but buys nothing at our sizes. Rejections are counted in data
// points because that is what OTLP's partial-success response reports.

func wrapMetrics(rm *metricsv1.ResourceMetrics) *colmetrics.ExportMetricsServiceRequest {
	return &colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{rm}}
}

func subsetMetrics(rm *metricsv1.ResourceMetrics, refs []ref) *metricsv1.ResourceMetrics {
	out := &metricsv1.ResourceMetrics{Resource: rm.Resource, SchemaUrl: rm.SchemaUrl}
	var cur *metricsv1.ScopeMetrics
	last := -1
	for _, r := range refs {
		src := rm.ScopeMetrics[r.scope]
		if r.scope != last {
			cur = &metricsv1.ScopeMetrics{Scope: src.Scope, SchemaUrl: src.SchemaUrl}
			out.ScopeMetrics = append(out.ScopeMetrics, cur)
			last = r.scope
		}
		cur.Metrics = append(cur.Metrics, src.Metrics[r.item])
	}
	return out
}

func dataPoints(m *metricsv1.Metric) int64 {
	return int64(len(m.GetGauge().GetDataPoints()) +
		len(m.GetSum().GetDataPoints()) +
		len(m.GetHistogram().GetDataPoints()) +
		len(m.GetExponentialHistogram().GetDataPoints()) +
		len(m.GetSummary().GetDataPoints()))
}

func splitMetrics(req *colmetrics.ExportMetricsServiceRequest, maxBytes int) ([]Message, int64, error) {
	var msgs []Message
	var rejected int64
	for _, rm := range req.GetResourceMetrics() {
		var refs []ref
		for si, sm := range rm.GetScopeMetrics() {
			for mi := range sm.GetMetrics() {
				refs = append(refs, ref{si, mi})
			}
		}
		if len(refs) == 0 {
			continue
		}
		key := serviceKey(rm.GetResource())

		whole := wrapMetrics(rm)
		if proto.Size(whole) <= maxBytes {
			b, err := marshal(whole)
			if err != nil {
				return nil, 0, err
			}
			msgs = append(msgs, Message{Key: key, Value: b})
			continue
		}

		groups, bad := chunk(refs, maxBytes, func(rs []ref) int {
			return proto.Size(wrapMetrics(subsetMetrics(rm, rs)))
		})
		for _, r := range bad {
			rejected += dataPoints(rm.ScopeMetrics[r.scope].Metrics[r.item])
		}
		for _, g := range groups {
			b, err := marshal(wrapMetrics(subsetMetrics(rm, g)))
			if err != nil {
				return nil, 0, err
			}
			msgs = append(msgs, Message{Key: key, Value: b})
		}
	}
	return msgs, rejected, nil
}
