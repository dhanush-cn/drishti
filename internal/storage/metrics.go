package storage

import (
	"cmp"
	"math"
	"slices"

	"github.com/dhanush-cn/drishti/internal/otlpmetrics"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// MetricColumns is the INSERT column list of drishti.metrics
// (003_metrics.sql), in RowBinary order. inserted_at and series_id are
// computed by the server.
var MetricColumns = []string{
	"ts", "start_ts", "received_at", "service", "instance", "metric", "kind", "monotonic",
	"temporality", "value", "count", "sum", "bounds", "buckets", "attributes",
	"resource_attributes", "kafka_partition", "kafka_offset",
}

// Metrics is the codec of otlp.metrics → drishti.metrics (docs/storage.md,
// decision 10). Points are stored as received: a cumulative point keeps its
// running total and start time, and deltas are computed at query time
// (internal/metricsql). Subtracting here would need the series' previous
// point, i.e. state carried between records, which the codec contract
// forbids (codec.go).
//
// Gauges, sums and explicit-bucket histograms are stored. Exponential
// histograms and summaries are not (none of the demo's metrics use them;
// checked on both recordings with kafkadump summary).
var Metrics = Codec{Name: "metrics", Columns: MetricColumns, NewDecoder: func() Decoder { return &metricsDecoder{} }}

// Enum8 values of 003_metrics.sql.
const (
	kindGauge     int8 = 1
	kindSum       int8 = 2
	kindHistogram int8 = 3
)

type metricsDecoder struct {
	req   colmetrics.ExportMetricsServiceRequest
	attrs []*commonv1.KeyValue // sort scratch
}

func (d *metricsDecoder) Decode(value []byte) (int, error) {
	proto.Reset(&d.req)
	if err := proto.Unmarshal(value, &d.req); err != nil {
		return 0, err
	}
	n := 0
	for _, rm := range d.req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				switch data := m.GetData().(type) {
				case *metricsv1.Metric_Gauge:
					n += len(data.Gauge.GetDataPoints())
				case *metricsv1.Metric_Sum:
					n += len(data.Sum.GetDataPoints())
				case *metricsv1.Metric_Histogram:
					n += len(data.Histogram.GetDataPoints())
				}
			}
		}
	}
	return n, nil
}

// Instance is the replica identity of a resource: service.instance.id, else
// container.id, else container.name, else host.name.
func Instance(attrs []*commonv1.KeyValue) string {
	for _, k := range []string{"service.instance.id", "container.id", "container.name", "host.name"} {
		if v := otlpmetrics.Attr(attrs, k); v != "" {
			return v
		}
	}
	return ""
}

func (d *metricsDecoder) Append(w *rowBuf, src Source) int {
	recvNs := uint64(src.Received.UnixNano())
	recvMs := src.Received.UnixMilli()
	rows := 0
	for _, rm := range d.req.GetResourceMetrics() {
		rattrs := rm.GetResource().GetAttributes()
		owner := otlpmetrics.OwnerOf(rattrs).Key()
		inst := Instance(rattrs)
		// One row's common head; the value columns differ per kind.
		head := func(name string, t, start uint64, kind int8, mono bool, temp metricsv1.AggregationTemporality) {
			if t == 0 {
				t = recvNs // never a row in 1970
			}
			w.dt64ns(t)
			w.dt64ns(start)
			w.i64(recvMs)
			w.str(owner)
			w.str(inst)
			w.str(name)
			w.u8(uint8(kind))
			if mono {
				w.u8(1)
			} else {
				w.u8(0)
			}
			w.u8(uint8(temp))
		}
		tail := func(attrs []*commonv1.KeyValue) {
			d.attrs = append(d.attrs[:0], attrs...)
			slices.SortFunc(d.attrs, func(a, b *commonv1.KeyValue) int { return cmp.Compare(a.GetKey(), b.GetKey()) })
			attrMap(w, d.attrs)
			attrMap(w, rattrs)
			w.u16(uint16(src.Partition))
			w.u64(uint64(src.Offset))
			rows++
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				name := m.GetName()
				switch data := m.GetData().(type) {
				case *metricsv1.Metric_Gauge:
					for _, p := range data.Gauge.GetDataPoints() {
						head(name, p.GetTimeUnixNano(), p.GetStartTimeUnixNano(), kindGauge, false, metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_UNSPECIFIED)
						w.f64(number(p))
						w.u64(0)
						w.f64(0)
						w.uvarint(0)
						w.uvarint(0)
						tail(p.GetAttributes())
					}
				case *metricsv1.Metric_Sum:
					for _, p := range data.Sum.GetDataPoints() {
						head(name, p.GetTimeUnixNano(), p.GetStartTimeUnixNano(), kindSum, data.Sum.GetIsMonotonic(), data.Sum.GetAggregationTemporality())
						w.f64(number(p))
						w.u64(0)
						w.f64(0)
						w.uvarint(0)
						w.uvarint(0)
						tail(p.GetAttributes())
					}
				case *metricsv1.Metric_Histogram:
					for _, p := range data.Histogram.GetDataPoints() {
						head(name, p.GetTimeUnixNano(), p.GetStartTimeUnixNano(), kindHistogram, true, data.Histogram.GetAggregationTemporality())
						w.f64(0)
						w.u64(p.GetCount())
						w.f64(p.GetSum())
						w.uvarint(uint64(len(p.GetExplicitBounds())))
						for _, b := range p.GetExplicitBounds() {
							w.f64(b)
						}
						w.uvarint(uint64(len(p.GetBucketCounts())))
						for _, c := range p.GetBucketCounts() {
							w.u64(c)
						}
						tail(p.GetAttributes())
					}
				}
			}
		}
	}
	return rows
}

func number(p *metricsv1.NumberDataPoint) float64 {
	switch v := p.GetValue().(type) {
	case *metricsv1.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	case *metricsv1.NumberDataPoint_AsDouble:
		return v.AsDouble
	}
	return 0
}

func (w *rowBuf) f64(v float64) { w.u64(math.Float64bits(v)) }
