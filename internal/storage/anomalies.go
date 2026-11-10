package storage

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/dhanush-cn/drishti/internal/detect"
)

// AnomalyColumns is the INSERT column list of drishti.anomalies
// (004_anomalies.sql), in RowBinary order.
var AnomalyColumns = []string{
	"id", "event", "rule", "service", "series", "opened_at", "detected_at", "evidence_at",
	"window_start", "window_end", "ev_window", "observed", "expected", "calls", "baseline_ratio",
	"log10_p", "detail", "template", "replayed", "raw", "received_at", "kafka_partition", "kafka_offset",
}

// Anomalies is the codec of the anomalies topic → drishti.anomalies: one row
// per detector event (JSON schema v1, docs/detector.md section 5).
var Anomalies = Codec{Name: "anomalies", Columns: AnomalyColumns, NewDecoder: func() Decoder { return &anomalyDecoder{} }}

type anomalyDecoder struct {
	ev   detect.Event
	raw  []byte
	keys []string
}

func (d *anomalyDecoder) Decode(value []byte) (int, error) {
	d.ev = detect.Event{}
	if err := json.Unmarshal(value, &d.ev); err != nil {
		return 0, err
	}
	if d.ev.Schema != 1 || d.ev.ID == "" || d.ev.Service == "" {
		return 0, fmt.Errorf("not an anomaly event v1 (schema %d, id %q, service %q)", d.ev.Schema, d.ev.ID, d.ev.Service)
	}
	d.raw = value
	return 1, nil
}

func (d *anomalyDecoder) Append(w *rowBuf, src Source) int {
	e := &d.ev
	ms := func(t time.Time) {
		if t.IsZero() {
			w.i64(0)
			return
		}
		w.i64(t.UnixMilli())
	}
	w.str(e.ID)
	w.str(e.Event)
	w.str(e.Rule)
	w.str(e.Service)
	d.keys = d.keys[:0]
	for k := range e.Series {
		d.keys = append(d.keys, k)
	}
	slices.SortFunc(d.keys, cmp.Compare) // map order is random: same event, same bytes
	w.uvarint(uint64(len(d.keys)))
	for _, k := range d.keys {
		w.str(k)
		w.str(e.Series[k])
	}
	ms(e.OpenedAt)
	ms(e.DetectedAt)
	ms(e.EvidenceAt)
	ms(e.WindowStart)
	ms(e.WindowEnd)
	w.str(e.Evidence.Window)
	w.f64(e.Evidence.Observed)
	w.f64(e.Evidence.Expected)
	w.f64(e.Evidence.Calls)
	w.f64(e.Evidence.Ratio)
	w.f64(e.Evidence.Log10P)
	w.str(e.Evidence.Detail)
	w.str(e.Template)
	if e.Replayed {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.bytes(d.raw)
	w.i64(src.Received.UnixMilli())
	w.u16(uint16(src.Partition))
	w.u64(uint64(src.Offset))
	return 1
}
