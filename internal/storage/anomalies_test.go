package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/chtest"
)

// A real event from capture 2 (docs/detector.md section 5).
const realEvent = `{"schema":1,"id":"f8b437a3a210c916","event":"opened","rule":"span_errors","service":"payment",` +
	`"series":{"span.kind":"SERVER","span.name":"oteldemo.PaymentService/Charge"},` +
	`"opened_at":"2026-11-03T17:08:55Z","detected_at":"2026-11-03T17:08:55Z","evidence_at":"2026-11-03T17:08:55Z",` +
	`"window_start":"2026-11-03T17:06:55Z","window_end":"2026-11-03T17:08:55Z",` +
	`"evidence":{"window":"2m0s","observed":3,"expected":0.02,"calls":4,"baseline_ratio":0.005,"log10_p":-6.303},` +
	`"detector":{"version":"dev","topic":"otlp.metrics","partition":5},"source":{"partition":5,"offsets":[16655,16830],"topic":"otlp.metrics"}}`

func TestAnomaliesCodec_SameEventSameBytes(t *testing.T) {
	src := Source{Partition: 2, Offset: 7, Received: time.Date(2026, 11, 3, 17, 8, 55, 100e6, time.UTC)}
	var first rowBuf
	d := Anomalies.NewDecoder()
	for i := range 20 { // series is a map: Go randomizes its order per iteration
		var w rowBuf
		if n, err := d.Decode([]byte(realEvent)); err != nil || n != 1 {
			t.Fatalf("Decode = %d, %v", n, err)
		}
		d.Append(&w, src)
		if i == 0 {
			first = w
		} else if string(w.b) != string(first.b) {
			t.Fatal("the same event encoded to different bytes (series map order leaked into the row)")
		}
	}
	for _, bad := range []string{`{"id":"x","service":"s"}`, `{"schema":1,"service":"s"}`, `not json`} {
		if _, err := d.Decode([]byte(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestAnomaliesCodec_RoundTripThroughClickHouse(t *testing.T) {
	s := chtest.New(t, "004_anomalies.sql")
	body, rows, err := Anomalies.Encode([][]byte{[]byte(realEvent)}, []Source{{Partition: 2, Offset: 7, Received: time.Date(2026, 11, 3, 17, 8, 55, 100e6, time.UTC)}})
	if err != nil || rows != 1 {
		t.Fatal(rows, err)
	}
	ch := &CH{URL: s.URL, User: s.User, Password: s.Password, Table: s.DB + ".anomalies", Columns: AnomalyColumns}
	if err := ch.Insert(context.Background(), "a", body); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(s.MustQuery("SELECT id, event, rule, service, series['span.name'], toUnixTimestamp64Milli(opened_at), toUnixTimestamp64Milli(window_start), ev_window, observed, expected, calls, baseline_ratio, log10_p, replayed, JSONExtractString(raw, 'detector', 'topic'), toUnixTimestamp64Milli(received_at) FROM " + s.DB + ".anomalies FORMAT TabSeparated"))
	want := "f8b437a3a210c916\topened\tspan_errors\tpayment\toteldemo.PaymentService/Charge\t1793725735000\t1793725615000\t2m0s\t3\t0.02\t4\t0.005\t-6.303\tfalse\totlp.metrics\t1793725735100"
	if got != want {
		t.Fatalf("read back:\n%s\nwant:\n%s", got, want)
	}
}
