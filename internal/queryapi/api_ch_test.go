package queryapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/chtest"
	"github.com/dhanush-cn/drishti/internal/storage"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

var (
	t0    = time.Date(2026, 11, 3, 17, 0, 0, 0, time.UTC)
	until = t0.Add(20 * time.Minute) // window (t0+10m, t0+20m], baseline (t0, t0+10m]
)

func str(v string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}
}

func kv(pairs ...string) []*commonv1.KeyValue {
	var out []*commonv1.KeyValue
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, &commonv1.KeyValue{Key: pairs[i], Value: str(pairs[i+1])})
	}
	return out
}

type fixture struct {
	logs, metrics [][]byte
	lsrc, msrc    []storage.Source
	anomalies     [][]byte
}

func (f *fixture) log(service string, at time.Time, sev logsv1.SeverityNumber, body string, attrs ...string) {
	b, _ := proto.Marshal(&collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource:  &resourcev1.Resource{Attributes: kv("service.name", service)},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{{TimeUnixNano: uint64(at.UnixNano()), SeverityNumber: sev, Body: str(body), Attributes: kv(attrs...)}}}},
	}}})
	f.logs = append(f.logs, b)
	f.lsrc = append(f.lsrc, storage.Source{Offset: int64(len(f.logs)), Received: at})
}

func (f *fixture) metric(res []*commonv1.KeyValue, m *metricsv1.Metric, at time.Time) {
	b, _ := proto.Marshal(&colmetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricsv1.ResourceMetrics{{
		Resource: &resourcev1.Resource{Attributes: res}, ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{m}}}}}})
	f.metrics = append(f.metrics, b)
	f.msrc = append(f.msrc, storage.Source{Offset: int64(len(f.metrics)), Received: at})
}

func calls(service, kind, span, status string, total float64, at time.Time) (*metricsv1.Metric, []*commonv1.KeyValue) {
	return &metricsv1.Metric{Name: "traces.span.metrics.calls", Data: &metricsv1.Metric_Sum{Sum: &metricsv1.Sum{IsMonotonic: true,
		AggregationTemporality: metricsv1.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		DataPoints: []*metricsv1.NumberDataPoint{{TimeUnixNano: uint64(at.UnixNano()), StartTimeUnixNano: uint64(t0.Add(-time.Hour).UnixNano()),
			Attributes: kv("span.kind", kind, "span.name", span, "status.code", status), Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: total}}}}}},
		kv("service.name", service)
}

func gauge(name string, v float64, at time.Time) *metricsv1.Metric {
	return &metricsv1.Metric{Name: name, Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{
		{TimeUnixNano: uint64(at.UnixNano()), Value: &metricsv1.NumberDataPoint_AsDouble{AsDouble: v}}}}}}
}

// build is a 25-minute stretch (t0−5m .. until+5m) with one thing per rule.
func build() *fixture {
	f := &fixture{}
	var ok, errs, chk, flagCalls float64
	for i := -10; i <= 50; i++ { // every 30 s
		at := t0.Add(time.Duration(i) * 30 * time.Second)
		ok += 10
		switch {
		case at.After(until):
			errs += 1000 // after until: must never be counted
		case at.After(until.Add(-10 * time.Minute)):
			errs += 10 // window: error ratio 0.5
		}
		chk += 4
		flagCalls += float64(i * i) // flag-system client spans: rate moves
		for _, m := range []struct {
			svc, kind, span, status string
			v                       float64
		}{
			{"payment", "SPAN_KIND_SERVER", "oteldemo.PaymentService/Charge", "STATUS_CODE_UNSET", ok},
			{"payment", "SPAN_KIND_SERVER", "oteldemo.PaymentService/Charge", "STATUS_CODE_ERROR", errs},
			{"checkout", "SPAN_KIND_CLIENT", "oteldemo.PaymentService/Charge", "STATUS_CODE_UNSET", chk},
			{"cart", "SPAN_KIND_CLIENT", "flagd.evaluation.v2.Service/ResolveBoolean", "STATUS_CODE_UNSET", flagCalls},
			{"cart", "SPAN_KIND_SERVER", "oteldemo.CartService/GetCart", "STATUS_CODE_UNSET", chk},
		} {
			// The error series exists from the start (value 0). A series born
			// with its first errors loses that first interval: the first point
			// of a series yields nothing (metricsql), as in the detector.
			mm, res := calls(m.svc, m.kind, m.span, m.status, m.v, at)
			f.metric(res, mm, at)
		}
		f.metric(kv("container.name", "email", "container.id", "e1"), gauge("container.memory.usage.total", float64(100+i)*1048576, at), at)
		f.metric(kv("container.name", "drishti-kafka"), gauge("container.cpu.utilization", 50, at), at)
		f.metric(kv("container.name", "flagd"), gauge("container.cpu.utilization", 5, at), at)
	}
	for i := 1; i <= 20; i++ {
		at := t0.Add(time.Duration(i) * time.Minute).Add(-time.Second)
		f.log("payment", at, 9, fmt.Sprintf("Charge request received. id=%d", i))
		if at.After(until.Add(-10 * time.Minute)) {
			f.log("payment", at, 13, "Payment request failed. Invalid token.")
		}
	}
	f.log("payment", until.Add(-time.Minute), 13, "Feature Flag paymentFailure enabled")
	f.log("payment", until.Add(-2*time.Minute), 9, "", "feature_flag_key", "paymentFailure")
	f.log("payment", until.Add(time.Minute), 17, "FUTURE line after until")
	f.log("flagd", until.Add(-time.Minute), 9, "flag changed")
	f.log("email", until.Add(-time.Minute), 9, "sent order confirmation") // makes email a known service

	ev := func(id, svc, event, tmpl string, opened, detected time.Time) {
		b, _ := json.Marshal(map[string]any{"schema": 1, "id": id, "event": event, "rule": "span_errors", "service": svc,
			"series": map[string]string{"span.kind": "SERVER"}, "opened_at": opened, "detected_at": detected, "evidence_at": detected,
			"window_start": opened.Add(-2 * time.Minute), "window_end": opened, "template": tmpl,
			"evidence": map[string]any{"window": "2m0s", "observed": 12, "expected": 0.1, "log10_p": -9}})
		f.anomalies = append(f.anomalies, b)
	}
	ev("a1", "payment", "opened", "", until.Add(-8*time.Minute), until.Add(-8*time.Minute))
	ev("a1", "payment", "resolved", "", until.Add(-8*time.Minute), until.Add(time.Minute)) // resolves after until: still open as of until
	ev("a2", "checkout", "opened", "", until.Add(time.Minute), until.Add(time.Minute))    // future
	ev("a3", "ad", "opened", "Feature Flag adManualGc enabled, performing a manual gc now", until.Add(-5*time.Minute), until.Add(-5*time.Minute))
	ev("a4", "load-generator", "opened", "", until.Add(-5*time.Minute), until.Add(-5*time.Minute))
	return f
}

func setup(t *testing.T) *httptest.Server {
	s := chtest.New(t, "001_logs.sql", "003_metrics.sql", "004_anomalies.sql")
	f := build()
	for _, x := range []struct {
		codec storage.Codec
		table string
		vals  [][]byte
		srcs  []storage.Source
	}{
		{storage.Logs, "logs", f.logs, f.lsrc},
		{storage.Metrics, "metrics", f.metrics, f.msrc},
		{storage.Anomalies, "anomalies", f.anomalies, make([]storage.Source, len(f.anomalies))},
	} {
		for i := range x.srcs {
			if x.srcs[i].Received.IsZero() {
				x.srcs[i] = storage.Source{Offset: int64(i), Received: t0}
			}
		}
		body, _, err := x.codec.Encode(x.vals, x.srcs)
		if err != nil {
			t.Fatal(err)
		}
		ch := &storage.CH{URL: s.URL, User: s.User, Password: s.Password, Table: s.DB + "." + x.table, Columns: x.codec.Columns}
		if err := ch.Insert(context.Background(), x.table, body); err != nil {
			t.Fatal(err)
		}
	}
	api := New(Config{CH: &CH{URL: s.URL, User: s.User, Password: s.Password}, DB: s.DB})
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)
	return srv
}

func call(t *testing.T, srv *httptest.Server, path string, params ...string) (int, Envelope, string) {
	t.Helper()
	v := url.Values{"until": {until.Format(time.RFC3339)}, "window": {"10m"}, "baseline": {"10m"}}
	for i := 0; i+1 < len(params); i += 2 {
		v.Set(params[i], params[i+1])
	}
	resp, err := http.Get(srv.URL + path + "?" + v.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var raw json.RawMessage
	json.NewDecoder(resp.Body).Decode(&raw)
	var env Envelope
	json.Unmarshal(raw, &env)
	return resp.StatusCode, env, string(raw)
}

func data[T any](t *testing.T, env Envelope) T {
	t.Helper()
	b, _ := json.Marshal(env.Data)
	var x T
	if err := json.Unmarshal(b, &x); err != nil {
		t.Fatal(err)
	}
	return x
}

func TestAPI_EndToEnd(t *testing.T) {
	srv := setup(t)

	t.Run("overview: as of until, names mapped, hidden gone", func(t *testing.T) {
		code, env, raw := call(t, srv, "/v1/overview")
		if code != 200 {
			t.Fatal(raw)
		}
		ov := data[Overview](t, env)
		byName := map[string]ServiceOverview{}
		for _, s := range ov.Services {
			byName[s.Service] = s
		}
		p, ok := byName["payment"]
		if !ok || p.ErrorRatio.Window == nil || *p.ErrorRatio.Window != 0.5 || p.ErrorRatio.Baseline == nil || *p.ErrorRatio.Baseline != 0 {
			t.Fatalf("payment error ratio %+v, want window 0.5 (nothing after until), baseline 0", p.ErrorRatio)
		}
		if len(p.Changes) == 0 {
			t.Errorf("payment: no changes listed")
		}
		seenUnchanged := false
		for _, s := range ov.Services {
			if len(s.Changes) == 0 {
				seenUnchanged = true
			} else if seenUnchanged {
				t.Errorf("%s has changes but comes after an unchanged service: changed services must come first", s.Service)
			}
		}
		if e := byName["email"]; len(e.Changes) != 1 || !strings.HasPrefix(e.Changes[0], "memory rising 2.0 MiB/min") {
			t.Errorf("email changes %v, want memory rising 2.0 MiB/min (1 MiB per 30 s)", e.Changes)
		}
		if e, ok := byName["email"]; !ok || e.MemoryMiB == nil || *e.MemoryMiB != 140 {
			t.Errorf("email (from container:email) memory = %+v, want 140 MiB, the last point at until", byName["email"].MemoryMiB)
		}
		for _, h := range []string{"container:drishti-kafka", "flagd", "container:flagd", "container:email"} {
			if _, ok := byName[h]; ok || strings.Contains(raw, `"`+h+`"`) {
				t.Errorf("%s in the overview", h)
			}
		}
		if env.MaskedLines != 2 {
			t.Errorf("masked_lines = %d, want 2 (the flag-naming body and the feature_flag attribute)", env.MaskedLines)
		}
		if l := p.LogErrorsPerM.Window; l == nil || *l != 1 {
			t.Errorf("payment warn/error lines per min = %v, want 1.0 (10 in 10 min; masked and future lines excluded)", l)
		}
	})

	t.Run("logs: masked and future lines never returned", func(t *testing.T) {
		_, env, raw := call(t, srv, "/v1/logs", "service", "payment", "limit", "50")
		lines := data[[]LogLine](t, env)
		if len(lines) != 20 {
			t.Errorf("%d lines, want 20 (10 info + 10 warn in the window)", len(lines))
		}
		for _, bad := range []string{"FUTURE", "paymentFailure", "Feature Flag", "feature_flag"} {
			if strings.Contains(raw, bad) {
				t.Errorf("%q in the response", bad)
			}
		}
		if env.MaskedLines != 2 {
			t.Errorf("masked_lines = %d, want 2", env.MaskedLines)
		}
		_, env, _ = call(t, srv, "/v1/logs", "service", "payment", "min_severity", "warn", "contains", "invalid TOKEN")
		if n := len(data[[]LogLine](t, env)); n != 10 {
			t.Errorf("warn + contains: %d lines, want 10", n)
		}
	})

	t.Run("log_templates: window vs baseline, masked excluded", func(t *testing.T) {
		_, env, raw := call(t, srv, "/v1/log_templates", "service", "payment")
		tp := data[Templates](t, env)
		if tp.LinesRead != 30 {
			t.Errorf("lines read %d, want 30 (20 info + 10 warn; masked and future excluded)", tp.LinesRead)
		}
		top := tp.Templates[0]
		if !strings.HasPrefix(top.Template, "Payment request failed") || !top.NewInWindow || top.Window != 10 {
			t.Errorf("top template %+v, want the new warn template, 10 in the window", top)
		}
		if strings.Contains(raw, "paymentFailure") || strings.Contains(raw, "FUTURE") {
			t.Error("masked or future text in templates")
		}
	})

	t.Run("anomalies: as of until, hidden gone, templates masked", func(t *testing.T) {
		_, env, raw := call(t, srv, "/v1/anomalies")
		as := data[[]Anomaly](t, env)
		ids := map[string]Anomaly{}
		for _, a := range as {
			ids[a.ID] = a
		}
		if a, ok := ids["a1"]; !ok || a.State != "open" {
			t.Errorf("a1 = %+v, want open (it resolves after until)", ids["a1"])
		}
		if _, ok := ids["a2"]; ok {
			t.Error("a2 was detected after until and must not exist yet")
		}
		if _, ok := ids["a4"]; ok {
			t.Error("a4 belongs to the load generator (hidden)")
		}
		if a := ids["a3"]; strings.Contains(a.Template, "adManualGc") || !strings.Contains(a.Template, "masked") {
			t.Errorf("a3 template %q not masked", a.Template)
		}
		if strings.Contains(raw, "adManualGc") {
			t.Error("flag name in the response")
		}
	})

	t.Run("spans: flag-system spans dropped, callee found", func(t *testing.T) {
		_, env, raw := call(t, srv, "/v1/spans", "service", "cart")
		if strings.Contains(raw, "flagd") {
			t.Errorf("flag-system span in cart's spans: %s", raw)
		}
		_, env, _ = call(t, srv, "/v1/spans", "service", "checkout")
		rows := data[[]SpanRow](t, env)
		if len(rows) != 1 || rows[0].Callee != "payment" {
			t.Errorf("checkout spans %+v, want the Charge client span with callee payment", rows)
		}
	})

	t.Run("timeseries: nothing after until", func(t *testing.T) {
		_, env, raw := call(t, srv, "/v1/timeseries", "service", "payment", "signal", "errors", "step", "60s")
		s := data[Series](t, env)
		total := 0.0
		for _, p := range s.Points {
			if p.T.After(until) {
				t.Errorf("point at %s, after until", p.T)
			}
			if p.V != nil {
				total += *p.V
			}
		}
		if total != 200 {
			t.Errorf("errors in the window = %v, want 200 (20 points x 10; the 1000s after until excluded): %s", total, raw)
		}
	})

	t.Run("bad requests", func(t *testing.T) {
		for _, c := range []struct {
			path   string
			params []string
			want   string
		}{
			{"/v1/overview", []string{"until", ""}, "until is required"},
			{"/v1/overview", []string{"window", "5h"}, "window must be between"},
			{"/v1/logs", []string{"service", "flagd"}, "unknown service"},
			{"/v1/logs", []string{"service", "container:drishti-kafka"}, "unknown service"},
			{"/v1/timeseries", []string{"service", "payment", "signal", "nope"}, "signal must be one of"},
		} {
			code, _, raw := call(t, srv, c.path, c.params...)
			if code != 400 || !strings.Contains(raw, c.want) {
				t.Errorf("%s %v: %d %s, want 400 %q", c.path, c.params, code, raw, c.want)
			}
		}
	})
}

// The SQL predicate and the Go Masker decide the same for every fixture line.
func TestMasker_SQLAgreesWithGo(t *testing.T) {
	s := chtest.New(t, "001_logs.sql")
	m := NewMasker(DemoFlags)
	lines := []struct {
		body  string
		attrs []string
	}{
		{"Feature Flag adManualGc enabled, performing a manual gc now", nil},
		{"High CPU-Load problempattern enabled", nil},
		{"API request failed", []string{"exception.message", "Error: Product Catalog Fail Feature Flag Enabled"}},
		{"", []string{"feature_flag_key", "intlShippingSlowdown"}},
		{"", []string{"Feature_Flag.variant", "off"}},
		{"POST /flagservice/x", nil},
		{"Payment request failed. Invalid token.", []string{"app.loyalty", "gold"}},
		{"Wasn't able to connect to redis", nil},
		{"ünïcode FEATURE FLAG", nil},
	}
	f := &fixture{}
	for _, l := range lines {
		f.log("svc", t0, 9, l.body, l.attrs...)
	}
	body, _, _ := storage.Logs.Encode(f.logs, f.lsrc)
	ch := &storage.CH{URL: s.URL, User: s.User, Password: s.Password, Table: s.DB + ".logs", Columns: storage.LogColumns}
	if err := ch.Insert(context.Background(), "m", body); err != nil {
		t.Fatal(err)
	}
	type row struct {
		Off    uint64 `json:"kafka_offset"`
		Masked uint8  `json:"masked"`
	}
	rows, err := Rows[row](context.Background(), &CH{URL: s.URL, User: s.User, Password: s.Password},
		"SELECT kafka_offset, "+MaskedSQL+" AS masked FROM "+s.DB+".logs ORDER BY kafka_offset",
		map[string]string{"mask_terms": stringArray(m.Terms())})
	if err != nil || len(rows) != len(lines) {
		t.Fatal(len(rows), err)
	}
	for i, r := range rows {
		l := lines[r.Off-1]
		attrs := map[string]string{}
		for j := 0; j+1 < len(l.attrs); j += 2 {
			attrs[l.attrs[j]] = l.attrs[j+1]
		}
		if goSays := m.Line(l.body, attrs); goSays != (r.Masked == 1) {
			t.Errorf("line %d %q %v: SQL masked=%d, Go %v", i, l.body, l.attrs, r.Masked, goSays)
		}
	}
}
