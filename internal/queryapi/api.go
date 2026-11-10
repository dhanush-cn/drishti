// Package queryapi is the agent's read-only view of Drishti (docs/evals.md
// section 3, Week 5 design E7): a few typed HTTP endpoints over ClickHouse,
// each answering one question an on-call engineer asks first.
//
//	GET /v1/services        which services exist (the names every other call takes)
//	GET /v1/overview        every service, now vs before: calls, errors, latency, logs, CPU, memory,
//	                        and a deterministic list of what changed (the triage step: no LLM)
//	GET /v1/anomalies       the detector's alerts as of `until`
//	GET /v1/spans           one service's operations, now vs before, with each client call's callee
//	GET /v1/timeseries      one signal of one service over time
//	GET /v1/log_templates   one service's log templates (Drain), now vs before
//	GET /v1/logs            one service's log lines, filtered
//
// Rules every endpoint follows:
//   - `until` is required, and nothing after it is ever read. An eval asks
//     as of a moment inside a recording, and the answer must not depend on
//     what happened later.
//   - Windows are bounded (MaxRange) and results are capped, so a tool call
//     costs a bounded number of tokens and a bounded query.
//   - Hidden services (Names) don't exist: not listed, not queryable, and
//     reported as unknown when asked for.
//   - Log lines that reveal the fault injection are masked (Masker) before
//     anything is counted or returned; responses say how many.
//   - Numbers from cumulative metrics are query-time deltas (internal/
//     metricsql), computed with the detector's rules.
package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dhanush-cn/drishti/internal/drain"
	"github.com/dhanush-cn/drishti/internal/metricsql"
)

type Config struct {
	CH       *CH
	DB       string   // drishti or drishti_eval
	Hidden   []string // DefaultHidden if nil
	Flags    []string // DemoFlags if nil
	MaxRange time.Duration
	// MaxLogRows caps the lines one log_templates call reads (newest first).
	MaxLogRows int
}

type API struct {
	cfg  Config
	mask *Masker
	mux  *http.ServeMux

	mu    sync.Mutex
	names map[int64]*Names // by until, truncated to the minute
}

func New(cfg Config) *API {
	if cfg.Hidden == nil {
		cfg.Hidden = DefaultHidden
	}
	if cfg.Flags == nil {
		cfg.Flags = DemoFlags
	}
	if cfg.MaxRange <= 0 {
		cfg.MaxRange = 2 * time.Hour
	}
	if cfg.MaxLogRows <= 0 {
		cfg.MaxLogRows = 50_000
	}
	a := &API{cfg: cfg, mask: NewMasker(cfg.Flags), mux: http.NewServeMux(), names: map[int64]*Names{}}
	a.handle("/v1/services", a.services)
	a.handle("/v1/overview", a.overview)
	a.handle("/v1/anomalies", a.anomalies)
	a.handle("/v1/spans", a.spans)
	a.handle("/v1/timeseries", a.timeseries)
	a.handle("/v1/log_templates", a.logTemplates)
	a.handle("/v1/logs", a.logs)
	return a
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) { a.mux.ServeHTTP(w, r) }

// ---- request plumbing ----

// badRequest is an error the caller can fix (shown to the agent as is).
type badRequest struct{ msg string }

func (e badRequest) Error() string { return e.msg }

func bad(format string, args ...any) error { return badRequest{fmt.Sprintf(format, args...)} }

// Envelope is every response: the time range actually used, how many log
// lines were withheld, and the data.
type Envelope struct {
	Until       time.Time `json:"until"`
	From        time.Time `json:"from"`
	Window      string    `json:"window"`
	Baseline    string    `json:"baseline,omitempty"`
	MaskedLines int64     `json:"masked_lines"`
	Data        any       `json:"data"`
}

// req is a parsed request: the time model every endpoint shares.
//
//	|------- baseline -------|------- window -------|
//	                     winStart                 until
type req struct {
	r        *http.Request
	until    time.Time
	window   time.Duration
	baseline time.Duration
	names    *Names
	masked   int64
}

func (q *req) winStart() time.Time  { return q.until.Add(-q.window) }
func (q *req) baseStart() time.Time { return q.winStart().Add(-q.baseline) }

func (a *API) handle(path string, fn func(context.Context, *req) (any, error)) {
	a.mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		q, err := a.parse(r)
		var data any
		if err == nil {
			data, err = fn(r.Context(), q)
		}
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			code := http.StatusInternalServerError
			if errors.As(err, new(badRequest)) {
				code = http.StatusBadRequest
			}
			w.WriteHeader(code)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		env := Envelope{Until: q.until, From: q.winStart(), Window: q.window.String(), MaskedLines: q.masked, Data: data}
		if q.baseline > 0 {
			env.Baseline = q.baseline.String()
		}
		json.NewEncoder(w).Encode(env)
	})
}

func (a *API) parse(r *http.Request) (*req, error) {
	v := r.URL.Query()
	q := &req{r: r, window: 10 * time.Minute, baseline: 30 * time.Minute}
	u := v.Get("until")
	if u == "" {
		return q, bad("until is required (RFC3339): every answer is as of a moment, and nothing after it is read")
	}
	t, err := time.Parse(time.RFC3339Nano, u)
	if err != nil {
		return q, bad("until: %v", err)
	}
	q.until = t.UTC()
	for _, p := range []struct {
		name string
		dst  *time.Duration
		min  time.Duration
	}{{"window", &q.window, time.Minute}, {"baseline", &q.baseline, 0}} {
		if s := v.Get(p.name); s != "" {
			d, err := time.ParseDuration(s)
			if err != nil {
				return q, bad("%s: %v", p.name, err)
			}
			*p.dst = d
		}
		if *p.dst < p.min || *p.dst > a.cfg.MaxRange {
			return q, bad("%s must be between %s and %s", p.name, p.min, a.cfg.MaxRange)
		}
	}
	names, err := a.namesAt(r.Context(), q.until)
	if err != nil {
		return q, err
	}
	q.names = names
	return q, nil
}

// service resolves the ?service= argument to storage owner keys.
func (q *req) service() (string, []string, error) {
	name := strings.TrimSpace(q.r.URL.Query().Get("service"))
	if name == "" {
		return "", nil, bad("service is required; known: %s", strings.Join(q.names.Known(), ", "))
	}
	owners := q.names.Owners(name)
	if owners == nil {
		return "", nil, bad("unknown service %q; known: %s", name, strings.Join(q.names.Known(), ", "))
	}
	return name, owners, nil
}

func (a *API) table(t string) string { return a.cfg.DB + "." + t }

// namesAt learns which services are instrumented (spans or logs in the 3 h
// before until).
func (a *API) namesAt(ctx context.Context, until time.Time) (*Names, error) {
	key := until.Truncate(time.Minute).Unix()
	a.mu.Lock()
	n := a.names[key]
	a.mu.Unlock()
	if n != nil {
		return n, nil
	}
	type row struct{ Service string }
	rows, err := Rows[row](ctx, a.cfg.CH, `
SELECT DISTINCT service FROM `+a.table("metrics")+`
WHERE metric = 'traces.span.metrics.calls' AND ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')}
UNION DISTINCT
SELECT DISTINCT service FROM `+a.table("logs")+`
WHERE ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')}`,
		map[string]string{"lo": ts(until.Add(-3 * time.Hour)), "until": ts(until)})
	if err != nil {
		return nil, err
	}
	var known []string
	for _, r := range rows {
		if !strings.Contains(r.Service, ":") && r.Service != "unknown_service" {
			known = append(known, r.Service)
		}
	}
	n = NewNames(known, a.cfg.Hidden)
	a.mu.Lock()
	if len(a.names) > 256 {
		clear(a.names)
	}
	a.names[key] = n
	a.mu.Unlock()
	return n, nil
}

// ---- /v1/services ----

type ServiceList struct {
	Services []string `json:"services"`
	Note     string   `json:"note"`
}

func (a *API) services(ctx context.Context, q *req) (any, error) {
	return ServiceList{Services: q.names.Known(),
		Note: "instrumented services (spans or logs). Infrastructure without instrumentation appears as container:<name> in overview."}, nil
}

// ---- span metric aggregates shared by overview, spans, timeseries ----

type spanAgg struct {
	calls, errors [2]float64 // [baseline, window]
	hist          [2][]uint64
	bounds        []float64
}

// spanAggs returns per (owner, kind, span name) call/error counts and
// latency buckets for baseline and window. owners nil = every service.
func (a *API) spanAggs(ctx context.Context, q *req, owners []string, kinds []string) (map[[3]string]*spanAgg, error) {
	params := metricsql.Params("traces.span.metrics.calls", q.baseStart(), q.until, owners)
	params["win"] = ts(q.winStart())
	params["kinds"] = stringArray(kinds)
	type callRow struct {
		Service string  `json:"service"`
		Kind    string  `json:"kind"`
		Span    string  `json:"span"`
		Status  string  `json:"status"`
		InWin   uint8   `json:"in_win"`
		N       float64 `json:"n"`
	}
	calls, err := Rows[callRow](ctx, a.cfg.CH, `
SELECT service, attributes['span.kind'] AS kind, attributes['span.name'] AS span, attributes['status.code'] AS status,
       ts > {win:DateTime64(9, 'UTC')} AS in_win, sum(delta) AS n
FROM (`+metricsql.SumDeltas(a.table("metrics"))+`)
WHERE has({kinds:Array(String)}, attributes['span.kind'])
GROUP BY service, kind, span, status, in_win`, params)
	if err != nil {
		return nil, err
	}
	params["metric"] = "traces.span.metrics.duration"
	type histRow struct {
		Service string    `json:"service"`
		Kind    string    `json:"kind"`
		Span    string    `json:"span"`
		InWin   uint8     `json:"in_win"`
		Bounds  []float64 `json:"bounds"`
		Buckets []uint64  `json:"buckets"`
	}
	hists, err := Rows[histRow](ctx, a.cfg.CH, `
SELECT service, attributes['span.kind'] AS kind, attributes['span.name'] AS span,
       ts > {win:DateTime64(9, 'UTC')} AS in_win, bounds, sumForEach(delta) AS buckets
FROM (`+metricsql.HistDeltas(a.table("metrics"))+`)
WHERE has({kinds:Array(String)}, attributes['span.kind'])
GROUP BY service, kind, span, in_win, bounds`, params)
	if err != nil {
		return nil, err
	}
	out := map[[3]string]*spanAgg{}
	get := func(s, k, sp string) *spanAgg {
		key := [3]string{s, k, sp}
		if out[key] == nil {
			out[key] = &spanAgg{}
		}
		return out[key]
	}
	// Spans of the flag system itself (clients' flagd.evaluation… calls, the
	// proxy's flagd-ui route) are dropped like log lines that name it: their
	// rate moves when a flag changes (found by scripts/leakcheck.py).
	for _, r := range calls {
		if a.mask.Text(r.Span) {
			continue
		}
		g := get(r.Service, r.Kind, r.Span)
		g.calls[r.InWin] += r.N
		if r.Status == "STATUS_CODE_ERROR" {
			g.errors[r.InWin] += r.N
		}
	}
	for _, r := range hists {
		if a.mask.Text(r.Span) {
			continue
		}
		g := get(r.Service, r.Kind, r.Span)
		if g.bounds != nil && !slices.Equal(g.bounds, r.Bounds) {
			continue // layout changed inside the range: keep the first
		}
		g.bounds = r.Bounds
		if g.hist[r.InWin] == nil {
			g.hist[r.InWin] = make([]uint64, len(r.Buckets))
		}
		for i, c := range r.Buckets {
			if i < len(g.hist[r.InWin]) {
				g.hist[r.InWin][i] += c
			}
		}
	}
	return out, nil
}

// Stat is one quantity now (window) and before (baseline, same unit).
type Stat struct {
	Window   *float64 `json:"window"`
	Baseline *float64 `json:"baseline"`
}

func num(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	r := math.Round(v*1000) / 1000
	return &r
}

func perMin(n float64, d time.Duration) float64 {
	if d <= 0 {
		return math.NaN()
	}
	return n / d.Minutes()
}

func ratio(a, b float64) float64 {
	if b == 0 {
		return math.NaN()
	}
	return a / b
}

// ---- /v1/overview ----

type ServiceOverview struct {
	Service        string   `json:"service"`
	CallsPerMin    Stat     `json:"server_calls_per_min"`
	ErrorRatio     Stat     `json:"server_error_ratio"`
	P95Ms          Stat     `json:"server_p95_ms"`
	LogLinesPerMin Stat     `json:"log_lines_per_min"`
	LogErrorsPerM  Stat     `json:"log_warn_error_lines_per_min"`
	CPU            Stat     `json:"cpu_utilization"`
	MemoryMiB      *float64 `json:"memory_mib,omitempty"`
	MemoryLimitPct *float64 `json:"memory_pct_of_limit,omitempty"`
	MemSlopeMiBMin *float64 `json:"memory_mib_per_min,omitempty"`
	Changes        []string `json:"changes,omitempty"`
}

type Overview struct {
	Services []ServiceOverview `json:"services"`
	Note     string            `json:"note"`
}

func (a *API) overview(ctx context.Context, q *req) (any, error) {
	rows := map[string]*ServiceOverview{}
	get := func(owner string) *ServiceOverview {
		name := q.names.Show(owner)
		if name == "" {
			return nil
		}
		if rows[name] == nil {
			rows[name] = &ServiceOverview{Service: name}
		}
		return rows[name]
	}
	spans, err := a.spanAggs(ctx, q, nil, []string{"SPAN_KIND_SERVER"})
	if err != nil {
		return nil, err
	}
	type svcAgg struct {
		calls, errs [2]float64
		hist        [2][]uint64
		bounds      []float64
	}
	bySvc := map[string]*svcAgg{}
	for k, g := range spans {
		s := bySvc[k[0]]
		if s == nil {
			s = &svcAgg{}
			bySvc[k[0]] = s
		}
		for i := range 2 {
			s.calls[i] += g.calls[i]
			s.errs[i] += g.errors[i]
			if g.hist[i] == nil {
				continue
			}
			if s.bounds == nil {
				s.bounds = g.bounds
			}
			if !slices.Equal(s.bounds, g.bounds) {
				continue
			}
			if s.hist[i] == nil {
				s.hist[i] = make([]uint64, len(g.hist[i]))
			}
			for j, c := range g.hist[i] {
				s.hist[i][j] += c
			}
		}
	}
	for owner, s := range bySvc {
		o := get(owner)
		if o == nil {
			continue
		}
		o.CallsPerMin = Stat{num(perMin(s.calls[1], q.window)), num(perMin(s.calls[0], q.baseline))}
		o.ErrorRatio = Stat{num(ratio(s.errs[1], s.calls[1])), num(ratio(s.errs[0], s.calls[0]))}
		o.P95Ms = Stat{num(Quantile(0.95, s.bounds, s.hist[1])), num(Quantile(0.95, s.bounds, s.hist[0]))}
	}

	// Logs, masked lines excluded in SQL.
	type logRow struct {
		Service string `json:"service"`
		InWin   uint8  `json:"in_win"`
		Lines   uint64 `json:"lines"`
		Errors  uint64 `json:"errors"`
		Masked  uint64 `json:"masked_n"`
	}
	lr, err := Rows[logRow](ctx, a.cfg.CH, `
SELECT service, ts > {win:DateTime64(9, 'UTC')} AS in_win,
       countIf(NOT masked) AS lines, countIf(NOT masked AND `+warnOrError+`) AS errors, countIf(masked) AS masked_n
FROM (SELECT service, ts, severity_number, severity_text, `+MaskedSQL+` AS masked FROM `+a.table("logs")+`
      WHERE ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')})
GROUP BY service, in_win`, map[string]string{"lo": ts(q.baseStart()), "win": ts(q.winStart()), "until": ts(q.until), "mask_terms": stringArray(a.mask.Terms())})
	if err != nil {
		return nil, err
	}
	logs := map[string]*[2][2]float64{}
	for _, r := range lr {
		o := get(r.Service)
		if o == nil {
			continue
		}
		q.masked += int64(r.Masked)
		if logs[o.Service] == nil {
			logs[o.Service] = &[2][2]float64{}
		}
		logs[o.Service][r.InWin][0] += float64(r.Lines)
		logs[o.Service][r.InWin][1] += float64(r.Errors)
	}
	for name, l := range logs {
		o := rows[name]
		o.LogLinesPerMin = Stat{num(perMin(l[1][0], q.window)), num(perMin(l[0][0], q.baseline))}
		o.LogErrorsPerM = Stat{num(perMin(l[1][1], q.window)), num(perMin(l[0][1], q.baseline))}
	}

	// Containers: CPU mean, memory now, its limit and its slope over the window.
	type ctrRow struct {
		Service string   `json:"service"`
		CPUWin  *float64 `json:"cpu_win"`
		CPUBase *float64 `json:"cpu_base"`
		Mem     *float64 `json:"mem"`
		Limit   *float64 `json:"lim"`
		Slope   *float64 `json:"slope"`
	}
	cr, err := Rows[ctrRow](ctx, a.cfg.CH, `
SELECT service,
       avgIf(value, metric = 'container.cpu.utilization' AND ts > {win:DateTime64(9, 'UTC')}) AS cpu_win,
       avgIf(value, metric = 'container.cpu.utilization' AND ts <= {win:DateTime64(9, 'UTC')}) AS cpu_base,
       argMaxIf(value, ts, metric = 'container.memory.usage.total') / 1048576 AS mem,
       argMaxIf(value, ts, metric = 'container.memory.usage.limit') / 1048576 AS lim,
       (simpleLinearRegressionIf(toUnixTimestamp64Milli(ts) / 60000, value / 1048576,
            metric = 'container.memory.usage.total' AND ts > {win:DateTime64(9, 'UTC')})).1 AS slope
FROM `+a.table("metrics")+`
WHERE metric IN ('container.cpu.utilization', 'container.memory.usage.total', 'container.memory.usage.limit')
  AND ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')}
GROUP BY service`, map[string]string{"lo": ts(q.baseStart()), "win": ts(q.winStart()), "until": ts(q.until)})
	if err != nil {
		return nil, err
	}
	for _, r := range cr {
		o := get(r.Service)
		if o == nil {
			continue
		}
		o.CPU = Stat{nz(r.CPUWin), nz(r.CPUBase)}
		o.MemoryMiB = nz(r.Mem)
		if r.Mem != nil && r.Limit != nil && *r.Limit > 0 {
			o.MemoryLimitPct = num(100 * *r.Mem / *r.Limit)
		}
		o.MemSlopeMiBMin = nz(r.Slope)
	}

	var out []ServiceOverview
	for _, o := range rows {
		o.Changes = changes(o)
		out = append(out, *o)
	}
	sort.Slice(out, func(i, j int) bool {
		if (len(out[i].Changes) > 0) != (len(out[j].Changes) > 0) {
			return len(out[i].Changes) > 0
		}
		return out[i].Service < out[j].Service
	})
	return Overview{Services: out, Note: "window = the last `window` before until; baseline = the `baseline` before that. " +
		"changes = deterministic thresholds (docs/evals.md section 3), not a diagnosis. " +
		"A p95 equal to the top histogram bound (15000 ms for span metrics) means at least that."}, nil
}

func nz(p *float64) *float64 {
	if p == nil {
		return nil
	}
	return num(*p)
}

// warnOrError is the detector's severity rule (logs_1m): WARN and above.
const warnOrError = `(severity_number >= 13 OR (severity_number = 0 AND lower(severity_text) IN ('warn', 'warning', 'error', 'err', 'fatal', 'critical')))`

// changes lists, in words, what moved between baseline and window. Fixed
// thresholds, chosen before any eval run (docs/evals.md section 3): this is
// the deterministic triage step, so it must not be tuned on the test set.
func changes(o *ServiceOverview) []string {
	var out []string
	v := func(s Stat) (float64, float64, bool) {
		if s.Window == nil || s.Baseline == nil {
			return 0, 0, false
		}
		return *s.Window, *s.Baseline, true
	}
	if w, b, ok := v(o.ErrorRatio); ok && w >= max(2*b, b+0.02) && w >= 0.01 {
		out = append(out, fmt.Sprintf("server error ratio %.3f (was %.3f)", w, b))
	} else if o.ErrorRatio.Window != nil && o.ErrorRatio.Baseline == nil && *o.ErrorRatio.Window >= 0.01 {
		out = append(out, fmt.Sprintf("server error ratio %.3f (no baseline)", *o.ErrorRatio.Window))
	}
	if w, b, ok := v(o.P95Ms); ok && w >= 2*b && w-b >= 20 {
		out = append(out, fmt.Sprintf("server p95 %.0f ms (was %.0f ms)", w, b))
	}
	if w, b, ok := v(o.CallsPerMin); ok && b >= 1 && (w >= 3*b || w <= b/3) {
		out = append(out, fmt.Sprintf("server calls %.1f/min (was %.1f/min)", w, b))
	}
	if w, b, ok := v(o.LogErrorsPerM); ok && w >= 0.5 && w >= 3*b {
		out = append(out, fmt.Sprintf("warn/error log lines %.1f/min (was %.1f/min)", w, b))
	}
	if w, b, ok := v(o.LogLinesPerMin); ok && b >= 1 && (w >= 3*b || w <= b/3) {
		out = append(out, fmt.Sprintf("log lines %.1f/min (was %.1f/min)", w, b))
	}
	if w, b, ok := v(o.CPU); ok && w >= 2*b && w-b >= 10 {
		out = append(out, fmt.Sprintf("CPU utilization %.1f (was %.1f)", w, b))
	}
	if o.MemSlopeMiBMin != nil && *o.MemSlopeMiBMin >= 1 {
		s := fmt.Sprintf("memory rising %.1f MiB/min", *o.MemSlopeMiBMin)
		if o.MemoryLimitPct != nil {
			s += fmt.Sprintf(" (%.0f%% of limit)", *o.MemoryLimitPct)
		}
		out = append(out, s)
	}
	return out
}

// ---- /v1/anomalies ----

type Anomaly struct {
	ID       string            `json:"id"`
	Service  string            `json:"service"`
	Rule     string            `json:"rule"`
	Series   map[string]string `json:"series,omitempty"`
	OpenedAt time.Time         `json:"opened_at"`
	State    string            `json:"state"` // open | resolved, as of until
	Window   string            `json:"window"`
	Observed float64           `json:"observed"`
	Expected float64           `json:"expected"`
	Log10P   float64           `json:"log10_p"`
	Detail   string            `json:"detail,omitempty"`
	Template string            `json:"template,omitempty"`
}

func (a *API) anomalies(ctx context.Context, q *req) (any, error) {
	type row struct {
		ID       string            `json:"id"`
		Service  string            `json:"service"`
		Rule     string            `json:"rule"`
		Series   map[string]string `json:"series"`
		Opened   int64             `json:"opened"`
		Last     string            `json:"last"`
		Window   string            `json:"ev_window"`
		Observed float64           `json:"observed"`
		Expected float64           `json:"expected"`
		Log10P   float64           `json:"log10_p"`
		Detail   string            `json:"detail"`
		Template string            `json:"template"`
	}
	// As of until: events detected after it don't exist yet.
	rows, err := Rows[row](ctx, a.cfg.CH, `
SELECT id, any(service) AS service, any(rule) AS rule, any(series) AS series,
       toUnixTimestamp64Milli(min(opened_at)) AS opened, argMax(event, detected_at) AS last,
       argMax(ev_window, detected_at) AS ev_window, argMax(observed, detected_at) AS observed,
       argMax(expected, detected_at) AS expected, argMin(log10_p, log10_p) AS log10_p,
       argMax(detail, detected_at) AS detail, any(template) AS template
FROM `+a.table("anomalies")+`
WHERE detected_at <= {until:DateTime64(3, 'UTC')} AND opened_at > {lo:DateTime64(3, 'UTC')}
GROUP BY id
ORDER BY opened, id
LIMIT 200`, map[string]string{"lo": ts(q.winStart()), "until": ts(q.until)})
	if err != nil {
		return nil, err
	}
	out := []Anomaly{}
	for _, r := range rows {
		name := q.names.Show(r.Service)
		if name == "" {
			continue
		}
		an := Anomaly{ID: r.ID, Service: name, Rule: r.Rule, Series: r.Series, OpenedAt: time.UnixMilli(r.Opened).UTC(),
			State: "open", Window: r.Window, Observed: r.Observed, Expected: r.Expected, Log10P: r.Log10P, Detail: r.Detail, Template: r.Template}
		if r.Last == "resolved" {
			an.State = "resolved"
		}
		if a.mask.Text(an.Template) {
			an.Template = "[masked: names the fault injection]"
			q.masked++
		}
		for k, v := range an.Series {
			if a.mask.Text(v) {
				an.Series[k] = "[masked]"
			}
		}
		out = append(out, an)
	}
	return out, nil
}

// ---- /v1/spans ----

type SpanRow struct {
	Kind        string `json:"kind"`
	Span        string `json:"span"`
	Callee      string `json:"callee,omitempty"` // for client spans: the service that serves this operation
	CallsPerMin Stat   `json:"calls_per_min"`
	ErrorRatio  Stat   `json:"error_ratio"`
	P95Ms       Stat   `json:"p95_ms"`
}

func (a *API) spans(ctx context.Context, q *req) (any, error) {
	_, owners, err := q.service()
	if err != nil {
		return nil, err
	}
	aggs, err := a.spanAggs(ctx, q, owners, []string{"SPAN_KIND_SERVER", "SPAN_KIND_CLIENT", "SPAN_KIND_CONSUMER", "SPAN_KIND_PRODUCER"})
	if err != nil {
		return nil, err
	}
	// Who serves what: SERVER span names of every service in the range.
	type srv struct {
		Service string `json:"service"`
		Span    string `json:"span"`
	}
	servers, err := Rows[srv](ctx, a.cfg.CH, `
SELECT DISTINCT service, attributes['span.name'] AS span FROM `+a.table("metrics")+`
WHERE metric = 'traces.span.metrics.calls' AND attributes['span.kind'] = 'SPAN_KIND_SERVER'
  AND ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')}`,
		map[string]string{"lo": ts(q.baseStart()), "until": ts(q.until)})
	if err != nil {
		return nil, err
	}
	serves := map[string][]string{}
	for _, s := range servers {
		if n := q.names.Show(s.Service); n != "" && !slices.Contains(owners, s.Service) && !slices.Contains(serves[s.Span], n) {
			serves[s.Span] = append(serves[s.Span], n)
		}
	}
	var out []SpanRow
	for k, g := range aggs {
		row := SpanRow{Kind: strings.TrimPrefix(k[1], "SPAN_KIND_"), Span: k[2],
			CallsPerMin: Stat{num(perMin(g.calls[1], q.window)), num(perMin(g.calls[0], q.baseline))},
			ErrorRatio:  Stat{num(ratio(g.errors[1], g.calls[1])), num(ratio(g.errors[0], g.calls[0]))},
			P95Ms:       Stat{num(Quantile(0.95, g.bounds, g.hist[1])), num(Quantile(0.95, g.bounds, g.hist[0]))}}
		if row.Kind == "CLIENT" {
			row.Callee = strings.Join(serves[k[2]], ",")
		}
		out = append(out, row)
	}
	// Most changed first: error ratio rise, then latency ratio, then name.
	score := func(r SpanRow) float64 {
		s := 0.0
		if r.ErrorRatio.Window != nil {
			b := 0.0
			if r.ErrorRatio.Baseline != nil {
				b = *r.ErrorRatio.Baseline
			}
			s += 10 * (*r.ErrorRatio.Window - b)
		}
		if r.P95Ms.Window != nil && r.P95Ms.Baseline != nil && *r.P95Ms.Baseline > 0 {
			s += math.Log2(math.Max(*r.P95Ms.Window / *r.P95Ms.Baseline, 1))
		}
		return s
	}
	sort.Slice(out, func(i, j int) bool {
		si, sj := score(out[i]), score(out[j])
		if si != sj {
			return si > sj
		}
		return out[i].Kind+out[i].Span < out[j].Kind+out[j].Span
	})
	if len(out) > 40 {
		out = out[:40]
	}
	return out, nil
}

// ---- /v1/timeseries ----

type Point struct {
	T time.Time `json:"t"`
	V *float64  `json:"v"`
}

type Series struct {
	Service string  `json:"service"`
	Signal  string  `json:"signal"`
	Unit    string  `json:"unit"`
	Step    string  `json:"step"`
	Points  []Point `json:"points"`
}

var signals = map[string]string{
	"calls": "server calls per step", "errors": "server errors per step", "error_ratio": "ratio",
	"p95_ms": "ms", "cpu": "container cpu utilization", "memory_mib": "MiB",
	"log_lines": "lines per step (masked excluded)", "log_errors": "warn/error lines per step (masked excluded)",
}

func (a *API) timeseries(ctx context.Context, q *req) (any, error) {
	name, owners, err := q.service()
	if err != nil {
		return nil, err
	}
	sig := q.r.URL.Query().Get("signal")
	if _, ok := signals[sig]; !ok {
		keys := make([]string, 0, len(signals))
		for k := range signals {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		return nil, bad("signal must be one of %s", strings.Join(keys, ", "))
	}
	step := time.Minute
	if s := q.r.URL.Query().Get("step"); s != "" {
		if step, err = time.ParseDuration(s); err != nil || step < 5*time.Second {
			return nil, bad("step: a duration of at least 5s")
		}
	}
	step = max(step, (q.window / 120).Round(time.Second)) // at most 120 points
	span := q.r.URL.Query().Get("span")
	stepS := strconv.Itoa(int(step / time.Second))
	params := metricsql.Params("traces.span.metrics.calls", q.winStart(), q.until, owners)
	params["step"] = stepS
	params["span"] = span
	params["mask_terms"] = stringArray(a.mask.Terms())
	params["lo"] = ts(q.winStart())
	params["until"] = ts(q.until)
	params["owners"] = stringArray(owners)
	bucket := `toStartOfInterval(ts, toIntervalSecond({step:UInt32}), 'UTC')`
	spanFilter := `({span:String} = '' OR attributes['span.name'] = {span:String}) AND attributes['span.kind'] = 'SPAN_KIND_SERVER'`
	type pt struct {
		T int64     `json:"t"`
		V *float64  `json:"v"`
		B []uint64  `json:"b"`
		X []float64 `json:"x"`
	}
	var sql string
	switch sig {
	case "calls", "errors", "error_ratio":
		val := map[string]string{"calls": "sum(delta)", "errors": "sumIf(delta, attributes['status.code'] = 'STATUS_CODE_ERROR')",
			"error_ratio": "if(sum(delta) = 0, NULL, sumIf(delta, attributes['status.code'] = 'STATUS_CODE_ERROR') / sum(delta))"}[sig]
		sql = `SELECT toUnixTimestamp64Milli(toDateTime64(` + bucket + `, 3)) AS t, ` + val + ` AS v
FROM (` + metricsql.SumDeltas(a.table("metrics")) + `) WHERE ` + spanFilter + ` GROUP BY t ORDER BY t`
	case "p95_ms":
		params["metric"] = "traces.span.metrics.duration"
		sql = `SELECT toUnixTimestamp64Milli(toDateTime64(` + bucket + `, 3)) AS t, any(bounds) AS x, sumForEach(delta) AS b
FROM (` + metricsql.HistDeltas(a.table("metrics")) + `) WHERE ` + spanFilter + ` GROUP BY t ORDER BY t`
	case "cpu", "memory_mib":
		params["metric"] = map[string]string{"cpu": "container.cpu.utilization", "memory_mib": "container.memory.usage.total"}[sig]
		scale := map[string]string{"cpu": "1", "memory_mib": "1048576"}[sig]
		sql = `SELECT toUnixTimestamp64Milli(toDateTime64(` + bucket + `, 3)) AS t, avg(value) / ` + scale + ` AS v
FROM (` + metricsql.Gauges(a.table("metrics")) + `) GROUP BY t ORDER BY t`
	case "log_lines", "log_errors":
		cond := "NOT " + MaskedSQL
		if sig == "log_errors" {
			cond += " AND " + warnOrError
		}
		sql = `SELECT toUnixTimestamp64Milli(toDateTime64(` + bucket + `, 3)) AS t, toFloat64(count()) AS v
FROM ` + a.table("logs") + `
WHERE has({owners:Array(String)}, service) AND ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')} AND ` + cond + `
GROUP BY t ORDER BY t`
	}
	rows, err := Rows[pt](ctx, a.cfg.CH, sql, params)
	if err != nil {
		return nil, err
	}
	out := Series{Service: name, Signal: sig, Unit: signals[sig], Step: step.String(), Points: []Point{}}
	for _, r := range rows {
		p := Point{T: time.UnixMilli(r.T).UTC(), V: nz(r.V)}
		if sig == "p95_ms" {
			p.V = num(Quantile(0.95, r.X, r.B))
		}
		out.Points = append(out.Points, p)
	}
	return out, nil
}

// ---- /v1/log_templates ----

type Template struct {
	Template     string  `json:"template"`
	Severity     string  `json:"severity"`
	Window       int     `json:"window_count"`
	BaselineRate float64 `json:"baseline_per_window"` // baseline count scaled to the window's length
	NewInWindow  bool    `json:"new_in_window"`
	Example      string  `json:"example"`
}

type Templates struct {
	Service   string     `json:"service"`
	LinesRead int        `json:"lines_read"`
	Truncated bool       `json:"truncated"`
	Templates []Template `json:"templates"`
}

func (a *API) logTemplates(ctx context.Context, q *req) (any, error) {
	name, owners, err := q.service()
	if err != nil {
		return nil, err
	}
	type line struct {
		T    int64  `json:"t"`
		Body string `json:"body"`
		Sev  string `json:"sev"`
	}
	lines, err := Rows[line](ctx, a.cfg.CH, `
SELECT toUnixTimestamp64Milli(ts) AS t, body,
       multiIf(severity_number >= 17, 'error', severity_number >= 13, 'warn', severity_number > 0, 'info',
               lower(severity_text) IN ('error','err','fatal','critical'), 'error', lower(severity_text) IN ('warn','warning'), 'warn', 'info') AS sev
FROM `+a.table("logs")+`
WHERE has({owners:Array(String)}, service) AND ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')} AND NOT `+MaskedSQL+`
ORDER BY ts DESC
LIMIT {cap:UInt32}`, map[string]string{"owners": stringArray(owners), "lo": ts(q.baseStart()), "until": ts(q.until),
		"mask_terms": stringArray(a.mask.Terms()), "cap": strconv.Itoa(a.cfg.MaxLogRows)})
	if err != nil {
		return nil, err
	}
	if q.masked, err = a.countMasked(ctx, owners, q.baseStart(), q.until); err != nil {
		return nil, err
	}
	// Oldest first, so templates form the way the detector's do.
	slices.Reverse(lines)
	p := drain.New(drain.DefaultConfig())
	type acc struct {
		c       *drain.Cluster
		win     int
		base    int
		sev     string
		example string
		first   int64
	}
	accs := map[*drain.Cluster]*acc{}
	win := q.winStart().UnixMilli()
	for _, l := range lines {
		text := l.Body
		if text == "" {
			text = "<empty body>"
		}
		c, _ := p.Add(text)
		x := accs[c]
		if x == nil {
			x = &acc{c: c, sev: l.Sev, first: l.T}
			accs[c] = x
		}
		if l.T > win {
			x.win++
			x.example = l.Body
		} else {
			x.base++
		}
		if l.Sev == "error" || (l.Sev == "warn" && x.sev == "info") {
			x.sev = l.Sev
		}
	}
	scale := 0.0
	if q.baseline > 0 {
		scale = q.window.Seconds() / q.baseline.Seconds()
	}
	var out []Template
	for _, x := range accs {
		ex := x.example
		if len(ex) > 200 {
			ex = ex[:200] + "…"
		}
		out = append(out, Template{Template: truncate(x.c.Template(), 200), Severity: x.sev, Window: x.win,
			BaselineRate: math.Round(float64(x.base)*scale*10) / 10, NewInWindow: x.base == 0 && x.win > 0, Example: ex})
	}
	// What moved: window count over its baseline expectation, new ones first.
	sort.Slice(out, func(i, j int) bool {
		di := float64(out[i].Window) - out[i].BaselineRate
		dj := float64(out[j].Window) - out[j].BaselineRate
		if di != dj {
			return di > dj
		}
		return out[i].Template < out[j].Template
	})
	if len(out) > 15 {
		out = out[:15]
	}
	return Templates{Service: name, LinesRead: len(lines), Truncated: len(lines) >= a.cfg.MaxLogRows, Templates: out}, nil
}

func (a *API) countMasked(ctx context.Context, owners []string, lo, until time.Time) (int64, error) {
	type row struct {
		N int64 `json:"n"`
	}
	r, err := Rows[row](ctx, a.cfg.CH, `SELECT count() AS n FROM `+a.table("logs")+`
WHERE has({owners:Array(String)}, service) AND ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')} AND `+MaskedSQL,
		map[string]string{"owners": stringArray(owners), "lo": ts(lo), "until": ts(until), "mask_terms": stringArray(a.mask.Terms())})
	if err != nil || len(r) == 0 {
		return 0, err
	}
	return r[0].N, nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// ---- /v1/logs ----

type LogLine struct {
	T          time.Time         `json:"t"`
	Severity   string            `json:"severity"`
	Body       string            `json:"body"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

func (a *API) logs(ctx context.Context, q *req) (any, error) {
	_, owners, err := q.service()
	if err != nil {
		return nil, err
	}
	v := q.r.URL.Query()
	limit := 20
	if s := v.Get("limit"); s != "" {
		if limit, err = strconv.Atoi(s); err != nil || limit < 1 || limit > 50 {
			return nil, bad("limit: 1..50")
		}
	}
	minSev := 0
	switch v.Get("min_severity") {
	case "", "info":
	case "warn":
		minSev = 13
	case "error":
		minSev = 17
	default:
		return nil, bad("min_severity: info, warn or error")
	}
	type row struct {
		T     int64             `json:"t"`
		Sev   string            `json:"sev"`
		Body  string            `json:"body"`
		Attrs map[string]string `json:"attrs"`
	}
	rows, err := Rows[row](ctx, a.cfg.CH, `
SELECT toUnixTimestamp64Milli(ts) AS t, if(severity_text != '', severity_text, toString(severity_number)) AS sev, body,
       mapFilter((k, x) -> k NOT LIKE '%stacktrace%', log_attributes) AS attrs
FROM `+a.table("logs")+`
WHERE has({owners:Array(String)}, service) AND ts > {lo:DateTime64(9, 'UTC')} AND ts <= {until:DateTime64(9, 'UTC')}
  AND ({contains:String} = '' OR positionCaseInsensitiveUTF8(body, {contains:String}) > 0)
  AND (severity_number >= {sev:UInt8} OR ({sev:UInt8} = 0))
  AND NOT `+MaskedSQL+`
ORDER BY ts DESC
LIMIT {limit:UInt32}`, map[string]string{"owners": stringArray(owners), "lo": ts(q.winStart()), "until": ts(q.until),
		"contains": v.Get("contains"), "sev": strconv.Itoa(minSev), "limit": strconv.Itoa(limit), "mask_terms": stringArray(a.mask.Terms())})
	if err != nil {
		return nil, err
	}
	if q.masked, err = a.countMasked(ctx, owners, q.winStart(), q.until); err != nil {
		return nil, err
	}
	out := []LogLine{}
	for _, r := range rows {
		for k, x := range r.Attrs {
			r.Attrs[k] = truncate(x, 200)
		}
		out = append(out, LogLine{T: time.UnixMilli(r.T).UTC(), Severity: r.Sev, Body: truncate(r.Body, 300), Attributes: r.Attrs})
	}
	return out, nil
}
