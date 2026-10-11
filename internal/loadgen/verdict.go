package loadgen

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Thresholds decide PASS/FAIL (docs/loadgen.md §10). Their values are still
// open (§13): until each one is chosen and justified, run.json marks them
// provisional and no number from the run may be quoted.
type Thresholds struct {
	Tolerance   float64       `json:"achieved_tolerance"` // achieved >= offered*(1-Tolerance)
	MaxLag      time.Duration `json:"max_schedule_lag_ns"`
	P99         time.Duration `json:"p99_bound_ns"`
	Provisional bool          `json:"provisional"`
}

// Verdict is computed by the tool, never by hand (§9).
type Verdict struct {
	Pass    bool     `json:"pass"`
	Reasons []string `json:"reasons"` // every condition that broke; empty on PASS
}

// Summary is the steady-state headline of a run.
type Summary struct {
	OfferedRecordsPerSec  float64 `json:"offered_records_per_sec"`
	AchievedRecordsPerSec float64 `json:"achieved_records_per_sec"` // ingested (acknowledged) records / steady duration
	P50us                 int64   `json:"p50_us"`
	P99us                 int64   `json:"p99_us"`
	P999us                int64   `json:"p999_us"`
	MaxUS                 int64   `json:"max_us"`
	Samples               int64   `json:"latency_samples"`
}

// Summarize computes the steady-state summary. Percentiles come from the one
// merged steady histogram, never from averaging per-interval percentiles.
func Summarize(r *Result, cfg Config) Summary {
	h := r.Steady.Hist
	return Summary{
		OfferedRecordsPerSec:  cfg.Rate,
		AchievedRecordsPerSec: achieved(r, cfg),
		P50us:                 h.ValueAtQuantile(50),
		P99us:                 h.ValueAtQuantile(99),
		P999us:                h.ValueAtQuantile(99.9),
		MaxUS:                 h.Max(),
		Samples:               h.TotalCount(),
	}
}

// Judge applies every §10 condition to the steady-state window and lists
// each one that broke, so a FAIL says which bottleneck to look at first.
func Judge(r *Result, cfg Config, s Summary, th Thresholds) Verdict {
	w := r.Steady
	var reasons []string
	// Judged on counts, not rates: acknowledged records vs records actually
	// offered in the window.
	if min := float64(w.OfferedRecords) * (1 - th.Tolerance); float64(w.IngestedRecords) < min {
		reasons = append(reasons, fmt.Sprintf("achieved=%.0f/s < offered=%.0f/s (ingested %d of %d records, tolerance %.1f%%)",
			s.AchievedRecordsPerSec, s.OfferedRecordsPerSec, w.IngestedRecords, w.OfferedRecords, th.Tolerance*100))
	}
	if len(w.NonOK) > 0 {
		codes := make([]string, 0, len(w.NonOK))
		for c := range w.NonOK {
			codes = append(codes, c)
		}
		sort.Strings(codes)
		parts := make([]string, len(codes))
		for i, c := range codes {
			parts[i] = fmt.Sprintf("%s=%d", c, w.NonOK[c])
		}
		// Any code fails, broken down by code: a new failure mode can never
		// pass silently (§10).
		reasons = append(reasons, "non_ok: "+strings.Join(parts, " "))
	}
	if w.Saturated > 0 {
		reasons = append(reasons, fmt.Sprintf("saturated=%d", w.Saturated))
	}
	if w.Unresolved > 0 {
		reasons = append(reasons, fmt.Sprintf("unresolved=%d", w.Unresolved))
	}
	if r.MaxScheduleLag > th.MaxLag {
		reasons = append(reasons, fmt.Sprintf("schedule_lag_max=%s", r.MaxScheduleLag.Round(time.Millisecond)))
	}
	if p99 := time.Duration(s.P99us) * time.Microsecond; p99 > th.P99 {
		reasons = append(reasons, fmt.Sprintf("p99=%s > bound %s", p99, th.P99))
	}
	if w.Scheduled == 0 {
		reasons = append(reasons, "no steady-state requests")
	}
	if reasons == nil {
		reasons = []string{}
	}
	return Verdict{Pass: len(reasons) == 0, Reasons: reasons}
}

// achieved is the offered rate scaled by the fraction of offered records the
// receiver acknowledged. Dividing ingested records by the nominal window
// would be wrong: the window holds a whole number of requests (e.g. 311 of
// a nominal 312.5 at 39.06 req/s), so a perfect run would report ~0.5% short
// and, at low request rates, could fail the tolerance on rounding alone.
// Found in the first end-to-end run.
func achieved(r *Result, cfg Config) float64 {
	if r.Steady.OfferedRecords == 0 {
		return 0
	}
	return cfg.Rate * float64(r.Steady.IngestedRecords) / float64(r.Steady.OfferedRecords)
}
