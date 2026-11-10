// Package metricsql computes metric deltas at query time from
// drishti.metrics, where points are stored as received (docs/storage.md,
// decision 10).
//
// The rules are the detector's (internal/otlpmetrics.Deltas), so a number
// the agent reads from ClickHouse means the same as the number an alert
// was raised on:
//
//   - The first point of a series yields nothing: its running total covers
//     everything since the series started, possibly hours.
//   - Cumulative sum: delta = value − previous value, unless the start time
//     changed or the value went down (a counter reset), then delta = value
//     (everything since the reset), as Prometheus treats counter resets.
//   - Cumulative histogram: bucket arrays subtracted element-wise; a reset
//     (start time changed, or any bucket went down) yields the new counts; a
//     change of bucket bounds yields nothing (old and new buckets can't be
//     subtracted).
//   - Delta temporality passes through.
//
// The previous point is the series' previous row in ts order: a window
// function over (series_id ORDER BY ts), which the table's sort key keeps
// sequential. Queries read Lookback before `from` so the first point inside
// the window still has its predecessor.
//
// Every query takes ClickHouse query parameters, never interpolated values:
// the metric name and services come from the agent (an LLM), which makes
// them untrusted input.
package metricsql

import (
	"fmt"
	"time"
)

// Lookback is read before `from` so the window's first point has a previous
// point. Span metrics flush every 5 s and docker_stats every 10 s; host
// metrics every 60 s, so 3 minutes covers every scraper with margin.
const Lookback = 3 * time.Minute

// Params are the query parameters every query here takes.
//
//	metric   String
//	from     DateTime64(9, 'UTC')  window start (exclusive)
//	until    DateTime64(9, 'UTC')  window end (inclusive); nothing after it is read
//
// The times are typed with an explicit UTC zone: a bare DateTime64 parameter
// is read in the server's time zone, and a server not on UTC shifted every
// window by its offset (found by the test below on an Asia/Calcutta server:
// every window came back empty).
//
//	lookback UInt32         seconds before from (Lookback)
//	services Array(String)  owner keys to keep; empty = all
func Params(metric string, from, until time.Time, services []string) map[string]string {
	return map[string]string{
		"metric":   metric,
		"from":     ts(from),
		"until":    ts(until),
		"lookback": fmt.Sprint(int(Lookback / time.Second)),
		"services": stringArray(services),
	}
}

func ts(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000000") }

func stringArray(xs []string) string {
	b := []byte{'['}
	for i, x := range xs {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, '\'')
		for _, c := range []byte(x) {
			if c == '\\' || c == '\'' {
				b = append(b, '\\')
			}
			b = append(b, c)
		}
		b = append(b, '\'')
	}
	return string(append(b, ']'))
}

const where = `metric = {metric:String}
      AND ts > {from:DateTime64(9, 'UTC')} - toIntervalSecond({lookback:UInt32}) AND ts <= {until:DateTime64(9, 'UTC')}
      AND (empty({services:Array(String)}) OR has({services:Array(String)}, service))`

// SumDeltas returns one row per point of a monotonic sum in (from, until]:
// service, instance, attributes, ts, delta. table is e.g. drishti.metrics.
func SumDeltas(table string) string {
	return `SELECT service, instance, attributes, ts,
       if(temporality = 'delta', value, if(start_ts != prev_start OR value < prev_value, value, value - prev_value)) AS delta
FROM (
    SELECT service, instance, attributes, ts, start_ts, value, temporality,
           lagInFrame(value) OVER w AS prev_value,
           lagInFrame(start_ts) OVER w AS prev_start,
           row_number() OVER w AS n
    FROM ` + table + `
    WHERE ` + where + `
      AND kind = 'sum' AND monotonic
    WINDOW w AS (PARTITION BY series_id ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
)
WHERE ts > {from:DateTime64(9, 'UTC')} AND (temporality = 'delta' OR n > 1)`
}

// HistDeltas returns one row per histogram point in (from, until]: service,
// instance, attributes, ts, bounds, delta (per-bucket increase).
//
// arrayResize keeps the lambdas' arrays the same length on rows where the
// layout changed. Those rows are dropped by the outer WHERE, but nothing
// guarantees the expression is never evaluated on them first, and unequal
// arrays make arrayMap throw.
func HistDeltas(table string) string {
	return `SELECT service, instance, attributes, ts, bounds,
       if(temporality = 'delta' OR start_ts != prev_start OR arrayExists((c, p) -> c < p, buckets, arrayResize(prev_buckets, length(buckets))),
          buckets, arrayMap((c, p) -> c - p, buckets, arrayResize(prev_buckets, length(buckets)))) AS delta
FROM (
    SELECT service, instance, attributes, ts, start_ts, bounds, buckets, temporality,
           lagInFrame(buckets) OVER w AS prev_buckets,
           lagInFrame(bounds) OVER w AS prev_bounds,
           lagInFrame(start_ts) OVER w AS prev_start,
           row_number() OVER w AS n
    FROM ` + table + `
    WHERE ` + where + `
      AND kind = 'histogram'
    WINDOW w AS (PARTITION BY series_id ORDER BY ts ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
)
WHERE ts > {from:DateTime64(9, 'UTC')}
  AND (temporality = 'delta' OR (n > 1 AND bounds = prev_bounds AND length(buckets) = length(prev_buckets)))`
}

// Gauges returns one row per gauge point (and per non-monotonic sum point,
// which is a level such as memory usage) in (from, until]: service,
// instance, attributes, ts, value.
func Gauges(table string) string {
	return `SELECT service, instance, attributes, ts, value
FROM ` + table + `
WHERE ` + where + `
  AND ts > {from:DateTime64(9, 'UTC')}
  AND (kind = 'gauge' OR (kind = 'sum' AND NOT monotonic))`
}
