package main

import (
	"cmp"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/dhanush-cn/drishti/internal/kdump"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"
)

// The summary answers, before any tuning: is the recording complete (every
// expected service present, logs and metrics), and did the collector change
// take effect (span metrics every ~5 s, not 60 s)?

type topicSum struct {
	Topic       string    `json:"topic"`
	Records     int64     `json:"records"`
	Partitions  int       `json:"partitions"`
	First, Last time.Time `json:"-"`
	FirstS      string    `json:"first"`
	LastS       string    `json:"last"`
}

type logSum struct {
	Service  string           `json:"service"`
	Messages int64            `json:"messages"`
	Rows     int64            `json:"rows"`
	BySev    map[string]int64 `json:"by_severity"`
}

type metricSum struct {
	Name           string  `json:"name"`
	Type           string  `json:"type"`
	Points         int64   `json:"points"`
	Series         int     `json:"series"`
	Services       int     `json:"services"`
	MedianInterval float64 `json:"median_interval_s"` // median over series of each series' median gap
}

type summary struct {
	File          string      `json:"file"`
	Topics        []topicSum  `json:"topics"`
	Logs          []logSum    `json:"logs_by_service"`
	Metrics       []metricSum `json:"metrics"`
	DecodeErrors  int64       `json:"decode_errors"`
	SpanMetricsS  float64     `json:"span_metrics_interval_s"`
	ContainerMemS float64     `json:"container_memory_interval_s"`
}

func summaryCmd(args []string) error {
	fs := flag.NewFlagSet("summary", flag.ExitOnError)
	jsonOut := fs.String("json", "", "also write the summary as JSON here")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		usage()
	}
	s, err := summarize(fs.Arg(0))
	if err != nil {
		return err
	}
	printSummary(os.Stdout, s)
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(s, "", " ")
		return os.WriteFile(*jsonOut, b, 0o644)
	}
	return nil
}

// resourceKey is the series owner, with the receiver's partitioning fallback
// (service.name, then container.name, then host.name).
func resourceKey(attrs []*commonv1.KeyValue) string {
	var svc, ctr, host string
	for _, kv := range attrs {
		switch kv.GetKey() {
		case "service.name":
			svc = kv.GetValue().GetStringValue()
		case "container.name":
			ctr = kv.GetValue().GetStringValue()
		case "host.name":
			host = kv.GetValue().GetStringValue()
		}
	}
	switch {
	case svc != "":
		return svc
	case ctr != "":
		return "container:" + ctr
	case host != "":
		return "host:" + host
	}
	return "unknown_service"
}

func attrString(attrs []*commonv1.KeyValue) string {
	parts := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		parts = append(parts, kv.GetKey()+"="+kv.GetValue().String())
	}
	slices.Sort(parts)
	return strings.Join(parts, ",")
}

type metricAcc struct {
	typ      string
	points   int64
	services map[string]bool
	times    map[string][]uint64 // series -> data point times
}

func summarize(path string) (*summary, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r, err := kdump.NewReader(f)
	if err != nil {
		return nil, err
	}
	s := &summary{File: path}
	topics := map[string]*topicSum{}
	parts := map[string]map[int32]bool{}
	logs := map[string]*logSum{}
	mets := map[string]*metricAcc{}
	var lreq collogs.ExportLogsServiceRequest
	var mreq colmetrics.ExportMetricsServiceRequest
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		ts := topics[rec.Topic]
		if ts == nil {
			ts = &topicSum{Topic: rec.Topic, First: rec.Timestamp}
			topics[rec.Topic] = ts
			parts[rec.Topic] = map[int32]bool{}
		}
		ts.Records++
		parts[rec.Topic][rec.Partition] = true
		if rec.Timestamp.Before(ts.First) {
			ts.First = rec.Timestamp
		}
		if rec.Timestamp.After(ts.Last) {
			ts.Last = rec.Timestamp
		}
		switch {
		case strings.Contains(rec.Topic, "logs"):
			proto.Reset(&lreq)
			if proto.Unmarshal(rec.Value, &lreq) != nil {
				s.DecodeErrors++
				continue
			}
			for _, rl := range lreq.GetResourceLogs() {
				key := resourceKey(rl.GetResource().GetAttributes())
				ls := logs[key]
				if ls == nil {
					ls = &logSum{Service: key, BySev: map[string]int64{}}
					logs[key] = ls
				}
				ls.Messages++
				for _, sl := range rl.GetScopeLogs() {
					for _, lr := range sl.GetLogRecords() {
						ls.Rows++
						ls.BySev[sevBucket(lr.GetSeverityNumber())]++
					}
				}
			}
		case strings.Contains(rec.Topic, "metrics"):
			proto.Reset(&mreq)
			if proto.Unmarshal(rec.Value, &mreq) != nil {
				s.DecodeErrors++
				continue
			}
			for _, rm := range mreq.GetResourceMetrics() {
				key := resourceKey(rm.GetResource().GetAttributes())
				for _, sm := range rm.GetScopeMetrics() {
					for _, m := range sm.GetMetrics() {
						acc := mets[m.GetName()]
						if acc == nil {
							acc = &metricAcc{services: map[string]bool{}, times: map[string][]uint64{}}
							mets[m.GetName()] = acc
						}
						acc.services[key] = true
						eachPoint(m, func(typ string, t uint64, attrs []*commonv1.KeyValue) {
							acc.typ = typ
							acc.points++
							sk := key + "|" + attrString(attrs)
							acc.times[sk] = append(acc.times[sk], t)
						})
					}
				}
			}
		}
	}
	for _, t := range topics {
		t.Partitions = len(parts[t.Topic])
		t.FirstS, t.LastS = t.First.UTC().Format(time.RFC3339), t.Last.UTC().Format(time.RFC3339)
		s.Topics = append(s.Topics, *t)
	}
	slices.SortFunc(s.Topics, func(a, b topicSum) int { return cmp.Compare(a.Topic, b.Topic) })
	for _, l := range logs {
		s.Logs = append(s.Logs, *l)
	}
	slices.SortFunc(s.Logs, func(a, b logSum) int { return cmp.Compare(b.Rows, a.Rows) })
	for name, a := range mets {
		var meds []float64
		for _, ts := range a.times {
			if g := medianGap(ts); g > 0 {
				meds = append(meds, g)
			}
		}
		ms := metricSum{Name: name, Type: a.typ, Points: a.points, Series: len(a.times), Services: len(a.services), MedianInterval: median(meds)}
		s.Metrics = append(s.Metrics, ms)
		switch name {
		case "traces.span.metrics.calls":
			s.SpanMetricsS = ms.MedianInterval
		case "container.memory.usage.total":
			s.ContainerMemS = ms.MedianInterval
		}
	}
	slices.SortFunc(s.Metrics, func(a, b metricSum) int { return cmp.Compare(b.Points, a.Points) })
	return s, nil
}

func eachPoint(m *metricsv1.Metric, fn func(typ string, t uint64, attrs []*commonv1.KeyValue)) {
	switch d := m.GetData().(type) {
	case *metricsv1.Metric_Gauge:
		for _, p := range d.Gauge.GetDataPoints() {
			fn("gauge", p.GetTimeUnixNano(), p.GetAttributes())
		}
	case *metricsv1.Metric_Sum:
		for _, p := range d.Sum.GetDataPoints() {
			fn("sum", p.GetTimeUnixNano(), p.GetAttributes())
		}
	case *metricsv1.Metric_Histogram:
		for _, p := range d.Histogram.GetDataPoints() {
			fn("histogram", p.GetTimeUnixNano(), p.GetAttributes())
		}
	case *metricsv1.Metric_ExponentialHistogram:
		for _, p := range d.ExponentialHistogram.GetDataPoints() {
			fn("exp_histogram", p.GetTimeUnixNano(), p.GetAttributes())
		}
	case *metricsv1.Metric_Summary:
		for _, p := range d.Summary.GetDataPoints() {
			fn("summary", p.GetTimeUnixNano(), p.GetAttributes())
		}
	}
}

// medianGap is the median spacing, in seconds, between a series' data points
// (repeated timestamps, e.g. a resent batch, are ignored).
func medianGap(ts []uint64) float64 {
	if len(ts) < 2 {
		return 0
	}
	sorted := slices.Clone(ts)
	slices.Sort(sorted)
	var gaps []float64
	for i := 1; i < len(sorted); i++ {
		if d := sorted[i] - sorted[i-1]; d > 0 {
			gaps = append(gaps, float64(d)/1e9)
		}
	}
	return median(gaps)
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Float64s(xs)
	m := len(xs) / 2
	if len(xs)%2 == 1 {
		return xs[m]
	}
	return (xs[m-1] + xs[m]) / 2
}

func sevBucket(n logsv1.SeverityNumber) string {
	switch {
	case n == 0:
		return "unset"
	case n < 9:
		return "trace_debug"
	case n < 13:
		return "info"
	case n < 17:
		return "warn"
	default:
		return "error_fatal"
	}
}

func printSummary(w io.Writer, s *summary) {
	fmt.Fprintf(w, "## %s\n\n", s.File)
	fmt.Fprintln(w, "| Topic | Records | Partitions | First | Last |\n|---|---|---|---|---|")
	for _, t := range s.Topics {
		fmt.Fprintf(w, "| %s | %d | %d | %s | %s |\n", t.Topic, t.Records, t.Partitions, t.FirstS, t.LastS)
	}
	fmt.Fprintf(w, "\nDecode errors: %d\n", s.DecodeErrors)
	fmt.Fprintf(w, "span metrics interval (traces.span.metrics.calls, median per series): %.1f s (expect ~5 with the extras change, ~60 without)\n", s.SpanMetricsS)
	fmt.Fprintf(w, "container memory interval (container.memory.usage.total): %.1f s (docker_stats default 10)\n\n", s.ContainerMemS)
	fmt.Fprintln(w, "| Service (logs) | Messages | Rows | trace/debug | info | warn | error+ | unset |\n|---|---|---|---|---|---|---|---|")
	for _, l := range s.Logs {
		fmt.Fprintf(w, "| %s | %d | %d | %d | %d | %d | %d | %d |\n", l.Service, l.Messages, l.Rows,
			l.BySev["trace_debug"], l.BySev["info"], l.BySev["warn"], l.BySev["error_fatal"], l.BySev["unset"])
	}
	fmt.Fprintln(w, "\n| Metric (top 60 by points) | Type | Points | Series | Services | Median interval s |\n|---|---|---|---|---|---|")
	for i, m := range s.Metrics {
		if i == 60 {
			break
		}
		fmt.Fprintf(w, "| %s | %s | %d | %d | %d | %.1f |\n", m.Name, m.Type, m.Points, m.Series, m.Services, m.MedianInterval)
	}
}
