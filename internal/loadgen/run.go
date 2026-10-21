package loadgen

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Exporter sends one request and returns how many records the receiver
// rejected via OTLP partial success. The gRPC implementation lives in
// cmd/loadgen; tests use fakes.
type Exporter func(ctx context.Context, req *collogs.ExportLogsServiceRequest) (rejected int64, err error)

// Histogram range: 1 µs to 60 s, 3 significant digits (fixed memory, exact
// enough at p99.9). Values are microseconds.
const (
	histMinUS  = 1
	histMaxUS  = 60_000_000
	histSigFig = 3
)

func newHist() *hdrhistogram.Histogram { return hdrhistogram.New(histMinUS, histMaxUS, histSigFig) }

// Config is one open-loop run at one offered rate.
type Config struct {
	Rate     float64       // offered log records per second
	Warmup   time.Duration // requests due in [0, Warmup) are warm-up (§4)
	Duration time.Duration // steady-state window after warm-up
	Timeout  time.Duration // client-side deadline per RPC (§4), collector default 5s
	Inflight int           // max concurrent RPCs; beyond it a request is saturated (§3)

	// Optional burst (§14): records/s during [BurstAt, BurstAt+BurstFor)
	// from the start of the run. It must lie inside the steady window.
	BurstRate         float64
	BurstAt, BurstFor time.Duration
}

// Window holds the counts for requests whose due time fell in one window.
type Window struct {
	Scheduled       int64                   `json:"scheduled_requests"`
	Saturated       int64                   `json:"saturated_requests"`
	OK              int64                   `json:"ok_requests"`
	OfferedRecords  int64                   `json:"offered_records"`
	IngestedRecords int64                   `json:"ingested_records"` // OK minus partial-success rejections (§1)
	RejectedRecords int64                   `json:"rejected_records"`
	NonOK           map[string]int64        `json:"non_ok_by_code"`
	Unresolved      int64                   `json:"unresolved_requests"`
	Hist            *hdrhistogram.Histogram `json:"-"`
}

func newWindow() *Window { return &Window{NonOK: map[string]int64{}, Hist: newHist()} }

// Result is everything a run measured.
type Result struct {
	Start          time.Time     `json:"start"`
	RequestRate    float64       `json:"request_rate"`
	RecordsPerReq  int64         `json:"records_per_request"`
	SentRequests   int           `json:"sent_requests"`
	MaxScheduleLag time.Duration `json:"max_schedule_lag_ns"`
	Warmup         *Window       `json:"warmup"`
	Steady         *Window       `json:"steady"`
	Burst          *Window       `json:"burst,omitempty"` // burst requests; also counted in Steady
	DrainTime      time.Duration `json:"drain_ns"`
}

// Run executes one open-loop run. Every request in pool must carry the same
// number of records (LogPool guarantees it), so offered records per second
// converts exactly to requests per second.
func Run(ctx context.Context, clk Clock, cfg Config, pool []*collogs.ExportLogsServiceRequest, export Exporter) (*Result, error) {
	if len(pool) == 0 {
		return nil, fmt.Errorf("empty pool")
	}
	perReq := CountLogRecords(pool[0])
	for _, r := range pool {
		if CountLogRecords(r) != perReq {
			return nil, fmt.Errorf("pool requests differ in record count")
		}
	}
	if cfg.Rate <= 0 || cfg.Duration <= 0 || cfg.Timeout <= 0 || cfg.Inflight < 1 {
		return nil, fmt.Errorf("rate, duration, timeout must be > 0 and inflight >= 1")
	}
	reqRate := cfg.Rate / float64(perReq)
	sched := Schedule{Start: clk.Now(), Rate: reqRate}
	res := &Result{RequestRate: reqRate, RecordsPerReq: perReq, Warmup: newWindow(), Steady: newWindow()}
	if cfg.BurstRate > 0 {
		if cfg.BurstFor <= 0 || cfg.BurstAt < cfg.Warmup || cfg.BurstAt+cfg.BurstFor > cfg.Warmup+cfg.Duration {
			return nil, fmt.Errorf("burst [%v, %v) must lie inside the steady window [%v, %v)", cfg.BurstAt, cfg.BurstAt+cfg.BurstFor, cfg.Warmup, cfg.Warmup+cfg.Duration)
		}
		sched.Burst = &Burst{At: cfg.BurstAt, For: cfg.BurstFor, Rate: cfg.BurstRate / float64(perReq)}
		res.Burst = newWindow()
	}
	total := sched.Requests(cfg.Warmup + cfg.Duration)
	if total < 1 {
		return nil, fmt.Errorf("run too short: %d requests at %.3f req/s", total, reqRate)
	}
	sched.Total = total
	res.Start = sched.Start
	steadyFrom := sched.Start.Add(cfg.Warmup)

	var (
		mu    sync.Mutex // guards windows and histograms (HdrHistogram is not goroutine-safe)
		wg    sync.WaitGroup
		slots = make(chan struct{}, cfg.Inflight)
	)
	// windows: the request's window, plus the burst window if it fell due
	// during the burst (burst requests count in Steady too: the verdict
	// judges the whole steady window, burst included).
	windows := func(due time.Time) []*Window {
		if due.Before(steadyFrom) {
			return []*Window{res.Warmup}
		}
		if b := sched.Burst; b != nil && !due.Before(sched.Start.Add(b.At)) && due.Before(sched.Start.Add(b.At+b.For)) {
			return []*Window{res.Steady, res.Burst}
		}
		return []*Window{res.Steady}
	}

	send := func(i int, due time.Time) {
		ws := windows(due)
		mu.Lock()
		for _, w := range ws {
			w.Scheduled++
			w.OfferedRecords += perReq
		}
		mu.Unlock()

		// Non-blocking acquire. Waiting for a slot would quietly turn the
		// run closed-loop; a saturated request is an omitted sample, so it
		// is counted and fails the run (§3, §10).
		select {
		case slots <- struct{}{}:
		default:
			mu.Lock()
			for _, w := range ws {
				w.Saturated++
			}
			mu.Unlock()
			return
		}
		wg.Add(1)
		go func(req *collogs.ExportLogsServiceRequest) {
			defer wg.Done()
			defer func() { <-slots }()
			// Background, not ctx: requests already sent must still be
			// waited for after the schedule ends (the drain, §4). The
			// client deadline bounds each one.
			callCtx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
			rejected, err := export(callCtx, req)
			cancel()
			done := clk.Now()
			// Latency from DUE time, not send time (§4): generator lateness
			// counts against the system, as it would for a real app.
			us := done.Sub(due).Microseconds()
			if us < histMinUS {
				us = histMinUS
			}
			if us > histMaxUS {
				us = histMaxUS
			}
			mu.Lock()
			defer mu.Unlock()
			for _, w := range ws {
				_ = w.Hist.RecordValue(us) // plain RecordValue: never the "corrected" form (§4)
				if err != nil {
					w.NonOK[codeOf(err)]++
					continue
				}
				w.OK++
				w.RejectedRecords += rejected
				w.IngestedRecords += perReq - rejected
			}
		}(pool[i%len(pool)])
	}

	res.SentRequests, res.MaxScheduleLag = sched.Run(ctx, clk, send)

	// Drain: every in-flight request resolves within its client deadline.
	// One that somehow does not (an exporter ignoring ctx) is counted as
	// unresolved, which fails the run instead of silently truncating the tail.
	drainStart := clk.Now()
	drained := make(chan struct{})
	go func() { wg.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(cfg.Timeout + 2*time.Second):
		mu.Lock()
		res.Steady.Unresolved = int64(len(slots))
		mu.Unlock()
	}
	res.DrainTime = clk.Now().Sub(drainStart)
	if ctx.Err() != nil {
		return res, ctx.Err()
	}
	return res, nil
}

// codeOf maps an export error to its gRPC code name. A bare context error
// (possible from a fake exporter) is mapped the way grpc-go would map it.
func codeOf(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded.String()
	case errors.Is(err, context.Canceled):
		return codes.Canceled.String()
	}
	return status.Code(err).String()
}
