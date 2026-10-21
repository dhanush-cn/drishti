package loadgen

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func testPool(t *testing.T, batch int) []*collogs.ExportLogsServiceRequest {
	t.Helper()
	p, err := LogPool(LogShape{BatchSize: batch, Services: 4, Resources: 1, BodyMin: 40, BodyMax: 80, AttrsMin: 1, AttrsMax: 3}, 8, "test", 1)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

var generous = Thresholds{Tolerance: 0.01, MaxLag: time.Second, P99: time.Second}

func ok(context.Context, *collogs.ExportLogsServiceRequest) (int64, error) { return 0, nil }

func run(t *testing.T, cfg Config, batch int, ex Exporter) (*Result, Summary, Verdict) {
	t.Helper()
	r, err := Run(context.Background(), RealClock, cfg, testPool(t, batch), ex)
	if err != nil {
		t.Fatal(err)
	}
	s := Summarize(r, cfg)
	return r, s, Judge(r, cfg, s, generous)
}

func TestRun_HealthyReceiverPasses(t *testing.T) {
	cfg := Config{Rate: 2000, Warmup: 100 * time.Millisecond, Duration: 500 * time.Millisecond, Timeout: time.Second, Inflight: 64}
	r, s, v := run(t, cfg, 10, ok) // 200 req/s
	if !v.Pass {
		t.Fatalf("healthy run failed: %v", v.Reasons)
	}
	w := r.Steady
	if w.Scheduled != 100 || w.OK != w.Scheduled {
		t.Fatalf("steady scheduled=%d ok=%d, want 100/100", w.Scheduled, w.OK)
	}
	if w.IngestedRecords != w.OfferedRecords || s.AchievedRecordsPerSec != 2000 {
		t.Fatalf("ingested %d of %d offered, achieved %.0f/s", w.IngestedRecords, w.OfferedRecords, s.AchievedRecordsPerSec)
	}
	if r.Warmup.Scheduled != 20 {
		t.Fatalf("warm-up scheduled %d, want 20 (classified by due time)", r.Warmup.Scheduled)
	}
	if s.Samples != w.Scheduled {
		t.Fatalf("histogram has %d samples for %d requests", s.Samples, w.Scheduled)
	}
}

// Coordinated omission, the reason the generator is open-loop (§2): the
// receiver stalls for 300 ms. Every request DUE during the stall must
// record its wait from its due time. A closed-loop generator would record
// one slow sample; this one must record roughly rate x stall of them.
func TestRun_StallIsSeenByEveryRequestDueDuringIt(t *testing.T) {
	var stallUntil atomic.Int64
	start := time.Now()
	stallFrom := start.Add(200 * time.Millisecond)
	stallUntil.Store(stallFrom.Add(300 * time.Millisecond).UnixNano())
	ex := func(ctx context.Context, _ *collogs.ExportLogsServiceRequest) (int64, error) {
		now := time.Now()
		if end := time.Unix(0, stallUntil.Load()); now.After(stallFrom) && now.Before(end) {
			time.Sleep(time.Until(end))
		}
		return 0, nil
	}
	cfg := Config{Rate: 2000, Warmup: 0, Duration: 800 * time.Millisecond, Timeout: 2 * time.Second, Inflight: 1024}
	r, _, _ := run(t, cfg, 10, ex) // 200 req/s, so ~60 requests are due in the stall
	slow := r.Steady.Hist.TotalCount() - countBelow(r, 100*time.Millisecond)
	if slow < 40 {
		t.Fatalf("only %d samples >= 100ms; the stall was omitted from the histogram", slow)
	}
	if p := time.Duration(r.Steady.Hist.Max()) * time.Microsecond; p < 250*time.Millisecond {
		t.Fatalf("max latency %v; the request due at the stall start waited ~300ms", p)
	}
}

func countBelow(r *Result, d time.Duration) int64 {
	var n int64
	for _, b := range r.Steady.Hist.Distribution() {
		if b.To < d.Microseconds() {
			n += b.Count
		}
	}
	return n
}

// Waiting for a free slot would make the run closed-loop. Saturation must
// be counted, never waited out, and must fail the run (§3, §10).
func TestRun_SaturationIsCountedAndFails(t *testing.T) {
	slow := func(ctx context.Context, _ *collogs.ExportLogsServiceRequest) (int64, error) {
		time.Sleep(50 * time.Millisecond)
		return 0, nil
	}
	cfg := Config{Rate: 2000, Duration: 300 * time.Millisecond, Timeout: time.Second, Inflight: 1}
	r, _, v := run(t, cfg, 10, slow)
	if r.Steady.Saturated == 0 {
		t.Fatal("no saturation counted with 1 slot and 50ms responses at 200 req/s")
	}
	if r.Steady.Saturated+r.Steady.OK != r.Steady.Scheduled {
		t.Fatalf("saturated %d + ok %d != scheduled %d", r.Steady.Saturated, r.Steady.OK, r.Steady.Scheduled)
	}
	if v.Pass || !hasReason(v, "saturated=") {
		t.Fatalf("verdict %+v, want FAIL with saturated", v)
	}
	if r.MaxScheduleLag > 20*time.Millisecond {
		t.Fatalf("schedule lag %v: the generator waited for slots instead of counting saturation", r.MaxScheduleLag)
	}
}

// Every non-OK code fails the run, broken down by code (§10).
func TestRun_AnyNonOKCodeFailsWithBreakdown(t *testing.T) {
	var n atomic.Int64
	ex := func(context.Context, *collogs.ExportLogsServiceRequest) (int64, error) {
		switch n.Add(1) % 10 {
		case 3:
			return 0, status.Error(codes.Unavailable, "kafka slow")
		case 7:
			return 0, status.Error(codes.ResourceExhausted, "too big")
		}
		return 0, nil
	}
	cfg := Config{Rate: 2000, Duration: 300 * time.Millisecond, Timeout: time.Second, Inflight: 64}
	r, _, v := run(t, cfg, 10, ex)
	if r.Steady.NonOK["Unavailable"] == 0 || r.Steady.NonOK["ResourceExhausted"] == 0 {
		t.Fatalf("non-OK by code = %v", r.Steady.NonOK)
	}
	if v.Pass || !hasReason(v, "non_ok: ResourceExhausted=") || !hasReason(v, "Unavailable=") {
		t.Fatalf("verdict %+v, want FAIL listing both codes", v)
	}
}

// The client deadline is the generator's own (§4): a receiver that never
// answers produces DEADLINE_EXCEEDED, counted as a failure, and the drain
// still finishes within the deadline instead of hanging or dropping them.
func TestRun_ClientDeadlineIsDeadlineExceededAndDrainCompletes(t *testing.T) {
	hang := func(ctx context.Context, _ *collogs.ExportLogsServiceRequest) (int64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	cfg := Config{Rate: 1000, Duration: 200 * time.Millisecond, Timeout: 150 * time.Millisecond, Inflight: 1024}
	start := time.Now()
	r, _, v := run(t, cfg, 10, hang)
	if got := r.Steady.NonOK["DeadlineExceeded"]; got != r.Steady.Scheduled {
		t.Fatalf("DeadlineExceeded=%d of %d scheduled", got, r.Steady.Scheduled)
	}
	if r.Steady.Unresolved != 0 {
		t.Fatalf("unresolved=%d; deadline should resolve every request", r.Steady.Unresolved)
	}
	if el := time.Since(start); el > time.Second {
		t.Fatalf("run took %v; drain should end ~timeout after the schedule", el)
	}
	if v.Pass || !hasReason(v, "DeadlineExceeded=") {
		t.Fatalf("verdict %+v", v)
	}
	// Latency of a timed-out request still enters the histogram (from due).
	if r.Steady.Hist.TotalCount() != r.Steady.Scheduled {
		t.Fatal("timed-out requests missing from the histogram")
	}
}

// Ingested = records in OK requests minus partial-success rejections (§1).
func TestRun_PartialSuccessIsSubtracted(t *testing.T) {
	ex := func(context.Context, *collogs.ExportLogsServiceRequest) (int64, error) { return 2, nil }
	cfg := Config{Rate: 2000, Duration: 300 * time.Millisecond, Timeout: time.Second, Inflight: 64}
	r, _, v := run(t, cfg, 10, ex)
	w := r.Steady
	if w.RejectedRecords != 2*w.OK || w.IngestedRecords != 8*w.OK {
		t.Fatalf("ok=%d rejected=%d ingested=%d", w.OK, w.RejectedRecords, w.IngestedRecords)
	}
	if v.Pass || !hasReason(v, "achieved=") {
		t.Fatalf("20%% rejected must fail the achieved check: %+v", v)
	}
}

func TestWrite_HlogRoundTripsPercentiles(t *testing.T) {
	cfg := Config{Rate: 2000, Warmup: 50 * time.Millisecond, Duration: 200 * time.Millisecond, Timeout: time.Second, Inflight: 64}
	r, s, v := run(t, cfg, 10, ok)
	dir := filepath.Join(t.TempDir(), DirName(r.Start, "abcd1234"))
	if err := Write(dir, &Report{RunID: "abcd1234", Result: r, Summary: s, Verdict: v}); err != nil {
		t.Fatal(err)
	}
	h, err := ReadHlog(filepath.Join(dir, "latency.hlog"))
	if err != nil {
		t.Fatal(err)
	}
	if h.TotalCount() != r.Steady.Hist.TotalCount() || h.ValueAtQuantile(99) != r.Steady.Hist.ValueAtQuantile(99) {
		t.Fatalf("decoded histogram differs: count %d vs %d", h.TotalCount(), r.Steady.Hist.TotalCount())
	}
}

func hasReason(v Verdict, sub string) bool {
	for _, r := range v.Reasons {
		if strings.Contains(r, sub) {
			return true
		}
	}
	return false
}

// lateClock oversleeps once, like a generator descheduled by the OS or a GC
// pause: requests due during the gap go out late, in a catch-up burst.
type lateClock struct {
	extra time.Duration
	after time.Time
	done  atomic.Bool
}

func (c *lateClock) Now() time.Time { return time.Now() }
func (c *lateClock) Sleep(ctx context.Context, d time.Duration) {
	if time.Now().After(c.after) && c.done.CompareAndSwap(false, true) {
		d += c.extra
	}
	RealClock.Sleep(ctx, d)
}

// Latency is measured from DUE time (§4). When the generator itself is late,
// the delay is real delay a real app's telemetry would have suffered, so it
// must count against the system. Measured from send time, these requests
// would look instant against a fast receiver, and the gap would vanish.
func TestRun_GeneratorLatenessCountsAgainstLatency(t *testing.T) {
	clk := &lateClock{extra: 200 * time.Millisecond, after: time.Now().Add(100 * time.Millisecond)}
	cfg := Config{Rate: 2000, Duration: 600 * time.Millisecond, Timeout: time.Second, Inflight: 1024}
	r, err := Run(context.Background(), clk, cfg, testPool(t, 10), ok) // instant receiver
	if err != nil {
		t.Fatal(err)
	}
	if r.MaxScheduleLag < 150*time.Millisecond {
		t.Fatalf("schedule lag %v; the 200ms oversleep was not seen", r.MaxScheduleLag)
	}
	if max := time.Duration(r.Steady.Hist.Max()) * time.Microsecond; max < 150*time.Millisecond {
		t.Fatalf("max latency %v against an instant receiver; generator lateness was not counted", max)
	}
	// ~40 requests (200 req/s x 200ms) were due during the gap.
	if slow := r.Steady.Hist.TotalCount() - countBelow(r, 50*time.Millisecond); slow < 25 {
		t.Fatalf("only %d late samples", slow)
	}
}

// Regression from the first end-to-end run: 20k records/s at 512 per request
// is 39.0625 req/s, so an 8s window holds 311 requests, not 312.5. A perfect
// run must still report achieved == offered and PASS.
func TestRun_NonIntegralRequestRateIsNotUnderReported(t *testing.T) {
	cfg := Config{Rate: 2000, Warmup: 100 * time.Millisecond, Duration: 433 * time.Millisecond, Timeout: time.Second, Inflight: 64}
	r, s, v := run(t, cfg, 7, ok) // 285.7 req/s: windows never hold a whole number
	if s.AchievedRecordsPerSec != cfg.Rate {
		t.Fatalf("achieved %.2f/s on a perfect run, want %.0f/s", s.AchievedRecordsPerSec, cfg.Rate)
	}
	if !v.Pass {
		t.Fatalf("perfect run failed: %v", v.Reasons)
	}
	_ = r
}

// A burst run: burst requests are counted in their own window and in the
// steady window the verdict judges.
func TestRun_BurstCountsInBurstAndSteadyWindows(t *testing.T) {
	cfg := Config{Rate: 1000, Warmup: 100 * time.Millisecond, Duration: 400 * time.Millisecond, Timeout: time.Second, Inflight: 256,
		BurstRate: 3000, BurstAt: 200 * time.Millisecond, BurstFor: 100 * time.Millisecond}
	r, s, v := run(t, cfg, 10, ok) // 100 req/s base, 300 req/s burst
	if !v.Pass {
		t.Fatalf("healthy burst run failed: %v", v.Reasons)
	}
	if r.Warmup.Scheduled != 10 || r.Burst.Scheduled != 30 || r.Steady.Scheduled != 60 {
		t.Fatalf("scheduled warm-up/burst/steady = %d/%d/%d, want 10/30/60 (steady = 30 base + 30 burst)",
			r.Warmup.Scheduled, r.Burst.Scheduled, r.Steady.Scheduled)
	}
	if s.Burst == nil || s.Burst.IngestedRecords != 300 || s.Burst.RecordsPerSec != 3000 {
		t.Fatalf("burst summary %+v", s.Burst)
	}
	bad := cfg
	bad.BurstAt = 0 // inside warm-up
	if _, err := Run(context.Background(), RealClock, bad, testPool(t, 10), ok); err == nil {
		t.Fatal("burst inside the warm-up accepted")
	}
}
