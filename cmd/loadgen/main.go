// Command loadgen is Drishti's open-loop OTLP load generator, Pass 1
// (docs/loadgen.md): one signal (logs), one offered rate, one run, a
// computed PASS/FAIL verdict written to run.json plus latency.hlog.
//
// Pass 1 numbers are internal: quoting needs env.json and the 3-repeat rule,
// which are Pass 2. Run it as a container on Drishti's network (§8), e.g.
//
//	docker compose --profile bench run --rm loadgen -rate 20000
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"

	"github.com/dhanush-cn/drishti/internal/loadgen"
)

// Set at build time (Dockerfile build args) so every run records the code
// that produced it.
var (
	gitCommit = "unknown"
	gitDirty  = "unknown"
)

func main() {
	var (
		target   = flag.String("target", "receiver:4317", "receiver gRPC address (container-to-container, §8)")
		sig      = flag.String("signal", "logs", "signal to send; Pass 1 supports logs only")
		rate     = flag.Float64("rate", 0, "offered load in log records per second (required)")
		warmup   = flag.Duration("warmup", 10*time.Second, "warm-up window, reported but never judged (§4)")
		duration = flag.Duration("duration", 60*time.Second, "steady-state window (§13: provisional)")
		timeout  = flag.Duration("timeout", 5*time.Second, "client deadline per RPC; collector's OTLP exporter default")
		inflight = flag.Int("inflight", 256, "max concurrent RPCs; beyond it a request is counted saturated and fails the run")
		batch    = flag.Int("batch", 512, "log records per request (provisional: OTel Logs SDK maxExportBatchSize default, §13)")
		services = flag.Int("services", 20, "distinct service.name keys; -services 1 reproduces the hot partition")
		poolSize = flag.Int("pool", 256, "distinct pre-generated requests sent in rotation")
		bodyMin  = flag.Int("body-min", 80, "min log body bytes")
		bodyMax  = flag.Int("body-max", 400, "max log body bytes")
		attrsMin = flag.Int("attrs-min", 5, "min attributes per record")
		attrsMax = flag.Int("attrs-max", 8, "max attributes per record")
		useGzip  = flag.Bool("gzip", true, "gzip requests, as the collector does by default")
		conns    = flag.Int("conns", 1, "gRPC connections; values > 1 are Pass 2")
		seed     = flag.Uint64("seed", 1, "payload generator seed")
		outDir   = flag.String("out", "/runs", "directory that receives <UTC>-<run_id>/")
		tol      = flag.Float64("tolerance", 0.01, "achieved >= offered*(1-tolerance) (§13: provisional)")
		maxLag   = flag.Duration("max-lag", 50*time.Millisecond, "max schedule lag (§13: provisional)")
		p99      = flag.Duration("p99", time.Second, "p99 bound for PASS (§13: provisional)")
	)
	flag.Parse()

	if *sig != "logs" {
		fail("-signal %q: Pass 1 supports logs only", *sig)
	}
	if *rate <= 0 {
		fail("-rate is required (log records per second)")
	}
	if *conns != 1 {
		fail("-conns > 1 is Pass 2 (N ClientConns, round-robin); not built yet")
	}

	runID := newRunID()
	shape := loadgen.LogShape{BatchSize: *batch, Services: *services, BodyMin: *bodyMin, BodyMax: *bodyMax, AttrsMin: *attrsMin, AttrsMax: *attrsMax}
	pool, err := loadgen.LogPool(shape, *poolSize, runID, *seed)
	if err != nil {
		fail("payload: %v", err)
	}
	var totalBytes int
	for _, r := range pool {
		totalBytes += proto.Size(r)
	}
	meanReq := totalBytes / len(pool)

	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail("dial: %v", err)
	}
	defer conn.Close()
	if err := waitHealthy(conn, 30*time.Second); err != nil {
		fail("receiver not healthy at %s: %v", *target, err)
	}

	client := collogs.NewLogsServiceClient(conn)
	var callOpts []grpc.CallOption
	compression := "none"
	if *useGzip {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
		compression = "gzip"
	}
	export := func(ctx context.Context, req *collogs.ExportLogsServiceRequest) (int64, error) {
		resp, err := client.Export(ctx, req, callOpts...)
		if err != nil {
			return 0, err
		}
		return resp.GetPartialSuccess().GetRejectedLogRecords(), nil
	}

	cfg := loadgen.Config{Rate: *rate, Warmup: *warmup, Duration: *duration, Timeout: *timeout, Inflight: *inflight}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Printf("run %s: %.0f records/s = %.1f req/s x %d records, %s warm-up + %s steady -> %s\n",
		runID, *rate, *rate/float64(*batch), *batch, *warmup, *duration, *target)
	res, runErr := loadgen.Run(ctx, loadgen.RealClock, cfg, pool, export)
	if res == nil {
		fail("run: %v", runErr)
	}

	th := loadgen.Thresholds{Tolerance: *tol, MaxLag: *maxLag, P99: *p99, Provisional: true}
	sum := loadgen.Summarize(res, cfg)
	verdict := loadgen.Judge(res, cfg, sum, th)
	if runErr != nil {
		verdict.Pass = false
		verdict.Reasons = append(verdict.Reasons, "interrupted: "+runErr.Error())
	}

	flags := map[string]string{}
	flag.VisitAll(func(f *flag.Flag) { flags[f.Name] = f.Value.String() })
	rep := &loadgen.Report{
		RunID:      runID,
		Signal:     *sig,
		Pass:       "1",
		Provenance: loadgen.Provenance{GitCommit: gitCommit, GitDirty: gitDirty != "false"},
		Flags:      flags,
		Shape: loadgen.ShapeReport{
			LogShape:            shape,
			ResourcesPerRequest: shape.ResourcesPerRequest(),
			PoolSize:            *poolSize,
			Compression:         compression,
			Conns:               *conns,
			BatchSizeSource:     "provisional: OTel Logs SDK BatchLogRecordProcessor maxExportBatchSize default (512)",
			MeanRequestBytes:    meanReq,
			MeanRecordBytes:     meanReq / *batch,
		},
		Config: loadgen.ConfigReport{
			RateRecordsPerSec: *rate, WarmupSec: warmup.Seconds(), DurationSec: duration.Seconds(),
			TimeoutSec: timeout.Seconds(), Inflight: *inflight, Target: *target,
		},
		Thresholds: th,
		Summary:    sum,
		Result:     res,
		CPU:        selfCPU(),
		Verdict:    verdict,
	}
	dir := filepath.Join(*outDir, loadgen.DirName(res.Start, runID))
	if err := loadgen.Write(dir, rep); err != nil {
		fail("write results: %v", err)
	}

	v := "PASS"
	if !verdict.Pass {
		v = "FAIL"
	}
	fmt.Printf("%s  offered=%.0f/s achieved=%.0f/s  p50=%s p99=%s p99.9=%s max=%s  lag_max=%s  (internal, thresholds provisional)\n",
		v, sum.OfferedRecordsPerSec, sum.AchievedRecordsPerSec,
		us(sum.P50us), us(sum.P99us), us(sum.P999us), us(sum.MaxUS), res.MaxScheduleLag.Round(time.Microsecond))
	for _, r := range verdict.Reasons {
		fmt.Println("  -", r)
	}
	fmt.Println("saved", dir)
	if !verdict.Pass {
		os.Exit(2)
	}
}

func waitHealthy(conn *grpc.ClientConn, max time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), max)
	defer cancel()
	hc := healthpb.NewHealthClient(conn)
	for {
		r, err := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		if err == nil && r.GetStatus() == healthpb.HealthCheckResponse_SERVING {
			return nil
		}
		select {
		case <-ctx.Done():
			if err == nil {
				err = fmt.Errorf("status %v", r.GetStatus())
			}
			return err
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func selfCPU() loadgen.GeneratorCPU {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	tv := func(t syscall.Timeval) float64 { return float64(t.Sec) + float64(t.Usec)/1e6 }
	return loadgen.GeneratorCPU{UserSec: tv(ru.Utime), SystemSec: tv(ru.Stime), GOMAXPROCS: runtime.GOMAXPROCS(0), NumGC: ms.NumGC}
}

func us(v int64) time.Duration {
	return (time.Duration(v) * time.Microsecond).Round(10 * time.Microsecond)
}

func newRunID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func fail(format string, a ...any) {
	fmt.Fprintf(os.Stderr, "loadgen: "+format+"\n", a...)
	os.Exit(1)
}
