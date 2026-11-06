// Command detector is the streaming anomaly detector (docs/detector.md).
//
// Live: consumes otlp.logs and otlp.metrics, produces anomaly events to
// anomalies and per-second activity to live.stats.
//
// Replay: -replay capture.kd.gz runs the same detector over a kafkadump
// recording (cmd/kafkadump) and writes the events as JSON lines, one per
// event, to -out. Every threshold below applies to both, so tuning on a
// recording exercises exactly what runs live.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dhanush-cn/drishti/internal/detect"
	"github.com/dhanush-cn/drishti/internal/detector"
	"github.com/dhanush-cn/drishti/internal/kdump"
	"github.com/dhanush-cn/drishti/internal/pprofsrv"
)

// version is set at build time (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	def := detect.DefaultConfig()
	host, _ := os.Hostname()
	var (
		brokers   = flag.String("brokers", env("DRISHTI_KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka bootstrap brokers")
		logsTopic = flag.String("logs-topic", env("DRISHTI_LOGS_TOPIC", detect.TopicLogs), "OTLP logs topic")
		metTopic  = flag.String("metrics-topic", env("DRISHTI_METRICS_TOPIC", detect.TopicMetrics), "OTLP metrics topic")
		outTopic  = flag.String("anomalies-topic", env("DRISHTI_ANOMALIES_TOPIC", "anomalies"), "topic anomaly events are produced to")
		liveTopic = flag.String("live-topic", env("DRISHTI_LIVE_TOPIC", "live.stats"), "topic per-second activity snapshots are produced to; empty = off")
		group     = flag.String("group", env("DRISHTI_DETECTOR_GROUP", "drishti-detector"), "consumer group (instances split partitions; nothing is committed)")
		instance  = flag.String("instance", env("DRISHTI_DETECTOR_INSTANCE", host), "this instance's name in live.stats")
		rewind    = flag.Duration("rewind", envDur("DRISHTI_DETECTOR_REWIND", 15*time.Minute), "on assignment, rebuild state from this far back (by record timestamp); at least warm-up + the longest window")
		grace     = flag.Duration("grace", envDur("DRISHTI_DETECTOR_GRACE", 2*time.Second), "a caught-up partition's clock follows the wall clock this far behind (records fetched just after their bucket ended)")
		liveEvery = flag.Duration("live-every", envDur("DRISHTI_DETECTOR_LIVE_EVERY", time.Second), "live.stats period")
		statsEv   = flag.Duration("stats-every", envDur("DRISHTI_DETECTOR_STATS_EVERY", 10*time.Second), "stats log line period")
		workers   = flag.Int("workers", envInt("DRISHTI_DETECTOR_WORKERS", 0), "partitions processed at once per poll; 0 = GOMAXPROCS")
		pprofAddr = flag.String("pprof-addr", env("DRISHTI_PPROF_ADDR", ""), "serve /debug/pprof/ on this address; empty = off")

		replay = flag.String("replay", env("DRISHTI_DETECTOR_REPLAY", ""), "replay this kafkadump file instead of consuming Kafka")
		out    = flag.String("out", env("DRISHTI_DETECTOR_OUT", "events.jsonl"), "replay: write events here as JSON lines")

		windows   = flag.String("windows", env("DRISHTI_DETECT_WINDOWS", durs(def.Windows)), "comma-separated windows every rule is evaluated over")
		alpha     = flag.Float64("log10-alpha", envFloat("DRISHTI_DETECT_LOG10_ALPHA", def.Log10Alpha), "a window fires below this log10 tail probability")
		minCount  = flag.Int("min-count", envInt("DRISHTI_DETECT_MIN_COUNT", def.MinCount), "and only with at least this many events in it")
		minRatio  = flag.Float64("min-ratio", envFloat("DRISHTI_DETECT_MIN_RATIO", def.MinRatio), "and at least this many times what the baseline expects")
		halfLife  = flag.Duration("half-life", envDur("DRISHTI_DETECT_HALF_LIFE", def.HalfLife), "baseline half-life")
		warmUp    = flag.Duration("warm-up", envDur("DRISHTI_DETECT_WARM_UP", def.WarmUp), "a service's history before anything about it is judged")
		floorHist = flag.Duration("floor-history", envDur("DRISHTI_DETECT_FLOOR_HISTORY", def.FloorHistory), "a never-seen event is expected at 0.5 per this much history (capped here)")
		minErr    = flag.Float64("min-error-ratio", envFloat("DRISHTI_DETECT_MIN_ERROR_RATIO", def.MinErrorRatio), "floor of a span's baseline error ratio")
		minSlow   = flag.Float64("min-slow-ratio", envFloat("DRISHTI_DETECT_MIN_SLOW_RATIO", def.MinSlowRatio), "floor of a span's baseline share of slow calls")
		slowCalls = flag.Float64("min-slow-calls", envFloat("DRISHTI_DETECT_MIN_SLOW_CALLS", def.MinSlowCalls), "calls a latency baseline needs behind it")
		disp      = flag.Bool("dispersion", envBool("DRISHTI_DETECT_DISPERSION", def.Dispersion), "judge counts against each series' own variance-to-mean ratio (quasi-Poisson), not Poisson")
		dropAct   = flag.Float64("drop-min-active", envFloat("DRISHTI_DETECT_DROP_MIN_ACTIVE", def.DropMinActive), "traffic drops are judged only for spans with calls in this share of baseline buckets")
		resolve   = flag.Duration("resolve-after", envDur("DRISHTI_DETECT_RESOLVE_AFTER", def.ResolveAfter), "an anomaly resolves after this long without firing")
		update    = flag.Duration("update-every", envDur("DRISHTI_DETECT_UPDATE_EVERY", def.UpdateEvery), "an open anomaly is re-sent this often")
		warnN     = flag.Int("new-warn-count", envInt("DRISHTI_DETECT_NEW_WARN_COUNT", def.NewWarnCount), "a new WARN template fires on its Nth line")
		warnIn    = flag.Duration("new-warn-within", envDur("DRISHTI_DETECT_NEW_WARN_WITHIN", def.NewWarnWithin), "within this long of its first")
		memWin    = flag.Duration("mem-window", envDur("DRISHTI_DETECT_MEM_WINDOW", def.MemWindow), "memory trend window")
		memR2     = flag.Float64("mem-min-r2", envFloat("DRISHTI_DETECT_MEM_MIN_R2", def.MemMinR2), "memory trend: minimum R² of the linear fit")
		memTTL    = flag.Duration("mem-time-to-limit", envDur("DRISHTI_DETECT_MEM_TIME_TO_LIMIT", def.MemTimeToLimit), "memory trend fires when the limit is this close at the current slope")
		memNear   = flag.Float64("mem-near-limit", envFloat("DRISHTI_DETECT_MEM_NEAR_LIMIT", def.MemNearLimit), "or when usage is above this share of the limit and rising")
		ignore    = flag.String("ignore-services", env("DRISHTI_DETECT_IGNORE_SERVICES", strings.Join(def.IgnoreServices, ",")), "comma-separated services never alerted on")
		idleEvict = flag.Duration("idle-evict", envDur("DRISHTI_DETECT_IDLE_EVICT", def.IdleEvict), "a series idle this long is forgotten")
		maxSeries = flag.Int("max-series", envInt("DRISHTI_DETECT_MAX_SERIES", def.MaxSeries), "series kept per partition; beyond it new series are not tracked (counted)")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg := def
	ws, err := parseDurs(*windows)
	if err != nil {
		log.Error("-windows", "err", err)
		os.Exit(2)
	}
	for _, w := range ws {
		if w%cfg.Bucket != 0 {
			log.Error("-windows: every window must be a whole number of buckets", "window", w.String(), "bucket", cfg.Bucket.String())
			os.Exit(2)
		}
	}
	cfg.Windows = ws
	cfg.Log10Alpha, cfg.MinCount, cfg.MinRatio = *alpha, *minCount, *minRatio
	cfg.HalfLife, cfg.WarmUp, cfg.FloorHistory = *halfLife, *warmUp, *floorHist
	cfg.MinErrorRatio, cfg.MinSlowRatio, cfg.MinSlowCalls, cfg.DropMinActive = *minErr, *minSlow, *slowCalls, *dropAct
	cfg.Dispersion = *disp
	cfg.ResolveAfter, cfg.UpdateEvery = *resolve, *update
	cfg.NewWarnCount, cfg.NewWarnWithin = *warnN, *warnIn
	cfg.MemWindow, cfg.MemMinR2, cfg.MemTimeToLimit, cfg.MemNearLimit = *memWin, *memR2, *memTTL, *memNear
	cfg.IgnoreServices = nil
	for _, s := range strings.Split(*ignore, ",") {
		if s = strings.TrimSpace(s); s != "" {
			cfg.IgnoreServices = append(cfg.IgnoreServices, s)
		}
	}
	cfg.IdleEvict, cfg.MaxSeries = *idleEvict, *maxSeries

	if *replay != "" {
		if err := runReplay(cfg, *replay, *out); err != nil {
			log.Error("replay", "err", err)
			os.Exit(1)
		}
		return
	}

	if *pprofAddr != "" {
		ps, err := pprofsrv.Start(*pprofAddr)
		if err != nil {
			log.Error("pprof listen", "addr", *pprofAddr, "err", err)
			os.Exit(1)
		}
		defer ps.Close(context.Background())
		log.Info("pprof listening", "addr", ps.Addr())
	}
	d, err := detector.New(detector.Config{
		Brokers: strings.Split(*brokers, ","), Group: *group,
		LogsTopic: *logsTopic, MetricsTopic: *metTopic, OutTopic: *outTopic, LiveTopic: *liveTopic,
		InstanceID: *instance, Version: version, Rewind: *rewind, Grace: *grace,
		LiveEvery: *liveEvery, StatsEvery: *statsEv, Workers: *workers, Detect: cfg, Log: log,
	})
	if err != nil {
		log.Error("detector", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("detector running", "version", version, "group", *group, "instance", *instance,
		"inputs", []string{*logsTopic, *metTopic}, "anomalies_topic", *outTopic, "live_topic", *liveTopic,
		"rewind", rewind.String(), "grace", grace.String(), "config", cfg)
	if err := d.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}

// runReplay prints one summary line (JSON) to stdout.
func runReplay(cfg detect.Config, in, out string) error {
	f, err := os.Open(in)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := kdump.NewReader(f)
	if err != nil {
		return err
	}
	defer r.Close()
	o, err := os.Create(out)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(o)
	enc := json.NewEncoder(w)
	d := detect.NewDetector(cfg, version)
	t0 := time.Now()
	events, opened := 0, 0
	var encErr error
	err = d.Replay(r.Next, func(e detect.Event) {
		if encErr == nil {
			encErr = enc.Encode(e)
		}
		events++
		if e.Event == "opened" {
			opened++
		}
	})
	if err == nil {
		err = encErr
	}
	if err == nil {
		err = w.Flush()
	}
	if cerr := o.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	var st detect.Stats
	for _, s := range d.Shards() {
		st.Records += s.Stats.Records
		st.Lines += s.Stats.Lines
		st.Points += s.Stats.Points
		st.DecodeErrors += s.Stats.DecodeErrors
		st.Dropped += s.Stats.Dropped
	}
	el := time.Since(t0)
	b, _ := json.Marshal(map[string]any{
		"replay": in, "out": out, "records": st.Records, "lines": st.Lines, "points": st.Points,
		"decode_errors": st.DecodeErrors, "series_dropped": st.Dropped, "events": events, "opened": opened,
		"seconds": el.Seconds(), "records_per_s": float64(st.Records) / el.Seconds(), "lines_per_s": float64(st.Lines) / el.Seconds(),
	})
	fmt.Println(string(b))
	return nil
}

func durs(ds []time.Duration) string {
	s := make([]string, len(ds))
	for i, d := range ds {
		s[i] = d.String()
	}
	return strings.Join(s, ",")
}

func parseDurs(s string) ([]time.Duration, error) {
	var out []time.Duration
	for _, f := range strings.Split(s, ",") {
		d, err := time.ParseDuration(strings.TrimSpace(f))
		if err != nil {
			return nil, err
		}
		if d <= 0 {
			return nil, fmt.Errorf("window %v: must be positive", d)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		return nil, errors.New("no windows")
	}
	return out, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
