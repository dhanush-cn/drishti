// Command dashboard serves the live view: anomalies and per-service activity
// from the detector's topics, pushed to the browser over Server-Sent Events
// (internal/dashboard).
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dhanush-cn/drishti/internal/dashboard"
	"github.com/dhanush-cn/drishti/internal/pprofsrv"
)

func main() {
	var (
		brokers   = flag.String("brokers", env("DRISHTI_KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka bootstrap brokers")
		anomalies = flag.String("anomalies-topic", env("DRISHTI_ANOMALIES_TOPIC", "anomalies"), "anomaly events topic")
		live      = flag.String("live-topic", env("DRISHTI_LIVE_TOPIC", "live.stats"), "live activity topic; empty = off")
		addr      = flag.String("addr", env("DRISHTI_DASHBOARD_ADDR", ":8088"), "HTTP listen address")
		retain    = flag.Duration("retain", envDur("DRISHTI_DASHBOARD_RETAIN", 15*time.Minute), "how much history is kept in memory (and read back on start)")
		heartbeat = flag.Duration("heartbeat", envDur("DRISHTI_DASHBOARD_HEARTBEAT", 15*time.Second), "SSE keep-alive comment period")
		statsEv   = flag.Duration("stats-every", envDur("DRISHTI_DASHBOARD_STATS_EVERY", 10*time.Second), "stats log line period")
		pprofAddr = flag.String("pprof-addr", env("DRISHTI_PPROF_ADDR", ""), "serve /debug/pprof/ on this address; empty = off")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *pprofAddr != "" {
		ps, err := pprofsrv.Start(*pprofAddr)
		if err != nil {
			log.Error("pprof listen", "addr", *pprofAddr, "err", err)
			os.Exit(1)
		}
		defer ps.Close(context.Background())
		log.Info("pprof listening", "addr", ps.Addr())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	st := dashboard.NewState(*retain)
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Error("listen", "addr", *addr, "err", err)
		os.Exit(1)
	}
	srv := &http.Server{
		Handler:           dashboard.Handler(st, *heartbeat),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: an SSE response lasts as long as the page is open.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			stop()
		}
	}()
	log.Info("dashboard running", "addr", lis.Addr().String(), "anomalies_topic", *anomalies, "live_topic", *live, "retain", retain.String())
	err = dashboard.Consume(ctx, dashboard.ConsumeConfig{Brokers: strings.Split(*brokers, ","), AnomaliesTopic: *anomalies,
		LiveTopic: *live, StatsEvery: *statsEv, Log: log}, st)
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	if err != nil {
		log.Error("stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("stopped")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
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
