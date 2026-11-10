// Command queryapi serves the agent's read-only view of Drishti
// (internal/queryapi, docs/evals.md section 3) over HTTP.
//
//	queryapi -listen :8090 -db drishti        # live data
//	queryapi -listen :8091 -db drishti_eval   # a replayed recording
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dhanush-cn/drishti/internal/queryapi"
)

func main() {
	var (
		listen = flag.String("listen", env("DRISHTI_QUERYAPI_LISTEN", ":8090"), "HTTP listen address")
		chURL  = flag.String("clickhouse-url", env("DRISHTI_CLICKHOUSE_URL", "http://localhost:8123"), "ClickHouse HTTP endpoint")
		chUser = flag.String("clickhouse-user", env("DRISHTI_CLICKHOUSE_USER", "drishti"), "ClickHouse user")
		chPass = flag.String("clickhouse-password", env("DRISHTI_CLICKHOUSE_PASSWORD", "drishti_dev"), "ClickHouse password")
		db     = flag.String("db", env("DRISHTI_QUERYAPI_DB", "drishti"), "database: drishti, or drishti_eval for a replayed recording")
		hidden = flag.String("hidden-services", env("DRISHTI_QUERYAPI_HIDDEN", strings.Join(queryapi.DefaultHidden, ",")), "services the agent never sees (flag system, load generator, observability stack)")
		flags  = flag.String("mask-flags", env("DRISHTI_QUERYAPI_MASK_FLAGS", strings.Join(queryapi.DemoFlags, ",")), "flag names whose mention masks a log line")
		maxRng = flag.Duration("max-range", envDur("DRISHTI_QUERYAPI_MAX_RANGE", 2*time.Hour), "longest window or baseline a call may ask for")
		qto    = flag.Duration("query-timeout", envDur("DRISHTI_QUERYAPI_QUERY_TIMEOUT", 10*time.Second), "per ClickHouse query")
		maxLog = flag.Int("max-log-rows", envInt("DRISHTI_QUERYAPI_MAX_LOG_ROWS", 50_000), "log lines one log_templates call reads (newest first)")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	api := queryapi.New(queryapi.Config{
		CH:         &queryapi.CH{URL: *chURL, User: *chUser, Password: *chPass, HTTP: &http.Client{}, Timeout: *qto},
		DB:         *db,
		Hidden:     split(*hidden),
		Flags:      split(*flags),
		MaxRange:   *maxRng,
		MaxLogRows: *maxLog,
	})
	srv := &http.Server{Addr: *listen, Handler: logged(log, api), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
	}()
	log.Info("queryapi listening", "addr", *listen, "db", *db)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
}

func logged(log *slog.Logger, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		h.ServeHTTP(w, r)
		log.Info("request", "path", r.URL.Path, "query", r.URL.RawQuery, "ms", time.Since(start).Milliseconds())
	})
}

func split(s string) []string {
	var out []string
	for _, x := range strings.Split(s, ",") {
		if x = strings.TrimSpace(x); x != "" {
			out = append(out, x)
		}
	}
	return out
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

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
