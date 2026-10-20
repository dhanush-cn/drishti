// Command storage consumes OTLP logs from Kafka and writes them to
// ClickHouse in batches, exactly once per Kafka record (docs/storage.md).
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/dhanush-cn/drishti/internal/pprofsrv"
	"github.com/dhanush-cn/drishti/internal/storage"
)

func main() {
	var (
		brokers   = flag.String("brokers", env("DRISHTI_KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka bootstrap brokers")
		topic     = flag.String("topic", env("DRISHTI_LOGS_TOPIC", "otlp.logs"), "Kafka topic to consume")
		group     = flag.String("group", env("DRISHTI_STORAGE_GROUP", "drishti-storage"), "consumer group")
		reset     = flag.String("reset", env("DRISHTI_STORAGE_RESET", "earliest"), "where a group with no committed offset starts: earliest|latest")
		chURL     = flag.String("clickhouse-url", env("DRISHTI_CLICKHOUSE_URL", "http://localhost:8123"), "ClickHouse HTTP endpoint")
		chUser    = flag.String("clickhouse-user", env("DRISHTI_CLICKHOUSE_USER", "drishti"), "ClickHouse user")
		chPass    = flag.String("clickhouse-password", env("DRISHTI_CLICKHOUSE_PASSWORD", "drishti_dev"), "ClickHouse password")
		table     = flag.String("table", env("DRISHTI_LOGS_TABLE", "drishti.logs"), "target table (clickhouse/schema/001_logs.sql)")
		maxRows   = flag.Int("batch-rows", envInt("DRISHTI_BATCH_ROWS", 100_000), "flush a partition's batch at this many rows")
		maxBytes  = flag.Int("batch-bytes", envInt("DRISHTI_BATCH_BYTES", 32<<20), "flush a partition's batch at this many encoded bytes")
		maxWait   = flag.Duration("batch-wait", envDur("DRISHTI_BATCH_WAIT", 2*time.Second), "flush a partition's batch this long after its first record (bounds event -> queryable at low traffic)")
		insertTO  = flag.Duration("insert-timeout", envDur("DRISHTI_INSERT_TIMEOUT", 30*time.Second), "one insert attempt")
		pprofAddr = flag.String("pprof-addr", env("DRISHTI_PPROF_ADDR", ""), "serve /debug/pprof/ on this address; empty = off")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *reset != "earliest" && *reset != "latest" {
		log.Error("-reset must be earliest or latest", "got", *reset)
		os.Exit(2)
	}

	ch := &storage.CH{URL: *chURL, User: *chUser, Password: *chPass, Table: *table, HTTP: &http.Client{}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := ch.Ping(ctx)
	cancel()
	if err != nil {
		log.Error("clickhouse check failed", "err", err)
		os.Exit(1)
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

	c, err := storage.New(storage.Config{
		Brokers:       strings.Split(*brokers, ","),
		Topic:         *topic,
		Group:         *group,
		ResetLatest:   *reset == "latest",
		Limits:        storage.Limits{MaxRows: *maxRows, MaxBytes: *maxBytes, MaxWait: *maxWait},
		InsertTimeout: *insertTO,
		CH:            ch,
		Log:           log,
	})
	if err != nil {
		log.Error("storage consumer", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("storage consumer running", "topic", *topic, "group", *group, "table", *table,
		"batch_rows", *maxRows, "batch_bytes", *maxBytes, "batch_wait", maxWait.String(), "reset", *reset)
	if err := c.Run(ctx); err != nil {
		log.Error("stopped with error (uncommitted batches are re-read on restart)", "err", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
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
