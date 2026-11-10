package storage

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/chtest"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// The metrics codec through the whole consumer, against in-memory Kafka and
// a real ClickHouse (DRISHTI_TEST_CLICKHOUSE_URL): every point stored once,
// including after a crash between an insert and its commit. The resume skip
// reads the metrics table's own ingest_progress projection.
func TestRun_MetricsEndToEndCrashLeavesNoDuplicates(t *testing.T) {
	s := chtest.New(t, "003_metrics.sql")
	const mtopic = "otlp.metrics"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, mtopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	val, _ := proto.Marshal(demoMetrics()) // 4 points per message
	produce := func(n int) {
		for i := range n {
			r := &kgo.Record{Topic: mtopic, Partition: int32(i % 3), Value: val, Timestamp: time.Date(2026, 11, 3, 17, 0, 0, 0, time.UTC)}
			if err := prod.ProduceSync(context.Background(), r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}
	count := func() int {
		out := strings.TrimSpace(s.MustQuery("SELECT count() FROM " + s.DB + ".metrics FORMAT TabSeparated"))
		var n int
		for _, c := range out {
			n = n*10 + int(c-'0')
		}
		return n
	}
	newConsumer := func(crash func() bool) *Consumer {
		c, err := New(Config{Brokers: cluster.ListenAddrs(), Topic: mtopic, Group: "drishti-storage-metrics",
			Limits: Limits{MaxRows: 100000, MaxBytes: 8 << 20, MaxWait: 30 * time.Millisecond}, InsertTimeout: 5 * time.Second,
			CH:    &CH{URL: s.URL, User: s.User, Password: s.Password, Table: s.DB + ".metrics"},
			Codec: Metrics, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), CrashAfterInsert: crash})
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	run := func(c *Consumer, want int) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx) }()
		deadline := time.After(15 * time.Second)
		for {
			select {
			case err := <-done:
				return err
			case <-deadline:
				cancel()
				return <-done
			case <-time.After(50 * time.Millisecond):
				if count() >= want {
					cancel()
					return <-done
				}
			}
		}
	}

	produce(30)
	var inserts atomic.Int32
	if err := run(newConsumer(func() bool { return inserts.Add(1) == 1 }), 1<<30); err != ErrCrash {
		t.Fatalf("first consumer: err = %v, want the simulated crash", err)
	}
	if count() == 0 {
		t.Fatal("nothing inserted before the crash")
	}
	produce(30)
	if err := run(newConsumer(nil), 60*4); err != nil {
		t.Fatal(err)
	}
	got := strings.TrimSpace(s.MustQuery("SELECT count(), uniqExact(kafka_partition, kafka_offset) FROM " + s.DB + ".metrics FORMAT TabSeparated"))
	if got != "240\t60" {
		t.Fatalf("rows, distinct records = %q, want 240 points from 60 records, each once", got)
	}
}
