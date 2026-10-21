package storage

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

const topic = "otlp.logs"

func sv(v string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}
}

// message builds one receiver-shaped Kafka value: one resource, n records,
// bodies "<tag>-<i>" so every row is distinguishable.
func message(service, tag string, n int) []byte {
	recs := make([]*logsv1.LogRecord, n)
	for i := range recs {
		recs[i] = &logsv1.LogRecord{TimeUnixNano: uint64(time.Now().UnixNano()), SeverityNumber: 9, Body: sv(fmt.Sprintf("%s-%d", tag, i))}
	}
	b, err := proto.Marshal(&collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource:  &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{Key: "service.name", Value: sv(service)}}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: recs}},
	}}})
	if err != nil {
		panic(err)
	}
	return b
}

type env struct {
	t       *testing.T
	cluster *kfake.Cluster
	prod    *kgo.Client
	ch      *fakeCH
}

func newEnv(t *testing.T, partitions int32) *env {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	return &env{t: t, cluster: c, prod: prod, ch: newFakeCH(t)}
}

// produce writes count messages of recsPer records to partition p and
// returns the total record count.
func (e *env) produce(p int32, tag string, count, recsPer int, ts time.Time) int {
	e.t.Helper()
	for i := 0; i < count; i++ {
		r := &kgo.Record{Topic: topic, Partition: p, Key: []byte("svc"), Value: message("svc", fmt.Sprintf("%s-p%d-m%d", tag, p, i), recsPer), Timestamp: ts}
		if err := e.prod.ProduceSync(context.Background(), r).FirstErr(); err != nil {
			e.t.Fatal(err)
		}
	}
	return count * recsPer
}

func (e *env) consumer(lim Limits, crash func() bool) *Consumer {
	e.t.Helper()
	c, err := New(Config{
		Brokers: e.cluster.ListenAddrs(), Topic: topic, Group: "drishti-storage",
		Limits: lim, InsertTimeout: 5 * time.Second, CH: e.ch.ch(),
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)), CrashAfterInsert: crash,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	return c
}

// runUntil runs c until want rows are in the fake (or timeout), then stops
// it and returns Run's error.
func (e *env) runUntil(c *Consumer, want int, timeout time.Duration) error {
	e.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.After(timeout)
	for {
		select {
		case err := <-done:
			cancel()
			return err
		case <-deadline:
			cancel()
			return <-done
		case <-time.After(20 * time.Millisecond):
			if len(e.ch.snapshot()) >= want {
				cancel()
				return <-done
			}
		}
	}
}

var fast = Limits{MaxRows: 1000, MaxBytes: 1 << 20, MaxWait: 50 * time.Millisecond}

func TestRun_StoresEveryRecordOnceAcrossPartitions(t *testing.T) {
	e := newEnv(t, 6)
	want := 0
	for p := int32(0); p < 6; p++ {
		want += e.produce(p, "a", 20, 7, time.Now())
	}
	if err := e.runUntil(e.consumer(fast, nil), want, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	rows := e.ch.snapshot()
	if len(rows) != want {
		t.Fatalf("rows = %d, want %d", len(rows), want)
	}
	if d := dups(rows); len(d) > 0 {
		t.Fatalf("duplicates: %v", d)
	}
	if q := e.ch.lastQuery; q["async_insert"] != "0" || !strings.HasPrefix(q["insert_deduplication_token"], topic+"-") {
		t.Fatalf("insert settings not sent: %v", q)
	}
}

// The core claim. Phase 1 is consumed in small time-cut batches; the
// consumer "dies" right after an insert, before committing. Phase 2 is
// produced while it is down, so the replacement consumer reads phase 1's
// tail and phase 2 as one backlog and cuts different batches: the dedup
// token cannot match, and only the stored-offset skip prevents duplicates.
func TestRun_CrashBetweenInsertAndCommitLeavesNoDuplicates(t *testing.T) {
	e := newEnv(t, 1)
	want := e.produce(0, "phase1", 5, 3, time.Now())

	var inserts atomic.Int32
	crash := func() bool { return inserts.Add(1) == 1 } // die after the first insert
	err := e.runUntil(e.consumer(Limits{MaxRows: 100000, MaxBytes: 8 << 20, MaxWait: 30 * time.Millisecond}, crash), 1<<30, 5*time.Second)
	if err != ErrCrash {
		t.Fatalf("first consumer: err = %v, want simulated crash", err)
	}
	stored := len(e.ch.snapshot())
	if stored == 0 {
		t.Fatal("nothing was inserted before the crash")
	}

	want += e.produce(0, "phase2", 5, 3, time.Now())
	if err := e.runUntil(e.consumer(Limits{MaxRows: 100000, MaxBytes: 8 << 20, MaxWait: 30 * time.Millisecond}, nil), want, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	rows := e.ch.snapshot()
	if d := dups(rows); len(d) > 0 {
		t.Fatalf("replay after crash duplicated rows (stored %d before crash): %v", stored, d)
	}
	if len(rows) != want {
		t.Fatalf("rows = %d, want %d", len(rows), want)
	}
}

// A failed insert is retried with the same bytes and token; nothing is
// committed or lost in between.
func TestRun_FailedInsertIsRetried(t *testing.T) {
	e := newEnv(t, 1)
	want := e.produce(0, "a", 10, 4, time.Now())
	e.ch.mu.Lock()
	e.ch.failInserts = 3
	e.ch.mu.Unlock()
	c := e.consumer(fast, nil)
	if err := e.runUntil(c, want, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if rows := e.ch.snapshot(); len(rows) != want || len(dups(rows)) > 0 {
		t.Fatalf("rows = %d (dups %v), want %d", len(rows), dups(rows), want)
	}
	if c.Stats.InsertRetries.Load() < 3 {
		t.Fatalf("retries = %d, want >= 3", c.Stats.InsertRetries.Load())
	}
}

// One insert never spans two received_at days (two table partitions = two
// parts = not atomic). The day cut and the row cut fall at different
// messages, so removing either one changes the result.
func TestAdd_CutsBatchAtDayBoundaryAndRowLimit(t *testing.T) {
	c := &Consumer{cfg: Config{Limits: Limits{MaxRows: 10, MaxBytes: 1 << 20, MaxWait: time.Hour}, Log: slog.Default()}, open: map[int32]*batch{}, skip: map[int32]int64{}}
	day1 := time.Date(2026, 10, 20, 23, 59, 59, 0, time.UTC)
	day2 := day1.Add(2 * time.Second)
	rec := func(off int64, ts time.Time, n int) *kgo.Record {
		return &kgo.Record{Topic: topic, Partition: 0, Offset: off, Timestamp: ts, Value: message("s", fmt.Sprint(off), n)}
	}
	c.add(rec(0, day1, 2))
	c.add(rec(1, day2, 2)) // new day: cut before it (rows are far below the limit)
	c.add(rec(2, day2, 2))
	c.add(rec(3, day2, 6)) // 2+2+6 = 10: at the limit, full: cut after it
	c.add(rec(4, day2, 1))
	bs := c.takeReady(time.Now(), true)
	type span struct{ first, last int64 }
	var got []span
	for _, b := range bs {
		got = append(got, span{b.first, b.last.Offset})
	}
	want := []span{{0, 0}, {1, 3}, {4, 4}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("batches = %v, want %v", got, want)
	}
}

// Offsets in the table from an earlier incarnation of the topic must stop
// the consumer, not silently skip every new record.
func TestRun_RefusesStoredOffsetsBeyondTopicEnd(t *testing.T) {
	e := newEnv(t, 1)
	e.produce(0, "a", 2, 1, time.Now())
	e.ch.mu.Lock()
	e.ch.rows = append(e.ch.rows, fakeRow{partition: 0, offset: 500})
	e.ch.mu.Unlock()
	err := e.runUntil(e.consumer(fast, nil), 1<<30, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "topic recreated") {
		t.Fatalf("err = %v, want refusal", err)
	}
}

func TestLimits_Validate(t *testing.T) {
	for _, l := range []Limits{
		{MaxRows: 1_048_449, MaxBytes: 1, MaxWait: time.Second},
		{MaxRows: 1, MaxBytes: 201 << 20, MaxWait: time.Second},
		{MaxRows: 1, MaxBytes: 1, MaxWait: 0},
	} {
		if l.Validate() == nil {
			t.Errorf("%+v: want error", l)
		}
	}
	if err := (Limits{MaxRows: 100000, MaxBytes: 32 << 20, MaxWait: 2 * time.Second}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestAnyString(t *testing.T) {
	arr := &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: &commonv1.ArrayValue{Values: []*commonv1.AnyValue{sv("a\"b"), {Value: &commonv1.AnyValue_IntValue{IntValue: 3}}}}}}
	kv := &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{KvlistValue: &commonv1.KeyValueList{Values: []*commonv1.KeyValue{{Key: "k", Value: arr}}}}}
	cases := map[string]*commonv1.AnyValue{
		"":                 nil,
		"plain":            sv("plain"),
		"true":             {Value: &commonv1.AnyValue_BoolValue{BoolValue: true}},
		"-7":               {Value: &commonv1.AnyValue_IntValue{IntValue: -7}},
		"1.5":              {Value: &commonv1.AnyValue_DoubleValue{DoubleValue: 1.5}},
		"0aff":             {Value: &commonv1.AnyValue_BytesValue{BytesValue: []byte{0x0a, 0xff}}},
		`["a\"b",3]`:       arr,
		`{"k":["a\"b",3]}`: kv,
	}
	for want, v := range cases {
		if got := anyString(v); got != want {
			t.Errorf("anyString = %q, want %q", got, want)
		}
	}
}

func TestSQLString(t *testing.T) {
	for in, want := range map[string]string{
		"drishti.logs": `'drishti.logs'`,
		`it's`:         `'it\'s'`,
		`a\b`:          `'a\\b'`,
	} {
		if got := sqlString(in); got != want {
			t.Errorf("sqlString(%q) = %s, want %s", in, got, want)
		}
	}
}
