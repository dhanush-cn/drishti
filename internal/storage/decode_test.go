package storage

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/loadgen"
	"github.com/twmb/franz-go/pkg/kgo"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

// bareConsumer is a Consumer with no Kafka client, for the decode stage.
func bareConsumer(lim Limits, workers int) *Consumer {
	return &Consumer{
		cfg:  Config{Topic: topic, Limits: lim, DecodeWorkers: workers, Log: slog.New(slog.NewTextHandler(io.Discard, nil))},
		open: map[int32]*batch{}, skip: map[int32]int64{}, decoders: map[int32]*collogs.ExportLogsServiceRequest{},
	}
}

// addAll runs one poll's worth of records through addFetch, grouped by
// partition the way Run does it.
func addAll(c *Consumer, recs ...*kgo.Record) decodeStats {
	idx := map[int32]int{}
	var byPart []partRecords
	for _, r := range recs {
		i, ok := idx[r.Partition]
		if !ok {
			i = len(byPart)
			idx[r.Partition] = i
			byPart = append(byPart, partRecords{partition: r.Partition})
		}
		byPart[i].recs = append(byPart[i].recs, r)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addFetch(byPart)
}

// polls builds a deterministic stream: 6 partitions, messages of varying
// size, a received_at day change part-way, split into several polls of
// uneven size so open batches carry across polls.
func polls() [][]*kgo.Record {
	day1 := time.Date(2026, 10, 24, 23, 59, 50, 0, time.UTC)
	var all [][]*kgo.Record
	off := map[int32]int64{}
	for poll := 0; poll < 7; poll++ {
		var recs []*kgo.Record
		for p := int32(0); p < 6; p++ {
			n := 3 + (poll*7+int(p)*5)%11 // uneven per partition and per poll
			for i := 0; i < n; i++ {
				ts := day1
				if poll >= 4 {
					ts = day1.Add(20 * time.Second) // next UTC day
				}
				o := off[p]
				off[p]++
				recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Offset: o, Timestamp: ts,
					Value: message(fmt.Sprintf("svc-%d", p), fmt.Sprintf("p%d-o%d", p, o), 1+int(o)%9)})
			}
		}
		all = append(all, recs)
	}
	return all
}

type cut struct {
	token string
	rows  int
	body  []byte
}

// run feeds every poll through a fresh consumer with the given number of
// decode workers and returns each partition's batches, in cut order.
func runPolls(t *testing.T, workers int, stream [][]*kgo.Record) map[int32][]cut {
	t.Helper()
	// MaxWait is an hour: batches are cut only by rows, bytes and day, so
	// the result cannot depend on timing. MaxRows is small enough to cut
	// many batches mid-poll and leave others open across polls.
	c := bareConsumer(Limits{MaxRows: 23, MaxBytes: 1 << 20, MaxWait: time.Hour}, workers)
	c.skip[2] = 4 // partition 2 resumes after offset 4: the skip must apply per partition
	for _, recs := range stream {
		addAll(c, recs...)
	}
	out := map[int32][]cut{}
	for _, b := range c.takeReady(time.Now(), true) {
		out[b.partition] = append(out[b.partition], cut{b.token(), b.rows, b.buf.b})
	}
	if c.Stats.Skipped.Load() != 5 {
		t.Fatalf("workers=%d: skipped %d records, want 5 (offsets 0..4 of partition 2)", workers, c.Stats.Skipped.Load())
	}
	return out
}

// Parallel decode must not change what is inserted. The reference is the
// same stream fed one record per poll on one worker: no poll holds two
// records, so nothing can be split or reordered. Both the serial (N=1) and
// the parallel (N=6) run over multi-record polls must cut exactly those
// batches, byte for byte, with the same dedup tokens, in the same order per
// partition. Revert-proofs: splitting a partition's records across two
// goroutines fails the comparison with the reference (it would pass a
// serial-vs-parallel comparison alone, since both runs split alike: the
// first version of this test had exactly that hole); sharing one decoder
// between partitions fails under -race; not copying the skip offset fails
// the skip count.
func TestAddFetch_BatchesDoNotDependOnPollsOrWorkers(t *testing.T) {
	// One stream for both runs: message() stamps time.Now() into every log
	// record, so two generated streams would differ (found when the first
	// version of this test failed on identical tokens with different bytes).
	stream := polls()
	var single [][]*kgo.Record
	for _, poll := range stream {
		for _, r := range poll {
			single = append(single, []*kgo.Record{r})
		}
	}
	ref := runPolls(t, 1, single)
	for _, workers := range []int{1, 6} {
		got := runPolls(t, workers, stream)
		sameBatches(t, fmt.Sprintf("workers=%d vs one record per poll", workers), ref, got)
	}
}

func sameBatches(t *testing.T, what string, serial, parallel map[int32][]cut) {
	t.Helper()
	if len(serial) != 6 {
		t.Fatalf("reference produced batches for %d partitions, want 6", len(serial))
	}
	total := 0
	for p := int32(0); p < 6; p++ {
		s, q := serial[p], parallel[p]
		if len(s) < 3 {
			t.Fatalf("partition %d: only %d batches, the test needs several cuts", p, len(s))
		}
		total += len(s)
		if len(s) != len(q) {
			t.Fatalf("%s: partition %d: %d batches in the reference, %d here", what, p, len(s), len(q))
		}
		for i := range s {
			if s[i].token != q[i].token || s[i].rows != q[i].rows || !bytes.Equal(s[i].body, q[i].body) {
				t.Fatalf("%s: partition %d batch %d differs: reference %s (%d rows, %d B), here %s (%d rows, %d B)",
					what, p, i, s[i].token, s[i].rows, len(s[i].body), q[i].token, q[i].rows, len(q[i].body))
			}
		}
	}
	t.Logf("%s: %d batches identical across 6 partitions", what, total)
}

// The partitions of one poll really decode at the same time, and never
// more of them than DecodeWorkers. The hook holds every decode until
// `want` are inside at once (or 2 s pass).
func TestAddFetch_DecodesPartitionsInParallelUpToTheLimit(t *testing.T) {
	for _, tc := range []struct{ workers, parts, want int }{
		{workers: 6, parts: 3, want: 3}, // all three partitions at once
		{workers: 2, parts: 5, want: 2}, // capped at two
	} {
		t.Run(fmt.Sprintf("workers=%d/parts=%d", tc.workers, tc.parts), func(t *testing.T) {
			c := bareConsumer(Limits{MaxRows: 1000, MaxBytes: 1 << 20, MaxWait: time.Hour}, tc.workers)
			var mu sync.Mutex
			cond := sync.NewCond(&mu)
			inside, maxInside := 0, 0
			c.testDecodeHook = func(int32) {
				mu.Lock()
				defer mu.Unlock()
				inside++
				maxInside = max(maxInside, inside)
				cond.Broadcast()
				deadline := time.Now().Add(2 * time.Second)
				for maxInside < tc.want && time.Now().Before(deadline) {
					// cond has no timed wait: poll it
					mu.Unlock()
					time.Sleep(time.Millisecond)
					mu.Lock()
				}
				// Stay inside, without holding mu, long enough for a goroutine
				// beyond the cap to get in. (Sleeping with mu held serialized
				// the goroutines and hid a removed cap: the first revert-proof
				// of this test stayed green.)
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				mu.Lock()
				inside--
			}
			var recs []*kgo.Record
			for p := int32(0); p < int32(tc.parts); p++ {
				recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Offset: 0, Timestamp: time.Now(), Value: message("s", "x", 3)})
			}
			st := addAll(c, recs...)
			if maxInside != tc.want {
				t.Fatalf("max partitions decoding at once = %d, want %d", maxInside, tc.want)
			}
			if st.workers != tc.want || st.parts != tc.parts {
				t.Fatalf("stats workers=%d parts=%d, want %d/%d", st.workers, st.parts, tc.want, tc.parts)
			}
			if st.busy < st.wall {
				t.Fatalf("busy %v < wall %v with %d partitions in parallel", st.busy, st.wall, tc.want)
			}
		})
	}
}

// End to end with six partitions decoded in parallel: every record stored
// once, each partition's inserts one at a time and in offset order with no
// gap. Revert-proof: enqueueing a partition's cut batches in reverse order
// (addFetch) makes the order check fail.
func TestRun_ParallelDecodeKeepsEachPartitionInOrder(t *testing.T) {
	e := newEnv(t, 6)
	e.ch.gate = func(uint16, uint64) { time.Sleep(5 * time.Millisecond) }
	const count, per = 40, 5
	want := 0
	for p := int32(0); p < 6; p++ {
		want += e.produce(p, "par", count, per, time.Now())
	}
	// 10 rows per batch = 2 messages: one poll of a partition cuts several
	// batches, so their order inside the poll matters.
	c := e.consumerWith(Limits{MaxRows: 10, MaxBytes: 1 << 20, MaxWait: 20 * time.Millisecond}, func(cfg *Config) { cfg.DecodeWorkers = 6 })
	if err := e.runUntil(c, want, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	rows := e.ch.snapshot()
	if len(rows) != want || len(dups(rows)) > 0 {
		t.Fatalf("rows = %d (dups %v), want %d", len(rows), dups(rows), want)
	}
	perOffset := map[uint16]map[uint64]int{}
	for _, r := range rows {
		if perOffset[r.partition] == nil {
			perOffset[r.partition] = map[uint64]int{}
		}
		perOffset[r.partition][r.offset]++
	}
	for p := uint16(0); p < 6; p++ {
		for o := uint64(0); o < count; o++ {
			if perOffset[p][o] != per {
				t.Fatalf("partition %d offset %d: %d rows, want %d", p, o, perOffset[p][o], per)
			}
		}
	}
	e.ch.tmu.Lock()
	defer e.ch.tmu.Unlock()
	for p, m := range e.ch.maxInflight {
		if m > 1 {
			t.Errorf("partition %d had %d inserts in flight at once", p, m)
		}
	}
	for p, firsts := range e.ch.order {
		for i := 1; i < len(firsts); i++ {
			if firsts[i] <= firsts[i-1] {
				t.Fatalf("partition %d inserts out of order: %v", p, firsts)
			}
		}
		if len(firsts) < 5 {
			t.Errorf("partition %d: only %d inserts, the test needs several", p, len(firsts))
		}
	}
}

// BenchmarkAddFetch measures the decode stage alone (no Kafka, no inserts):
// one poll of 6 partitions × 6 messages, each message one load-generator
// request in its default shape (512 records, 80–400 B bodies, 5–8
// attributes; cmd/loadgen flags). That is roughly a full fetch at the Week 3
// ceiling. rows/s is the stage's throughput; compare workers=1 (the serial
// loop) with more. Machine-dependent: only laptop runs count, and only as a
// bound on the decode stage, never as a pipeline number.
func BenchmarkAddFetch(b *testing.B) {
	shape := loadgen.LogShape{BatchSize: 512, Services: 20, Resources: 1, BodyMin: 80, BodyMax: 400, AttrsMin: 5, AttrsMax: 8}
	pool, err := loadgen.LogPool(shape, 40, "bench", 1)
	if err != nil {
		b.Fatal(err)
	}
	var recs []*kgo.Record
	rows := 0
	for p := int32(0); p < 6; p++ {
		for i := 0; i < 6; i++ {
			req := pool[int(p)*6+i]
			v, err := proto.Marshal(req)
			if err != nil {
				b.Fatal(err)
			}
			rows += int(loadgen.CountLogRecords(req))
			recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Offset: int64(i), Timestamp: time.Now(), Value: v})
		}
	}
	for _, workers := range []int{1, 2, 4, 6} {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			c := bareConsumer(Limits{MaxRows: 1_000_000, MaxBytes: 200 << 20, MaxWait: time.Hour}, workers)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				addAll(c, recs...)
				c.takeReady(time.Now(), true) // drop the batches: measure decode only
			}
			b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
