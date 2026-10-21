package storage

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func kvMap(kv []any) map[string]any {
	m := map[string]any{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i].(string)] = kv[i+1]
	}
	return m
}

func TestLoopWindow_Attrs(t *testing.T) {
	t0 := time.Unix(1000, 0)
	w := loopWindow{start: t0}
	// Two full polls that each flushed, one empty poll: 10 s of wall time.
	w.observe(iteration{msgs: 30, parts: 6, batches: 6, poll: 1 * time.Second, add: 500 * time.Millisecond, flush: 2 * time.Second, commit: 100 * time.Millisecond})
	w.observe(iteration{msgs: 18, parts: 3, batches: 2, poll: 2 * time.Second, add: 300 * time.Millisecond, flush: 1 * time.Second, commit: 100 * time.Millisecond})
	w.observe(iteration{poll: 2 * time.Second})
	f := fetchSnapshot{batches: 9, records: 48, compressed: 9 * 900 << 10, uncompressed: 9 * 4 << 20}
	got := kvMap(w.attrs(t0.Add(10*time.Second), f, 700_000, 1<<20))

	want := map[string]any{
		"window_s": 10.0, "iters": int64(3), "rows_per_s": int64(70_000),
		"poll_pct": 50.0, "add_pct": 8.0, "flush_pct": 30.0, "commit_pct": 2.0, "other_pct": 10.0,
		"polls_empty": int64(1), "msgs": int64(48),
		"msgs_per_poll": 24.0, "max_msgs_per_poll": int64(30), "parts_per_poll": 4.5,
		"kib_per_part_fetch": 900.0, "cap_kib": int64(1024),
		"fetch_compressed_mib": int64(7), "fetch_uncompressed_mib": int64(36),
		"flushes": int64(2), "batches_per_flush": 4.0, "avg_flush_ms": 1500.0, "max_flush_ms": int64(2000),
		"avg_iter_ms": 3333.3,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v (%T), want %v (%T)", k, got[k], got[k], v, v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d keys, want %d: %v", len(got), len(want), got)
	}
}

func TestLoopWindow_EmptyWindowHasNoNaN(t *testing.T) {
	t0 := time.Unix(1000, 0)
	w := loopWindow{start: t0}
	got := kvMap(w.attrs(t0, fetchSnapshot{}, 0, 1<<20))
	b, err := json.Marshal(got) // NaN or Inf would fail here
	if err != nil {
		t.Fatalf("empty window renders badly: %v (%v)", err, got)
	}
	_ = b
}

// syncBuf collects JSON log lines from the consumer.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) lines(msg string) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, l := range bytes.Split(s.b.Bytes(), []byte("\n")) {
		var m map[string]any
		if json.Unmarshal(l, &m) == nil && m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

// The loop lines must account for every Kafka message the consumer read,
// see the fetch bytes through the hook, and report the real fetch cap.
func TestRun_LoopLineAccountsForEveryMessage(t *testing.T) {
	e := newEnv(t, 3)
	ts := time.Now()
	const count, per = 7, 50
	want := 0
	for p := int32(0); p < 3; p++ {
		want += e.produce(p, "loop", count, per, ts)
	}
	var logs syncBuf
	// The logger goes in before New: franz-go starts group management
	// inside New, so onAssigned can log before New returns (setting it
	// afterwards was a data race, found by -race on a fresh-clone run).
	c := e.consumerWith(fast, func(cfg *Config) { cfg.Log = slog.New(slog.NewJSONHandler(&logs, nil)) })
	if err := e.runUntil(c, want, 20*time.Second); err != nil {
		t.Fatal(err)
	}

	loops := logs.lines("loop")
	if len(loops) == 0 {
		t.Fatal("no loop line logged")
	}
	var msgs, compressed, flushes float64
	for _, l := range loops {
		msgs += l["msgs"].(float64)
		compressed += l["kib_per_part_fetch"].(float64)
		flushes += l["flushes"].(float64)
		if l["cap_kib"].(float64) != 1024 {
			t.Errorf("cap_kib = %v, want 1024 (franz-go default FetchMaxPartitionBytes)", l["cap_kib"])
		}
	}
	if msgs != 3*count {
		t.Errorf("loop lines counted %v messages, want %d", msgs, 3*count)
	}
	if compressed <= 0 {
		t.Errorf("kib_per_part_fetch never > 0: fetch hook not wired? %v", loops)
	}
	if flushes == 0 {
		t.Errorf("no flush counted, but rows were inserted: %v", loops)
	}
	lim := logs.lines("fetch limits")
	if len(lim) != 1 || lim[0]["fetch_max_partition_bytes"].(float64) != 1<<20 {
		t.Errorf("fetch limits line = %v", lim)
	}
}
