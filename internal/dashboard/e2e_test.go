package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func kafka(t *testing.T) (*kfake.Cluster, *kgo.Client) {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(6, "anomalies"), kfake.SeedTopics(1, "live.stats"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	p, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return c, p
}

func produce(t *testing.T, p *kgo.Client, topic string, ts time.Time, v []byte) {
	t.Helper()
	if err := p.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Key: []byte("cart"), Timestamp: ts, Value: v}).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func consume(t *testing.T, c *kfake.Cluster, st *State) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = Consume(ctx, ConsumeConfig{Brokers: c.ListenAddrs(), AnomaliesTopic: "anomalies", LiveTopic: "live.stats",
			StatsEvery: time.Hour, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}, st)
	}()
	t.Cleanup(func() { cancel(); <-done })
}

// On start the dashboard reads Retain back (by record time), not the whole
// topic, then follows it.
func TestConsume_RewindsRetainThenFollows(t *testing.T) {
	c, p := kafka(t)
	now := time.Now()
	produce(t, p, "anomalies", now.Add(-time.Hour), ev("ancient", "opened", now.Add(-time.Hour), false))
	produce(t, p, "anomalies", now.Add(-5*time.Minute), ev("recent", "opened", now.Add(-5*time.Minute), false))
	st := NewState(15 * time.Minute)
	consume(t, c, st)
	waitFor(t, func() bool { snap, ch := st.Subscribe(1); st.Unsubscribe(ch); return len(snap.Anomalies) == 1 })
	produce(t, p, "anomalies", time.Now(), ev("live", "opened", time.Now(), false))
	waitFor(t, func() bool { snap, ch := st.Subscribe(1); st.Unsubscribe(ch); return len(snap.Anomalies) == 2 })
	snap, ch := st.Subscribe(1)
	st.Unsubscribe(ch)
	for _, a := range snap.Anomalies {
		if strings.Contains(string(a), "ancient") {
			t.Fatal("read an anomaly from before the retain window")
		}
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatal("condition not met in 10 s")
}

// TestPageE2E drives the real page in headless Chromium: Kafka -> Consume ->
// SSE -> rendered card, timed from the event's detected_at. Needs Python
// Playwright; set DRISHTI_DASHBOARD_E2E=1 to run it.
func TestPageE2E(t *testing.T) {
	if os.Getenv("DRISHTI_DASHBOARD_E2E") == "" {
		t.Skip("set DRISHTI_DASHBOARD_E2E=1 (needs python3 with playwright)")
	}
	c, p := kafka(t)
	st := NewState(15 * time.Minute)
	consume(t, c, st)
	srv := httptest.NewServer(Handler(st, time.Second))
	defer srv.Close()

	shot := os.Getenv("DRISHTI_DASHBOARD_SHOT")
	if shot == "" {
		shot = filepath.Join(t.TempDir(), "page.png")
	}
	cmd := exec.Command("python3", "testdata/page_e2e.py", srv.URL+"/", shot)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = io.WriteString(stdin, "QUIT\n"); _ = cmd.Wait() }()
	out := bufio.NewScanner(stdout)
	read := func() map[string]any {
		t.Helper()
		if !out.Scan() {
			t.Fatalf("page script ended: %v", out.Err())
		}
		var m map[string]any
		if l := out.Text(); l != "READY" {
			if err := json.Unmarshal([]byte(l), &m); err != nil {
				t.Fatalf("page script: %q", l)
			}
		}
		if errs, _ := m["errors"].([]any); len(errs) > 0 {
			t.Fatalf("page errors: %v", errs)
		}
		return m
	}
	read() // READY

	stopLive := make(chan struct{})
	liveDone := make(chan struct{})
	go func() { // one snapshot a second, as the detector sends them
		defer close(liveDone)
		for {
			now := time.Now()
			_ = p.ProduceSync(context.Background(), &kgo.Record{Topic: "live.stats", Timestamp: now, Value: liveSnap(now)}).FirstErr()
			select {
			case <-stopLive:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	defer func() { close(stopLive); <-liveDone }()

	// One real event per rule (from the 3 Nov recording's replay), re-stamped
	// now: every card kind renders its evidence.
	var lat []float64
	evs, err := os.ReadFile("testdata/events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	for i, l := range strings.Split(strings.TrimSpace(string(evs)), "\n") {
		var e map[string]any
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("e2e-%d", i)
		now := time.Now().UTC().Format(time.RFC3339Nano)
		e["id"], e["opened_at"], e["detected_at"], e["evidence_at"] = id, now, now, now
		b, _ := json.Marshal(e)
		produce(t, p, "anomalies", time.Now(), b)
		_, _ = io.WriteString(stdin, "WAIT "+id+" opened\n")
		m := read()
		text := m["text"].(string)
		for _, bad := range []string{"undefined", "NaN", "null"} {
			if strings.Contains(text, bad) {
				t.Errorf("%s card shows %q: %q", e["rule"], bad, text)
			}
		}
		if !strings.Contains(text, e["service"].(string)) {
			t.Errorf("%s card %q: no service", e["rule"], text)
		}
		lat = append(lat, m["rendered_ms"].(float64))
	}
	time.Sleep(3 * time.Second) // a few live snapshots
	_, _ = io.WriteString(stdin, "ROWS\n")
	rows := read()
	if r := rows["rows"].(string); !strings.Contains(r, "cart") || !strings.Contains(r, "10.0") || strings.Contains(r, "NaN") {
		t.Fatalf("services table %q: want cart at 10.0 calls/s", r)
	}
	// The first one resolves (same event, as the detector sends it).
	var first map[string]any
	_ = json.Unmarshal([]byte(strings.SplitN(string(evs), "\n", 2)[0]), &first)
	first["id"], first["event"], first["detected_at"] = "e2e-0", "resolved", time.Now().UTC().Format(time.RFC3339Nano)
	resolvedEv, _ := json.Marshal(first)
	produce(t, p, "anomalies", time.Now(), resolvedEv)
	_, _ = io.WriteString(stdin, "WAIT e2e-0 resolved\n")
	read()
	_, _ = io.WriteString(stdin, "SHOT\n")
	read()
	worst := 0.0
	for _, l := range lat {
		worst = max(worst, l)
	}
	t.Logf("detected_at -> rendered on the page, ms: %v (worst %.0f)", lat, worst)
	if worst > 2000 {
		t.Fatalf("worst detected -> rendered %.0f ms: want under 2 s", worst)
	}
}
