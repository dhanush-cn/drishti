package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func ev(id, kind string, detected time.Time, replayed bool) []byte {
	b, _ := json.Marshal(map[string]any{"schema": 1, "id": id, "event": kind, "rule": "span_errors", "service": "cart",
		"opened_at": detected, "detected_at": detected, "evidence_at": detected, "replayed": replayed})
	return b
}

func liveSnap(at time.Time) []byte {
	b, _ := json.Marshal(map[string]any{"schema": 1, "instance": "a", "at": at, "period_ms": 1000, "max_record_ts": at,
		"services": map[string]any{"cart": map[string]any{"logs": 5, "calls": 10}}})
	return b
}

func TestAddAnomaly_OnlyChangesAreBroadcast(t *testing.T) {
	st := NewState(15 * time.Minute)
	_, ch := st.Subscribe(16)
	t0 := time.Now()
	steps := []struct {
		raw  []byte
		want bool
		why  string
	}{
		{ev("a", "opened", t0, false), true, "new anomaly"},
		{ev("a", "opened", t0.Add(time.Minute), true), false, "a restarted detector's copy of the opened event"},
		{ev("a", "updated", t0.Add(30*time.Second), false), true, "fresher evidence"},
		{ev("a", "updated", t0.Add(10*time.Second), false), false, "an older update"},
		{ev("a", "resolved", t0.Add(2*time.Minute), false), true, "resolved"},
		{ev("a", "updated", t0.Add(3*time.Minute), false), false, "an update after resolved (out of order)"},
		{ev("a", "resolved", t0.Add(4*time.Minute), true), false, "a copy of resolved"},
		{ev("b", "resolved", t0, true), false, "opened and resolved before the dashboard started reading"},
		{ev("b", "opened", t0, true), false, "its opened, arriving after (other partition order)"},
		{[]byte("not json"), false, "garbage"},
	}
	for _, s := range steps {
		if got := st.AddAnomaly(s.raw); got != s.want {
			t.Errorf("%s: broadcast=%v, want %v", s.why, got, s.want)
		}
	}
	if n := len(ch); n != 3 {
		t.Errorf("%d messages queued, want 3", n)
	}
}

func TestPrune(t *testing.T) {
	st := NewState(15 * time.Minute)
	now := time.Now()
	st.now = func() time.Time { return now.Add(-20 * time.Minute) }
	st.AddAnomaly(ev("old", "resolved", now.Add(-20*time.Minute), false))
	st.AddLive(liveSnap(now.Add(-20 * time.Minute)))
	st.now = func() time.Time { return now }
	st.AddAnomaly(ev("new", "opened", now, false))
	st.AddLive(liveSnap(now))
	st.Prune()
	snap, ch := st.Subscribe(1)
	st.Unsubscribe(ch)
	if len(snap.Anomalies) != 1 || len(snap.Live) != 1 || !strings.Contains(string(snap.Anomalies[0]), `"new"`) {
		t.Fatalf("after prune: %d anomalies, %d live: want only the recent one of each", len(snap.Anomalies), len(snap.Live))
	}
}

// A browser that stops reading must not hold the others up: it is dropped
// (its page reconnects and gets a fresh snapshot).
func TestSlowSubscriberIsDropped(t *testing.T) {
	st := NewState(15 * time.Minute)
	_, slow := st.Subscribe(2)
	_, fast := st.Subscribe(16)
	done := make(chan struct{})
	go func() {
		for i := range 5 {
			st.AddAnomaly(ev(fmt.Sprint(i), "opened", time.Now(), false))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked on a subscriber that doesn't read")
	}
	if len(fast) != 5 {
		t.Errorf("reading subscriber got %d of 5", len(fast))
	}
	n := 0
	for range slow {
		n++
	}
	if n != 2 {
		t.Errorf("slow subscriber: %d queued before it was closed, want its buffer (2)", n)
	}
	if st.TakeStats().Dropped != 1 {
		t.Error("drop not counted")
	}
}

// The SSE stream: init with the state, then each change; heartbeats while
// idle.
func TestSSE(t *testing.T) {
	st := NewState(15 * time.Minute)
	st.AddAnomaly(ev("a", "opened", time.Now(), false))
	srv := httptest.NewServer(Handler(st, 100*time.Millisecond))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	next := func() (event, data string) {
		for sc.Scan() {
			l := sc.Text()
			switch {
			case strings.HasPrefix(l, "event: "):
				event = l[7:]
			case strings.HasPrefix(l, "data: "):
				data = l[6:]
			case strings.HasPrefix(l, ": "):
				return "heartbeat", ""
			case l == "" && event != "":
				return event, data
			}
		}
		t.Fatalf("stream ended: %v", sc.Err())
		return
	}
	e, d := next()
	var snap Snapshot
	if e != "init" || json.Unmarshal([]byte(d), &snap) != nil || len(snap.Anomalies) != 1 {
		t.Fatalf("first event %q %s: want init with the one anomaly", e, d)
	}
	if e, _ := next(); e != "heartbeat" {
		t.Fatalf("idle stream sent %q, want a heartbeat", e)
	}
	sent := time.Now()
	st.AddAnomaly(ev("b", "opened", sent, false))
	for {
		e, d := next()
		if e == "heartbeat" {
			continue
		}
		if e != "anomaly" || !strings.Contains(d, `"id":"b"`) {
			t.Fatalf("got %q %s, want anomaly b", e, d)
		}
		break
	}
	if lat := time.Since(sent); lat > time.Second {
		t.Fatalf("change reached the stream after %v", lat)
	}
}

func TestPageIsServed(t *testing.T) {
	srv := httptest.NewServer(Handler(NewState(time.Minute), time.Second))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		b.WriteString(sc.Text())
	}
	if resp.StatusCode != 200 || !strings.Contains(b.String(), `new EventSource("/events")`) {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if r, _ := http.Get(srv.URL + "/nope"); r.StatusCode != 404 {
		t.Fatalf("unknown path: %d", r.StatusCode)
	}
}

// A change is flushed to the page when it happens, not with the next
// heartbeat (an hour here).
func TestSSE_ChangesAreFlushedAtOnce(t *testing.T) {
	st := NewState(15 * time.Minute)
	srv := httptest.NewServer(Handler(st, time.Hour))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() && sc.Text() != "" { // the init event
	}
	sent := time.Now()
	st.AddAnomaly(ev("b", "opened", sent, false))
	for sc.Scan() {
		if strings.Contains(sc.Text(), `"id":"b"`) {
			if lat := time.Since(sent); lat > time.Second {
				t.Fatalf("change reached the stream after %v", lat)
			}
			return
		}
	}
	t.Fatalf("stream ended before the change: %v", sc.Err())
}
