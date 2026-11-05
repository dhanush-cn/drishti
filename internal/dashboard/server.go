package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

//go:embed web/index.html
var indexHTML []byte

// Handler serves the page, the SSE stream and a JSON snapshot. Its own mux:
// this process never serves http.DefaultServeMux (see internal/pprofsrv).
func Handler(st *State, heartbeat time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, _ *http.Request) {
		snap, ch := st.Subscribe(1)
		st.Unsubscribe(ch)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snap)
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) { serveSSE(w, r, st, heartbeat) })
	return mux
}

// serveSSE: an "init" event with the current state, then every change as it
// happens. A comment line every heartbeat keeps proxies from closing an idle
// stream and lets the server notice a gone client.
func serveSSE(w http.ResponseWriter, r *http.Request, st *State, heartbeat time.Duration) {
	rc := http.NewResponseController(w)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Accel-Buffering", "no")
	snap, ch := st.Subscribe(1024)
	defer st.Unsubscribe(ch)
	init, _ := json.Marshal(snap)
	if writeEvent(w, "init", init) != nil || rc.Flush() != nil {
		return
	}
	t := time.NewTicker(heartbeat)
	defer t.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case m, ok := <-ch:
			if !ok {
				return // fell behind: the page reconnects and gets a fresh init
			}
			if writeEvent(w, m.Event, m.Data) != nil {
				return
			}
			// Drain what is already queued before flushing: one write per
			// burst, not per event.
			for more := true; more; {
				select {
				case m, ok := <-ch:
					if !ok {
						return
					}
					if writeEvent(w, m.Event, m.Data) != nil {
						return
					}
				default:
					more = false
				}
			}
			if rc.Flush() != nil {
				return
			}
		case <-t.C:
			if _, err := fmt.Fprint(w, ": hb\n\n"); err != nil || rc.Flush() != nil {
				return
			}
		}
	}
}

// writeEvent writes one SSE event. data is one line of JSON (encoding/json
// never emits a raw newline), so it needs no splitting.
func writeEvent(w http.ResponseWriter, event string, data []byte) error {
	_, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
	return err
}

// ConsumeConfig: where the two streams are.
type ConsumeConfig struct {
	Brokers        []string
	AnomaliesTopic string
	LiveTopic      string
	StatsEvery     time.Duration
	Log            *slog.Logger
}

// Consume reads both topics from Retain ago (by record timestamp) into st
// until ctx is done. No consumer group: every dashboard reads everything.
func Consume(ctx context.Context, cfg ConsumeConfig, st *State) error {
	topics := []string{cfg.AnomaliesTopic}
	if cfg.LiveTopic != "" {
		topics = append(topics, cfg.LiveTopic)
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("drishti-dashboard"),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AfterMilli(time.Now().Add(-st.Retain).UnixMilli())),
		// Anomalies are rare: don't wait to fill a fetch.
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer cl.Close()
	last := time.Now()
	for ctx.Err() == nil {
		// At most a second: pruning and the stats line must run while
		// nothing arrives (the detector down).
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		if fs.IsClientClosed() {
			break
		}
		fs.EachError(func(t string, p int32, err error) {
			if ctx.Err() == nil && !errors.Is(err, context.DeadlineExceeded) {
				cfg.Log.Warn("fetch error", "topic", t, "partition", p, "err", err)
			}
		})
		fs.EachRecord(func(r *kgo.Record) {
			switch r.Topic {
			case cfg.AnomaliesTopic:
				st.AddAnomaly(r.Value)
			case cfg.LiveTopic:
				st.AddLive(r.Value)
			}
		})
		if time.Since(last) >= cfg.StatsEvery {
			last = time.Now()
			st.Prune()
			s := st.TakeStats()
			cfg.Log.Info("dashboard", "anomalies", s.Anomalies, "open", s.Open, "live_snapshots", s.Live,
				"subscribers", s.Subscribers, "subscribers_dropped", s.Dropped, "opened_delivered", s.Delivered,
				"detected_to_sent_p50_ms", s.DelayP50.Milliseconds(), "detected_to_sent_p99_ms", s.DelayP99.Milliseconds(),
				"detected_to_sent_max_ms", s.DelayMax.Milliseconds())
		}
	}
	return nil
}
