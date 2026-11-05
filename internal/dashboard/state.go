// Package dashboard serves the live view (docs/detector.md, D6): it reads the
// anomalies and live.stats topics, keeps the last Retain of both in memory,
// and pushes them to browsers over Server-Sent Events.
//
// Why SSE and not WebSocket: the data flows one way (server to page), SSE is
// plain HTTP (no upgrade, works through any proxy), EventSource reconnects by
// itself, and it needs no dependency. The page never sends anything but the
// initial GET.
//
// Why no store (Redis, ClickHouse) in between: the detector already wrote
// both streams to Kafka, which keeps them for days. A dashboard that restarts
// rewinds Retain and has the same state again; several dashboards are
// independent readers.
package dashboard

import (
	"encoding/json"
	"slices"
	"sort"
	"sync"
	"time"
)

// anomaly is what the dashboard needs of an event; the event itself is
// passed to the page untouched.
type anomaly struct {
	ID         string    `json:"id"`
	Event      string    `json:"event"`
	DetectedAt time.Time `json:"detected_at"`
	Replayed   bool      `json:"replayed"`
}

type entry struct {
	raw  json.RawMessage
	meta anomaly
	seen time.Time // when the dashboard last received it
}

type liveEntry struct {
	raw json.RawMessage
	at  time.Time
}

// Message is one SSE event.
type Message struct {
	Event string // "anomaly" | "live"
	Data  []byte
}

// State holds the last Retain of anomalies (latest event per ID) and live
// snapshots, and fans changes out to subscribers.
type State struct {
	Retain time.Duration
	now    func() time.Time

	mu        sync.Mutex
	anomalies map[string]*entry
	live      []liveEntry // in arrival order
	subs      map[chan Message]struct{}

	// detected_at -> broadcast delay of live opened anomalies (the Kafka hop
	// plus this process), for the stats log line.
	delays  []time.Duration
	dropped int64 // subscribers dropped for not keeping up
}

func NewState(retain time.Duration) *State {
	return &State{Retain: retain, now: time.Now, anomalies: map[string]*entry{}, subs: map[chan Message]struct{}{}}
}

// rank orders events of one anomaly: a later kind replaces an earlier one,
// and within a kind the later detection does.
var rank = map[string]int{"opened": 0, "updated": 1, "resolved": 2}

// newer reports whether b supersedes a: a later stage of the lifecycle, or
// a later "updated" (fresher evidence). A second "opened" or "resolved" is a
// copy (a restarted detector's rewind re-emits them with a new detected_at).
func newer(a, b anomaly) bool {
	if rank[b.Event] != rank[a.Event] {
		return rank[b.Event] > rank[a.Event]
	}
	return b.Event == "updated" && b.DetectedAt.After(a.DetectedAt)
}

// AddAnomaly records one event from the anomalies topic. It is broadcast
// only if it changes what the page shows: a detector that restarts re-emits
// what it finds in its rewind under the same IDs (at-least-once), and those
// copies, or anything older than what is held, are absorbed here.
func (s *State) AddAnomaly(raw []byte) bool {
	var m anomaly
	if json.Unmarshal(raw, &m) != nil || m.ID == "" {
		return false
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.anomalies[m.ID]; e != nil {
		e.seen = now
		if !newer(e.meta, m) {
			return false
		}
		e.raw, e.meta = slices.Clone(raw), m
	} else {
		if m.Event == "resolved" && m.Replayed {
			// Opened and resolved before this dashboard started reading:
			// history, not news. Kept so a late "opened" copy is absorbed.
			s.anomalies[m.ID] = &entry{raw: slices.Clone(raw), meta: m, seen: now}
			return false
		}
		s.anomalies[m.ID] = &entry{raw: slices.Clone(raw), meta: m, seen: now}
	}
	if !m.Replayed && m.Event == "opened" {
		s.delays = append(s.delays, now.Sub(m.DetectedAt))
	}
	s.broadcast(Message{"anomaly", raw})
	return true
}

// AddLive records one live.stats snapshot.
func (s *State) AddLive(raw []byte) {
	var m struct {
		At time.Time `json:"at"`
	}
	if json.Unmarshal(raw, &m) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live = append(s.live, liveEntry{raw: slices.Clone(raw), at: m.At})
	s.broadcast(Message{"live", raw})
}

// Prune forgets what is older than Retain.
func (s *State) Prune() {
	cut := s.now().Add(-s.Retain)
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, e := range s.anomalies {
		if e.seen.Before(cut) && e.meta.DetectedAt.Before(cut) {
			delete(s.anomalies, id)
		}
	}
	i := sort.Search(len(s.live), func(i int) bool { return !s.live[i].at.Before(cut) })
	s.live = append(s.live[:0], s.live[i:]...)
}

// Snapshot is what a page gets on connect.
type Snapshot struct {
	Anomalies []json.RawMessage `json:"anomalies"`
	Live      []json.RawMessage `json:"live"`
}

// Subscribe returns the current state and a channel of changes from then
// on. The channel is closed if the subscriber falls behind by more than buf
// messages (a stalled browser must not hold the others up), and by
// Unsubscribe.
func (s *State) Subscribe(buf int) (Snapshot, chan Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snap Snapshot
	ids := make([]string, 0, len(s.anomalies))
	for id := range s.anomalies {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := s.anomalies[ids[i]].meta, s.anomalies[ids[j]].meta
		if !a.DetectedAt.Equal(b.DetectedAt) {
			return a.DetectedAt.Before(b.DetectedAt)
		}
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		snap.Anomalies = append(snap.Anomalies, s.anomalies[id].raw)
	}
	for _, l := range s.live {
		snap.Live = append(snap.Live, l.raw)
	}
	ch := make(chan Message, buf)
	s.subs[ch] = struct{}{}
	return snap, ch
}

func (s *State) Unsubscribe(ch chan Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subs[ch]; ok {
		delete(s.subs, ch)
		close(ch)
	}
}

// broadcast never blocks: a full subscriber is dropped (its page
// reconnects and gets a fresh snapshot). Caller holds mu.
func (s *State) broadcast(m Message) {
	for ch := range s.subs {
		select {
		case ch <- m:
		default:
			delete(s.subs, ch)
			close(ch)
			s.dropped++
		}
	}
}

// Stats are for the log line.
type Stats struct {
	Anomalies, Open, Live, Subscribers int
	Dropped                            int64
	DelayP50, DelayP99, DelayMax       time.Duration
	Delivered                          int
}

// TakeStats returns counts and the broadcast delays (detected_at -> sent to
// subscribers) of opened anomalies since the last call.
func (s *State) TakeStats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := Stats{Anomalies: len(s.anomalies), Live: len(s.live), Subscribers: len(s.subs), Dropped: s.dropped, Delivered: len(s.delays)}
	for _, e := range s.anomalies {
		if e.meta.Event != "resolved" {
			st.Open++
		}
	}
	if n := len(s.delays); n > 0 {
		slices.Sort(s.delays)
		st.DelayP50, st.DelayP99, st.DelayMax = s.delays[n/2], s.delays[min(n-1, n*99/100)], s.delays[n-1]
	}
	s.delays = s.delays[:0]
	return st
}
