package detect

import (
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"time"
)

// Rule names, as they appear in events.
const (
	RuleTemplateNew   = "log_template_new"
	RuleTemplateSpike = "log_template_spike"
	RuleErrorLogSpike = "log_error_spike"
	RuleSpanErrors    = "span_errors"
	RuleSpanSlow      = "span_slow"
	RuleTrafficSurge  = "span_traffic_surge"
	RuleTrafficDrop   = "span_traffic_drop"
	RuleMemoryGrowth  = "container_memory_growth"
)

// Event is one message on the anomalies topic (schema v1, docs/detector.md
// section 5). JSON field names are the contract with the dashboard and the
// Week 5 agent.
type Event struct {
	Schema      int               `json:"schema"`
	ID          string            `json:"id"`
	Event       string            `json:"event"` // opened | updated | resolved
	Rule        string            `json:"rule"`
	Service     string            `json:"service"`
	Series      map[string]string `json:"series,omitempty"`
	OpenedAt    time.Time         `json:"opened_at"`   // bucket end at which it first fired
	DetectedAt  time.Time         `json:"detected_at"` // when this event was produced (shard clock in replay)
	EvidenceAt  time.Time         `json:"evidence_at"` // newest record (Kafka timestamp) behind the evaluation
	WindowStart time.Time         `json:"window_start"`
	WindowEnd   time.Time         `json:"window_end"`
	Evidence    Evidence          `json:"evidence"`
	Template    string            `json:"template,omitempty"`
	Replayed    bool              `json:"replayed,omitempty"` // produced while catching up on the start-up rewind
	Detector    DetectorInfo      `json:"detector"`
	Source      *Source           `json:"source,omitempty"`
}

type Evidence struct {
	Window   string  `json:"window"`          // the window that fired, e.g. "2m0s"
	Observed float64 `json:"observed"`        // count in the window (errors, slow calls, lines, calls)
	Expected float64 `json:"expected"`        // what the baseline expects in the same window
	Calls    float64 `json:"calls,omitempty"` // denominator for ratio rules
	Ratio    float64 `json:"baseline_ratio,omitempty"`
	Log10P   float64 `json:"log10_p"`          // log10 of the tail probability
	Detail   string  `json:"detail,omitempty"` // e.g. the slow edge, memory slope and time to limit
}

type DetectorInfo struct {
	Version   string `json:"version"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
}

// Source makes an alert reproducible: the evals and the agent can fetch the
// exact records of this partition behind it.
type Source struct {
	Partition int32    `json:"partition"`
	Offsets   [2]int64 `json:"offsets"` // [first, last] offsets of the shard's records in the window, per topic below
	Topic     string   `json:"topic"`
}

// anomalyID is deterministic: the same records always open the same anomaly
// with the same ID, so a detector that restarts and replays its rewind
// re-emits it under the ID it had, and consumers deduplicate by ID
// (at-least-once delivery, D5).
func anomalyID(rule, series string, opened time.Time) string {
	h := sha1.New()
	h.Write([]byte(rule))
	h.Write([]byte{0})
	h.Write([]byte(series))
	h.Write([]byte{0})
	h.Write([]byte(strconv.FormatInt(opened.UnixMilli(), 10)))
	return hex.EncodeToString(h.Sum(nil))[:16]
}
