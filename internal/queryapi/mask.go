package queryapi

import (
	"strings"
)

// Masker hides log lines that reveal the fault injection itself (Week 5
// design E7 and change 2). The demo's faults are feature flags; a line that
// names one hands the agent the answer, which no real incident does.
//
// Found in both recordings (docs/evals.md section 3):
//
//	ad        "Feature Flag adManualGc enabled, performing a manual gc now"
//	ad        "High CPU-Load problempattern enabled"         (names no flag)
//	frontend  exception.message "… Product Catalog Fail Feature Flag Enabled"
//	shipping  attributes feature_flag_key=intlShippingSlowdown … on every order, flag on or off
//	frontend-proxy  access lines for /flagservice/… (flagd's own traffic)
//
// A line is masked when its body or any attribute value contains a flag
// name or one of the phrases below (case-insensitive), or when it has an
// attribute whose key starts with feature_flag. Masked lines are dropped
// before anything is counted or shown, and every response says how many
// were dropped: the agent is told data was withheld, never what.
type Masker struct {
	terms []string // lower-case
}

// Phrases that reveal the flag system without naming a flag.
var maskPhrases = []string{"feature flag", "featureflag", "feature_flag", "problempattern", "flagd", "flagservice"}

// DemoFlags are the OTel Demo 3.1.0 flags (src/flagd/demo.flagd.json).
var DemoFlags = []string{
	"adFailure", "adHighCpu", "adManualGc", "cartFailure", "emailMemoryLeak", "failedReadinessProbe",
	"imageSlowLoad", "intlShippingSlowdown", "kafkaQueueProblems", "llmInaccurateResponse", "llmRateLimitError",
	"loadGeneratorFloodHomepage", "paymentFailure", "paymentUnreachable", "productCatalogFailure",
	"productCatalogLockContention", "recommendationCacheFailure",
}

func NewMasker(flags []string) *Masker {
	m := &Masker{}
	for _, f := range flags {
		if f = strings.TrimSpace(f); f != "" {
			m.terms = append(m.terms, strings.ToLower(f))
		}
	}
	m.terms = append(m.terms, maskPhrases...)
	return m
}

// Terms are the lower-case needles, for the SQL predicate (MaskedSQL).
func (m *Masker) Terms() []string { return m.terms }

// MaskedSQL is the same rule as Line, as a ClickHouse predicate over a logs
// row, taking the terms as {mask_terms:Array(String)}. Log endpoints filter
// in SQL so every count and every line they return excludes exactly the
// same rows; TestMasker_SQLAgreesWithGo checks the two implementations.
const MaskedSQL = `(multiSearchAnyCaseInsensitiveUTF8(body, {mask_terms:Array(String)})
     OR arrayExists(v -> multiSearchAnyCaseInsensitiveUTF8(v, {mask_terms:Array(String)}), mapValues(log_attributes))
     OR arrayExists(k -> startsWith(lower(k), 'feature_flag'), mapKeys(log_attributes)))`

// Text reports whether s must be masked.
func (m *Masker) Text(s string) bool {
	if s == "" {
		return false
	}
	low := strings.ToLower(s)
	for _, t := range m.terms {
		if strings.Contains(low, t) {
			return true
		}
	}
	return false
}

// Line reports whether a log line (body + attributes) must be masked.
func (m *Masker) Line(body string, attrs map[string]string) bool {
	if m.Text(body) {
		return true
	}
	for k, v := range attrs {
		if strings.HasPrefix(strings.ToLower(k), "feature_flag") || m.Text(v) {
			return true
		}
	}
	return false
}
