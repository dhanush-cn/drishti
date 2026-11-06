// Package detect is the streaming detector (docs/detector.md): it turns
// OTLP logs and metrics into per-series counts in 5 s buckets, compares
// recent windows against each series' own baseline with one statistical
// primitive (how surprising is this count?), and emits anomaly events.
//
// State is per input partition (a Shard: one partition of otlp.logs or of
// otlp.metrics). Every series belongs to exactly one shard because records are
// keyed by service (or container) and a series reads one topic. A shard is
// complete on its own: N detector instances split partitions with no shared
// state.
//
// Time is the Kafka record timestamp (the receiver's produce time), never the
// event's own timestamp: clocks in the demo's containers can skew, and
// receive time is near-monotonic per partition, so windows close without a
// reordering buffer. The shard's clock is the newest record timestamp it has
// seen; an idle shard is advanced from the wall clock by the caller.
package detect

import "time"

// Config holds every threshold. Defaults are the values tuned on the 3 Nov
// demo recording (docs/detector.md, "Tuning"); each is a flag in
// cmd/detector.
type Config struct {
	Bucket time.Duration // counting resolution: 5 s, the span_metrics flush

	// Windows each rule is evaluated over, at every bucket boundary. Short
	// windows catch bursts fast; long ones catch trickles (at 5 Locust users
	// a 50% payment failure is ~1 error per 50 s, which never reaches k_min
	// in 30 s).
	Windows []time.Duration

	Log10Alpha float64 // a window fires below this log10 tail probability: -6
	MinCount   int     // and only with at least this many events: 3
	MinRatio   float64 // and observed >= MinRatio x expected: 3

	// Baselines: exponentially weighted per-bucket rates with this
	// half-life, frozen while the series has an open anomaly.
	HalfLife time.Duration
	// Before a series' shard has this much history, nothing fires: every
	// template is new and every rate is unknown at startup.
	WarmUp time.Duration
	// A series that has never had an event still has a nonzero expected
	// rate: half an event over the history seen so far (capped at
	// FloorHistory). Without it a series' first error would be infinitely
	// surprising.
	FloorHistory  time.Duration
	MinErrorRatio float64 // floor of the baseline error ratio: 0.5%
	MinSlowRatio  float64 // floor of the baseline share of slow calls: 1%
	MinSlowCalls  float64 // a latency baseline needs this many calls behind it: 50
	// A traffic drop is only judged for series with calls in at least this
	// share of their baseline buckets: 0.9.
	DropMinActive float64
	// Dispersion: judge counts and ratios against each series' own
	// variance-to-mean ratio (quasi-Poisson) instead of assuming Poisson.
	Dispersion bool

	// Lifecycle: an anomaly is "resolved" after ResolveAfter without firing;
	// while open, an "updated" event goes out every UpdateEvery.
	ResolveAfter time.Duration
	UpdateEvery  time.Duration

	// New log templates after warm-up: ERROR+ fire on the first line, WARN
	// on the NewWarnCount-th line within NewWarnWithin.
	NewWarnCount  int
	NewWarnWithin time.Duration

	// Memory trend per container: least squares over MemWindow; fire when
	// the slope is positive with R² >= MemMinR2 and the limit is less than
	// MemTimeToLimit away at that slope, or usage is above MemNearLimit of
	// the limit and still rising.
	MemWindow      time.Duration
	MemMinR2       float64
	MemTimeToLimit time.Duration
	MemNearLimit   float64

	// IgnoreServices are never alerted on (their series still count in the
	// live stats). The load generator is the users, not the system.
	IgnoreServices []string

	// Memory bounds per shard: series idle this long are dropped, and no
	// more than MaxSeries are kept (new ones beyond are not tracked).
	IdleEvict time.Duration
	MaxSeries int
}

func DefaultConfig() Config {
	return Config{
		Bucket:         5 * time.Second,
		Windows:        []time.Duration{30 * time.Second, 2 * time.Minute, 5 * time.Minute},
		Log10Alpha:     -6,
		MinCount:       3,
		MinRatio:       3,
		HalfLife:       10 * time.Minute,
		WarmUp:         10 * time.Minute,
		FloorHistory:   30 * time.Minute,
		MinErrorRatio:  0.005,
		MinSlowRatio:   0.01,
		MinSlowCalls:   50,
		DropMinActive:  0.9,
		Dispersion:     true,
		ResolveAfter:   60 * time.Second,
		UpdateEvery:    30 * time.Second,
		NewWarnCount:   2,
		NewWarnWithin:  5 * time.Minute,
		MemWindow:      5 * time.Minute,
		MemMinR2:       0.8,
		MemTimeToLimit: 15 * time.Minute,
		MemNearLimit:   0.9,
		IgnoreServices: []string{"load-generator"},
		IdleEvict:      30 * time.Minute,
		MaxSeries:      50000,
	}
}

// ringLen is how many buckets a series keeps: enough for the longest window.
func (c Config) ringLen() int {
	longest := c.Bucket
	for _, w := range c.Windows {
		longest = max(longest, w)
	}
	return int(longest / c.Bucket)
}
