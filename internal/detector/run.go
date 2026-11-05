// Package detector runs the streaming detector (internal/detect) against
// Kafka: it consumes otlp.logs and otlp.metrics, keeps one detect.Shard per
// input partition, and produces anomaly events to the anomalies topic and
// per-second activity snapshots to live.stats (docs/detector.md).
//
// It commits nothing. A detector's state is its shards' windows and
// baselines, rebuilt on start by rewinding Rewind into every assigned
// partition (by record timestamp), so a restart or a partition moving to
// another instance costs one rewind, not an offset protocol. Events are
// at-least-once: anomaly IDs are deterministic, so a rewind re-emits an
// anomaly under the ID it had and consumers deduplicate.
package detector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dhanush-cn/drishti/internal/detect"
)

type Config struct {
	Brokers      []string
	Group        string
	LogsTopic    string
	MetricsTopic string
	OutTopic     string // anomalies
	LiveTopic    string // live.stats; empty = off
	InstanceID   string // names this process in live.stats
	Version      string

	// Rewind: on assignment, start this far back in time (by record
	// timestamp) to rebuild windows and baselines. Must cover the detector's
	// warm-up plus its longest window, or the first minutes after a restart
	// can't fire.
	Rewind time.Duration
	// Grace: a caught-up shard's clock follows the wall clock this far
	// behind, so a record produced just before a bucket boundary but fetched
	// after it still lands in its own bucket.
	Grace time.Duration
	// LiveEvery: live.stats period.
	LiveEvery time.Duration
	// StatsEvery: the log line's period.
	StatsEvery time.Duration
	// Workers: shards processed at once per poll; 0 = GOMAXPROCS.
	Workers int

	Detect detect.Config
	Log    *slog.Logger

	now       func() time.Time // tests
	extraOpts []kgo.Opt        // tests
}

// Detector is one running detector instance.
type Detector struct {
	cfg   Config
	cl    *kgo.Client
	det   *detect.Detector
	start time.Time

	mu       sync.Mutex
	assigned map[detect.Input]bool // what the group gave us (rebalance callbacks write, the loop reads)
	changes  []change              // revokes not yet applied to the shards

	lag map[detect.Input]int64 // high watermark - next offset, per input, from the last fetch

	produced, produceErrs atomic.Int64
	shardCount            atomic.Int64 // len(shards), for tests and nothing else that races the loop
	lastLive, lastStats   time.Time
	maxRecord             time.Time
	loopBusy              time.Duration
}

type change struct {
	in     detect.Input
	reason string
}

func New(cfg Config) (*Detector, error) {
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.Workers <= 0 {
		cfg.Workers = runtime.GOMAXPROCS(0)
	}
	if cfg.Rewind < cfg.Detect.WarmUp {
		cfg.Log.Warn("rewind is shorter than the warm-up: nothing can fire until the warm-up has passed live",
			"rewind", cfg.Rewind.String(), "warm_up", cfg.Detect.WarmUp.String())
	}
	d := &Detector{cfg: cfg, start: cfg.now(), assigned: map[detect.Input]bool{},
		lag: map[detect.Input]int64{}}
	d.det = detect.NewDetector(cfg.Detect, cfg.Version)
	d.det.OnShard = func(s *detect.Shard) {
		s.DetectedAt = func(time.Time) time.Time { return d.cfg.now().UTC() }
		// Evaluated over rewound records: the event is a re-emission (or a
		// late first emission) of something that happened before this
		// process started.
		s.Replayed = func(bucketEnd time.Time) bool { return bucketEnd.Before(d.start) }
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("drishti-detector"),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.LogsTopic, cfg.MetricsTopic),
		// Nothing is committed: offsets are chosen by time on every
		// assignment (below), so a committed offset would only be ignored.
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.AdjustFetchOffsetsFn(d.rewind),
		// Rebalance callbacks then never run while the loop is processing:
		// they only record what changed; the loop drops shards itself.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(d.onAssigned),
		kgo.OnPartitionsRevoked(d.onRevoked),
		kgo.OnPartitionsLost(d.onLost),
		// Producer side: anomalies are small and rare; acks=all so an alert
		// isn't lost to a broker failover.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
		kgo.RecordDeliveryTimeout(30 * time.Second),
	}
	cl, err := kgo.NewClient(append(opts, cfg.extraOpts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	d.cl = cl
	return d, nil
}

// rewind starts every newly assigned partition Rewind back in time. It runs
// per assignment, so a partition taken over an hour after start still
// rewinds 15 minutes, not to the process start.
func (d *Detector) rewind(_ context.Context, offs map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	at := d.cfg.now().Add(-d.cfg.Rewind).UnixMilli()
	for _, ps := range offs {
		for p := range ps {
			ps[p] = kgo.NewOffset().AfterMilli(at)
		}
	}
	return offs, nil
}

func (d *Detector) onAssigned(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for t, ps := range m {
		for _, p := range ps {
			d.assigned[detect.Input{Topic: t, Partition: p}] = true
		}
	}
	d.cfg.Log.Info("partitions assigned", "partitions", m, "rewind", d.cfg.Rewind.String())
}

func (d *Detector) onRevoked(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	d.release(m, "revoked")
}

func (d *Detector) onLost(_ context.Context, _ *kgo.Client, m map[string][]int32) {
	d.release(m, "lost")
}

func (d *Detector) release(m map[string][]int32, why string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for t, ps := range m {
		for _, p := range ps {
			in := detect.Input{Topic: t, Partition: p}
			delete(d.assigned, in)
			d.changes = append(d.changes, change{in, why})
		}
	}
	d.cfg.Log.Info("partitions "+why, "partitions", m)
}

// applyChanges drops the shards of partitions this instance no longer owns.
// Their anomalies are not resolved here: the new owner rewinds and takes
// them over under the same IDs.
func (d *Detector) applyChanges() {
	d.mu.Lock()
	ch := d.changes
	d.changes = nil
	d.mu.Unlock()
	for _, c := range ch {
		d.det.Drop(c.in.Topic, c.in.Partition)
		delete(d.lag, c.in)
	}
}

func (d *Detector) owns(in detect.Input) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.assigned[in]
}

// Run consumes until ctx is done, then flushes what was produced.
func (d *Detector) Run(ctx context.Context) error {
	defer func() {
		d.cl.AllowRebalance() // Close leaves the group, which is a rebalance
		d.cl.Close()
	}()
	d.lastLive, d.lastStats = d.cfg.now(), d.cfg.now()
	for ctx.Err() == nil {
		// Poll at most one bucket long: idle shards' clocks must keep moving.
		pctx, cancel := context.WithTimeout(ctx, min(d.cfg.Detect.Bucket, time.Second))
		fs := d.cl.PollRecords(pctx, 10000)
		cancel()
		if fs.IsClientClosed() {
			break
		}
		t0 := time.Now()
		d.applyChanges()
		var fatal error
		fs.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				d.cfg.Log.Warn("fetch error", "topic", t, "partition", p, "err", err)
				if errors.Is(err, kgo.ErrClientClosed) {
					fatal = err
				}
			}
		})
		if fatal != nil {
			return fatal
		}
		d.produce(d.process(fs))
		d.shardCount.Store(int64(len(d.det.Shards())))
		d.produce(d.tick())
		d.loopBusy += time.Since(t0)
		d.publishLive(ctx)
		d.logStats()
		d.cl.AllowRebalance()
	}
	fctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := d.cl.Flush(fctx); err != nil {
		return fmt.Errorf("flush on shutdown: %w", err)
	}
	return nil
}

type work struct {
	in   detect.Input
	sh   *detect.Shard
	recs []*kgo.Record
	evs  []detect.Event
}

// process runs every shard that has records, up to Workers at once. A shard
// is touched by one goroutine only; shards share nothing.
func (d *Detector) process(fs kgo.Fetches) []detect.Event {
	var ws []*work
	fs.EachPartition(func(p kgo.FetchTopicPartition) {
		in := detect.Input{Topic: p.Topic, Partition: p.Partition}
		if len(p.Records) == 0 || !d.owns(in) {
			return // revoked since it was fetched: the new owner reads it
		}
		last := p.Records[len(p.Records)-1]
		d.lag[in] = max(p.HighWatermark-last.Offset-1, 0)
		ws = append(ws, &work{in: in, sh: d.det.Shard(p.Topic, p.Partition), recs: p.Records})
		if last.Timestamp.After(d.maxRecord) {
			d.maxRecord = last.Timestamp
		}
	})
	sem := make(chan struct{}, d.cfg.Workers)
	var wg sync.WaitGroup
	for _, w := range ws {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			for _, r := range w.recs {
				w.evs = append(w.evs, w.sh.Record(r)...)
			}
		}()
	}
	wg.Wait()
	slices.SortFunc(ws, func(a, b *work) int { return compareInput(a.in, b.in) })
	var evs []detect.Event
	for _, w := range ws {
		evs = append(evs, w.evs...)
	}
	return evs
}

// tick moves caught-up shards' clocks to the wall clock (minus Grace): a
// partition that has gone quiet must still close its windows, so silences
// are detected and anomalies resolve. A shard still catching up (lag > 0)
// is left on its records' clock: advancing it would close buckets its
// fetched-but-unread records belong to.
func (d *Detector) tick() []detect.Event {
	now := d.cfg.now().Add(-d.cfg.Grace)
	var evs []detect.Event
	for in, sh := range d.det.Shards() {
		if d.lag[in] > 0 || !d.owns(in) {
			continue
		}
		evs = append(evs, sh.Advance(now)...)
	}
	// Shards are a map: order the output by input, keeping each shard's own
	// order.
	slices.SortStableFunc(evs, func(a, b detect.Event) int {
		return compareInput(detect.Input{Topic: a.Detector.Topic, Partition: a.Detector.Partition},
			detect.Input{Topic: b.Detector.Topic, Partition: b.Detector.Partition})
	})
	return evs
}

func compareInput(a, b detect.Input) int {
	if c := strings.Compare(a.Topic, b.Topic); c != 0 {
		return c
	}
	return int(a.Partition - b.Partition)
}

// produce sends events to the anomalies topic, keyed by service (all of a
// service's anomalies in one partition, in order).
func (d *Detector) produce(evs []detect.Event) {
	for _, e := range evs {
		b, err := json.Marshal(e)
		if err != nil {
			d.produceErrs.Add(1)
			d.cfg.Log.Error("encode event", "err", err, "rule", e.Rule, "service", e.Service)
			continue
		}
		if e.Event == "opened" && !e.Replayed {
			d.cfg.Log.Info("anomaly", "id", e.ID, "rule", e.Rule, "service", e.Service, "series", e.Series,
				"window", e.Evidence.Window, "observed", e.Evidence.Observed, "expected", e.Evidence.Expected,
				"evidence_to_detected_ms", e.DetectedAt.Sub(e.EvidenceAt).Milliseconds())
		}
		d.cl.Produce(context.Background(), &kgo.Record{Topic: d.cfg.OutTopic, Key: []byte(e.Service), Value: b},
			func(_ *kgo.Record, err error) {
				if err != nil {
					d.produceErrs.Add(1)
					d.cfg.Log.Warn("produce anomaly", "err", err)
					return
				}
				d.produced.Add(1)
			})
	}
}

// LiveSnapshot is one live.stats record: per-service activity over the last
// period, from this instance's shards. The dashboard sums instances.
type LiveSnapshot struct {
	Schema      int                        `json:"schema"`
	Instance    string                     `json:"instance"`
	At          time.Time                  `json:"at"`
	PeriodMs    int64                      `json:"period_ms"`
	MaxRecordTS time.Time                  `json:"max_record_ts"` // newest record seen: how fresh the numbers are
	Services    map[string]detect.LiveStat `json:"services"`
}

func (d *Detector) publishLive(ctx context.Context) {
	now := d.cfg.now()
	if d.cfg.LiveTopic == "" || now.Sub(d.lastLive) < d.cfg.LiveEvery {
		return
	}
	// Shards accumulate activity until taken, so taking it once per period
	// is the period's sum (open_anomalies is the level now).
	services := map[string]detect.LiveStat{}
	for _, sh := range d.det.Shards() {
		for svc, v := range sh.TakeLive() {
			a := services[svc]
			a.Logs += v.Logs
			a.Warn += v.Warn
			a.Calls += v.Calls
			a.Errors += v.Errors
			a.Open += v.Open
			services[svc] = a
		}
	}
	snap := LiveSnapshot{Schema: 1, Instance: d.cfg.InstanceID, At: now.UTC(), PeriodMs: now.Sub(d.lastLive).Milliseconds(),
		MaxRecordTS: d.maxRecord.UTC(), Services: services}
	d.lastLive = now
	b, err := json.Marshal(snap)
	if err != nil {
		d.cfg.Log.Error("encode live stats", "err", err)
		return
	}
	// Losing one is harmless (the next replaces it): no retries beyond the
	// client's, no error count.
	d.cl.Produce(ctx, &kgo.Record{Topic: d.cfg.LiveTopic, Key: []byte(d.cfg.InstanceID), Value: b}, nil)
}

func (d *Detector) logStats() {
	now := d.cfg.now()
	if now.Sub(d.lastStats) < d.cfg.StatsEvery {
		return
	}
	var st detect.Stats
	series := 0
	for _, sh := range d.det.Shards() {
		st.Records += sh.Stats.Records
		st.Lines += sh.Stats.Lines
		st.Points += sh.Stats.Points
		st.Events += sh.Stats.Events
		st.Evicted += sh.Stats.Evicted
		st.Dropped += sh.Stats.Dropped
		st.DecodeErrors += sh.Stats.DecodeErrors
		series += sh.Series()
	}
	var lag int64
	for _, l := range d.lag {
		lag += l
	}
	busy := float64(d.loopBusy) / float64(now.Sub(d.lastStats)) * 100
	d.loopBusy = 0
	d.lastStats = now
	d.cfg.Log.Info("detector",
		"shards", len(d.det.Shards()), "series", series, "records", st.Records, "lines", st.Lines, "points", st.Points,
		"events", st.Events, "produced", d.produced.Load(), "produce_errors", d.produceErrs.Load(),
		"decode_errors", st.DecodeErrors, "series_dropped", st.Dropped, "series_evicted", st.Evicted,
		"lag_records", lag, "behind_ms", max(now.Sub(d.maxRecord).Milliseconds(), 0), "loop_busy_pct", int(busy))
}
