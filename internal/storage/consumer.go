// Package storage moves OTLP logs from Kafka into ClickHouse.
//
// Delivery into ClickHouse is exactly-once per Kafka record, without
// transactions (docs/storage.md):
//
//  1. Every row carries its Kafka (partition, offset).
//  2. An insert holds one partition's offset range and one table partition
//     (day), so it is one part, and a part is atomic: either all its rows
//     (and their offsets) are visible or none are.
//  3. The Kafka offset is committed only after ClickHouse acknowledged the
//     insert. A crash in between replays those records.
//  4. On partition assignment the consumer reads max(kafka_offset) per
//     partition from the table (an aggregate projection makes that cheap)
//     and drops replayed records at or below it.
//
// A retried insert after an ambiguous failure (timeout: written or not?)
// re-sends the identical batch with the same insert_deduplication_token.
package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	Brokers []string
	Topic   string
	Group   string
	// ResetLatest: where a group with no committed offset starts. Earliest
	// is the production answer (no data skipped on first deploy); latest is
	// for this laptop, where Kafka holds hours of benchmark backlog with
	// stale timestamps that nobody wants in ClickHouse.
	ResetLatest   bool
	Limits        Limits
	InsertTimeout time.Duration // one HTTP insert attempt
	PollWait      time.Duration // max wait in one poll, so age-based flushes fire on idle partitions
	CH            *CH
	Log           *slog.Logger

	// crashAfterInsert (tests only): when it returns true after an insert
	// succeeded, Run returns errCrash before committing, as if the process
	// had died between the two steps.
	crashAfterInsert func() bool
}

var errCrash = errors.New("simulated crash after insert, before commit")

// Stats are cumulative counters, logged every 10 s.
type Stats struct {
	Records, Rows, Inserts, InsertRetries, InsertBytes, Skipped, DecodeErrors atomic.Int64
	InsertNanos                                                               atomic.Int64
}

type Consumer struct {
	cfg   Config
	cl    *kgo.Client // set after NewClient returns: never read it from callbacks
	Stats Stats

	mu      sync.Mutex
	open    map[int32]*batch // being filled
	ready   []*batch         // full or due, to flush this iteration
	skip    map[int32]int64  // records at or below this offset are already stored
	fatal   error            // set by a callback; Run returns it
	reqPool collogs.ExportLogsServiceRequest
}

func New(cfg Config) (*Consumer, error) {
	if err := cfg.Limits.Validate(); err != nil {
		return nil, err
	}
	if cfg.PollWait <= 0 {
		cfg.PollWait = max(cfg.Limits.MaxWait/4, 50*time.Millisecond)
	}
	if cfg.InsertTimeout <= 0 {
		cfg.InsertTimeout = 30 * time.Second
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	c := &Consumer{cfg: cfg, open: map[int32]*batch{}, skip: map[int32]int64{}}
	reset := kgo.NewOffset().AtStart()
	if cfg.ResetLatest {
		reset = kgo.NewOffset().AtEnd()
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("drishti-storage"),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(reset),
		// Offsets are committed by hand, after the insert (step 3).
		kgo.DisableAutoCommit(),
		// A rebalance may only happen between loop iterations, when every
		// batch that will be committed has been flushed and committed. The
		// callbacks below then only have to drop unflushed state.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onRevoked),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	c.cl = cl
	return c, nil
}

// onAssigned loads what the table already holds for the new partitions
// (step 4). It blocks the group join until ClickHouse answers: consuming
// without it could insert duplicates.
//
// It uses the client it is handed, not c.cl: kgo.NewClient starts group
// management immediately, so this can run before New has stored c.cl (the
// race detector caught exactly that in the first version).
func (c *Consumer) onAssigned(ctx context.Context, cl *kgo.Client, assigned map[string][]int32) {
	parts := assigned[c.cfg.Topic]
	if len(parts) == 0 {
		return
	}
	var prog map[int32]int64
	for attempt := 0; ; attempt++ {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		var err error
		prog, err = c.cfg.CH.Progress(pctx)
		cancel()
		if err == nil {
			break
		}
		c.cfg.Log.Warn("progress query failed; retrying", "err", err, "attempt", attempt)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff(attempt)):
		}
	}
	// Guard: a stored offset at or past the partition's end means the
	// table's offsets are from another incarnation of the topic (deleted
	// and recreated, offsets restarted at 0). Skipping by them would drop
	// every new record silently, so refuse instead.
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, c.cfg.Topic)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.fatal = fmt.Errorf("list end offsets: %w", err)
		return
	}
	for _, p := range parts {
		stored, ok := prog[p]
		if !ok {
			delete(c.skip, p)
			continue
		}
		if end, found := ends.Lookup(c.cfg.Topic, p); found && end.Err == nil && stored >= end.Offset {
			c.fatal = fmt.Errorf("partition %d: table holds offset %d but the topic ends at %d: topic recreated? empty the table (TRUNCATE) or use a new one", p, stored, end.Offset)
			return
		}
		c.skip[p] = stored
	}
	c.cfg.Log.Info("partitions assigned", "partitions", parts, "stored_max_offsets", prog)
}

// onRevoked drops unflushed batches: they were never committed, so the
// partition's next owner re-reads them from the committed offset.
func (c *Consumer) onRevoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range revoked[c.cfg.Topic] {
		delete(c.open, p)
		delete(c.skip, p)
	}
	c.cfg.Log.Info("partitions revoked", "partitions", revoked[c.cfg.Topic])
}

// add appends one Kafka record to its partition's batch. Called with mu held.
func (c *Consumer) add(r *kgo.Record) {
	c.Stats.Records.Add(1)
	if s, ok := c.skip[r.Partition]; ok && r.Offset <= s {
		c.Stats.Skipped.Add(1)
		return
	}
	req := &c.reqPool
	proto.Reset(req)
	if err := proto.Unmarshal(r.Value, req); err != nil {
		// A message the receiver produced but we cannot decode would wedge
		// the partition forever if retried; log it with its coordinates
		// and move on. Counted, never silent.
		c.Stats.DecodeErrors.Add(1)
		c.cfg.Log.Error("undecodable message skipped", "partition", r.Partition, "offset", r.Offset, "err", err)
		return
	}
	n := 0
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	day := dayOf(r.Timestamp)
	b := c.open[r.Partition]
	// Cut before adding when the message would cross a day or the row
	// limit: a message is never split, so each insert stays one part.
	if b != nil && b.rows > 0 && (b.day != day || b.rows+n > c.cfg.Limits.MaxRows) {
		c.ready = append(c.ready, b)
		b = nil
	}
	if b == nil {
		b = &batch{topic: r.Topic, partition: r.Partition, day: day, first: r.Offset, started: time.Now()}
		c.open[r.Partition] = b
	}
	b.rows += AppendLogs(&b.buf, req, Source{Partition: r.Partition, Offset: r.Offset, Received: r.Timestamp})
	b.last = r
	if b.full(c.cfg.Limits) {
		c.ready = append(c.ready, b)
		delete(c.open, r.Partition)
	}
}

// takeReady returns the batches to flush now (full, cut, or older than
// MaxWait). Called with mu held.
func (c *Consumer) takeReady(now time.Time, all bool) []*batch {
	out := c.ready
	c.ready = nil
	for p, b := range c.open {
		if b.rows > 0 && (all || b.due(c.cfg.Limits, now)) {
			out = append(out, b)
			delete(c.open, p)
		}
	}
	return out
}

// flush inserts batches concurrently (one per partition at most per
// iteration in steady state), each retried with backoff until it succeeds
// or ctx ends. Retries resend identical bytes with the same token.
//
// An attempt already in flight is never cancelled by ctx (it runs under
// WithoutCancel plus InsertTimeout): cancelling it would turn a clean
// shutdown into an ambiguous insert (written or not?) for no gain. ctx only
// stops further retries. Found by running the crash test 20x: a shutdown
// that landed mid-insert returned "flush: context canceled".
func (c *Consumer) flush(ctx context.Context, bs []*batch) error {
	errs := make([]error, len(bs))
	var wg sync.WaitGroup
	for i, b := range bs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for attempt := 0; ; attempt++ {
				ictx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.InsertTimeout)
				start := time.Now()
				err := c.cfg.CH.Insert(ictx, b.token(), b.buf.b)
				cancel()
				if err == nil {
					c.Stats.Inserts.Add(1)
					c.Stats.Rows.Add(int64(b.rows))
					c.Stats.InsertBytes.Add(int64(len(b.buf.b)))
					c.Stats.InsertNanos.Add(int64(time.Since(start)))
					return
				}
				c.Stats.InsertRetries.Add(1)
				c.cfg.Log.Warn("insert failed; retrying same batch", "token", b.token(), "rows", b.rows, "attempt", attempt, "err", err)
				select {
				case <-ctx.Done():
					errs[i] = ctx.Err()
					return
				case <-time.After(backoff(attempt)):
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// commit commits offset last+1 for each flushed batch's partition. A failed
// commit is not data loss: the rows are stored, and step 4 skips them when
// they are replayed.
func (c *Consumer) commit(ctx context.Context, bs []*batch) {
	if len(bs) == 0 {
		return
	}
	lasts := make([]*kgo.Record, len(bs))
	for i, b := range bs {
		lasts[i] = b.last
	}
	if err := c.cl.CommitRecords(ctx, lasts...); err != nil {
		c.cfg.Log.Warn("offset commit failed (rows are stored; replay will be skipped)", "err", err)
	}
}

// Run consumes until ctx ends, then flushes and commits what it holds.
func (c *Consumer) Run(ctx context.Context) error {
	defer func() {
		// Leaving the group is itself a rebalance, so with
		// BlockRebalanceOnPoll Close waits forever unless it is allowed
		// first (found by TestRun_*: every test hung in LeaveGroup).
		c.cl.AllowRebalance()
		c.cl.Close()
	}()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		pctx, cancel := context.WithTimeout(ctx, c.cfg.PollWait)
		fs := c.cl.PollFetches(pctx)
		cancel()
		fs.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				c.cfg.Log.Warn("fetch error", "topic", t, "partition", p, "err", err)
			}
		})

		c.mu.Lock()
		if c.fatal != nil {
			err := c.fatal
			c.mu.Unlock()
			return err
		}
		fs.EachRecord(c.add)
		stopping := ctx.Err() != nil
		bs := c.takeReady(time.Now(), stopping)
		c.mu.Unlock()

		// While running, a failing insert retries until ctx ends. When
		// stopping, ctx is already done, so the final flush gets one
		// attempt per batch (plus the in-flight rule in flush).
		if err := c.flush(ctx, bs); err != nil {
			// Not committed, so not lost: the next owner re-reads them.
			return fmt.Errorf("flush: %w", err)
		}
		if c.cfg.crashAfterInsert != nil && len(bs) > 0 && c.cfg.crashAfterInsert() {
			return errCrash
		}
		// The commit must happen even if ctx was cancelled during the
		// flush; skipping it would only cost a skipped replay, but there
		// is no reason to.
		cctx, cancelC := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		c.commit(cctx, bs)
		cancelC()
		if stopping {
			c.logStats()
			return nil
		}
		c.cl.AllowRebalance()
		select {
		case <-tick.C:
			c.logStats()
		default:
		}
	}
}

func (c *Consumer) logStats() {
	s := &c.Stats
	ins := s.Inserts.Load()
	var avg time.Duration
	if ins > 0 {
		avg = time.Duration(s.InsertNanos.Load() / ins)
	}
	c.cfg.Log.Info("stats",
		"records", s.Records.Load(), "rows", s.Rows.Load(), "inserts", ins,
		"insert_retries", s.InsertRetries.Load(), "insert_mib", s.InsertBytes.Load()>>20,
		"avg_insert_ms", avg.Milliseconds(), "skipped_replayed", s.Skipped.Load(), "decode_errors", s.DecodeErrors.Load())
}
