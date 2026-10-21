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
//
// Inserts run in the background, overlapped with polling and encoding
// (docs/storage.md, "Background inserts"). Step 4 adds one rule for that:
// per partition, inserts land in offset order, and an insert starts only
// after the previous one for that partition succeeded. A later batch landing
// before an earlier one failed would raise max(kafka_offset) past records
// that are not stored, and the resume would skip them: loss.
package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
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
	// MaxPendingBytes bounds encoded batches handed to inserts but not yet
	// stored (in flight + queued behind an in-flight insert of the same
	// partition). Above it the loop stops polling until inserts catch up:
	// that is the backpressure, and it bounds memory.
	MaxPendingBytes int
	PollWait        time.Duration // max wait in one poll, so age-based flushes fire on idle partitions
	CH              *CH
	Log             *slog.Logger

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

	// Loop instrumentation (loopstats.go): window is Run's alone; fetch is
	// written by franz-go's fetch goroutines through the hook.
	window   loopWindow
	fetch    fetchStats
	lastRows int64

	mu      sync.Mutex
	open    map[int32]*batch // being filled
	ready   []*batch         // full or due, to hand to an insert this iteration
	skip    map[int32]int64  // records at or below this offset are already stored
	fatal   error            // set by a callback or an insert; Run returns it
	reqPool collogs.ExportLogsServiceRequest

	// Background inserts, all under mu. idle is signalled whenever an insert
	// finishes, so waiters (backpressure, revoke, shutdown) re-check.
	parts   map[int32]*partState
	pending int                   // bytes of in-flight + queued batches
	stored  map[int32]*kgo.Record // stored but not yet committed: last record per partition
	dead    bool                  // simulated crash: no further commits or inserts
	idle    *sync.Cond
	inserts sync.WaitGroup
}

// partState is one partition's insert pipeline: at most one insert in
// flight, later batches queued in offset order behind it.
type partState struct {
	busy     bool
	queue    []*batch
	revoking bool // revoke/lost in progress: start nothing new
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
	if cfg.MaxPendingBytes <= 0 {
		cfg.MaxPendingBytes = 192 << 20
	}
	if cfg.MaxPendingBytes < cfg.Limits.MaxBytes {
		return nil, fmt.Errorf("max pending bytes %d is below one batch (%d): the loop could never hand a full batch to an insert", cfg.MaxPendingBytes, cfg.Limits.MaxBytes)
	}
	c := &Consumer{cfg: cfg, open: map[int32]*batch{}, skip: map[int32]int64{},
		parts: map[int32]*partState{}, stored: map[int32]*kgo.Record{}}
	c.idle = sync.NewCond(&c.mu)
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
		// A rebalance may only happen between loop iterations. Inserts may
		// still be in flight then, so the revoke callback waits for the
		// revoked partitions' inserts and commits them before letting go:
		// the next owner reads max(kafka_offset) from the table, and an
		// insert landing after that read would be stored twice.
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(c.onAssigned),
		kgo.OnPartitionsRevoked(c.onRevoked),
		kgo.OnPartitionsLost(c.onLost),
		kgo.WithHooks(&c.fetch),
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

// onRevoked hands partitions over cleanly: it waits for their in-flight
// inserts, commits what is stored, and drops queued and open batches. Those
// were never inserted or committed, so the next owner re-reads them.
func (c *Consumer) onRevoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	parts := revoked[c.cfg.Topic]
	last := c.release(parts)
	if len(last) > 0 {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		if err := cl.CommitRecords(cctx, last...); err != nil {
			c.cfg.Log.Warn("commit on revoke failed (rows are stored; replay will be skipped)", "err", err)
		}
		cancel()
	}
	c.cfg.Log.Info("partitions revoked", "partitions", parts)
}

// onLost: the group already moved the partitions (session expired), so a
// commit would be refused. Waiting for in-flight inserts still narrows the
// window in which one lands after the new owner read the table.
func (c *Consumer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	c.release(lost[c.cfg.Topic])
	c.cfg.Log.Warn("partitions lost", "partitions", lost[c.cfg.Topic])
}

// release stops the pipelines of parts, waits for their in-flight inserts,
// drops their unstored state and returns the stored, uncommitted records.
func (c *Consumer) release(parts []int32) []*kgo.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	var last []*kgo.Record
	for _, p := range parts {
		if ps := c.parts[p]; ps != nil {
			ps.revoking = true
			for ps.busy {
				c.idle.Wait()
			}
			for _, b := range ps.queue {
				c.pending -= len(b.buf.b)
			}
			delete(c.parts, p)
		}
		if r := c.stored[p]; r != nil {
			last = append(last, r)
			delete(c.stored, p)
		}
		delete(c.open, p)
		delete(c.skip, p)
	}
	c.ready = slices.DeleteFunc(c.ready, func(b *batch) bool { return slices.Contains(parts, b.partition) })
	return last
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

// enqueue hands batches to their partitions' pipelines, in the order they
// were cut (offset order within a partition). Called with mu held.
func (c *Consumer) enqueue(ctx context.Context, bs []*batch) {
	for _, b := range bs {
		ps := c.parts[b.partition]
		if ps == nil {
			ps = &partState{}
			c.parts[b.partition] = ps
		}
		c.pending += len(b.buf.b)
		ps.queue = append(ps.queue, b)
		if !ps.busy {
			c.startNext(ctx, ps)
		}
	}
}

// startNext starts the insert of ps's oldest queued batch. Called with mu
// held; the insert itself runs without it.
func (c *Consumer) startNext(ctx context.Context, ps *partState) {
	if ps.busy || ps.revoking || c.dead || len(ps.queue) == 0 {
		return
	}
	b := ps.queue[0]
	ps.queue = ps.queue[1:]
	ps.busy = true
	c.inserts.Add(1)
	go c.insert(ctx, ps, b)
}

// insert writes one batch, retrying with backoff until it succeeds or ctx
// ends. Retries resend identical bytes with the same token.
//
// An attempt already in flight is never cancelled by ctx (it runs under
// WithoutCancel plus InsertTimeout): cancelling it would turn a clean
// shutdown into an ambiguous insert (written or not?) for no gain. ctx only
// stops further retries. Found by running the crash test 20x: a shutdown
// that landed mid-insert returned "flush: context canceled".
//
// On success the next queued batch of the same partition starts; on final
// failure the partition's queue is dropped (never inserted out of order) and
// the error stops Run.
func (c *Consumer) insert(ctx context.Context, ps *partState, b *batch) {
	defer c.inserts.Done()
	var err error
	for attempt := 0; ; attempt++ {
		ictx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.InsertTimeout)
		start := time.Now()
		err = c.cfg.CH.Insert(ictx, b.token(), b.buf.b)
		cancel()
		if err == nil {
			c.Stats.Inserts.Add(1)
			c.Stats.Rows.Add(int64(b.rows))
			c.Stats.InsertBytes.Add(int64(len(b.buf.b)))
			c.Stats.InsertNanos.Add(int64(time.Since(start)))
			break
		}
		c.Stats.InsertRetries.Add(1)
		c.cfg.Log.Warn("insert failed; retrying same batch", "token", b.token(), "rows", b.rows, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			err = fmt.Errorf("insert %s: %w", b.token(), ctx.Err())
		case <-time.After(backoff(attempt)):
			continue
		}
		break
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.idle.Broadcast()
	ps.busy = false
	c.pending -= len(b.buf.b)
	if err == nil && c.cfg.crashAfterInsert != nil && c.cfg.crashAfterInsert() {
		err = errCrash
	}
	if c.dead {
		return
	}
	if err != nil {
		// Nothing after this batch may land before it: drop the queue (not
		// committed, so re-read after restart) and stop.
		for _, q := range ps.queue {
			c.pending -= len(q.buf.b)
		}
		ps.queue = nil
		if err == errCrash {
			c.dead = true // as if the process died here: no more inserts or commits
		}
		if c.fatal == nil {
			c.fatal = err
		}
		return
	}
	c.stored[b.partition] = b.last
	c.startNext(ctx, ps)
}

// waitFor blocks (mu held) until cond is true or Run must stop.
func (c *Consumer) waitFor(cond func() bool) {
	for !cond() && c.fatal == nil {
		c.idle.Wait()
	}
}

func (c *Consumer) allIdle() bool {
	for _, ps := range c.parts {
		if ps.busy || len(ps.queue) > 0 {
			return false
		}
	}
	return true
}

// commitStored commits offset last+1 for every partition whose inserts
// stored something since the last commit. A failed commit is not data
// loss: the rows are stored, and step 4 skips them when they are replayed.
//
// Only Run calls it, between PollFetches and AllowRebalance, so it never
// overlaps a revoke callback (which commits its own partitions).
func (c *Consumer) commitStored(ctx context.Context) {
	c.mu.Lock()
	if c.dead || len(c.stored) == 0 {
		c.mu.Unlock()
		return
	}
	last := make([]*kgo.Record, 0, len(c.stored))
	for p, r := range c.stored {
		last = append(last, r)
		delete(c.stored, p)
	}
	c.mu.Unlock()
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := c.cl.CommitRecords(cctx, last...); err != nil {
		c.cfg.Log.Warn("offset commit failed (rows are stored; replay will be skipped)", "err", err)
	}
}

// Run consumes until ctx ends, then inserts and commits what it holds.
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
	c.cfg.Log.Info("fetch limits",
		"fetch_max_bytes", c.cl.OptValue(kgo.FetchMaxBytes),
		"fetch_max_partition_bytes", c.cl.OptValue(kgo.FetchMaxPartitionBytes),
		"max_concurrent_fetches", c.cl.OptValue(kgo.MaxConcurrentFetches),
		"poll_wait", c.cfg.PollWait.String(), "max_pending_bytes", c.cfg.MaxPendingBytes)
	c.window = loopWindow{start: time.Now()}
	for {
		var it iteration
		t0 := time.Now()
		pctx, cancel := context.WithTimeout(ctx, c.cfg.PollWait)
		fs := c.cl.PollFetches(pctx)
		cancel()
		t1 := time.Now()
		it.poll = t1.Sub(t0)
		fs.EachError(func(t string, p int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				c.cfg.Log.Warn("fetch error", "topic", t, "partition", p, "err", err)
			}
		})

		c.mu.Lock()
		if err := c.fatal; err != nil {
			c.mu.Unlock()
			return c.stop(err)
		}
		fs.EachPartition(func(p kgo.FetchTopicPartition) {
			if n := len(p.Records); n > 0 {
				it.msgs += n
				it.parts++
			}
		})
		fs.EachRecord(c.add)
		stopping := ctx.Err() != nil
		bs := c.takeReady(time.Now(), stopping)
		c.enqueue(ctx, bs)
		t2 := time.Now()
		it.add = t2.Sub(t1)
		it.batches = len(bs)

		// Backpressure: past the pending budget, stop polling until inserts
		// catch up. When stopping, wait for everything (each queued batch
		// gets its attempt; ctx only stops retries).
		if stopping {
			c.waitFor(c.allIdle)
		} else {
			c.waitFor(func() bool { return c.pending <= c.cfg.MaxPendingBytes })
		}
		err := c.fatal
		c.mu.Unlock()
		t3 := time.Now()
		it.flush = t3.Sub(t2)
		if err != nil {
			return c.stop(err)
		}

		c.commitStored(ctx)
		it.commit = time.Since(t3)
		c.window.observe(it)
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

// stop ends Run on an error: no new inserts start, the in-flight ones
// finish (stopping them mid-request would make them ambiguous), and what
// they stored is committed unless this is the simulated crash.
func (c *Consumer) stop(err error) error {
	c.mu.Lock()
	for _, ps := range c.parts {
		ps.revoking = true
	}
	c.mu.Unlock()
	c.inserts.Wait()
	c.commitStored(context.Background())
	if err == errCrash {
		return err
	}
	return fmt.Errorf("insert: %w", err)
}

func (c *Consumer) logStats() {
	s := &c.Stats
	now := time.Now()
	rows := s.Rows.Load()
	partCap, _ := c.cl.OptValue(kgo.FetchMaxPartitionBytes).(int32)
	c.cfg.Log.Info("loop", c.window.attrs(now, c.fetch.take(), rows-c.lastRows, partCap)...)
	c.lastRows = rows
	c.window = loopWindow{start: now}
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
