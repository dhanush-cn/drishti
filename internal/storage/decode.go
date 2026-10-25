package storage

import (
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

// Parallel decode (docs/storage.md, "Parallel decode").
//
// After background inserts (patch 17) the loop's only serial work was
// decoding every Kafka record (proto.Unmarshal) and encoding its rows
// (AppendLogs), on one goroutine: ~1 core, ~125–142k rows/s on the laptop.
// Batches are per Kafka partition and nothing in one partition's batch
// depends on another's, so each poll's records are decoded fork-join: one
// goroutine per partition in the poll (at most DecodeWorkers at once), each
// working through its partition's records in offset order, joined before
// the loop goes on.
//
// Why this keeps the exactly-once rules (package doc):
//
//   - One goroutine per partition, records in offset order, so a batch is
//     still built from one partition's consecutive records and cut by the
//     same rules (day change, row limit, never split a message): one insert
//     = one partition range = one day = one part.
//   - Each partition's cut batches come back in the order they were cut and
//     are enqueued in that order, so the insert pipeline (≤1 in flight per
//     partition, offset order, commit after insert) sees exactly what the
//     serial loop gave it. Order across partitions never mattered.
//   - The fork-join sits between PollFetches and AllowRebalance. With
//     BlockRebalanceOnPoll, franz-go runs no OnPartitions{Assigned,Revoked,
//     Lost} callback in that window (checked in the v1.22.1 source: all
//     three go through waitAndAddRebalance). So release() and onAssigned
//     never see a half-decoded partition, and the decoders need no
//     synchronization with them.
//
// Shared state: the goroutines touch no map of the Consumer. Before the
// fork, under mu, each partition's open batch is taken out of c.open and
// its skip offset copied out of c.skip; after the join, under mu, the open
// batches are put back and the cut ones appended to c.ready. mu is released
// in between, so insert goroutines finishing meanwhile are not held up;
// they never touch open batches, the skip map or the decoders.

// partWork is one partition's share of a poll, owned by one goroutine
// between the fork and the join.
type partWork struct {
	partition int32
	recs      []*kgo.Record

	// Copied from c.skip under mu before the fork. The map itself is only
	// written by onAssigned/release, which cannot run until AllowRebalance,
	// i.e. after the join; a copy keeps the goroutines off the map anyway.
	skip    int64
	hasSkip bool

	open  *batch // taken out of c.open; nil if the partition had none
	ready []*batch
	dec   *collogs.ExportLogsServiceRequest // reused across messages of this partition

	// Accumulated locally, added to Stats once per partition per poll:
	// per-record atomics from six goroutines would share a cache line.
	records, skipped, decodeErrs int64
	busy                         time.Duration // wall time inside decodePartition
}

// decodeStats is what one fork-join measured, for the loop line.
type decodeStats struct {
	parts   int           // partitions with records in the poll
	workers int           // effective parallelism: min(DecodeWorkers, parts)
	wall    time.Duration // fork to join
	busy    time.Duration // sum of per-partition decode time
	slowest time.Duration // the partition the join waited for
}

// decodeWorkers returns the configured parallelism (>= 1).
func (c *Consumer) decodeWorkers() int { return max(c.cfg.DecodeWorkers, 1) }

// addFetch decodes one poll's records into their partitions' batches.
// Called with mu held; returns with mu held, but releases it while the
// partitions decode. byPart must hold each partition once, its records in
// offset order (as franz-go returns them).
func (c *Consumer) addFetch(byPart []partRecords) decodeStats {
	works := make([]*partWork, 0, len(byPart))
	for _, pr := range byPart {
		if len(pr.recs) == 0 {
			continue
		}
		w := &partWork{partition: pr.partition, recs: pr.recs, open: c.open[pr.partition], dec: c.decoder(pr.partition)}
		delete(c.open, pr.partition)
		w.skip, w.hasSkip = c.skip[pr.partition]
		works = append(works, w)
	}
	st := decodeStats{parts: len(works), workers: min(c.decodeWorkers(), len(works))}
	if len(works) == 0 {
		return st
	}

	c.mu.Unlock()
	start := time.Now()
	if st.workers <= 1 {
		// One worker: decode on the loop goroutine, no goroutines at all.
		// This is the serial loop of patch 17, which keeps -decode-workers 1
		// a faithful control for the benchmark.
		for _, w := range works {
			c.decodePartition(w)
		}
	} else {
		sem := make(chan struct{}, st.workers)
		var wg sync.WaitGroup
		for _, w := range works {
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				c.decodePartition(w)
			}()
		}
		wg.Wait()
	}
	st.wall = time.Since(start)
	c.mu.Lock()

	for _, w := range works {
		if w.open != nil {
			c.open[w.partition] = w.open
		}
		c.ready = append(c.ready, w.ready...)
		c.Stats.Records.Add(w.records)
		c.Stats.Skipped.Add(w.skipped)
		c.Stats.DecodeErrors.Add(w.decodeErrs)
		st.busy += w.busy
		st.slowest = max(st.slowest, w.busy)
	}
	return st
}

// partRecords is one partition's records from a poll.
type partRecords struct {
	partition int32
	recs      []*kgo.Record
}

// byPartition groups a poll's records by partition for addFetch, ordered
// by partition so runs are reproducible.
func byPartition(fs kgo.Fetches, topic string) []partRecords {
	var out []partRecords
	fs.EachPartition(func(p kgo.FetchTopicPartition) {
		if p.Topic == topic && len(p.Records) > 0 {
			out = append(out, partRecords{p.Partition, p.Records})
		}
	})
	slices.SortFunc(out, func(a, b partRecords) int { return int(a.partition) - int(b.partition) })
	return out
}

// decoder returns partition p's reusable request. Called with mu held, by
// the loop only; release() drops it with the partition.
func (c *Consumer) decoder(p int32) *collogs.ExportLogsServiceRequest {
	d := c.decoders[p]
	if d == nil {
		d = &collogs.ExportLogsServiceRequest{}
		c.decoders[p] = d
	}
	return d
}

// decodePartition appends w's records to w.open, cutting batches into
// w.ready. It touches nothing but w (and the logger, which is safe for
// concurrent use): that is what lets partitions run in parallel.
func (c *Consumer) decodePartition(w *partWork) {
	start := time.Now()
	defer func() { w.busy = time.Since(start) }()
	if c.testDecodeHook != nil {
		c.testDecodeHook(w.partition)
	}
	lim := c.cfg.Limits
	for _, r := range w.recs {
		w.records++
		if w.hasSkip && r.Offset <= w.skip {
			w.skipped++
			continue
		}
		proto.Reset(w.dec)
		if err := proto.Unmarshal(r.Value, w.dec); err != nil {
			// A message the receiver produced but we cannot decode would
			// wedge the partition forever if retried; log it with its
			// coordinates and move on. Counted, never silent.
			w.decodeErrs++
			c.cfg.Log.Error("undecodable message skipped", "partition", r.Partition, "offset", r.Offset, "err", err)
			continue
		}
		n := 0
		for _, rl := range w.dec.GetResourceLogs() {
			for _, sl := range rl.GetScopeLogs() {
				n += len(sl.GetLogRecords())
			}
		}
		day := dayOf(r.Timestamp)
		b := w.open
		// Cut before adding when the message would cross a day or the row
		// limit: a message is never split, so each insert stays one part.
		if b != nil && b.rows > 0 && (b.day != day || b.rows+n > lim.MaxRows) {
			w.ready = append(w.ready, b)
			b = nil
		}
		if b == nil {
			b = &batch{topic: r.Topic, partition: r.Partition, day: day, first: r.Offset, started: time.Now()}
		}
		b.rows += AppendLogs(&b.buf, w.dec, Source{Partition: r.Partition, Offset: r.Offset, Received: r.Timestamp})
		b.last = r
		if b.full(lim) {
			w.ready = append(w.ready, b)
			b = nil
		}
		w.open = b
	}
}
