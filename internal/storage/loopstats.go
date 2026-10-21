package storage

import (
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Loop instrumentation (docs/storage.md, "Reading the loop line").
//
// The question it answers: at the ~70k rows/s ceiling, where does one
// iteration of Run spend its wall time, and how much does one poll bring?
// The loop runs poll → add → wait → commit, with inserts in the background
// (before patch 17 the wait was the whole insert). The four phase times of
// a window add up to (almost) its wall time, and their shares say which
// phase is the limit:
//
//   - poll high, polls full: waiting for the next fetch round trip; the
//     fetch size (FetchMaxPartitionBytes) bounds each iteration.
//   - flush high: the loop stands still waiting for inserts: since
//     background inserts, only at the pending-bytes budget (ClickHouse
//     slower than Kafka) or at shutdown.
//   - add high: decode + RowBinary encode on one goroutine is the limit
//     (it would also show as ~1 core of consumer CPU).
//
// Counters are per window (reset at every "loop" log line), not
// cumulative, so each line describes the last ~10 s on its own.

// iteration is what one pass through Run measured.
type iteration struct {
	msgs    int // Kafka records (one OTLP resource each) returned by the poll
	parts   int // partitions with at least one record in the poll
	batches int // batches handed to inserts by this iteration
	poll    time.Duration
	add     time.Duration // add for every record + takeReady, under mu
	flush   time.Duration // waiting for inserts (backpressure or shutdown)
	commit  time.Duration
}

// loopWindow accumulates iterations. Only Run's goroutine touches it.
type loopWindow struct {
	start                    time.Time
	iters, emptyPolls        int64
	msgs, maxMsgs            int64
	partFetches              int64
	flushes, batches         int64
	poll, add, flush, commit time.Duration
	maxFlush                 time.Duration
}

func (w *loopWindow) observe(it iteration) {
	w.iters++
	if it.msgs == 0 {
		w.emptyPolls++
	}
	w.msgs += int64(it.msgs)
	w.maxMsgs = max(w.maxMsgs, int64(it.msgs))
	w.partFetches += int64(it.parts)
	if it.batches > 0 {
		w.flushes++
		w.batches += int64(it.batches)
		w.maxFlush = max(w.maxFlush, it.flush)
	}
	w.poll += it.poll
	w.add += it.add
	w.flush += it.flush
	w.commit += it.commit
}

// fetchStats counts what franz-go decoded from fetch responses, via the
// OnFetchBatchRead hook. That hook runs on the client's fetch goroutines,
// hence atomics. CompressedBytes is what FetchMaxPartitionBytes limits, so
// compressed bytes per partition-fetch, set against that limit, says
// whether fetches come back full.
type fetchStats struct {
	batches, records, compressed, uncompressed atomic.Int64
}

var _ kgo.HookFetchBatchRead = (*fetchStats)(nil)

func (f *fetchStats) OnFetchBatchRead(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.FetchBatchMetrics) {
	f.batches.Add(1)
	f.records.Add(int64(m.NumRecords))
	f.compressed.Add(int64(m.CompressedBytes))
	f.uncompressed.Add(int64(m.UncompressedBytes))
}

type fetchSnapshot struct{ batches, records, compressed, uncompressed int64 }

// take returns the counts since the last take and resets them.
func (f *fetchStats) take() fetchSnapshot {
	return fetchSnapshot{f.batches.Swap(0), f.records.Swap(0), f.compressed.Swap(0), f.uncompressed.Swap(0)}
}

// attrs renders a window as slog key/value pairs. rows is the number of
// rows inserted during the window; partCap is FetchMaxPartitionBytes.
//
// The broker fills a partition's share of a fetch with whole record
// batches up to partCap (only a first batch larger than partCap is sent
// alone, whole), so full fetches show as kib_per_part_fetch a little under
// cap_kib, never far above it.
func (w *loopWindow) attrs(now time.Time, f fetchSnapshot, rows int64, partCap int32) []any {
	wall := now.Sub(w.start)
	pct := func(d time.Duration) float64 {
		if wall <= 0 {
			return 0
		}
		return round1(100 * float64(d) / float64(wall))
	}
	per := func(a, b int64) float64 {
		if b == 0 {
			return 0
		}
		return round1(float64(a) / float64(b))
	}
	secs := wall.Seconds()
	rate := 0.0
	if secs > 0 {
		rate = float64(rows) / secs
	}
	other := wall - w.poll - w.add - w.flush - w.commit
	return []any{
		"window_s", round1(secs),
		"iters", w.iters,
		"rows_per_s", int64(rate),
		// Where the wall time went. Serial loop: these sum to ~100.
		"poll_pct", pct(w.poll), "add_pct", pct(w.add), "flush_pct", pct(w.flush),
		"commit_pct", pct(w.commit), "other_pct", pct(other),
		// What one poll brings.
		"polls_empty", w.emptyPolls, "msgs", w.msgs,
		"msgs_per_poll", per(w.msgs, w.iters-w.emptyPolls), "max_msgs_per_poll", w.maxMsgs,
		"parts_per_poll", per(w.partFetches, w.iters-w.emptyPolls),
		"kib_per_part_fetch", per(f.compressed>>10, w.partFetches), "cap_kib", int64(partCap) >> 10,
		"fetch_compressed_mib", f.compressed >> 20, "fetch_uncompressed_mib", f.uncompressed >> 20,
		// Flushes: how long the loop stood still for ClickHouse.
		"flushes", w.flushes, "batches_per_flush", per(w.batches, w.flushes),
		"avg_flush_ms", per(w.flush.Milliseconds(), w.flushes), "max_flush_ms", w.maxFlush.Milliseconds(),
		"avg_iter_ms", per(wall.Milliseconds(), w.iters),
	}
}

func round1(x float64) float64 { return float64(int64(x*10+0.5)) / 10 }
