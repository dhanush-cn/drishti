package kdump

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Range is one partition's offsets [Start, End) to dump.
type Range struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
	Records   int64  `json:"records_written"`
}

// Ranges turns a time window [from, to) into offset ranges per partition:
// Start is the first offset whose timestamp is at or after from, End the
// first at or after to (the end offset if none is). Offsets, not timestamps,
// bound the dump: they are exactly what a consumer seeking by time reads, and
// they are recorded in the manifest so the dump can be checked against Kafka.
//
// A topic that doesn't exist is an error, not an empty dump: a typo in -topics
// must not produce a capture that silently lacks a signal.
func Ranges(ctx context.Context, adm *kadm.Client, topics []string, from, to time.Time) ([]Range, error) {
	if !to.After(from) {
		return nil, fmt.Errorf("empty window: from %s is not before to %s", from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	starts, err := adm.ListOffsetsAfterMilli(ctx, from.UnixMilli(), topics...)
	if err != nil {
		return nil, fmt.Errorf("offsets at from: %w", err)
	}
	ends, err := adm.ListOffsetsAfterMilli(ctx, to.UnixMilli(), topics...)
	if err != nil {
		return nil, fmt.Errorf("offsets at to: %w", err)
	}
	var out []Range
	for _, t := range topics {
		ps, ok := starts[t]
		if !ok {
			return nil, fmt.Errorf("topic %q not found", t)
		}
		for p, s := range ps {
			if s.Err != nil {
				return nil, fmt.Errorf("topic %q partition %d: %w", t, p, s.Err)
			}
			e, ok := ends.Lookup(t, p)
			if !ok || e.Err != nil {
				return nil, fmt.Errorf("topic %q partition %d: no end offset (%v)", t, p, e.Err)
			}
			out = append(out, Range{Topic: t, Partition: p, Start: s.Offset, End: max(e.Offset, s.Offset)})
		}
	}
	slices.SortFunc(out, func(a, b Range) int {
		if a.Topic != b.Topic {
			if a.Topic < b.Topic {
				return -1
			}
			return 1
		}
		return int(a.Partition - b.Partition)
	})
	return out, nil
}

// Dump consumes every range to its end and writes the records to w. It uses
// direct partition consumption, no consumer group: nothing is committed and
// no group's offsets move, so it can run next to the live consumers.
// Records[i] of each range is filled in.
func Dump(ctx context.Context, brokers []string, ranges []Range, w *Writer) error {
	todo := map[string]map[int32]kgo.Offset{}
	left := map[string]map[int32]*Range{}
	for i := range ranges {
		r := &ranges[i]
		r.Records = 0
		if r.End <= r.Start {
			continue
		}
		if todo[r.Topic] == nil {
			todo[r.Topic] = map[int32]kgo.Offset{}
			left[r.Topic] = map[int32]*Range{}
		}
		todo[r.Topic][r.Partition] = kgo.NewOffset().At(r.Start)
		left[r.Topic][r.Partition] = r
	}
	if len(todo) == 0 {
		return nil
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumePartitions(todo),
		kgo.ClientID("drishti-kafkadump"), kgo.FetchMaxPartitionBytes(4<<20))
	if err != nil {
		return err
	}
	defer cl.Close()
	open := 0
	for _, ps := range left {
		open += len(ps)
	}
	lastProgress := time.Now()
	for open > 0 {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		if err := ctx.Err(); err != nil {
			return err
		}
		var ferr error
		fs.EachError(func(t string, p int32, err error) {
			if ferr == nil && pctx.Err() == nil {
				ferr = fmt.Errorf("fetch %s/%d: %w", t, p, err)
			}
		})
		if ferr != nil {
			return ferr
		}
		var werr error
		fs.EachRecord(func(rec *kgo.Record) {
			r := left[rec.Topic][rec.Partition]
			if werr != nil || r == nil || rec.Offset >= r.End {
				return
			}
			if werr = w.Write(rec); werr != nil {
				return
			}
			r.Records++
			lastProgress = time.Now()
			// Offsets are contiguous on these topics (no compaction, no
			// transactions: the receiver's producer is idempotent only), so
			// the partition is done once its last offset below End arrived.
			if rec.Offset == r.End-1 {
				delete(left[rec.Topic], rec.Partition)
				open--
			}
		})
		if werr != nil {
			return werr
		}
		if open > 0 && time.Since(lastProgress) > 30*time.Second {
			var stuck []string
			for t, ps := range left {
				for p, r := range ps {
					stuck = append(stuck, fmt.Sprintf("%s/%d (want offsets up to %d, got %d records)", t, p, r.End-1, r.Records))
				}
			}
			slices.Sort(stuck)
			return fmt.Errorf("no progress for 30 s; unfinished: %v (offset gap or retention deleted the range?)", stuck)
		}
	}
	return nil
}
