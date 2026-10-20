package storage

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// batch accumulates one Kafka partition's rows, already RowBinary-encoded,
// so a flush is a single HTTP body with no re-encoding.
//
// One batch = one partition, never mixed, for two reasons:
//   - Offsets are only ordered within a partition, so "this insert covers
//     offsets [first, last] of partition p" is a well-defined claim that
//     the commit and the resume logic can rely on.
//   - The dedup token can name exactly that range.
//
// One batch = one ClickHouse partition (day of received_at) too: an insert
// that spans two days writes two parts, and two parts from one insert are
// not atomic (one can land without the other), which would break the
// offsets-in-the-part resume. A message is never split across batches.
type batch struct {
	topic     string
	partition int32
	day       int64 // received_at day (unix days, UTC)
	buf       rowBuf
	rows      int
	first     int64
	last      *kgo.Record // committed after the flush (offset+1)
	started   time.Time   // wall time of the first message: age-based flush
}

func dayOf(t time.Time) int64 { return t.UTC().Unix() / 86400 }

// token names the batch for insert_deduplication_token. A retry of the
// same batch (same first..last, same bytes) is deduplicated by ClickHouse.
func (b *batch) token() string {
	return fmt.Sprintf("%s-%d-%d-%d", b.topic, b.partition, b.first, b.last.Offset)
}

// Limits decide when a batch is flushed: whichever comes first.
//
// MaxRows and MaxBytes must stay below the server's insert block limits
// (min_insert_block_size_rows 1,048,449 / min_insert_block_size_bytes
// 256 MiB by default): a larger insert is cut into several blocks, i.e.
// several parts, and stops being atomic. Validate enforces it.
type Limits struct {
	MaxRows  int
	MaxBytes int
	MaxWait  time.Duration // bounds event → queryable when traffic is low
}

func (l Limits) Validate() error {
	switch {
	case l.MaxRows < 1 || l.MaxRows >= 1_048_449:
		return fmt.Errorf("batch rows %d: must be 1..1048448 (below ClickHouse's insert block size, or one insert becomes several parts)", l.MaxRows)
	case l.MaxBytes < 1 || l.MaxBytes > 200<<20:
		// The byte check runs after a whole message is added, so a batch
		// can overshoot by one message; 200 MiB leaves that margin below
		// the server's 256 MiB block threshold.
		return fmt.Errorf("batch bytes %d: must be 1..200 MiB (below ClickHouse's 256 MiB insert block bytes, with one message of margin)", l.MaxBytes)
	case l.MaxWait <= 0:
		return fmt.Errorf("batch wait must be > 0")
	}
	return nil
}

func (b *batch) full(l Limits) bool { return b.rows >= l.MaxRows || len(b.buf.b) >= l.MaxBytes }
func (b *batch) due(l Limits, now time.Time) bool {
	return b.rows > 0 && (b.full(l) || now.Sub(b.started) >= l.MaxWait)
}
