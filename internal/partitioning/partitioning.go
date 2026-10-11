// Package partitioning owns the one Kafka partitioner Drishti uses.
//
// The receiver configures its producer with New(), and anything that needs
// to predict where a key lands (the load generator's partition accounting,
// docs/loadgen.md §7) calls ForKey on the same kind of object. The hash, the
// positive mask and the modulo therefore come from one implementation:
// franz-go's. Nothing here reimplements murmur2.
package partitioning

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

// New returns the partitioner the receiver produces with.
//
// It is franz-go's default, made explicit: UniformBytesPartitioner(64KiB,
// adaptive, keys=true, murmur2). Keyed records (every Drishti message is
// keyed by service.name) are hashed exactly like the Java client, so the
// key -> partition mapping of data already in Kafka does not change.
// TestNew_MatchesLibraryDefault proves the equivalence; the pinned pairs in
// TestForKey_ObservedPairs prove it against what Kafka actually stored.
func New() kgo.Partitioner {
	return kgo.UniformBytesPartitioner(64<<10, true, true, nil)
}

// ErrNilKey: unkeyed records are spread by buffered bytes and backlog, so
// they have no deterministic partition to predict.
var ErrNilKey = errors.New("partitioning: key is nil; unkeyed records have no predictable partition")

// ForKey returns the partition index (0..n-1) that p assigns to a record
// with this key on a topic with n partitions.
//
// n must come from topic metadata (kadm), never from a flag: a topic
// recreated with a different count would otherwise give a confident, wrong
// answer.
func ForKey(p kgo.Partitioner, topic string, key []byte, n int) (int, error) {
	if key == nil {
		return 0, ErrNilKey
	}
	if n <= 0 {
		return 0, errors.New("partitioning: partition count must be positive")
	}
	tp := p.ForTopic(topic)
	r := &kgo.Record{Topic: topic, Key: key}
	// The uniform-bytes partitioner implements only PartitionByBackup; its
	// plain Partition method panics by design. For keyed records it hashes
	// the key and returns before touching the backlog iterator.
	if bp, ok := tp.(kgo.TopicBackupPartitioner); ok {
		return bp.PartitionByBackup(r, n, &noBacklog{n: n}), nil
	}
	return tp.Partition(r, n), nil
}

// noBacklog reports zero buffered records for every partition. It is only
// consulted for unkeyed records, which ForKey rejects; it exists so the
// interface is satisfied safely rather than with a nil.
type noBacklog struct{ n, i int }

func (b *noBacklog) Next() (int, int64) { i := b.i; b.i++; return i, 0 }
func (b *noBacklog) Rem() int           { return b.n - b.i }
