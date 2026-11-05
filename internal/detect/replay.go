package detect

import (
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Detector routes records to the shard of their input partition (topic,
// partition). The live consumer and the replay both go through it, so tuning
// on a recording exercises the code that runs live.
//
// One shard per input partition, not per partition number: otlp.logs/0 and
// otlp.metrics/0 are fetched independently and can be minutes apart (a
// rewind catching up, a recording whose file interleaves partitions as they
// were fetched). A shard's clock is its records' timestamps, so it must see
// one ordered stream. No rule combines a log series with a metric series, so
// nothing is lost by splitting them. (Found on the 3 Nov recording: with one
// shard per partition number, metric deltas 13 minutes old landed in the
// current bucket and read as 300x traffic surges.)
type Detector struct {
	cfg     Config
	version string
	shards  map[Input]*Shard

	// OnShard, if set, is called for every shard created (the live consumer
	// sets DetectedAt / Replayed hooks there).
	OnShard func(*Shard)
}

func NewDetector(cfg Config, version string) *Detector {
	return &Detector{cfg: cfg, version: version, shards: map[Input]*Shard{}}
}

// Input is one input partition.
type Input struct {
	Topic     string
	Partition int32
}

// Shard returns the shard of one input partition, creating it.
func (d *Detector) Shard(topic string, p int32) *Shard {
	in := Input{topic, p}
	s := d.shards[in]
	if s == nil {
		s = NewShard(d.cfg, topic, p, d.version)
		if d.OnShard != nil {
			d.OnShard(s)
		}
		d.shards[in] = s
	}
	return s
}

// Shards returns every shard.
func (d *Detector) Shards() map[Input]*Shard { return d.shards }

// Drop forgets an input partition (it moved to another detector instance).
func (d *Detector) Drop(topic string, p int32) { delete(d.shards, Input{topic, p}) }

// Record feeds one Kafka record to its shard. Callers that process
// partitions in parallel call (*Shard).Record on each shard instead: shards
// are independent.
func (d *Detector) Record(r *kgo.Record) []Event {
	return d.Shard(r.Topic, r.Partition).Record(r)
}

// Next is a source of records, e.g. (*kdump.Reader).Next.
type Next func() (*kgo.Record, error)

// Replay feeds every record from next, in order, then advances all shards
// past the end by the resolve time so open anomalies resolve. Each shard's
// clock is its own records' timestamps, exactly as live.
func (d *Detector) Replay(next Next, emit func(Event)) error {
	var last time.Time
	for {
		r, err := next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if r.Timestamp.After(last) {
			last = r.Timestamp
		}
		for _, e := range d.Record(r) {
			emit(e)
		}
	}
	// In input order, not map order, so two replays of one recording write
	// byte-identical output.
	ins := sortedInputs(d)
	for _, in := range ins {
		for _, e := range d.shards[in].Advance(last.Add(d.cfg.ResolveAfter + 2*d.cfg.Bucket)) {
			emit(e)
		}
	}
	return nil
}

func sortedInputs(d *Detector) []Input {
	ins := make([]Input, 0, len(d.shards))
	for in := range d.shards {
		ins = append(ins, in)
	}
	slices.SortFunc(ins, func(a, b Input) int {
		if c := strings.Compare(a.Topic, b.Topic); c != 0 {
			return c
		}
		return int(a.Partition - b.Partition)
	})
	return ins
}
