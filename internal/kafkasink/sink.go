// Package kafkasink publishes ingest messages to Kafka with franz-go.
package kafkasink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dhanush-cn/drishti/internal/ingest"
	"github.com/dhanush-cn/drishti/internal/partitioning"
)

type Config struct {
	Brokers      []string
	LogsTopic    string
	MetricsTopic string

	// Linger: how long a partition batch waits for more records before it is
	// sent. Small linger = lower latency, more requests. Set explicitly so
	// the trade-off is visible instead of inherited from a library default.
	Linger time.Duration

	// MaxBufferedBytes bounds memory held in the producer while Kafka is
	// slow. When it is full, Produce blocks; with the export deadline that
	// becomes a retryable error to the sender (backpressure), never an
	// unbounded queue in this process.
	MaxBufferedBytes int

	// BatchMaxBytes must stay at or below the topic's max.message.bytes.
	BatchMaxBytes int32

	// DeliveryTimeout fails a record that has not been delivered this long
	// after it was produced. franz-go's default is unlimited: during a long
	// outage, records whose export already failed (and that the sender is
	// retrying) would otherwise pin buffer space indefinitely.
	DeliveryTimeout time.Duration

	// Compression of produce batches: lz4 (default), zstd, snappy, gzip or
	// none. Consumers need no setting: the codec travels in each batch.
	Compression string
}

// Codec maps a -compression name to franz-go's codec. Unknown names are an
// error, not a silent fallback: a typo would otherwise benchmark lz4 under
// another label.
func Codec(name string) (kgo.CompressionCodec, error) {
	switch name {
	case "", "lz4":
		return kgo.Lz4Compression(), nil
	case "zstd":
		return kgo.ZstdCompression(), nil
	case "snappy":
		return kgo.SnappyCompression(), nil
	case "gzip":
		return kgo.GzipCompression(), nil
	case "none":
		return kgo.NoCompression(), nil
	}
	return kgo.CompressionCodec{}, fmt.Errorf("unknown compression %q (lz4, zstd, snappy, gzip, none)", name)
}

type Sink struct {
	cl     *kgo.Client
	topics map[ingest.Signal]string
}

func New(cfg Config) (*Sink, error) {
	codec, err := Codec(cfg.Compression)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("drishti-receiver"),
		// acks=all is required for the idempotent producer (franz-go's
		// default): broker-side retries cannot create duplicates. With
		// replication factor 1 it costs nothing extra today and is correct
		// if replicas are added later.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(cfg.Linger),
		// lz4 by default: cheap CPU on the hot path. -compression switches
		// codecs so week3b.sh can compare receiver CPU, Kafka bytes and
		// consumer CPU per codec (docs/receiver.md decision 6).
		kgo.ProducerBatchCompression(codec),
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),
		kgo.MaxBufferedBytes(cfg.MaxBufferedBytes),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		// Explicit, not inherited: the load generator predicts key ->
		// partition with the same constructor (docs/loadgen.md §7). It is
		// franz-go's default (murmur2 for keyed records), so existing data
		// keeps its mapping.
		kgo.RecordPartitioner(partitioning.New()),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Sink{
		cl: cl,
		topics: map[ingest.Signal]string{
			ingest.Logs:    cfg.LogsTopic,
			ingest.Metrics: cfg.MetricsTopic,
		},
	}, nil
}

// CheckTopics fails fast at startup if a topic is missing. Auto-create is
// off on the broker on purpose: partition count is a design decision
// (scripts/create-topics.sh), not something a typo should create.
func (s *Sink) CheckTopics(ctx context.Context) error {
	want := make([]string, 0, len(s.topics))
	for _, t := range s.topics {
		want = append(want, t)
	}
	details, err := kadm.NewClient(s.cl).ListTopics(ctx, want...)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	var errs []error
	for _, t := range want {
		d, ok := details[t]
		if !ok || d.Err != nil {
			errs = append(errs, fmt.Errorf("topic %q missing (run scripts/create-topics.sh)", t))
		}
	}
	return errors.Join(errs...)
}

// Publish produces all messages and returns only after Kafka acknowledged
// every one, or ctx ended. If some succeed and some fail, the sender retries
// the whole export and the successful ones are written twice: delivery is
// at-least-once, and the storage consumer must deduplicate (week 2).
func (s *Sink) Publish(ctx context.Context, sig ingest.Signal, msgs []ingest.Message) error {
	topic, ok := s.topics[sig]
	if !ok {
		return fmt.Errorf("no topic for signal %q", sig)
	}
	if len(msgs) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		// Timestamp is left zero: franz-go stamps produce time, which is the
		// receive time used later for event->queryable latency.
		recs[i] = &kgo.Record{
			Topic: topic,
			Key:   m.Key,
			Value: m.Value,
			Headers: []kgo.RecordHeader{
				{Key: "otlp.signal", Value: []byte(sig)},
				{Key: "content-type", Value: []byte("application/x-protobuf")},
			},
		}
	}
	// Not ProduceSync. Found in the live kill test: once records are in
	// franz-go's buffer and the broker dies after metadata was fetched,
	// ProduceSync keeps waiting on delivery retries past ctx's deadline, so
	// the export hung until the sender gave up (DeadlineExceeded instead of
	// our UNAVAILABLE). ctx bounds getting INTO the buffer, not delivery.
	//
	// So we produce asynchronously and stop waiting when ctx ends. Records
	// already buffered may still be delivered later; the sender got an error
	// and will retry, so they can arrive twice. That is the at-least-once
	// contract, handled by idempotent storage writes (week 2).
	// DeliveryTimeout stops such orphans from retrying forever.
	var (
		mu        sync.Mutex
		firstErr  error
		remaining = int64(len(recs))
		done      = make(chan struct{})
	)
	for _, r := range recs {
		s.cl.Produce(ctx, r, func(_ *kgo.Record, err error) {
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
			}
			if atomic.AddInt64(&remaining, -1) == 0 {
				close(done)
			}
		})
	}
	select {
	case <-done:
		mu.Lock()
		defer mu.Unlock()
		return firstErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close flushes anything still buffered, then closes the client.
func (s *Sink) Close(ctx context.Context) error {
	err := s.cl.Flush(ctx)
	s.cl.Close()
	return err
}
