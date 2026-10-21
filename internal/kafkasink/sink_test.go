package kafkasink

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dhanush-cn/drishti/internal/ingest"
	"github.com/dhanush-cn/drishti/internal/partitioning"
)

// kfake is franz-go's in-memory Kafka: it speaks the real wire protocol,
// so these tests exercise the real producer, not a mock.
func cluster(t *testing.T, topics ...string) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(6, topics...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func newSink(t *testing.T, brokers []string) *Sink {
	t.Helper()
	s, err := New(Config{
		Brokers: brokers, LogsTopic: "otlp.logs", MetricsTopic: "otlp.metrics",
		Linger: 5 * time.Millisecond, MaxBufferedBytes: 16 << 20, BatchMaxBytes: 1_000_000,
		DeliveryTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// Bounded: in the unreachable-broker test a flush can never finish.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.Close(ctx)
	})
	return s
}

func TestPublish_RoundTripKeysHeadersAndPartitioning(t *testing.T) {
	brokers := cluster(t, "otlp.logs", "otlp.metrics")
	s := newSink(t, brokers)

	msgs := []ingest.Message{
		{Key: []byte("cart"), Value: []byte("v1")},
		{Key: []byte("checkout"), Value: []byte("v2")},
		{Key: []byte("cart"), Value: []byte("v3")},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Publish(ctx, ingest.Logs, msgs); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("otlp.logs"))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var got []*kgo.Record
	for len(got) < len(msgs) {
		f := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out after %d of %d records", len(got), len(msgs))
		}
		got = append(got, f.Records()...)
	}

	partOf := map[string]int32{}
	for _, r := range got {
		key := string(r.Key)
		if p, seen := partOf[key]; seen && p != r.Partition {
			t.Fatalf("key %q landed on partitions %d and %d; per-service ordering broken", key, p, r.Partition)
		}
		partOf[key] = r.Partition
		var signal string
		for _, h := range r.Headers {
			if h.Key == "otlp.signal" {
				signal = string(h.Value)
			}
		}
		if signal != "logs" {
			t.Fatalf("otlp.signal header=%q, want logs", signal)
		}
		if r.Timestamp.IsZero() {
			t.Fatal("record has no produce timestamp")
		}
	}
	if _, ok := partOf["cart"]; !ok || len(partOf) != 2 {
		t.Fatalf("keys not preserved: got %v, want cart and checkout", partOf)
	}
}

func TestCheckTopics_FailsFastWhenMissing(t *testing.T) {
	brokers := cluster(t, "otlp.logs") // metrics topic deliberately absent
	s := newSink(t, brokers)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := s.CheckTopics(ctx)
	if err == nil || !strings.Contains(err.Error(), "otlp.metrics") {
		t.Fatalf("want error naming otlp.metrics, got %v", err)
	}
}

func TestCheckTopics_OKWhenPresent(t *testing.T) {
	s := newSink(t, cluster(t, "otlp.logs", "otlp.metrics"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.CheckTopics(ctx); err != nil {
		t.Fatal(err)
	}
}

// The failure the live `docker compose stop kafka` test exposed: the broker
// was healthy (metadata cached, connection open) and then died. Publish must
// still return at its deadline. With ProduceSync this test hangs.
func TestPublish_BrokerDiesMidRunReturnsAtDeadline(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(6, "otlp.logs", "otlp.metrics"))
	if err != nil {
		t.Fatal(err)
	}
	s := newSink(t, c.ListenAddrs())
	msg := []ingest.Message{{Key: []byte("cart"), Value: []byte("v")}}

	warm, cancelWarm := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWarm()
	if err := s.Publish(warm, ingest.Logs, msg); err != nil {
		t.Fatalf("warm-up publish: %v", err)
	}

	c.Close() // the broker dies after the client learned about it

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Publish(ctx, ingest.Logs, msg) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("publish to a dead broker reported success")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("publish ignored its deadline after the broker died")
	}
}

// Kafka unreachable: Publish must give up when ctx ends instead of hanging,
// so the export returns UNAVAILABLE in time and the sender keeps the data.
func TestPublish_UnreachableKafkaReturnsAtDeadline(t *testing.T) {
	s := newSink(t, []string{"127.0.0.1:1"}) // nothing listens here
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- s.Publish(ctx, ingest.Logs, []ingest.Message{{Key: []byte("k"), Value: []byte("v")}}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("publish to an unreachable broker reported success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publish ignored its deadline")
	}
}

// The sink must route every key exactly where partitioning.ForKey predicts,
// because the load generator's partition accounting (docs/loadgen.md §7)
// relies on that prediction. Swapping the sink's partitioner for anything
// else (e.g. a Sarama-compatible hasher) fails here.
func TestPublish_PartitionsMatchSharedPartitioner(t *testing.T) {
	brokers := cluster(t, "otlp.logs", "otlp.metrics")
	s := newSink(t, brokers)

	var msgs []ingest.Message
	for i := 0; i < 30; i++ {
		msgs = append(msgs, ingest.Message{Key: []byte(fmt.Sprintf("svc-%d", i)), Value: []byte("v")})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Publish(ctx, ingest.Logs, msgs); err != nil {
		t.Fatalf("publish: %v", err)
	}

	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("otlp.logs"))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	p := partitioning.New()
	seen := 0
	for seen < len(msgs) {
		f := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out after %d of %d records", seen, len(msgs))
		}
		f.EachRecord(func(r *kgo.Record) {
			seen++
			want, err := partitioning.ForKey(p, "otlp.logs", r.Key, 6)
			if err != nil {
				t.Fatal(err)
			}
			if int(r.Partition) != want {
				t.Errorf("key %q on partition %d, shared partitioner predicts %d", r.Key, r.Partition, want)
			}
		})
	}
}

// codecSeen records the compression codec of every batch a consumer reads.
type codecSeen struct {
	mu    sync.Mutex
	types map[uint8]int
}

func (c *codecSeen) OnFetchBatchRead(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.FetchBatchMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.types[m.CompressionType]++
}

// Each -compression name must put that codec on the wire (FetchBatchMetrics
// reports the batch's codec: 0 none, 1 gzip, 2 snappy, 3 lz4, 4 zstd), and
// the consumer must read it back without any setting of its own.
func TestPublish_CompressionCodecOnTheWire(t *testing.T) {
	for name, want := range map[string]uint8{"none": 0, "gzip": 1, "snappy": 2, "lz4": 3, "": 3, "zstd": 4} {
		t.Run("codec="+name, func(t *testing.T) {
			brokers := cluster(t, "otlp.logs", "otlp.metrics")
			s, err := New(Config{
				Brokers: brokers, LogsTopic: "otlp.logs", MetricsTopic: "otlp.metrics",
				Linger: time.Millisecond, MaxBufferedBytes: 16 << 20, BatchMaxBytes: 1_000_000,
				DeliveryTimeout: 30 * time.Second, Compression: name,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer s.cl.Close()
			value := bytes.Repeat([]byte("otlp-payload "), 400)
			msgs := []ingest.Message{{Key: []byte("cart"), Value: value}, {Key: []byte("ad"), Value: value}}
			if err := s.Publish(context.Background(), ingest.Logs, msgs); err != nil {
				t.Fatal(err)
			}
			seen := &codecSeen{types: map[uint8]int{}}
			cons, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics("otlp.logs"),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.WithHooks(seen))
			if err != nil {
				t.Fatal(err)
			}
			defer cons.Close()
			got := 0
			// Generous: this checks codecs, not speed. A fresh consumer
			// needs metadata, an offset lookup and a fetch, and 5 s once
			// ran out on a VM busy with -race builds of other packages.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			var fetchErrs []error
			for got < len(msgs) && ctx.Err() == nil {
				fs := cons.PollFetches(ctx)
				fs.EachError(func(_ string, _ int32, err error) {
					if ctx.Err() == nil {
						fetchErrs = append(fetchErrs, err)
					}
				})
				fs.EachRecord(func(r *kgo.Record) {
					if !bytes.Equal(r.Value, value) {
						t.Errorf("value changed in transit")
					}
					got++
				})
			}
			if got != len(msgs) {
				t.Fatalf("read back %d of %d (fetch errors: %v)", got, len(msgs), fetchErrs)
			}
			seen.mu.Lock()
			defer seen.mu.Unlock()
			if len(seen.types) != 1 || seen.types[want] == 0 {
				t.Fatalf("batch codecs on the wire %v, want only %d", seen.types, want)
			}
		})
	}
}

func TestCodec_UnknownNameIsAnError(t *testing.T) {
	if _, err := Codec("lz-4"); err == nil {
		t.Fatal("typo accepted")
	}
	if _, err := New(Config{Brokers: []string{"localhost:1"}, Compression: "zstandard"}); err == nil {
		t.Fatal("New accepted an unknown codec")
	}
}
