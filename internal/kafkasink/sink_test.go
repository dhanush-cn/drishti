package kafkasink

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dhanush-cn/drishti/internal/ingest"
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
