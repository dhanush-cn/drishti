package partitioning

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Ground truth, not a computed expectation: these are the partitions Kafka
// actually stored for these keys in the 10 Oct smoke run (cmd/otlpsmoke,
// otlp.logs, 6 partitions). If this test fails, the receiver would now route
// existing services to different partitions than the data already in the
// topic. (First written with the shorthand "svc-N" keys, it failed: murmur2
// of "svc-0" is p5. The test is not vacuous.)
func TestForKey_ObservedPairs(t *testing.T) {
	observed := map[string]int{"smoke-svc-0": 2, "smoke-svc-1": 0, "smoke-svc-2": 0}
	p := New()
	for key, want := range observed {
		got, err := ForKey(p, "otlp.logs", []byte(key), 6)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("key %q -> partition %d, observed in Kafka: %d", key, got, want)
		}
	}
}

func TestForKey_RejectsNilKeyAndBadCount(t *testing.T) {
	if _, err := ForKey(New(), "t", nil, 6); !errors.Is(err, ErrNilKey) {
		t.Fatalf("nil key: got %v, want ErrNilKey", err)
	}
	if _, err := ForKey(New(), "t", []byte("k"), 0); err == nil {
		t.Fatal("n=0: want error")
	}
}

// The explicit constructor must reproduce franz-go's default exactly. A
// client built with NO partitioner option (the library default, which the
// receiver used before this package existed) produces real records to an
// in-memory cluster; every one must land where ForKey(New()) predicts.
func TestNew_MatchesLibraryDefault(t *testing.T) {
	const topic, parts = "otlp.logs", 6
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(parts, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...)) // default partitioner
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	keys := []string{"unknown_service", "cart", "checkout", "frontend", "ad"}
	for i := 0; i < 40; i++ {
		keys = append(keys, fmt.Sprintf("svc-%d", i))
	}
	for _, k := range keys {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(k)}).FirstErr(); err != nil {
			t.Fatalf("produce %q: %v", k, err)
		}
	}

	cons, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cons.Close()

	p := New()
	seen := 0
	for seen < len(keys) {
		f := cons.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out after %d of %d records", seen, len(keys))
		}
		f.EachRecord(func(r *kgo.Record) {
			seen++
			want, err := ForKey(p, topic, r.Key, parts)
			if err != nil {
				t.Fatal(err)
			}
			if int(r.Partition) != want {
				t.Errorf("key %q: library default put it on %d, ForKey(New()) predicts %d", r.Key, r.Partition, want)
			}
		})
	}
}
