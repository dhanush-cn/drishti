package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dhanush-cn/drishti/internal/kdump"
	"github.com/dhanush-cn/drishti/internal/partitioning"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type cluster struct {
	t     *testing.T
	c     *kfake.Cluster
	cl    *kgo.Client
	adm   *kadm.Client
	addrs string
}

func newCluster(t *testing.T) *cluster {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return &cluster{t: t, c: c, cl: cl, adm: kadm.NewClient(cl), addrs: strings.Join(c.ListenAddrs(), ",")}
}

func (k *cluster) create(topic string, parts int32, retention string) {
	k.t.Helper()
	cfg := map[string]*string{"retention.ms": &retention, "retention.bytes": &retention}
	if _, err := k.adm.CreateTopic(context.Background(), parts, 1, cfg, topic); err != nil {
		k.t.Fatal(err)
	}
}

func (k *cluster) consumeAll(topic string, n int) []*kgo.Record {
	k.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(k.c.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		k.t.Fatal(err)
	}
	defer cl.Close()
	var out []*kgo.Record
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			k.t.Fatalf("%s: got %d records, want %d", topic, len(out), n)
		}
		out = append(out, fs.Records()...)
	}
	return out
}

// recording writes a .kd.gz with records on both topics, old timestamps,
// several partitions.
func recording(t *testing.T) (string, []*kgo.Record) {
	old := time.Date(2026, 11, 3, 17, 0, 0, 0, time.UTC) // a week before "now": 24 h retention would drop it
	var recs []*kgo.Record
	for i := range 12 {
		topic := []string{"otlp.logs", "otlp.metrics"}[i%2]
		recs = append(recs, &kgo.Record{Topic: topic, Partition: int32(i % 3), Offset: int64(100 + i), Key: []byte("svc" + string(rune('a'+i%4))),
			Value: []byte{byte(i), 1, 2, 3}, Timestamp: old.Add(time.Duration(i) * time.Second)})
	}
	recs = append(recs, &kgo.Record{Topic: "other", Partition: 0, Value: []byte("not loaded"), Timestamp: old})
	path := filepath.Join(t.TempDir(), "rec.kd.gz")
	f, _ := os.Create(path)
	w, err := kdump.NewWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	f.Close()
	return path, recs
}

func TestLoad_ReplaysRecordsWithTheirPartitionKeyAndTimestamp(t *testing.T) {
	k := newCluster(t)
	k.create("eval.otlp.logs", 3, "-1")
	k.create("eval.otlp.metrics", 3, "-1")
	path, recs := recording(t)
	if err := loadCmd(context.Background(), []string{"-brokers", k.addrs, "-in", path}); err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{"otlp.logs", "otlp.metrics"} {
		got := k.consumeAll("eval."+topic, 6)
		if len(got) != 6 {
			t.Fatalf("%s: %d records, want 6", topic, len(got))
		}
		byVal := map[string]*kgo.Record{}
		for _, r := range got {
			byVal[string(r.Value)] = r
		}
		for _, w := range recs {
			if w.Topic != topic {
				continue
			}
			g := byVal[string(w.Value)]
			if g == nil {
				t.Fatalf("%s: record %v missing", topic, w.Value)
			}
			if g.Partition != w.Partition || !bytes.Equal(g.Key, w.Key) || !g.Timestamp.Equal(w.Timestamp) {
				t.Errorf("%s: got p%d key %q ts %s, want p%d key %q ts %s", topic, g.Partition, g.Key, g.Timestamp, w.Partition, w.Key, w.Timestamp)
			}
		}
	}
}

func TestLoad_RefusesTopicsThatWouldDuplicateOrDeleteTheRecording(t *testing.T) {
	path, _ := recording(t)

	k := newCluster(t)
	k.create("eval.otlp.logs", 3, "-1")
	k.create("eval.otlp.metrics", 3, "-1")
	if err := loadCmd(context.Background(), []string{"-brokers", k.addrs, "-in", path}); err != nil {
		t.Fatal(err)
	}
	err := loadCmd(context.Background(), []string{"-brokers", k.addrs, "-in", path})
	if err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("second load: err = %v, want a refusal (it would store everything twice)", err)
	}

	k2 := newCluster(t)
	k2.create("eval.otlp.logs", 3, "86400000")
	k2.create("eval.otlp.metrics", 3, "-1")
	err = loadCmd(context.Background(), []string{"-brokers", k2.addrs, "-in", path})
	if err == nil || !strings.Contains(err.Error(), "retention") {
		t.Fatalf("24 h retention: err = %v, want a refusal (Kafka would delete week-old records)", err)
	}

	k3 := newCluster(t)
	k3.create("eval.otlp.logs", 2, "-1")
	k3.create("eval.otlp.metrics", 3, "-1")
	err = loadCmd(context.Background(), []string{"-brokers", k3.addrs, "-in", path})
	if err == nil || !strings.Contains(err.Error(), "partitions") {
		t.Fatalf("2 partitions for a 3-partition recording: err = %v, want a refusal", err)
	}

	k4 := newCluster(t)
	err = loadCmd(context.Background(), []string{"-brokers", k4.addrs, "-in", path})
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("no topics: err = %v, want a refusal", err)
	}
}

func TestLoadEvents_KeysByServiceOnTheDetectorsPartitionAtDetectedAt(t *testing.T) {
	k := newCluster(t)
	k.create("eval.anomalies", 6, "-1")
	path := filepath.Join(t.TempDir(), "events.jsonl")
	lines := `{"schema":1,"id":"a1","event":"opened","service":"payment","detected_at":"2026-11-03T17:08:55Z"}
{"schema":1,"id":"a2","event":"opened","service":"cart","detected_at":"2026-11-03T17:09:01.5Z"}
`
	os.WriteFile(path, []byte(lines), 0o644)
	if err := loadEventsCmd(context.Background(), []string{"-brokers", k.addrs, "-in", path}); err != nil {
		t.Fatal(err)
	}
	got := k.consumeAll("eval.anomalies", 2)
	for _, r := range got {
		want, _ := partitioning.ForKey(partitioning.New(), "eval.anomalies", r.Key, 6)
		if int(r.Partition) != want {
			t.Errorf("%s on partition %d, the detector's partitioner says %d", r.Key, r.Partition, want)
		}
		switch string(r.Key) {
		case "payment":
			if !r.Timestamp.Equal(time.Date(2026, 11, 3, 17, 8, 55, 0, time.UTC)) {
				t.Errorf("payment timestamp %s", r.Timestamp)
			}
		case "cart":
			if !r.Timestamp.Equal(time.Date(2026, 11, 3, 17, 9, 1, 500e6, time.UTC)) {
				t.Errorf("cart timestamp %s", r.Timestamp)
			}
		default:
			t.Errorf("unexpected key %q", r.Key)
		}
	}
	bad := filepath.Join(t.TempDir(), "bad.jsonl")
	os.WriteFile(bad, []byte(`{"id":"x"}`+"\n"), 0o644)
	k2 := newCluster(t)
	k2.create("eval.anomalies", 6, "-1")
	if err := loadEventsCmd(context.Background(), []string{"-brokers", k2.addrs, "-in", bad}); err == nil {
		t.Fatal("a line without service/detected_at was accepted")
	}
}
