package kdump

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWriterReader_RoundTripKeepsEveryField(t *testing.T) {
	recs := []*kgo.Record{
		{Topic: "otlp.logs", Partition: 3, Offset: 0, Timestamp: time.UnixMilli(1_793_000_000_123), Key: []byte("cart"), Value: []byte{0, 1, 2, 0xff}},
		{Topic: "otlp.metrics", Partition: 0, Offset: 1 << 40, Timestamp: time.UnixMilli(1_793_000_000_124), Key: nil, Value: []byte{}},
		{Topic: "otlp.logs", Partition: 5, Offset: 7, Timestamp: time.UnixMilli(1), Key: []byte("k"), Value: bytes.Repeat([]byte("v"), 70_000)},
	}
	var buf bytes.Buffer
	w, err := NewWriter(&buf)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range recs {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if got.Topic != want.Topic || got.Partition != want.Partition || got.Offset != want.Offset ||
			!got.Timestamp.Equal(want.Timestamp) || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) {
			t.Fatalf("record %d: got %s/%d@%d ts=%v key=%q len=%d, want %s/%d@%d ts=%v key=%q len=%d", i,
				got.Topic, got.Partition, got.Offset, got.Timestamp, got.Key, len(got.Value),
				want.Topic, want.Partition, want.Offset, want.Timestamp, want.Key, len(want.Value))
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last record: %v, want io.EOF", err)
	}
}

// A file cut off mid-record (a crash during dump, a partial copy to Windows)
// must fail loudly, never look like a clean shorter recording.
func TestReader_TruncatedFileIsAnErrorNotEOF(t *testing.T) {
	var raw bytes.Buffer
	w, _ := NewWriter(&raw)
	for i := 0; i < 50; i++ {
		_ = w.Write(&kgo.Record{Topic: "t", Offset: int64(i), Value: bytes.Repeat([]byte{byte(i)}, 1000)})
	}
	_ = w.Close()
	// Re-compress a truncated plain stream: the gzip layer is intact, the
	// record stream inside it stops in the middle of a record.
	r, _ := NewReader(bytes.NewReader(raw.Bytes()))
	var plain bytes.Buffer
	plain.WriteString(header)
	_, _ = io.Copy(&plain, r.br)
	cut := plain.Bytes()[:plain.Len()-500]
	var again bytes.Buffer
	gw, _ := NewWriter(&again)
	_, _ = gw.bw.Write(cut[len(header):])
	_ = gw.Close()

	rr, err := NewReader(bytes.NewReader(again.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		_, err := rr.Next()
		if err == nil {
			n++
			continue
		}
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("after %d records: %v, want io.ErrUnexpectedEOF", n, err)
		}
		break
	}
	if n != 49 {
		t.Fatalf("read %d whole records before the cut, want 49", n)
	}
}

// A cut exactly between two fields: the varint read finds no bytes at all and
// returns io.EOF, which Next must not pass on as a clean end of file.
func TestReader_CutBetweenFieldsIsAnErrorNotEOF(t *testing.T) {
	var buf bytes.Buffer
	w, _ := NewWriter(&buf)
	_ = w.Write(&kgo.Record{Topic: "t", Offset: 1, Value: []byte("whole")})
	_, _ = w.bw.Write([]byte{1, 't'}) // the next record's topic, then nothing
	_ = w.Close()
	r, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatalf("first record: %v", err)
	}
	if _, err := r.Next(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("cut after a topic field: %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestReader_RejectsOtherFiles(t *testing.T) {
	if _, err := NewReader(strings.NewReader("plain text")); !errors.Is(err, ErrFormat) {
		t.Fatalf("plain text: %v, want ErrFormat", err)
	}
}

// Dump writes exactly the records whose offsets fall in [Start, End) for
// each partition, and nothing from outside the time window. Records are
// produced with timestamps one second apart, the window cuts through the
// middle of each partition.
func TestDump_WritesExactlyTheWindow(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, "otlp.logs", "otlp.metrics"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	prod, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	t0 := time.Unix(1_793_000_000, 0)
	type key struct {
		topic string
		p     int32
		i     int
	}
	sent := map[key]string{}
	for _, topic := range []string{"otlp.logs", "otlp.metrics"} {
		for p := int32(0); p < 3; p++ {
			for i := 0; i < 20; i++ {
				v := fmt.Sprintf("%s-%d-%d", topic, p, i)
				r := &kgo.Record{Topic: topic, Partition: p, Timestamp: t0.Add(time.Duration(i) * time.Second), Value: []byte(v)}
				if err := prod.ProduceSync(context.Background(), r).FirstErr(); err != nil {
					t.Fatal(err)
				}
				sent[key{topic, p, i}] = v
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adm := kadm.NewClient(prod)
	// [t0+5s, t0+12s): records 5..11 of every partition.
	ranges, err := Ranges(ctx, adm, []string{"otlp.logs", "otlp.metrics"}, t0.Add(5*time.Second), t0.Add(12*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 6 {
		t.Fatalf("ranges = %v, want 6", ranges)
	}
	var buf bytes.Buffer
	w, _ := NewWriter(&buf)
	if err := Dump(ctx, c.ListenAddrs(), ranges, w); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for _, r := range ranges {
		if r.Start != 5 || r.End != 12 || r.Records != 7 {
			t.Errorf("%s/%d: range [%d,%d) records %d, want [5,12) records 7", r.Topic, r.Partition, r.Start, r.End, r.Records)
		}
	}
	rd, err := NewReader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	last := map[string]int64{}
	got := 0
	for {
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got++
		i := int(rec.Offset)
		if want := sent[key{rec.Topic, rec.Partition, i}]; string(rec.Value) != want || i < 5 || i > 11 {
			t.Fatalf("unexpected record %s/%d@%d %q", rec.Topic, rec.Partition, rec.Offset, rec.Value)
		}
		if !rec.Timestamp.Equal(t0.Add(time.Duration(i) * time.Second)) {
			t.Fatalf("%s/%d@%d timestamp %v", rec.Topic, rec.Partition, rec.Offset, rec.Timestamp)
		}
		k := fmt.Sprintf("%s/%d", rec.Topic, rec.Partition)
		if prev, ok := last[k]; ok && rec.Offset != prev+1 {
			t.Fatalf("%s: offset %d after %d (not in order)", k, rec.Offset, prev)
		}
		last[k] = rec.Offset
	}
	if got != 42 {
		t.Fatalf("read %d records, want 42 (6 partitions x 7)", got)
	}
}

func TestRanges_UnknownTopicIsAnError(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, "otlp.logs"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, _ := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = Ranges(ctx, kadm.NewClient(cl), []string{"otlp.logs", "otlp.metricz"}, time.Unix(0, 0), time.Now())
	if err == nil || !strings.Contains(err.Error(), "otlp.metricz") {
		t.Fatalf("err = %v, want unknown topic otlp.metricz", err)
	}
}
