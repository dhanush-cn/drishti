package storage

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// stubDecoder says every value holds len(value) rows and writes one byte per
// row, so batch contents are easy to read back.
type stubDecoder struct{ last []byte }

func (d *stubDecoder) Decode(v []byte) (int, error) {
	if len(v) == 0 {
		return 0, errors.New("empty")
	}
	d.last = v
	return len(v), nil
}

func (d *stubDecoder) Append(w *rowBuf, _ Source) int {
	w.b = append(w.b, d.last...)
	return len(d.last)
}

var stubCodec = Codec{Name: "stub", Columns: []string{"x"}, NewDecoder: func() Decoder { return &stubDecoder{} }}

// The consumer must cut a batch before a message that would cross the row
// limit, using the count from Decode (a message is never split, so each
// insert stays below the server's block size = one part). With the codec
// seam the count comes from the codec, not from the logs proto.
func TestCodecSeam_CutsBeforeAMessageThatWouldCrossTheRowLimit(t *testing.T) {
	c := bareConsumer(Limits{MaxRows: 5, MaxBytes: 1 << 20, MaxWait: time.Hour}, 1)
	c.cfg.Codec = stubCodec
	ts := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	addAll(c,
		&kgo.Record{Topic: topic, Partition: 0, Offset: 10, Timestamp: ts, Value: []byte("aaa")},
		&kgo.Record{Topic: topic, Partition: 0, Offset: 11, Timestamp: ts, Value: []byte("bbb")},
	)
	if len(c.ready) != 1 {
		t.Fatalf("ready batches = %d, want 1 (cut before offset 11: 3+3 rows > 5)", len(c.ready))
	}
	if got := string(c.ready[0].buf.b); got != "aaa" || c.ready[0].rows != 3 {
		t.Fatalf("first batch = %q (%d rows), want \"aaa\" (3 rows)", got, c.ready[0].rows)
	}
	if b := c.open[0]; b == nil || string(b.buf.b) != "bbb" || b.first != 11 {
		t.Fatalf("open batch = %+v, want offset 11 alone", b)
	}
}

// A codec decoder error skips the record (counted), like an undecodable
// logs message, and the partition goes on.
func TestCodecSeam_UndecodableRecordIsSkippedAndCounted(t *testing.T) {
	c := bareConsumer(Limits{MaxRows: 100, MaxBytes: 1 << 20, MaxWait: time.Hour}, 1)
	c.cfg.Codec = stubCodec
	ts := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	addAll(c,
		&kgo.Record{Topic: topic, Partition: 0, Offset: 1, Timestamp: ts, Value: nil},
		&kgo.Record{Topic: topic, Partition: 0, Offset: 2, Timestamp: ts, Value: []byte("ok")},
	)
	if n := c.Stats.DecodeErrors.Load(); n != 1 {
		t.Fatalf("decode errors = %d, want 1", n)
	}
	if b := c.open[0]; b == nil || string(b.buf.b) != "ok" || b.first != 2 {
		t.Fatalf("open batch = %+v, want offset 2 only", b)
	}
}

// The INSERT column list and the bytes the codec writes must describe the
// same layout, or ClickHouse reads garbage (or worse, plausible values).
func TestNew_RejectsClickHouseColumnsThatDifferFromTheCodec(t *testing.T) {
	_, err := New(Config{Brokers: []string{"127.0.0.1:1"}, Topic: "t", Group: "g",
		Limits: Limits{MaxRows: 10, MaxBytes: 1 << 20, MaxWait: time.Second},
		CH:     &CH{Columns: LogColumns}, Codec: stubCodec})
	if err == nil || !strings.Contains(err.Error(), "differ") {
		t.Fatalf("err = %v, want a column mismatch error", err)
	}
	ch := &CH{}
	c, err := New(Config{Brokers: []string{"127.0.0.1:1"}, Topic: "t", Group: "g",
		Limits: Limits{MaxRows: 10, MaxBytes: 1 << 20, MaxWait: time.Second},
		CH:     ch, Codec: stubCodec})
	if err != nil {
		t.Fatal(err)
	}
	c.cl.Close()
	if strings.Join(ch.Columns, ",") != "x" {
		t.Fatalf("CH columns = %v, want the codec's [x]", ch.Columns)
	}
}
