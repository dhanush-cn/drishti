// Package kdump writes and reads Kafka records to and from a file, so that a
// recorded stretch of demo traffic can be replayed into the detector exactly
// as it arrived (docs/detector.md, section 9).
//
// Why a file of raw records rather than ClickHouse rows or JSON: the detector
// consumes Kafka records (partition, offset, timestamp, key, protobuf value).
// Replaying the same bytes through the same code path is the only way tuning
// on a recording says something about the live detector. ClickHouse holds only
// logs, decoded and flattened; JSON would re-encode the protobuf values.
//
// Format: a gzip stream holding the header line "drishti-kafkadump v1\n",
// then one record after another:
//
//	uvarint len(topic) | topic | varint partition | varint offset |
//	varint timestamp (unix ms) | uvarint len(key) | key | uvarint len(value) | value
//
// Kafka timestamps are milliseconds, so nothing is lost. Records of one
// partition appear in offset order; partitions interleave as they were fetched.
package kdump

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const header = "drishti-kafkadump v1\n"

// Writer appends records to a dump. Close flushes; without it the gzip
// stream is truncated and the file unreadable past the last flushed block.
type Writer struct {
	gz  *gzip.Writer
	bw  *bufio.Writer
	tmp []byte
	n   int64
}

func NewWriter(w io.Writer) (*Writer, error) {
	gz, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(gz, 1<<20)
	if _, err := bw.WriteString(header); err != nil {
		return nil, err
	}
	return &Writer{gz: gz, bw: bw}, nil
}

func (w *Writer) Write(r *kgo.Record) error {
	b := w.tmp[:0]
	b = binary.AppendUvarint(b, uint64(len(r.Topic)))
	b = append(b, r.Topic...)
	b = binary.AppendVarint(b, int64(r.Partition))
	b = binary.AppendVarint(b, r.Offset)
	b = binary.AppendVarint(b, r.Timestamp.UnixMilli())
	b = binary.AppendUvarint(b, uint64(len(r.Key)))
	b = append(b, r.Key...)
	b = binary.AppendUvarint(b, uint64(len(r.Value)))
	w.tmp = b
	if _, err := w.bw.Write(b); err != nil {
		return err
	}
	if _, err := w.bw.Write(r.Value); err != nil {
		return err
	}
	w.n++
	return nil
}

// Records is how many records were written.
func (w *Writer) Records() int64 { return w.n }

func (w *Writer) Close() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.gz.Close()
}

// Reader reads records back in file order.
type Reader struct {
	gz *gzip.Reader
	br *bufio.Reader
}

// ErrFormat means the input is not a dump this package wrote.
var ErrFormat = errors.New("not a drishti-kafkadump v1 file")

func NewReader(r io.Reader) (*Reader, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFormat, err)
	}
	br := bufio.NewReaderSize(gz, 1<<20)
	h := make([]byte, len(header))
	if _, err := io.ReadFull(br, h); err != nil || string(h) != header {
		return nil, ErrFormat
	}
	return &Reader{gz: gz, br: br}, nil
}

// maxField bounds a length prefix, so a corrupt file fails cleanly instead of
// allocating gigabytes. Kafka's own message.max.bytes here is 1 MiB; the
// receiver splits anything over 900 KiB (docs/receiver.md).
const maxField = 64 << 20

// Next returns the next record, or io.EOF at a clean end of file. A file that
// ends inside a record returns io.ErrUnexpectedEOF.
func (r *Reader) Next() (*kgo.Record, error) {
	tl, err := binary.ReadUvarint(r.br)
	if err == io.EOF {
		return nil, io.EOF
	}
	if err != nil {
		return nil, unexpected(err)
	}
	topic, err := r.field(tl)
	if err != nil {
		return nil, err
	}
	p, err := binary.ReadVarint(r.br)
	if err != nil {
		return nil, unexpected(err)
	}
	off, err := binary.ReadVarint(r.br)
	if err != nil {
		return nil, unexpected(err)
	}
	ms, err := binary.ReadVarint(r.br)
	if err != nil {
		return nil, unexpected(err)
	}
	kl, err := binary.ReadUvarint(r.br)
	if err != nil {
		return nil, unexpected(err)
	}
	key, err := r.field(kl)
	if err != nil {
		return nil, err
	}
	vl, err := binary.ReadUvarint(r.br)
	if err != nil {
		return nil, unexpected(err)
	}
	val, err := r.field(vl)
	if err != nil {
		return nil, err
	}
	if len(key) == 0 {
		key = nil
	}
	return &kgo.Record{Topic: string(topic), Partition: int32(p), Offset: off,
		Timestamp: time.UnixMilli(ms), Key: key, Value: val}, nil
}

func (r *Reader) field(n uint64) ([]byte, error) {
	if n > maxField {
		return nil, fmt.Errorf("%w: field of %d bytes", ErrFormat, n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r.br, b); err != nil {
		return nil, unexpected(err)
	}
	return b, nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *Reader) Close() error { return r.gz.Close() }
