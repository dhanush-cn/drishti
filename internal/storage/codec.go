package storage

import (
	"fmt"
	"slices"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

// Codec is what one storage process writes: one Kafka message type into one
// table (docs/storage.md, "One process per signal").
//
// Everything that makes the consumer exactly-once (offsets in the part, one
// partition range per insert, commit after insert, resume from the
// ingest_progress projection) lives in the consumer and does not depend on
// the codec. A codec only has to keep one promise: the rows of a record are
// a pure function of that record (its value and its Source). No state may
// carry from one record to the next, or a replayed record could produce
// different rows than the first time, and the dedup token and the resume
// skip would stop meaning "already stored".
type Codec struct {
	Name    string
	Columns []string // INSERT column list, in RowBinary order
	// NewDecoder returns a decoder for one Kafka partition. The consumer
	// keeps one per partition and calls it from one goroutine at a time.
	NewDecoder func() Decoder
}

// Decoder turns Kafka record values into rows. It may reuse buffers between
// records (that is why it exists), never data.
type Decoder interface {
	// Decode parses one record value and returns how many rows it holds.
	// The consumer needs the count before appending: a message is never
	// split, so a batch is cut before a message that would cross the row
	// limit. An error means the value can never be decoded.
	Decode(value []byte) (rows int, err error)
	// Append writes the rows of the value last passed to Decode and
	// returns their number (equal to Decode's).
	Append(w *rowBuf, src Source) int
}

// Logs is the codec of otlp.logs → drishti.logs (001_logs.sql).
var Logs = Codec{Name: "logs", Columns: LogColumns, NewDecoder: func() Decoder { return &logsDecoder{} }}

type logsDecoder struct {
	req collogs.ExportLogsServiceRequest
}

func (d *logsDecoder) Decode(value []byte) (int, error) {
	proto.Reset(&d.req)
	if err := proto.Unmarshal(value, &d.req); err != nil {
		return 0, err
	}
	n := 0
	for _, rl := range d.req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
	}
	return n, nil
}

func (d *logsDecoder) Append(w *rowBuf, src Source) int { return AppendLogs(w, &d.req, src) }

// codecs by -signal name.
var codecs = map[string]Codec{Logs.Name: Logs, Metrics.Name: Metrics, Anomalies.Name: Anomalies}

// CodecFor returns the codec named by cmd/storage's -signal.
func CodecFor(name string) (Codec, error) {
	c, ok := codecs[name]
	if !ok {
		return Codec{}, fmt.Errorf("unknown signal %q (want one of %v)", name, CodecNames())
	}
	return c, nil
}

// CodecNames lists the known signals, sorted.
func CodecNames() []string {
	names := make([]string, 0, len(codecs))
	for n := range codecs {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// Encode runs records through a fresh decoder outside the consumer, in order,
// and returns the RowBinary body an insert would send. For tests and tools
// that load fixtures into ClickHouse with the consumer's exact encoding.
func (c Codec) Encode(values [][]byte, srcs []Source) ([]byte, int, error) {
	d := c.NewDecoder()
	var w rowBuf
	rows := 0
	for i, v := range values {
		if _, err := d.Decode(v); err != nil {
			return nil, 0, err
		}
		rows += d.Append(&w, srcs[i])
	}
	return w.b, rows, nil
}
