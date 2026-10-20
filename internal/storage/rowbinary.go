package storage

import (
	"encoding/binary"
	"math"
)

// RowBinary is ClickHouse's row-oriented binary input format: each row is
// its columns in order, little-endian fixed-width numbers, strings as
// unsigned LEB128 length + bytes. No column names, no per-row framing.
//
// Why RowBinary over HTTP instead of the native protocol client
// (clickhouse-go): the server-side insert path (parse → block → sort →
// compress → part) is the same; RowBinary is the cheapest row format to
// produce (no escaping, unlike TSV/JSON) and to parse; and net/http keeps
// the hot path in code we own and can profile, with no new dependency.
// What we give up: the native protocol ships columnar blocks the server
// does not have to transpose, and client-side LZ4. Revisit if pprof shows
// the server's RowBinary parsing as the storage bottleneck.
//
// Type mapping used by the logs table (docs/storage.md):
//
//	String, LowCardinality(String)  uvarint len + bytes (LowCardinality is transparent)
//	UInt8/16/32/64                  1/2/4/8 bytes LE
//	DateTime64(P)                   Int64 LE, ticks of 10^-P s
//	Map(K, V)                       uvarint count + count × (K, V)
type rowBuf struct{ b []byte }

func (w *rowBuf) uvarint(v uint64) { w.b = binary.AppendUvarint(w.b, v) }
func (w *rowBuf) str(s string)     { w.uvarint(uint64(len(s))); w.b = append(w.b, s...) }
func (w *rowBuf) bytes(p []byte)   { w.uvarint(uint64(len(p))); w.b = append(w.b, p...) }
func (w *rowBuf) u8(v uint8)       { w.b = append(w.b, v) }
func (w *rowBuf) u16(v uint16)     { w.b = binary.LittleEndian.AppendUint16(w.b, v) }
func (w *rowBuf) u32(v uint32)     { w.b = binary.LittleEndian.AppendUint32(w.b, v) }
func (w *rowBuf) u64(v uint64)     { w.b = binary.LittleEndian.AppendUint64(w.b, v) }
func (w *rowBuf) i64(v int64)      { w.u64(uint64(v)) }

// dt64ns writes a DateTime64(9) from unix nanoseconds. OTLP carries fixed64
// nanos; values beyond int64 (year 2262) are clamped rather than wrapped.
func (w *rowBuf) dt64ns(unixNano uint64) {
	if unixNano > math.MaxInt64 {
		unixNano = math.MaxInt64
	}
	w.i64(int64(unixNano))
}
