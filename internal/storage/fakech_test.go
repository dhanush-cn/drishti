package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
)

// fakeCH stands in for ClickHouse's HTTP interface: it decodes RowBinary
// inserts (the encoding itself is cross-checked against a real ClickHouse
// 26.9 parser, docs/storage.md), honours insert_deduplication_token like a
// table with non_replicated_deduplication_window, and answers the progress
// and ping queries. failInserts makes the next N inserts fail with 500.
type fakeCH struct {
	mu          sync.Mutex
	rows        []fakeRow
	tokens      map[string]bool
	inserts     int
	dedupHits   int
	failInserts int
	lastQuery   map[string]string
	srv         *httptest.Server

	// Background-insert tests. gate runs before an insert is applied,
	// outside mu, so it can block one insert while others proceed;
	// failIf fails an insert with 500. tmu guards the tracking below.
	gate        func(partition uint16, first uint64)
	failIf      func(partition uint16, first uint64) bool
	tmu         sync.Mutex
	inflight    map[uint16]int
	maxInflight map[uint16]int // per partition: must never exceed 1
	allInflight int
	maxAll      int
	order       map[uint16][]uint64 // first offset of each applied insert, per partition
}

type fakeRow struct {
	service   string
	body      string
	partition uint16
	offset    uint64
	tsNs      int64
	recvMs    int64
}

func newFakeCH(t *testing.T) *fakeCH {
	f := &fakeCH{tokens: map[string]bool{}, inflight: map[uint16]int{}, maxInflight: map[uint16]int{}, order: map[uint16][]uint64{}}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeCH) ch() *CH {
	return &CH{URL: f.srv.URL, User: "u", Password: "p", Table: "drishti.logs"}
}

func (f *fakeCH) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	q := r.URL.Query()
	if strings.HasPrefix(q.Get("query"), "INSERT") {
		if rows, err := decodeRows(body); err == nil && len(rows) > 0 {
			p, first := rows[0].partition, rows[0].offset
			f.tmu.Lock()
			f.inflight[p]++
			f.maxInflight[p] = max(f.maxInflight[p], f.inflight[p])
			f.allInflight++
			f.maxAll = max(f.maxAll, f.allInflight)
			gate, failIf := f.gate, f.failIf
			f.tmu.Unlock()
			defer func() {
				f.tmu.Lock()
				f.inflight[p]--
				f.allInflight--
				f.tmu.Unlock()
			}()
			if gate != nil {
				gate(p, first)
			}
			if failIf != nil && failIf(p, first) {
				http.Error(w, "Code: 999. DB::Exception: injected for this batch", 500)
				return
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastQuery = map[string]string{}
	for k := range q {
		f.lastQuery[k] = q.Get(k)
	}
	query := q.Get("query")
	switch {
	case strings.HasPrefix(query, "INSERT INTO drishti.logs"):
		if f.failInserts > 0 {
			f.failInserts--
			http.Error(w, "Code: 999. DB::Exception: injected", 500)
			return
		}
		rows, err := decodeRows(body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		f.inserts++
		if tok := q.Get("insert_deduplication_token"); tok != "" {
			if f.tokens[tok] {
				f.dedupHits++
				return // ClickHouse: 200, block silently deduplicated
			}
			f.tokens[tok] = true
		}
		f.rows = append(f.rows, rows...)
		if len(rows) > 0 {
			f.tmu.Lock()
			f.order[rows[0].partition] = append(f.order[rows[0].partition], rows[0].offset)
			f.tmu.Unlock()
		}
	case strings.Contains(string(body), "max(kafka_offset)"):
		max := map[uint16]uint64{}
		for _, r := range f.rows {
			if r.offset >= max[r.partition] {
				max[r.partition] = r.offset
			}
		}
		for p, o := range max {
			fmt.Fprintf(w, "%d\t%d\n", p, o)
		}
	case strings.Contains(string(body), "system.columns"):
		fmt.Fprintln(w, "1")
	default:
		http.Error(w, "unexpected query: "+query+string(body), 400)
	}
}

func (f *fakeCH) snapshot() []fakeRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeRow(nil), f.rows...)
}

// copies counts rows per (partition, offset, body): a duplicate Kafka
// message shows up as a count of 2 for each of its rows.
func copies(rows []fakeRow) map[string]int {
	m := map[string]int{}
	for _, r := range rows {
		m[fmt.Sprintf("%d/%d/%s", r.partition, r.offset, r.body)]++
	}
	return m
}

func dups(rows []fakeRow) []string {
	var out []string
	for k, n := range copies(rows) {
		if n > 1 {
			out = append(out, fmt.Sprintf("%s x%d", k, n))
		}
	}
	sort.Strings(out)
	return out
}

// ---- test-side RowBinary decoder for the LogColumns layout ----

type rd struct {
	b   []byte
	err error
}

func (d *rd) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b)
	if n <= 0 {
		d.err = fmt.Errorf("bad uvarint")
		return 0
	}
	d.b = d.b[n:]
	return v
}
func (d *rd) take(n int) []byte {
	if d.err != nil || len(d.b) < n {
		d.err = fmt.Errorf("short row")
		return nil
	}
	p := d.b[:n]
	d.b = d.b[n:]
	return p
}
func (d *rd) str() string { return string(d.take(int(d.uvarint()))) }

// fixed returns n bytes, zero-filled when the input is short (d.err is
// set). It copies: appending to a sub-slice of d.b would overwrite the next
// row in place (that bug made every insert fail as "short row").
func (d *rd) fixed(n int) []byte {
	out := make([]byte, 8)
	copy(out, d.take(n))
	return out
}
func (d *rd) u64() uint64 { return binary.LittleEndian.Uint64(d.fixed(8)) }
func (d *rd) u32() uint32 { return binary.LittleEndian.Uint32(d.fixed(4)) }
func (d *rd) u16() uint16 { return binary.LittleEndian.Uint16(d.fixed(2)) }
func (d *rd) u8() uint8 {
	p := d.take(1)
	if p == nil {
		return 0
	}
	return p[0]
}
func (d *rd) kvmap() {
	n := d.uvarint()
	for i := uint64(0); i < n; i++ {
		d.str()
		d.str()
	}
}

func decodeRows(b []byte) ([]fakeRow, error) {
	d := &rd{b: b}
	var out []fakeRow
	for len(d.b) > 0 && d.err == nil {
		var r fakeRow
		r.tsNs = int64(d.u64()) // ts
		d.u64()                 // observed_ts
		r.recvMs = int64(d.u64())
		r.service = d.str()
		d.u8()
		d.str()
		r.body = d.str()
		d.str()
		d.str()
		d.u32()
		d.str()
		d.str()
		d.kvmap()
		d.kvmap()
		r.partition = d.u16()
		r.offset = d.u64()
		out = append(out, r)
	}
	return out, d.err
}
