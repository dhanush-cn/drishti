package storage

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// CH is a minimal ClickHouse HTTP client: one INSERT shape and one query.
type CH struct {
	URL      string // e.g. http://clickhouse:8123
	User     string
	Password string
	Table    string // e.g. drishti.logs
	HTTP     *http.Client
}

// InsertSettings are sent with every insert, explicitly, because the
// consumer's correctness depends on them and server defaults move between
// releases (26.x turned async_insert on by default in some builds):
//
//   - async_insert=0: the insert is a part on disk when the call returns,
//     and one insert is one part. With async inserts the server buffers and
//     merges inserts, so "returned OK" no longer means "written", and the
//     offsets-in-the-part resume (docs/storage.md) loses its footing.
//   - insert_deduplication_token: a retried insert of the same batch is
//     dropped server-side (needs non_replicated_deduplication_window on the
//     table). Covers the ambiguous case: timeout after the server wrote it.
//   - date_time_input_format is irrelevant for RowBinary (binary ticks).
func insertQuery(table string, cols []string) string {
	return "INSERT INTO " + table + " (" + strings.Join(cols, ", ") + ") FORMAT RowBinary"
}

// Insert sends one RowBinary batch. A non-2xx answer is an error carrying
// ClickHouse's exception text.
func (c *CH) Insert(ctx context.Context, token string, body []byte) error {
	q := url.Values{}
	q.Set("query", insertQuery(c.Table, LogColumns))
	q.Set("async_insert", "0")
	q.Set("insert_deduplication_token", token)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/?"+q.Encode(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	return c.do(req, nil)
}

// Progress returns, per Kafka partition, the highest offset stored in the
// table. The query is answered from the ingest_progress projection (one
// tiny pre-aggregated row per partition per part), not by scanning the
// offset column; verify with EXPLAIN (docs/storage.md).
func (c *CH) Progress(ctx context.Context) (map[int32]int64, error) {
	q := "SELECT kafka_partition, max(kafka_offset) FROM " + c.Table + " GROUP BY kafka_partition FORMAT TabSeparated"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/", strings.NewReader(q))
	if err != nil {
		return nil, err
	}
	out := map[int32]int64{}
	err = c.do(req, func(r io.Reader) error {
		sc := bufio.NewScanner(r)
		for sc.Scan() {
			f := strings.Split(sc.Text(), "\t")
			if len(f) != 2 {
				return fmt.Errorf("progress: unexpected line %q", sc.Text())
			}
			p, err1 := strconv.ParseInt(f[0], 10, 32)
			o, err2 := strconv.ParseInt(f[1], 10, 64)
			if err1 != nil || err2 != nil {
				return fmt.Errorf("progress: unexpected line %q", sc.Text())
			}
			out[int32(p)] = o
		}
		return sc.Err()
	})
	return out, err
}

// Ping fails fast at startup when the server or the table is missing.
func (c *CH) Ping(ctx context.Context) error {
	q := "SELECT count() FROM system.columns WHERE database || '.' || table = " + sqlString(c.Table) + " AND name = 'kafka_offset' FORMAT TabSeparated"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/", strings.NewReader(q))
	if err != nil {
		return err
	}
	return c.do(req, func(r io.Reader) error {
		b, _ := io.ReadAll(r)
		if strings.TrimSpace(string(b)) != "1" {
			return fmt.Errorf("table %s with column kafka_offset not found (run scripts/create-tables.sh)", c.Table)
		}
		return nil
	})
}

func (c *CH) do(req *http.Request, read func(io.Reader) error) error {
	req.Header.Set("X-ClickHouse-User", c.User)
	req.Header.Set("X-ClickHouse-Key", c.Password)
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if read != nil {
		return read(resp.Body)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// sqlString is a ClickHouse string literal. Not strconv.Quote: in ClickHouse
// SQL "double quotes" delimit identifiers, so a Go-quoted table name was
// read as a column name (found end to end against a real ClickHouse engine:
// UNKNOWN_IDENTIFIER `drishti.logs`).
func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// backoff is the retry schedule for inserts: 100 ms doubling to 5 s.
func backoff(attempt int) time.Duration {
	d := 100 * time.Millisecond << min(attempt, 6)
	return min(d, 5*time.Second)
}
