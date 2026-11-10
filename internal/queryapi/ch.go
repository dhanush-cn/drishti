package queryapi

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CH runs read-only queries over ClickHouse's HTTP interface.
//
// Every request carries readonly=2 (no writes, settings allowed), a time
// limit and a result-row cap: the parameters come from an LLM's tool calls,
// so a bad argument must cost a fast error, never a scan of the whole table
// or a write. Values are always ClickHouse query parameters ({name:Type}),
// never pasted into SQL.
type CH struct {
	URL, User, Password string
	HTTP                *http.Client
	Timeout             time.Duration // per query; default 10 s
	MaxRows             int           // default 200000
}

// Rows runs q and decodes each JSONEachRow line into a new T.
func Rows[T any](ctx context.Context, c *CH, q string, params map[string]string) ([]T, error) {
	v := url.Values{}
	v.Set("query", q+"\nFORMAT JSONEachRow")
	v.Set("readonly", "2")
	to := c.Timeout
	if to <= 0 {
		to = 10 * time.Second
	}
	v.Set("max_execution_time", fmt.Sprint(int(to/time.Second)))
	maxRows := c.MaxRows
	if maxRows <= 0 {
		maxRows = 200_000
	}
	v.Set("max_result_rows", fmt.Sprint(maxRows))
	v.Set("result_overflow_mode", "throw")
	// Plain JSON numbers for 64-bit integers (the default quotes them).
	v.Set("output_format_json_quote_64bit_integers", "0")
	for k, p := range params {
		v.Set("param_"+k, p)
	}
	ctx, cancel := context.WithTimeout(ctx, to+5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL+"/?"+v.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-ClickHouse-User", c.User)
	req.Header.Set("X-ClickHouse-Key", c.Password)
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("clickhouse HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var out []T
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var x T
		if err := json.Unmarshal(sc.Bytes(), &x); err != nil {
			return nil, fmt.Errorf("decode row: %w: %.200s", err, sc.Text())
		}
		out = append(out, x)
	}
	return out, sc.Err()
}

// ts formats a time as a DateTime64(9, 'UTC') parameter value (see
// internal/metricsql: a bare DateTime64 parameter is read in the server's
// time zone).
func ts(t time.Time) string { return t.UTC().Format("2006-01-02 15:04:05.000000000") }

// stringArray formats an Array(String) parameter value.
func stringArray(xs []string) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range xs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteByte('\'')
		b.WriteString(strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(x))
		b.WriteByte('\'')
	}
	b.WriteByte(']')
	return b.String()
}
