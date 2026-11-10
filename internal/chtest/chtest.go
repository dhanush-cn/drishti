// Package chtest runs tests against a real ClickHouse over HTTP, when one is
// given in DRISHTI_TEST_CLICKHOUSE_URL (e.g. http://localhost:8123 on the
// laptop, user drishti). Without it those tests skip: `go test ./...` stays
// self-contained.
//
// Each test gets its own database, created from the repo's schema files with
// "drishti." renamed, and dropped at the end.
package chtest

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// Server is a ClickHouse HTTP endpoint with a test database.
type Server struct {
	URL, User, Password, DB string
	t                       testing.TB
}

var seq atomic.Int64

// New skips the test unless DRISHTI_TEST_CLICKHOUSE_URL is set, then creates
// a fresh database and applies the given schema files (names under
// clickhouse/schema/) into it.
func New(t testing.TB, schemas ...string) *Server {
	t.Helper()
	u := os.Getenv("DRISHTI_TEST_CLICKHOUSE_URL")
	if u == "" {
		t.Skip("DRISHTI_TEST_CLICKHOUSE_URL not set")
	}
	s := &Server{URL: u, User: envOr("DRISHTI_TEST_CLICKHOUSE_USER", "drishti"), Password: envOr("DRISHTI_TEST_CLICKHOUSE_PASSWORD", "drishti_dev"),
		DB: fmt.Sprintf("drishti_test_%d_%d", time.Now().UnixNano()%1_000_000_000, seq.Add(1)), t: t}
	s.Exec("CREATE DATABASE " + s.DB)
	t.Cleanup(func() { s.Exec("DROP DATABASE IF EXISTS " + s.DB) })
	for _, f := range schemas {
		b, err := os.ReadFile(filepath.Join(RepoRoot(), "clickhouse", "schema", f))
		if err != nil {
			t.Fatal(err)
		}
		for _, st := range Statements(strings.ReplaceAll(string(b), "drishti.", s.DB+".")) {
			s.Exec(st)
		}
	}
	return s
}

// RepoRoot is the module root (where go.mod is), found from this file.
func RepoRoot() string {
	_, f, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(f), "..", "..")
}

var lineComment = regexp.MustCompile(`(?m)^\s*--.*$|\s--\s.*$`)

// Statements splits a schema file into statements: comments first (they
// contain semicolons), then on ";".
func Statements(sql string) []string {
	sql = lineComment.ReplaceAllString(sql, "")
	var out []string
	for _, st := range strings.Split(sql, ";") {
		if st = strings.TrimSpace(st); st != "" {
			out = append(out, st)
		}
	}
	return out
}

// Exec runs one statement and fails the test on error.
func (s *Server) Exec(q string) {
	s.t.Helper()
	if _, err := s.Query(q); err != nil {
		s.t.Fatalf("clickhouse: %v\n%s", err, q)
	}
}

// Query runs one query and returns the body.
func (s *Server) Query(q string) (string, error) { return s.QueryParams(q, nil) }

// QueryParams runs one query with ClickHouse query parameters ({name:Type}
// placeholders, sent as param_name).
func (s *Server) QueryParams(q string, params map[string]string) (string, error) {
	v := url.Values{"query": {q}}
	for k, p := range params {
		v.Set("param_"+k, p)
	}
	req, err := http.NewRequest(http.MethodPost, s.URL+"/?"+v.Encode(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-ClickHouse-User", s.User)
	req.Header.Set("X-ClickHouse-Key", s.Password)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return string(b), nil
}

// MustQuery is Query that fails the test on error.
func (s *Server) MustQuery(q string) string {
	s.t.Helper()
	out, err := s.Query(q)
	if err != nil {
		s.t.Fatalf("clickhouse: %v\n%s", err, q)
	}
	return out
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
