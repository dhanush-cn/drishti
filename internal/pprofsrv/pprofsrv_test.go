package pprofsrv

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func get(t *testing.T, url string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, b
}

// A CPU profile must come back as a gzipped pprof protobuf, which is what
// `go tool pprof` reads. This is the request the Week 2 driver makes.
func TestStart_ServesCPUProfile(t *testing.T) {
	s, err := Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	code, body := get(t, "http://"+s.Addr()+"/debug/pprof/profile?seconds=1")
	if code != http.StatusOK {
		t.Fatalf("profile status %d: %s", code, body)
	}
	zr, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("profile is not gzip (pprof format): %v", err)
	}
	if _, err := io.ReadAll(zr); err != nil {
		t.Fatalf("profile gzip body: %v", err)
	}
}

func TestHandler_ServesNamedProfiles(t *testing.T) {
	srv := httptest.NewServer(Handler())
	defer srv.Close()
	for _, p := range []string{"/debug/pprof/", "/debug/pprof/heap", "/debug/pprof/allocs", "/debug/pprof/goroutine?debug=1"} {
		if code, body := get(t, srv.URL+p); code != http.StatusOK {
			t.Errorf("%s: status %d: %.200s", p, code, body)
		}
	}
}

func TestClose_StopsListening(t *testing.T) {
	s, err := Start("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := s.Addr()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get("http://" + addr + "/debug/pprof/"); err == nil {
		t.Fatal("still serving after Close")
	}
}
