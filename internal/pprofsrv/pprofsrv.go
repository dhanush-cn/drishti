// Package pprofsrv serves Go's runtime profiles on a dedicated listener.
//
// Why a separate listener and mux, not the gRPC port and not
// http.DefaultServeMux:
//   - The OTLP port is the data path. Profiling must not share its listener,
//     its limits or its exposure.
//   - Importing net/http/pprof registers its handlers on
//     http.DefaultServeMux in the package's init, whether or not we use
//     them (a test asserting otherwise failed, 20 Oct). So the rule is: this
//     process never serves DefaultServeMux; Handler() builds its own mux and
//     only Start's listener serves it.
//   - Off unless an address is given. When on, compose binds it to
//     127.0.0.1 on the host, so it is reachable from the laptop only.
//
// Cost: an idle pprof server costs nothing. A CPU profile samples at 100 Hz
// only while /debug/pprof/profile is being fetched (a few % overhead for
// that window); heap and allocs profiles read counters the runtime already
// keeps (MemProfileRate 512 KiB default). Block and mutex profiling stay off.
package pprofsrv

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"time"
)

// Handler returns a mux with the standard pprof endpoints under
// /debug/pprof/. Named profiles (heap, allocs, goroutine, ...) are served by
// pprof.Index.
func Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// Server is a running pprof listener.
type Server struct {
	srv *http.Server
	lis net.Listener
}

// Start listens on addr and serves Handler() in the background. The write
// timeout must exceed the longest profile we ask for (profile?seconds=30),
// or the server cuts the response off mid-profile.
func Start(addr string) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{
		lis: lis,
		srv: &http.Server{
			Handler:           Handler(),
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      120 * time.Second,
		},
	}
	go func() {
		if err := s.srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = err // listener closed underneath us; nothing to report to
		}
	}()
	return s, nil
}

// Addr is the bound address (useful with ":0" in tests).
func (s *Server) Addr() string { return s.lis.Addr().String() }

// Close stops the listener; an in-flight profile is cut off.
func (s *Server) Close(ctx context.Context) error { return s.srv.Shutdown(ctx) }
