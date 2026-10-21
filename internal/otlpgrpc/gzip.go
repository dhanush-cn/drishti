package otlpgrpc

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/gzip"
	"google.golang.org/grpc/encoding"
)

// gzipCodec replaces grpc-go's gzip compressor (same name, "gzip", so the
// collector's default compression lands here).
//
// Why (Week 2 receiver profile at 150k rec/s): gzip decoding was 24.7% of
// the receiver's CPU, 9.7% of it in gRPC's mem.Reader.ReadByte. The
// standard library's inflater reads its input one byte at a time through
// the io.ByteReader interface, and gRPC hands it a mem.Reader over a list of
// buffers, so every byte is an interface call into that reader.
//
// Here the compressed message (small: tens of KiB for a 512-record batch)
// is first copied in bulk into a pooled buffer, then inflated from a
// *bytes.Reader by klauspost/compress, whose inflater has a specialised
// loop for that concrete type (no per-byte interface call) and is faster
// than the standard library's in general.
//
// Limits are unchanged: gRPC rejects a compressed message over
// MaxRecvMsgSize before calling Decompress, and caps what it reads from
// the returned reader at MaxRecvMsgSize+1 (zip bombs).
type gzipCodec struct {
	writers sync.Pool // *gzip.Writer
	readers sync.Pool // *gzipReader
}

func init() {
	// Must run after grpc/encoding/gzip's init if that package is linked
	// in: package inits run in dependency order, and this package no
	// longer imports it, so nothing in the receiver registers it later.
	// TestGzipCompressorIsOurs checks what is registered.
	encoding.RegisterCompressor(&gzipCodec{})
}

func (c *gzipCodec) Name() string { return "gzip" }

func (c *gzipCodec) Compress(w io.Writer) (io.WriteCloser, error) {
	z, _ := c.writers.Get().(*gzip.Writer)
	if z == nil {
		z = gzip.NewWriter(w)
	} else {
		z.Reset(w)
	}
	return &gzipWriter{Writer: z, pool: &c.writers}, nil
}

type gzipWriter struct {
	*gzip.Writer
	pool *sync.Pool
}

func (w *gzipWriter) Close() error {
	defer w.pool.Put(w.Writer)
	return w.Writer.Close()
}

type gzipReader struct {
	in   bytes.Buffer // the compressed message, copied in bulk
	br   bytes.Reader
	z    gzip.Reader
	init bool
	pool *sync.Pool
}

func (c *gzipCodec) Decompress(r io.Reader) (io.Reader, error) {
	g, _ := c.readers.Get().(*gzipReader)
	if g == nil {
		g = &gzipReader{pool: &c.readers}
	}
	g.in.Reset()
	if _, err := g.in.ReadFrom(r); err != nil {
		c.readers.Put(g)
		return nil, err
	}
	g.br.Reset(g.in.Bytes())
	var err error
	if g.init {
		err = g.z.Reset(&g.br)
	} else {
		var z *gzip.Reader
		z, err = gzip.NewReader(&g.br)
		if err == nil {
			g.z = *z
			g.init = true
		}
	}
	if err != nil {
		c.readers.Put(g)
		return nil, err
	}
	return g, nil
}

func (g *gzipReader) Read(p []byte) (int, error) { return g.z.Read(p) }

// Close returns the reader to the pool; gRPC calls it when it is done.
func (g *gzipReader) Close() error {
	err := g.z.Close()
	if g.in.Cap() > 4<<20 {
		g.in = bytes.Buffer{} // don't pin a rare huge request in the pool
	}
	g.pool.Put(g)
	return err
}
