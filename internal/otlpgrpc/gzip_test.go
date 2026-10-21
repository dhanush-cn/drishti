package otlpgrpc

import (
	"bytes"
	stdgzip "compress/gzip"
	"crypto/rand"
	"encoding/hex"
	"io"
	"testing"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

func TestGzipCompressorIsOurs(t *testing.T) {
	if _, ok := encoding.GetCompressor("gzip").(*gzipCodec); !ok {
		t.Fatalf("registered gzip compressor is %T, want *gzipCodec", encoding.GetCompressor("gzip"))
	}
}

// realistic returns a gzipped 512-record request with random hex bodies
// (like the load generator's: they don't compress away), split over
// several mem buffers the way gRPC delivers a large message.
func realistic(t testing.TB) (plain []byte, chunks [][]byte) {
	req := logsReq(512, 0)
	for _, r := range req.ResourceLogs[0].ScopeLogs[0].LogRecords {
		b := make([]byte, 200)
		rand.Read(b)
		r.Body.Value = &commonv1.AnyValue_StringValue{StringValue: hex.EncodeToString(b)}
	}
	plain, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var z bytes.Buffer
	w := stdgzip.NewWriter(&z)
	w.Write(plain)
	w.Close()
	for b := z.Bytes(); len(b) > 0; {
		n := min(16<<10, len(b))
		chunks = append(chunks, b[:n])
		b = b[n:]
	}
	return plain, chunks
}

func memReader(chunks [][]byte) *mem.Reader {
	var bs mem.BufferSlice
	for _, c := range chunks {
		bs = append(bs, mem.SliceBuffer(c))
	}
	return bs.Reader()
}

func TestGzipRoundTrip(t *testing.T) {
	c := encoding.GetCompressor("gzip")
	plain, chunks := realistic(t)
	cases := map[string][]byte{"empty": nil, "small": []byte("hello"), "512 records": plain}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 3; i++ { // pooled readers and writers are reused
				var z bytes.Buffer
				w, err := c.Compress(&z)
				if err != nil {
					t.Fatal(err)
				}
				w.Write(in)
				w.Close()
				// stdlib can read what we wrote
				sr, err := stdgzip.NewReader(bytes.NewReader(z.Bytes()))
				if err != nil {
					t.Fatal(err)
				}
				if got, _ := io.ReadAll(sr); !bytes.Equal(got, in) {
					t.Fatalf("stdlib read back %d bytes, want %d", len(got), len(in))
				}
				// we can read what stdlib (the collector's Go gzip) wrote
				r, err := c.Decompress(memReader([][]byte{z.Bytes()}))
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				r.(io.Closer).Close()
				if err != nil || !bytes.Equal(got, in) {
					t.Fatalf("read back %d bytes (err %v), want %d", len(got), err, len(in))
				}
			}
		})
	}
	r, err := c.Decompress(memReader(chunks))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	r.(io.Closer).Close()
	if !bytes.Equal(got, plain) {
		t.Fatal("multi-buffer message did not round-trip")
	}
	if _, err := c.Decompress(memReader([][]byte{[]byte("not gzip")})); err == nil {
		t.Fatal("garbage accepted as gzip")
	}
}

// BenchmarkDecompress: what the receiver did before (grpc-go's gzip =
// compress/gzip over gRPC's mem.Reader) against gzipCodec, on one realistic
// 512-record request. Relative numbers only; laptop numbers come from pprof.
func BenchmarkDecompress(b *testing.B) {
	plain, chunks := realistic(b)
	b.Run("stdlib-over-mem.Reader", func(b *testing.B) {
		b.SetBytes(int64(len(plain)))
		var zr *stdgzip.Reader
		for b.Loop() {
			r := memReader(chunks)
			if zr == nil {
				zr, _ = stdgzip.NewReader(r)
			} else {
				zr.Reset(r)
			}
			io.Copy(io.Discard, zr)
		}
	})
	b.Run("gzipCodec", func(b *testing.B) {
		b.SetBytes(int64(len(plain)))
		c := encoding.GetCompressor("gzip")
		for b.Loop() {
			r, _ := c.Decompress(memReader(chunks))
			io.Copy(io.Discard, r)
			r.(io.Closer).Close()
		}
	})
}
