package loadgen

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/dhanush-cn/drishti/internal/ingest"
	"github.com/dhanush-cn/drishti/internal/otlpgrpc"
)

type capSink struct {
	mu  sync.Mutex
	got [][]byte
}

func (c *capSink) Publish(_ context.Context, _ ingest.Signal, msgs []ingest.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range msgs {
		c.got = append(c.got, m.Value)
	}
	return nil
}

// Against the real receiver server: pre-compressed frames arrive as the
// same requests the pool holds, through the receiver's gzip decoder (a
// frame not marked gzip, or gzipped twice, would fail to decode).
func TestPrecompressed_ReceiverGetsThePoolRequests(t *testing.T) {
	pool, err := LogPool(LogShape{BatchSize: 64, Services: 4, Resources: 1, BodyMin: 50, BodyMax: 120, AttrsMin: 2, AttrsMax: 4}, 4, "pc", 7)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := PrecompressLogs(pool)
	if err != nil {
		t.Fatal(err)
	}
	sink := &capSink{}
	gs, _ := otlpgrpc.NewServer(ingest.New(sink, ingest.Config{MaxMessageBytes: 900 << 10, PublishTimeout: time.Second}), 16<<20)
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithCompressor(PassGzip{}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	codec := NewFrameCodec()
	for _, r := range pool {
		var resp collogs.ExportLogsServiceResponse
		if err := conn.Invoke(context.Background(), collogs.LogsService_Export_FullMethodName, frames[r], &resp, grpc.ForceCodecV2(codec)); err != nil {
			t.Fatal(err)
		}
	}
	if len(sink.got) != len(pool) {
		t.Fatalf("receiver published %d messages, want %d", len(sink.got), len(pool))
	}
	for i, v := range sink.got {
		var got collogs.ExportLogsServiceRequest
		if err := proto.Unmarshal(v, &got); err != nil || !proto.Equal(&got, pool[i]) {
			t.Fatalf("message %d is not pool request %d (err %v)", i, i, err)
		}
	}
}

// What a send costs the generator: marshal + gzip per send (-gzip,
// -stamp=false before) vs nothing (pre-compressed). Relative only.
func BenchmarkSendEncoding(b *testing.B) {
	pool, _ := LogPool(LogShape{BatchSize: 512, Services: 1, Resources: 1, BodyMin: 80, BodyMax: 400, AttrsMin: 5, AttrsMax: 8}, 1, "b", 1)
	b.Run("marshal+gzip per send", func(b *testing.B) {
		for b.Loop() {
			PrecompressLogs(pool)
		}
	})
}
