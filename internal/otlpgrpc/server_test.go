package otlpgrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/dhanush-cn/drishti/internal/ingest"
)

type fakeSink struct {
	err  error
	msgs int
}

func (f *fakeSink) Publish(_ context.Context, _ ingest.Signal, msgs []ingest.Message) error {
	if f.err != nil {
		return f.err
	}
	f.msgs += len(msgs)
	return nil
}

// start runs the real server over an in-memory listener.
func start(t *testing.T, sink ingest.Sink, maxRecv, maxMsg int) collogs.LogsServiceClient {
	t.Helper()
	ing := ingest.New(sink, ingest.Config{MaxMessageBytes: maxMsg, PublishTimeout: time.Second})
	gs, _ := NewServer(ing, maxRecv)
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return collogs.NewLogsServiceClient(conn)
}

func logsReq(n, bodySize int) *collogs.ExportLogsServiceRequest {
	var recs []*logsv1.LogRecord
	for i := 0; i < n; i++ {
		recs = append(recs, &logsv1.LogRecord{Body: &commonv1.AnyValue{
			Value: &commonv1.AnyValue_StringValue{StringValue: strings.Repeat("a", bodySize)},
		}})
	}
	return &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "cart"}},
		}}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: recs}},
	}}}
}

// The collector sends gzip by default. The compressor is named by string,
// not by importing the gzip package here, so this test fails if the import
// in server.go is removed.
func TestExport_GzipFromCollectorIsAccepted(t *testing.T) {
	sink := &fakeSink{}
	c := start(t, sink, 16<<20, 900<<10)
	if _, err := c.Export(context.Background(), logsReq(10, 50), grpc.UseCompressor("gzip")); err != nil {
		t.Fatalf("gzip export failed: %v", err)
	}
	if sink.msgs != 1 {
		t.Fatalf("published %d messages, want 1", sink.msgs)
	}
}

func TestExport_KafkaFailureIsUnavailableWithRetryInfo(t *testing.T) {
	c := start(t, &fakeSink{err: errors.New("broker down")}, 16<<20, 900<<10)
	_, err := c.Export(context.Background(), logsReq(1, 10))
	st := status.Convert(err)
	if st.Code() != codes.Unavailable {
		t.Fatalf("code=%v, want UNAVAILABLE (retryable per OTLP spec)", st.Code())
	}
	var retry *errdetails.RetryInfo
	for _, d := range st.Details() {
		if ri, ok := d.(*errdetails.RetryInfo); ok {
			retry = ri
		}
	}
	if retry == nil || retry.GetRetryDelay().AsDuration() != RetryDelay {
		t.Fatalf("missing or wrong RetryInfo: %v", retry)
	}
}

func TestExport_RequestOverMaxRecvIsResourceExhausted(t *testing.T) {
	c := start(t, &fakeSink{}, 64<<10, 900<<10)                  // 64 KiB server limit
	_, err := c.Export(context.Background(), logsReq(200, 1024)) // ~200 KiB
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("code=%v, want RESOURCE_EXHAUSTED from MaxRecvMsgSize", status.Code(err))
	}
}

func TestExport_PartialSuccessInResponse(t *testing.T) {
	c := start(t, &fakeSink{}, 16<<20, 10<<10)
	req := logsReq(3, 10)
	req.ResourceLogs[0].ScopeLogs[0].LogRecords[1].Body = &commonv1.AnyValue{
		Value: &commonv1.AnyValue_StringValue{StringValue: strings.Repeat("b", 50<<10)},
	}
	resp, err := c.Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetPartialSuccess().GetRejectedLogRecords(); got != 1 {
		t.Fatalf("rejected_log_records=%d, want 1", got)
	}
}
