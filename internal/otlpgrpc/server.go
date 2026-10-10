// Package otlpgrpc is the thin gRPC transport for OTLP. All real work is in
// package ingest; this file only decodes, calls it, and maps errors to
// OTLP-spec status codes.
package otlpgrpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	// Registers the gzip decompressor. The collector's otlp exporter sends
	// gzip by default; without this import every export fails with
	// UNIMPLEMENTED "grpc: Decompressor is not installed".
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/dhanush-cn/drishti/internal/ingest"
)

// RetryDelay is sent in RetryInfo with UNAVAILABLE responses. It is a hint;
// the collector's retry_on_failure backoff takes over from there.
const RetryDelay = time.Second

// NewServer builds a gRPC server with the OTLP logs and metrics services,
// standard health checking, and reflection (so grpcurl works without .proto
// files: the debuggability we gave up by not doing OTLP/HTTP).
func NewServer(ing *ingest.Ingester, maxRecvBytes int) (*grpc.Server, *health.Server) {
	// grpc-go's default limit is 4 MiB per message. A big collector batch
	// can exceed it and fail with RESOURCE_EXHAUSTED, which looks like
	// backpressure but is not. Set it explicitly and size collector batches
	// to stay under it.
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvBytes))

	collogs.RegisterLogsServiceServer(gs, &logsServer{ing: ing})
	colmetrics.RegisterMetricsServiceServer(gs, &metricsServer{ing: ing})

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(gs, hs)
	reflection.Register(gs)
	return gs, hs
}

type logsServer struct {
	collogs.UnimplementedLogsServiceServer
	ing *ingest.Ingester
}

func (s *logsServer) Export(ctx context.Context, req *collogs.ExportLogsServiceRequest) (*collogs.ExportLogsServiceResponse, error) {
	res, err := s.ing.ExportLogs(ctx, req)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &collogs.ExportLogsServiceResponse{}
	if res.Rejected > 0 {
		resp.PartialSuccess = &collogs.ExportLogsPartialSuccess{
			RejectedLogRecords: res.Rejected,
			ErrorMessage:       res.RejectReason,
		}
	}
	return resp, nil
}

type metricsServer struct {
	colmetrics.UnimplementedMetricsServiceServer
	ing *ingest.Ingester
}

func (s *metricsServer) Export(ctx context.Context, req *colmetrics.ExportMetricsServiceRequest) (*colmetrics.ExportMetricsServiceResponse, error) {
	res, err := s.ing.ExportMetrics(ctx, req)
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &colmetrics.ExportMetricsServiceResponse{}
	if res.Rejected > 0 {
		resp.PartialSuccess = &colmetrics.ExportMetricsPartialSuccess{
			RejectedDataPoints: res.Rejected,
			ErrorMessage:       res.RejectReason,
		}
	}
	return resp, nil
}

// toStatus maps ingest errors to OTLP-spec gRPC codes.
//
// Kafka slow/unavailable -> UNAVAILABLE (+RetryInfo). The OTLP spec lists
// UNAVAILABLE as retryable, so the collector keeps the batch in its sending
// queue and retries: the backlog lives in the sender, not in our memory.
// Anything else is our bug -> INTERNAL (not retryable: retrying a bug just
// repeats it).
func toStatus(err error) error {
	if errors.Is(err, ingest.ErrPublish) {
		st := status.New(codes.Unavailable, err.Error())
		if withInfo, derr := st.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(RetryDelay)}); derr == nil {
			st = withInfo
		}
		return st.Err()
	}
	return status.Error(codes.Internal, err.Error())
}
