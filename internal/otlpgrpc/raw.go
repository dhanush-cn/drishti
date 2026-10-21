package otlpgrpc

import (
	"context"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// Pass-through for logs (docs/receiver.md decision 7).
//
// The generated LogsService handler decodes the request into structs and
// drops the bytes. ingest then re-encodes each resource as a Kafka value:
// for a one-resource request (all observed collector traffic) that is
// proto size + marshal of bytes we already had. To keep them, the logs
// service is registered with its own handler that decodes into rawLogs, and
// the server's codec, for that one type, keeps a copy of the wire bytes
// next to the decoded request. Every other message (responses, health,
// reflection, metrics) goes to gRPC's proto codec unchanged.

// rawLogs is the request as decoded structs plus its uncompressed bytes.
type rawLogs struct {
	raw []byte
	req collogs.ExportLogsServiceRequest
}

type rawCodec struct{ base encoding.CodecV2 }

func newRawCodec() rawCodec { return rawCodec{base: encoding.GetCodecV2("proto")} }

// Name must stay "proto": it is the content-subtype clients negotiate.
func (rawCodec) Name() string { return "proto" }

func (c rawCodec) Marshal(v any) (mem.BufferSlice, error) { return c.base.Marshal(v) }

func (c rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	r, ok := v.(*rawLogs)
	if !ok {
		return c.base.Unmarshal(data, v)
	}
	// gRPC frees data when this returns, and the bytes must outlive the
	// call (they become the Kafka value, possibly still buffered after the
	// export returned), so they are copied into memory gRPC doesn't own.
	// The re-marshal this replaces allocated the same size.
	r.raw = data.Materialize()
	return proto.Unmarshal(r.raw, &r.req)
}

// logsServiceDesc is collogs.LogsService_ServiceDesc with a handler that
// decodes into rawLogs. Same service name, method and metadata, so clients
// and reflection (grpcurl) see the standard OTLP service.
var logsServiceDesc = grpc.ServiceDesc{
	ServiceName: collogs.LogsService_ServiceDesc.ServiceName,
	HandlerType: (*collogs.LogsServiceServer)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "Export",
		Handler:    exportLogsRawHandler,
	}},
	Streams:  []grpc.StreamDesc{},
	Metadata: collogs.LogsService_ServiceDesc.Metadata,
}

func exportLogsRawHandler(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
	in := new(rawLogs)
	if err := dec(in); err != nil {
		return nil, err
	}
	s := srv.(*logsServer)
	if interceptor == nil {
		return s.exportRaw(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: collogs.LogsService_Export_FullMethodName}
	return interceptor(ctx, &in.req, info, func(ctx context.Context, _ any) (any, error) { return s.exportRaw(ctx, in) })
}
