package loadgen

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

// Pre-compressed payloads (docs/loadgen.md §12, triggered by the Week 2
// generator profile: level-6 gzip was 56% of the generator's CPU at 150k,
// proto marshal 12.7%, and the generator took ~40% of the laptop's CPU in
// receiver-only sweeps, so the "receiver knee" partly measured the
// generator).
//
// With -stamp=false every send of pool request k is byte-identical, so it
// is marshalled and gzipped once, before the run, with the same encoder
// gRPC would use (compress/gzip, default level). Each send then hands those
// bytes to gRPC unchanged: frameCodec passes them through as the message,
// and passGzip marks the frame gzip-compressed without compressing again.
// The receiver sees the same wire it would see from -gzip.

// Frame is an already-encoded (and already-gzipped) request body.
type Frame struct{ B []byte }

// PrecompressLogs marshals and gzips every pool request once.
func PrecompressLogs(pool []*collogs.ExportLogsServiceRequest) (map[*collogs.ExportLogsServiceRequest]*Frame, error) {
	out := make(map[*collogs.ExportLogsServiceRequest]*Frame, len(pool))
	for _, r := range pool {
		b, err := proto.Marshal(r)
		if err != nil {
			return nil, fmt.Errorf("marshal pool request: %w", err)
		}
		var z bytes.Buffer
		w := gzip.NewWriter(&z) // gRPC's gzip compressor: compress/gzip, default level
		w.Write(b)
		if err := w.Close(); err != nil {
			return nil, err
		}
		out[r] = &Frame{B: z.Bytes()}
	}
	return out, nil
}

// FrameCodec sends a *Frame as-is and handles every other message (the
// response) with gRPC's proto codec. Use it per call (grpc.ForceCodecV2).
type FrameCodec struct{ base encoding.CodecV2 }

func NewFrameCodec() FrameCodec { return FrameCodec{base: encoding.GetCodecV2("proto")} }

func (FrameCodec) Name() string { return "proto" }

func (c FrameCodec) Marshal(v any) (mem.BufferSlice, error) {
	if f, ok := v.(*Frame); ok {
		return mem.BufferSlice{mem.SliceBuffer(f.B)}, nil
	}
	return c.base.Marshal(v)
}

func (c FrameCodec) Unmarshal(data mem.BufferSlice, v any) error { return c.base.Unmarshal(data, v) }

// PassGzip is a per-connection compressor (grpc.WithCompressor) that labels
// frames "gzip" and writes them unchanged: they were gzipped up front. It is
// not registered globally, so nothing else in the process is affected.
type PassGzip struct{}

func (PassGzip) Type() string                   { return "gzip" }
func (PassGzip) Do(w io.Writer, p []byte) error { _, err := w.Write(p); return err }
