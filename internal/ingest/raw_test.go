package ingest

import (
	"bytes"
	"context"
	"testing"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

func newIng(sink Sink, maxMsg int) *Ingester {
	return New(sink, Config{MaxMessageBytes: maxMsg, PublishTimeout: time.Second})
}

func TestExportLogsRaw_OneResourcePublishesTheRequestBytes(t *testing.T) {
	req := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{logsResource("cart", 50, 40)}}
	raw, _ := proto.Marshal(req)
	sink := &fakeSink{}
	ing := newIng(sink, 1<<20)
	if _, err := ing.ExportLogsRaw(context.Background(), req, raw); err != nil {
		t.Fatal(err)
	}
	if len(sink.got) != 1 || string(sink.got[0].Key) != "cart" {
		t.Fatalf("messages %d, key %q", len(sink.got), sink.got[0].Key)
	}
	if &sink.got[0].Value[0] != &raw[0] {
		t.Fatal("value is not the request's bytes (re-encoded?)")
	}
	if rl := decodeLogs(t, sink.got[0].Value); !proto.Equal(rl, req.ResourceLogs[0]) {
		t.Fatal("value does not decode to the request's resource")
	}
	if pt, sp := ing.LogsPaths(); pt != 1 || sp != 0 {
		t.Fatalf("paths pass-through=%d split=%d, want 1/0", pt, sp)
	}
}

// Everything else takes the split path, which produces what it always did.
func TestExportLogsRaw_OtherShapesAreSplitAsBefore(t *testing.T) {
	two := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{logsResource("cart", 3, 10), logsResource("ad", 4, 10)}}
	big := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{logsResource("cart", 200, 100)}}
	empty := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{Resource: resource("cart"), ScopeLogs: []*logsv1.ScopeLogs{{}}}}}
	for name, tc := range map[string]struct {
		req    *collogs.ExportLogsServiceRequest
		maxMsg int
	}{
		"two resources":            {two, 1 << 20},
		"one resource over limit":  {big, 8 << 10},
		"one resource, no records": {empty, 1 << 20},
	} {
		t.Run(name, func(t *testing.T) {
			raw, _ := proto.Marshal(tc.req)
			viaRaw, viaSplit := &fakeSink{}, &fakeSink{}
			ing := newIng(viaRaw, tc.maxMsg)
			r1, err1 := ing.ExportLogsRaw(context.Background(), tc.req, raw)
			r2, err2 := newIng(viaSplit, tc.maxMsg).ExportLogs(context.Background(), tc.req)
			if err1 != nil || err2 != nil || r1 != r2 {
				t.Fatalf("raw %v/%v, split %v/%v", r1, err1, r2, err2)
			}
			if len(viaRaw.got) != len(viaSplit.got) {
				t.Fatalf("raw path %d messages, split path %d", len(viaRaw.got), len(viaSplit.got))
			}
			for i := range viaRaw.got {
				if !bytes.Equal(viaRaw.got[i].Value, viaSplit.got[i].Value) || !bytes.Equal(viaRaw.got[i].Key, viaSplit.got[i].Key) {
					t.Fatalf("message %d differs from the split path", i)
				}
			}
			if pt, sp := ing.LogsPaths(); pt != 0 || sp != 1 {
				t.Fatalf("paths pass-through=%d split=%d, want 0/1", pt, sp)
			}
		})
	}
}

type nopSink struct{}

func (nopSink) Publish(context.Context, Signal, []Message) error { return nil }

// BenchmarkExportLogs512: the receiver's per-request work after gRPC
// decoded the request, split (decode already done, re-encode) vs raw.
func BenchmarkExportLogs512(b *testing.B) {
	req := &collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{logsResource("cart", 512, 400)}}
	raw, _ := proto.Marshal(req)
	ing := newIng(nopSink{}, 900<<10)
	b.Run("split", func(b *testing.B) {
		for b.Loop() {
			ing.ExportLogs(context.Background(), req)
		}
	})
	b.Run("raw", func(b *testing.B) {
		for b.Loop() {
			ing.ExportLogsRaw(context.Background(), req, raw)
		}
	})
}
