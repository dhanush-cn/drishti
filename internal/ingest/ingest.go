// Package ingest is the transport-agnostic core of the OTLP receiver.
//
// It takes decoded OTLP export requests (the same Go structs whether they
// arrived over gRPC or, later, HTTP), splits them into Kafka-sized messages,
// and hands those to a Sink. It knows nothing about gRPC or Kafka.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
)

// Signal identifies the OTLP signal type. Each signal goes to its own topic
// because each has its own schema and its own downstream consumers.
type Signal string

const (
	Logs    Signal = "logs"
	Metrics Signal = "metrics"
)

// Message is one Kafka message: one resource's worth of telemetry.
// Value is a protobuf-encoded Export*ServiceRequest holding exactly one
// Resource*, so consumers decode it with the standard OTLP types.
type Message struct {
	Key   []byte // service.name: same service -> same partition -> ordered
	Value []byte
}

// Sink durably publishes messages. Publish must return nil only once every
// message is acknowledged by the broker; the receiver's "OK" to the sender
// depends on that.
type Sink interface {
	Publish(ctx context.Context, sig Signal, msgs []Message) error
}

// ErrPublish marks failures where the data did not reach Kafka. Transports
// map it to a retryable response (gRPC UNAVAILABLE, later HTTP 503) so the
// sender keeps the data and retries.
var ErrPublish = errors.New("publish to sink failed")

// Result is what the transport reports back to the sender.
type Result struct {
	Rejected     int64  // log records or metric data points dropped permanently
	RejectReason string // empty when nothing was rejected
}

// Config bounds the ingester's behaviour.
type Config struct {
	// MaxMessageBytes caps one Kafka message value. Must stay below the
	// broker/topic max.message.bytes (Kafka default ~1 MiB).
	MaxMessageBytes int
	// PublishTimeout bounds how long one export waits for Kafka acks.
	// Keep it below the sender's own timeout (collector default 5s) so the
	// sender gets a meaningful status instead of its own deadline error.
	PublishTimeout time.Duration
}

// Ingester turns OTLP requests into published Kafka messages.
type Ingester struct {
	sink Sink
	cfg  Config

	// Counted per logs request: sent as received vs split and re-encoded.
	passThrough, split atomic.Int64
}

// LogsPaths reports how many logs requests went each way.
func (i *Ingester) LogsPaths() (passThrough, split int64) {
	return i.passThrough.Load(), i.split.Load()
}

func New(sink Sink, cfg Config) *Ingester {
	return &Ingester{sink: sink, cfg: cfg}
}

// ExportLogs splits, publishes and waits for acks.
func (i *Ingester) ExportLogs(ctx context.Context, req *collogs.ExportLogsServiceRequest) (Result, error) {
	i.split.Add(1)
	msgs, rejected, err := splitLogs(req, i.cfg.MaxMessageBytes)
	if err != nil {
		return Result{}, err
	}
	return i.publish(ctx, Logs, msgs, rejected, "log record")
}

// ExportLogsRaw is ExportLogs for a request whose uncompressed wire bytes
// are known. A request with exactly one resource that has records and fits
// one message is published as those bytes: they already are a valid
// ExportLogsServiceRequest with one ResourceLogs, which is the Kafka value
// format, so decoding it gives the same records a re-encode would. Only
// the key (service.name) needs the decoded struct. Anything else takes the
// normal split path.
//
// Week 2 profile at 150k rec/s: proto size + marshal for the re-encode
// were ~9% and more of the receiver's CPU, for bytes it already had.
func (i *Ingester) ExportLogsRaw(ctx context.Context, req *collogs.ExportLogsServiceRequest, raw []byte) (Result, error) {
	if rls := req.GetResourceLogs(); len(rls) == 1 && len(raw) <= i.cfg.MaxMessageBytes && hasLogRecords(rls[0]) {
		i.passThrough.Add(1)
		return i.publish(ctx, Logs, []Message{{Key: serviceKey(rls[0].GetResource()), Value: raw}}, 0, "log record")
	}
	return i.ExportLogs(ctx, req)
}

// ExportMetrics splits, publishes and waits for acks.
func (i *Ingester) ExportMetrics(ctx context.Context, req *colmetrics.ExportMetricsServiceRequest) (Result, error) {
	msgs, rejected, err := splitMetrics(req, i.cfg.MaxMessageBytes)
	if err != nil {
		return Result{}, err
	}
	return i.publish(ctx, Metrics, msgs, rejected, "metric")
}

func (i *Ingester) publish(ctx context.Context, sig Signal, msgs []Message, rejected int64, unit string) (Result, error) {
	res := Result{Rejected: rejected}
	if rejected > 0 {
		res.RejectReason = fmt.Sprintf("%d item(s) dropped: a single %s exceeds the %d-byte message limit", rejected, unit, i.cfg.MaxMessageBytes)
	}
	if len(msgs) == 0 {
		return res, nil
	}

	ctx, cancel := context.WithTimeout(ctx, i.cfg.PublishTimeout)
	defer cancel()
	if err := i.sink.Publish(ctx, sig, msgs); err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrPublish, err)
	}
	return res, nil
}
