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
}

func New(sink Sink, cfg Config) *Ingester {
	return &Ingester{sink: sink, cfg: cfg}
}

// ExportLogs splits, publishes and waits for acks.
func (i *Ingester) ExportLogs(ctx context.Context, req *collogs.ExportLogsServiceRequest) (Result, error) {
	msgs, rejected, err := splitLogs(req, i.cfg.MaxMessageBytes)
	if err != nil {
		return Result{}, err
	}
	return i.publish(ctx, Logs, msgs, rejected, "log record")
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
