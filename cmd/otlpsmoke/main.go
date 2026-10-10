// Command otlpsmoke sends one OTLP logs export and one metrics export to the
// receiver, gzip-compressed like the collector does, and prints the result.
// It is a manual smoke test, not the load generator.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/encoding/gzip"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

func main() {
	addr := flag.String("addr", "localhost:14317", "receiver address")
	services := flag.Int("services", 3, "number of distinct service.name resources")
	records := flag.Int("records", 100, "log records per service")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fail(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	now := uint64(time.Now().UnixNano())
	logsReq := &collogs.ExportLogsServiceRequest{}
	metricsReq := &colmetrics.ExportMetricsServiceRequest{}
	for s := 0; s < *services; s++ {
		res := &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{
			Key:   "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("smoke-svc-%d", s)}},
		}}}
		var recs []*logsv1.LogRecord
		for i := 0; i < *records; i++ {
			recs = append(recs, &logsv1.LogRecord{
				TimeUnixNano: now,
				SeverityText: "INFO",
				Body:         &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: fmt.Sprintf("smoke log %d", i)}},
			})
		}
		logsReq.ResourceLogs = append(logsReq.ResourceLogs, &logsv1.ResourceLogs{
			Resource:  res,
			ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: recs}},
		})
		metricsReq.ResourceMetrics = append(metricsReq.ResourceMetrics, &metricsv1.ResourceMetrics{
			Resource: res,
			ScopeMetrics: []*metricsv1.ScopeMetrics{{Metrics: []*metricsv1.Metric{{
				Name: "smoke.requests",
				Data: &metricsv1.Metric_Gauge{Gauge: &metricsv1.Gauge{DataPoints: []*metricsv1.NumberDataPoint{{
					TimeUnixNano: now,
					Value:        &metricsv1.NumberDataPoint_AsInt{AsInt: int64(s)},
				}}}},
			}}}},
		})
	}

	start := time.Now()
	lr, err := collogs.NewLogsServiceClient(conn).Export(ctx, logsReq, grpc.UseCompressor("gzip"))
	if err != nil {
		fail(fmt.Errorf("logs export: %w", err))
	}
	fmt.Printf("logs: %d services x %d records OK in %v, partial_success=%v\n", *services, *records, time.Since(start), lr.GetPartialSuccess())

	start = time.Now()
	mr, err := colmetrics.NewMetricsServiceClient(conn).Export(ctx, metricsReq, grpc.UseCompressor("gzip"))
	if err != nil {
		fail(fmt.Errorf("metrics export: %w", err))
	}
	fmt.Printf("metrics: %d services OK in %v, partial_success=%v\n", *services, time.Since(start), mr.GetPartialSuccess())
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
