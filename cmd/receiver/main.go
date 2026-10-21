// Command receiver accepts OTLP logs and metrics over gRPC and publishes
// them to Kafka. It returns OK to the sender only after Kafka acknowledged
// the data, so a crash never loses data the sender believes was delivered.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/dhanush-cn/drishti/internal/ingest"
	"github.com/dhanush-cn/drishti/internal/kafkasink"
	"github.com/dhanush-cn/drishti/internal/otlpgrpc"
	"github.com/dhanush-cn/drishti/internal/pprofsrv"
)

func main() {
	var (
		listen         = flag.String("listen", env("DRISHTI_LISTEN", ":14317"), "gRPC listen address (default :14317 for a native run, since the demo collector holds 4317 on the host; the container sets :4317 via DRISHTI_LISTEN)")
		brokers        = flag.String("brokers", env("DRISHTI_KAFKA_BROKERS", "localhost:9092"), "comma-separated Kafka bootstrap brokers")
		logsTopic      = flag.String("logs-topic", env("DRISHTI_LOGS_TOPIC", "otlp.logs"), "Kafka topic for logs")
		metricsTopic   = flag.String("metrics-topic", env("DRISHTI_METRICS_TOPIC", "otlp.metrics"), "Kafka topic for metrics")
		maxRecv        = flag.Int("max-recv-bytes", envInt("DRISHTI_MAX_RECV_BYTES", 16<<20), "max decompressed gRPC request size")
		maxMsg         = flag.Int("max-message-bytes", envInt("DRISHTI_MAX_MESSAGE_BYTES", 900<<10), "max Kafka message value; below broker max.message.bytes (~1 MiB)")
		publishTimeout = flag.Duration("publish-timeout", envDur("DRISHTI_PUBLISH_TIMEOUT", 4*time.Second), "max wait for Kafka acks per export; keep below the sender's timeout (collector: 5s)")
		linger         = flag.Duration("linger", envDur("DRISHTI_LINGER", 5*time.Millisecond), "producer linger")
		deliveryTO     = flag.Duration("delivery-timeout", envDur("DRISHTI_DELIVERY_TIMEOUT", 30*time.Second), "give up on an undelivered record after this long (its export already failed and the sender is retrying)")
		bufBytes       = flag.Int("max-buffered-bytes", envInt("DRISHTI_MAX_BUFFERED_BYTES", 64<<20), "producer memory bound before Produce blocks")
		compression    = flag.String("compression", env("DRISHTI_KAFKA_COMPRESSION", "lz4"), "Kafka batch compression: lz4, zstd, snappy, gzip, none")
		pprofAddr      = flag.String("pprof-addr", env("DRISHTI_PPROF_ADDR", ""), "serve /debug/pprof/ on this address (e.g. :6060); empty = off")
	)
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	sink, err := kafkasink.New(kafkasink.Config{
		Brokers:          strings.Split(*brokers, ","),
		LogsTopic:        *logsTopic,
		MetricsTopic:     *metricsTopic,
		Linger:           *linger,
		MaxBufferedBytes: *bufBytes,
		DeliveryTimeout:  *deliveryTO,
		Compression:      *compression,
		BatchMaxBytes:    1_000_000, // just under Kafka's default max.message.bytes (1,048,588)
	})
	if err != nil {
		log.Error("kafka client", "err", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	err = sink.CheckTopics(ctx)
	cancel()
	if err != nil {
		log.Error("startup topic check failed", "err", err)
		os.Exit(1)
	}

	if *pprofAddr != "" {
		ps, err := pprofsrv.Start(*pprofAddr)
		if err != nil {
			log.Error("pprof listen", "addr", *pprofAddr, "err", err)
			os.Exit(1)
		}
		defer ps.Close(context.Background())
		log.Info("pprof listening", "addr", ps.Addr())
	}

	ing := ingest.New(sink, ingest.Config{MaxMessageBytes: *maxMsg, PublishTimeout: *publishTimeout})
	gs, hs := otlpgrpc.NewServer(ing, *maxRecv)

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "addr", *listen, "err", err)
		os.Exit(1)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		<-stop
		log.Info("shutting down")
		// 1. Report NOT_SERVING so health-checking clients stop sending.
		hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		// 2. Stop accepting new RPCs and wait for in-flight exports. Each
		//    in-flight export already waits for Kafka acks, so when this
		//    returns, everything we said OK to is in Kafka.
		done := make(chan struct{})
		go func() { gs.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(*publishTimeout + 2*time.Second):
			gs.Stop() // unblocked senders get an error and retry elsewhere/later
		}
	}()

	log.Info("receiver listening", "addr", *listen, "brokers", *brokers, "logs_topic", *logsTopic, "metrics_topic", *metricsTopic, "compression", *compression)
	if err := gs.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
	// Serve returns as soon as GracefulStop closes the listener, while
	// in-flight exports may still be producing. Closing the producer now
	// would fail them, so wait for the drain to finish first.
	<-drained

	// 3. Flush and close the producer.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sink.Close(ctx); err != nil {
		log.Error("flush on shutdown", "err", err)
		os.Exit(1)
	}
	log.Info("stopped cleanly")
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
