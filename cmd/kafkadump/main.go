// Command kafkadump records Kafka records to a file, summarizes a recording,
// and replays one into Kafka for the evals (internal/kdump, docs/detector.md
// section 9, docs/evals.md).
//
//	kafkadump dump -from 2026-11-04T10:00:00Z -to 2026-11-04T12:00:00Z -out capture.kd.gz
//	kafkadump summary capture.kd.gz
//	kafkadump load -in capture.kd.gz -prefix eval.
//	kafkadump load-events -in events.jsonl -topic eval.anomalies
//
// dump writes capture.kd.gz plus capture.kd.gz.manifest.json (offset range and
// record count per partition, so the file can be checked against Kafka).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/dhanush-cn/drishti/internal/kdump"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "dump":
		err = dumpCmd(ctx, os.Args[2:])
	case "summary":
		err = summaryCmd(os.Args[2:])
	case "load":
		err = loadCmd(ctx, os.Args[2:])
	case "load-events":
		err = loadEventsCmd(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafkadump:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kafkadump dump -from RFC3339 -to RFC3339 -out FILE [-brokers B] [-topics T1,T2]\n       kafkadump summary [-json OUT] FILE\n       kafkadump load -in FILE [-prefix eval.] [-topics T1,T2] [-brokers B]\n       kafkadump load-events -in events.jsonl [-topic eval.anomalies] [-brokers B]")
	os.Exit(2)
}

type manifest struct {
	From    time.Time     `json:"from"`
	To      time.Time     `json:"to"`
	Brokers []string      `json:"brokers"`
	Ranges  []kdump.Range `json:"ranges"`
	Records int64         `json:"records"`
	Took    string        `json:"took"`
}

func dumpCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("dump", flag.ExitOnError)
	brokers := fs.String("brokers", envOr("DRISHTI_KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers")
	topics := fs.String("topics", "otlp.logs,otlp.metrics", "comma-separated topics")
	from := fs.String("from", "", "window start, RFC3339 (inclusive)")
	to := fs.String("to", "", "window end, RFC3339 (exclusive)")
	out := fs.String("out", "", "output file (.kd.gz)")
	_ = fs.Parse(args)
	if *from == "" || *to == "" || *out == "" {
		usage()
	}
	f, err := time.Parse(time.RFC3339Nano, *from)
	if err != nil {
		return fmt.Errorf("-from: %w", err)
	}
	t, err := time.Parse(time.RFC3339Nano, *to)
	if err != nil {
		return fmt.Errorf("-to: %w", err)
	}
	bs := strings.Split(*brokers, ",")
	adm, err := kgo.NewClient(kgo.SeedBrokers(bs...))
	if err != nil {
		return err
	}
	defer adm.Close()
	ranges, err := kdump.Ranges(ctx, kadm.NewClient(adm), strings.Split(*topics, ","), f, t)
	if err != nil {
		return err
	}
	file, err := os.Create(*out)
	if err != nil {
		return err
	}
	w, err := kdump.NewWriter(file)
	if err != nil {
		return err
	}
	start := time.Now()
	if err := kdump.Dump(ctx, bs, ranges, w); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	m := manifest{From: f, To: t, Brokers: bs, Ranges: ranges, Records: w.Records(), Took: time.Since(start).Round(time.Millisecond).String()}
	for _, r := range ranges {
		if want := r.End - r.Start; r.Records != want {
			return fmt.Errorf("%s/%d: wrote %d records, offsets say %d", r.Topic, r.Partition, r.Records, want)
		}
	}
	b, _ := json.MarshalIndent(m, "", " ")
	if err := os.WriteFile(*out+".manifest.json", b, 0o644); err != nil {
		return err
	}
	fmt.Printf("wrote %d records to %s in %s\n", m.Records, *out, m.Took)
	return nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
