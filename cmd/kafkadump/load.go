package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/dhanush-cn/drishti/internal/kdump"
	"github.com/dhanush-cn/drishti/internal/partitioning"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// load replays a recording into Kafka for the evals (Week 5 design E6):
// every record goes to "<prefix><topic>", same partition, key, value and
// record timestamp. The normal consumers then store it, so evals run on
// data that went through the real path.
//
// The original timestamps are the point (alerts, windows and received_at
// must match the live run), and they are also the trap: on the normal
// topics Kafka's 24 h retention would delete a week-old recording, and the
// 7-day TTL in ClickHouse drops expired rows on insert. The eval topics are
// created with unlimited retention and the eval database without TTL
// (scripts/create-eval.sh); load refuses topics that would delete them.
//
// It refuses non-empty target topics: a second load would store everything
// twice (the storage consumer is exactly-once per Kafka record, and a
// reloaded record is a new record).
func loadCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	brokers := fs.String("brokers", envOr("DRISHTI_KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers")
	in := fs.String("in", "", "recording (.kd.gz)")
	prefix := fs.String("prefix", "eval.", "target topic = prefix + recorded topic")
	topics := fs.String("topics", "otlp.logs,otlp.metrics", "recorded topics to load")
	_ = fs.Parse(args)
	if *in == "" || *prefix == "" {
		return errors.New("usage: kafkadump load -in FILE [-prefix eval.] [-topics T1,T2] [-brokers B]")
	}
	want := strings.Split(*topics, ",")
	f, err := os.Open(*in)
	if err != nil {
		return err
	}
	defer f.Close()
	r, err := kdump.NewReader(f)
	if err != nil {
		return err
	}
	cl, err := newProducer(*brokers)
	if err != nil {
		return err
	}
	defer cl.Close()
	targets := make([]string, len(want))
	for i, t := range want {
		targets[i] = *prefix + t
	}
	parts, err := checkTargets(ctx, kadm.NewClient(cl), targets)
	if err != nil {
		return err
	}

	start := time.Now()
	p := newPipe(ctx, cl)
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if !slices.Contains(want, rec.Topic) {
			continue
		}
		to := *prefix + rec.Topic
		if rec.Partition >= parts[to] {
			return fmt.Errorf("%s has %d partitions, the recording has %s/%d", to, parts[to], rec.Topic, rec.Partition)
		}
		p.send(&kgo.Record{Topic: to, Partition: rec.Partition, Key: rec.Key, Value: rec.Value, Timestamp: rec.Timestamp})
	}
	n, err := p.wait()
	if err != nil {
		return err
	}
	return report(ctx, kadm.NewClient(cl), targets, n, time.Since(start))
}

// loadEvents produces detector events (cmd/detector -replay output, JSON
// lines) to a topic, keyed by service like the live detector, with the
// event's detected_at as record timestamp.
func loadEventsCmd(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("load-events", flag.ExitOnError)
	brokers := fs.String("brokers", envOr("DRISHTI_KAFKA_BROKERS", "localhost:9092"), "Kafka bootstrap brokers")
	in := fs.String("in", "", "events, JSON lines (cmd/detector -replay -out)")
	topic := fs.String("topic", "eval.anomalies", "target topic")
	_ = fs.Parse(args)
	if *in == "" {
		return errors.New("usage: kafkadump load-events -in events.jsonl [-topic eval.anomalies] [-brokers B]")
	}
	f, err := os.Open(*in)
	if err != nil {
		return err
	}
	defer f.Close()
	cl, err := newProducer(*brokers)
	if err != nil {
		return err
	}
	defer cl.Close()
	parts, err := checkTargets(ctx, kadm.NewClient(cl), []string{*topic})
	if err != nil {
		return err
	}
	part := partitioning.New()
	start := time.Now()
	p := newPipe(ctx, cl)
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	line := 0
	for sc.Scan() {
		line++
		b := slices.Clone(sc.Bytes())
		if len(strings.TrimSpace(string(b))) == 0 {
			continue
		}
		var ev struct {
			Service    string    `json:"service"`
			DetectedAt time.Time `json:"detected_at"`
		}
		if err := json.Unmarshal(b, &ev); err != nil || ev.Service == "" || ev.DetectedAt.IsZero() {
			return fmt.Errorf("%s:%d: not an anomaly event (need service and detected_at): %v", *in, line, err)
		}
		pn, err := partitioning.ForKey(part, *topic, []byte(ev.Service), int(parts[*topic]))
		if err != nil {
			return err
		}
		p.send(&kgo.Record{Topic: *topic, Partition: int32(pn), Key: []byte(ev.Service), Value: b, Timestamp: ev.DetectedAt})
	}
	if err := sc.Err(); err != nil {
		return err
	}
	n, err := p.wait()
	if err != nil {
		return err
	}
	return report(ctx, kadm.NewClient(cl), []string{*topic}, n, time.Since(start))
}

func newProducer(brokers string) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		// Every record carries its partition: the recording's (load), or the
		// one the live detector's partitioner gives its key (load-events).
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchMaxBytes(16<<20),
		kgo.MaxBufferedBytes(256<<20),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
}

// checkTargets returns each topic's partition count, refusing a missing
// topic, a non-empty one, or one whose retention would delete old records.
func checkTargets(ctx context.Context, adm *kadm.Client, topics []string) (map[string]int32, error) {
	md, err := adm.Metadata(ctx, topics...)
	if err != nil {
		return nil, err
	}
	parts := map[string]int32{}
	for _, t := range topics {
		d, ok := md.Topics[t]
		if !ok || d.Err != nil {
			return nil, fmt.Errorf("topic %s missing (run scripts/create-eval.sh): %v", t, d.Err)
		}
		parts[t] = int32(len(d.Partitions))
	}
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return nil, err
	}
	var nonEmpty []string
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			nonEmpty = append(nonEmpty, fmt.Sprintf("%s/%d (end %d)", o.Topic, o.Partition, o.Offset))
		}
	})
	if len(nonEmpty) > 0 {
		slices.Sort(nonEmpty)
		return nil, fmt.Errorf("target topics not empty, a second load would store everything twice (delete and recreate them): %s", strings.Join(nonEmpty, ", "))
	}
	cfgs, err := adm.DescribeTopicConfigs(ctx, topics...)
	if err != nil {
		return nil, err
	}
	for _, rc := range cfgs {
		for _, c := range rc.Configs {
			if (c.Key == "retention.ms" || c.Key == "retention.bytes") && c.MaybeValue() != "-1" {
				return nil, fmt.Errorf("%s has %s=%s: Kafka would delete a recording replayed with its original timestamps; create it with -1 (scripts/create-eval.sh)", rc.Name, c.Key, c.MaybeValue())
			}
		}
	}
	return parts, nil
}

// pipe produces asynchronously and keeps the first error.
type pipe struct {
	ctx  context.Context
	cl   *kgo.Client
	sent map[string]int64
	errc chan error
}

func newPipe(ctx context.Context, cl *kgo.Client) *pipe {
	return &pipe{ctx: ctx, cl: cl, sent: map[string]int64{}, errc: make(chan error, 1)}
}

func (p *pipe) send(r *kgo.Record) {
	p.sent[r.Topic]++
	p.cl.Produce(p.ctx, r, func(_ *kgo.Record, err error) {
		if err != nil {
			select {
			case p.errc <- err:
			default:
			}
		}
	})
}

func (p *pipe) wait() (map[string]int64, error) {
	if err := p.cl.Flush(p.ctx); err != nil {
		return nil, err
	}
	select {
	case err := <-p.errc:
		return nil, fmt.Errorf("produce: %w", err)
	default:
	}
	return p.sent, nil
}

// report checks every produced record arrived (end offsets = counts, the
// topics were empty) and prints one line per topic.
func report(ctx context.Context, adm *kadm.Client, topics []string, sent map[string]int64, took time.Duration) error {
	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return err
	}
	got := map[string]int64{}
	ends.Each(func(o kadm.ListedOffset) { got[o.Topic] += o.Offset })
	for _, t := range topics {
		fmt.Printf("%s: %d records\n", t, got[t])
		if got[t] != sent[t] {
			return fmt.Errorf("%s: produced %d, topic holds %d", t, sent[t], got[t])
		}
	}
	fmt.Printf("loaded in %s\n", took.Round(time.Millisecond))
	return nil
}
