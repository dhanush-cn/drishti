package detector

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/dhanush-cn/drishti/internal/detect"
	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

type harness struct {
	t       *testing.T
	cluster *kfake.Cluster
	prod    *kgo.Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1),
		kfake.SeedTopics(2, detect.TopicLogs, detect.TopicMetrics, "anomalies"),
		kfake.SeedTopics(1, "live.stats"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	p, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return &harness{t: t, cluster: c, prod: p}
}

func logLine(svc string, sev logsv1.SeverityNumber, body string) []byte {
	b, _ := proto.Marshal(&collogs.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{{Key: "service.name",
			Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: svc}}}}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{{SeverityNumber: sev,
			Body: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: body}}}}}},
	}}})
	return b
}

func (h *harness) send(topic string, p int32, ts time.Time, v []byte) {
	h.t.Helper()
	if err := h.prod.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Partition: p, Timestamp: ts, Value: v}).FirstErr(); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) detector(instance string, opts ...kgo.Opt) *Detector {
	h.t.Helper()
	dc := detect.DefaultConfig()
	d, err := New(Config{
		Brokers: h.cluster.ListenAddrs(), Group: "drishti-detector",
		LogsTopic: detect.TopicLogs, MetricsTopic: detect.TopicMetrics, OutTopic: "anomalies", LiveTopic: "live.stats",
		InstanceID: instance, Version: "test", Rewind: 15 * time.Minute, Grace: time.Second,
		LiveEvery: time.Second, StatsEvery: time.Hour, Detect: dc,
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		extraOpts: opts,
	})
	if err != nil {
		h.t.Fatal(err)
	}
	return d
}

func run(d *Detector) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = d.Run(ctx)
	}()
	return func() { cancel(); <-done }
}

// watch consumes topic from the start and hands each value to fn until fn
// returns true or the timeout passes.
func (h *harness) watch(topic string, timeout time.Duration, fn func(r *kgo.Record) bool) bool {
	h.t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(h.cluster.ListenAddrs()...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		h.t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		done := false
		fs.EachRecord(func(r *kgo.Record) {
			if !done && fn(r) {
				done = true
			}
		})
		if done {
			return true
		}
	}
	return false
}

// Start-up rewinds by time (not to the start of the topic), rebuilds the
// warm-up from the rewound records, re-emits what fired in them as
// replayed, and then detects live within a bucket plus the grace.
func TestRewindThenLiveDetection(t *testing.T) {
	h := newHarness(t)
	now := time.Now()
	// Older than the rewind: never read.
	for i := range 10 {
		h.send(detect.TopicLogs, 0, now.Add(-40*time.Minute+time.Duration(i)*time.Second), logLine("cart", logsv1.SeverityNumber_SEVERITY_NUMBER_INFO, "too old to be read"))
	}
	// 14 minutes of history inside the rewind, with a new ERROR template 2
	// minutes ago.
	n := 0
	for ts := now.Add(-14 * time.Minute); ts.Before(now); ts = ts.Add(5 * time.Second) {
		h.send(detect.TopicLogs, 0, ts, logLine("cart", logsv1.SeverityNumber_SEVERITY_NUMBER_INFO, "cart loaded items=3"))
		n++
	}
	h.send(detect.TopicLogs, 0, now.Add(-2*time.Minute), logLine("cart", logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR, "failed to charge card id=5"))
	n++

	// One record batch per fetch: the rewind takes hundreds of polls, and
	// the wall-clock tick runs between them. A shard still catching up must
	// stay on its records' clock.
	d := h.detector("a", kgo.FetchMaxBytes(1), kgo.FetchMaxPartitionBytes(1))
	stop := run(d)
	var stopOnce sync.Once
	defer stopOnce.Do(stop)

	var replayed detect.Event
	if !h.watch("anomalies", 20*time.Second, func(r *kgo.Record) bool {
		_ = json.Unmarshal(r.Value, &replayed)
		return replayed.Event == "opened" && replayed.Rule == detect.RuleTemplateNew
	}) {
		t.Fatal("no anomaly from the rewound records")
	}
	if !replayed.Replayed || string(replayed.Service) != "cart" {
		t.Fatalf("rewound anomaly %+v: want replayed=true on cart", replayed)
	}

	// Live: keep the stream going, then a new ERROR template now.
	liveCtx, liveStop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for liveCtx.Err() == nil {
			_ = h.prod.ProduceSync(liveCtx, &kgo.Record{Topic: detect.TopicLogs, Partition: 0, Timestamp: time.Now(),
				Value: logLine("cart", logsv1.SeverityNumber_SEVERITY_NUMBER_INFO, "cart loaded items=3")})
			select {
			case <-liveCtx.Done():
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()
	time.Sleep(2 * time.Second)
	// The stream goes quiet with the error: only the wall-clock tick can
	// close its bucket.
	liveStop()
	wg.Wait()
	sent := time.Now()
	h.send(detect.TopicLogs, 0, sent, logLine("cart", logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR, "disk quota exceeded user=7"))
	var live detect.Event
	if !h.watch("anomalies", 20*time.Second, func(r *kgo.Record) bool {
		var e detect.Event
		_ = json.Unmarshal(r.Value, &e)
		if e.Event == "opened" && !e.Replayed && e.Rule == detect.RuleTemplateNew {
			live = e
			return true
		}
		return false
	}) {
		t.Fatal("no live anomaly")
	}
	lat := live.DetectedAt.Sub(sent)
	// The line's bucket closes at its end (<= 5 s later) plus the grace;
	// one poll timeout (1 s) of slack.
	if lat < 0 || lat > 5*time.Second+time.Second+2*time.Second {
		t.Fatalf("detected %v after the line was sent: want within a bucket + grace", lat)
	}
	t.Logf("line -> detected_at %v", lat.Round(time.Millisecond))
	// detected_at is the wall clock when the bucket was judged: its end plus
	// the grace, not the bucket end itself.
	if live.DetectedAt.Before(live.WindowEnd.Add(time.Second)) {
		t.Fatalf("detected_at %v is before the window end %v + grace", live.DetectedAt, live.WindowEnd)
	}

	var snap LiveSnapshot
	if !h.watch("live.stats", 10*time.Second, func(r *kgo.Record) bool {
		_ = json.Unmarshal(r.Value, &snap)
		return snap.Services["cart"].Logs > 0 && snap.Services["cart"].Open > 0
	}) || snap.Instance != "a" {
		t.Fatalf("no live.stats with cart activity (last %+v)", snap)
	}
	stopOnce.Do(stop)

	sh := d.det.Shards()[detect.Input{Topic: detect.TopicLogs, Partition: 0}]
	if sh == nil {
		t.Fatal("no shard for otlp.logs/0")
	}
	// The INFO and two ERROR templates, and cart's WARN+ series: a fifth
	// would be the "too old to be read" template.
	if got := sh.Series(); got != 4 {
		t.Fatalf("%d series: want 4, none from records older than the rewind", got)
	}
	if sh.Stats.Records < int64(n) {
		t.Fatalf("%d records read: want at least the %d rewound", sh.Stats.Records, n)
	}
}

// Two instances split the partitions; each drops the shards of what it gave
// up, so no input is evaluated twice.
func TestRebalanceDropsShards(t *testing.T) {
	h := newHarness(t)
	feed := func() {
		for _, topic := range []string{detect.TopicLogs} {
			for p := range int32(2) {
				h.send(topic, p, time.Now(), logLine("svc", logsv1.SeverityNumber_SEVERITY_NUMBER_INFO, "hello"))
			}
		}
	}
	feed()
	a := h.detector("a")
	stopA := run(a)
	defer func() {
		if stopA != nil {
			stopA()
		}
	}()
	waitOwned(t, a, 4)
	waitShards(t, a, 2) // both logs partitions have records

	b := h.detector("b")
	stopB := run(b)
	waitOwned(t, b, 2)
	waitOwned(t, a, 2)
	for range 3 {
		feed()
		time.Sleep(time.Second)
	}
	stopB()
	stopA()
	stopA = nil

	sa, sb := a.det.Shards(), b.det.Shards()
	for in := range sa {
		if sb[in] != nil {
			t.Errorf("%v evaluated by both instances", in)
		}
	}
	if len(sa)+len(sb) != 2 {
		t.Errorf("shards a=%v b=%v: want the 2 logs partitions between them", keys(sa), keys(sb))
	}
}

func keys(m map[detect.Input]*detect.Shard) []detect.Input {
	var out []detect.Input
	for k := range m {
		out = append(out, k)
	}
	return out
}

func waitOwned(t *testing.T, d *Detector, n int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		got := len(d.assigned)
		d.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("instance %s: never owned %d partitions", d.cfg.InstanceID, n)
}

// waitShards waits for the loop to have created n shards.
func waitShards(t *testing.T, d *Detector, n int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if int(d.shardCount.Load()) >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("instance %s: never had %d shards", d.cfg.InstanceID, n)
}
