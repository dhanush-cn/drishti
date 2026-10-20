package loadgen

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

// RunIDAttr marks every resource with the run that produced it, so Week 2
// can count records per run in ClickHouse (docs/loadgen.md §6).
const RunIDAttr = "drishti.loadgen.run_id"

// LogShape fixes the shape of every log record in a run. It is reported with
// every number (§1): the same records/sec at 100 B and at 1 KB per record are
// different claims.
type LogShape struct {
	BatchSize int `json:"batch_size"` // log records per request
	Services  int `json:"services"`   // distinct service.name keys across the run
	// Resources is the requested resources (services) per request. Default 1:
	// observed collector traffic carries exactly one resource per request
	// (docs/loadgen.md §13), because without a batch processor the collector
	// forwards each SDK export, and an SDK export is one service. 20 is kept
	// as a sweep point. The effective value is ResourcesPerRequest().
	Resources int `json:"resources_per_request_requested"`
	BodyMin   int `json:"body_min_bytes"`
	BodyMax   int `json:"body_max_bytes"` // body length range, inclusive
	AttrsMin  int `json:"attrs_min"`
	AttrsMax  int `json:"attrs_max"` // attributes per record, inclusive
}

func (s LogShape) Validate() error {
	switch {
	case s.BatchSize < 1:
		return fmt.Errorf("batch size must be >= 1")
	case s.Services < 1:
		return fmt.Errorf("services must be >= 1")
	case s.Resources < 1:
		return fmt.Errorf("resources per request must be >= 1")
	case s.BodyMin < 1 || s.BodyMax < s.BodyMin:
		return fmt.Errorf("body range %d..%d invalid", s.BodyMin, s.BodyMax)
	case s.AttrsMin < 0 || s.AttrsMax < s.AttrsMin || s.AttrsMax > len(attrKeys):
		return fmt.Errorf("attrs range %d..%d invalid (max %d)", s.AttrsMin, s.AttrsMax, len(attrKeys))
	}
	return nil
}

// ResourcesPerRequest is min(Resources, Services, BatchSize): a request never
// repeats a service, and a resource never carries zero records.
func (s LogShape) ResourcesPerRequest() int {
	n := s.Resources
	if s.Services < n {
		n = s.Services
	}
	if s.BatchSize < n {
		n = s.BatchSize
	}
	return n
}

// PoolSizeFor rounds k up so that every service appears equally often across
// the pool (pool slots = k x resources per request must divide evenly by
// Services). Otherwise, with 1 resource per request, 256 requests over 20
// services would send some keys 13 times per rotation and others 12: an 8%
// skew built into the generator rather than measured.
func (s LogShape) PoolSizeFor(k int) int {
	nres := s.ResourcesPerRequest()
	for (k*nres)%s.Services != 0 {
		k++
	}
	return k
}

// ServiceName is the key a resource is produced under. How these keys spread
// over partitions is reported per run (docs/loadgen.md §7), never assumed.
func ServiceName(i int) string { return fmt.Sprintf("svc-%d", i) }

// Templates are the log "shapes" (Drain's ground truth later, §6). Tokens in
// {braces} are filled per record. Templated text, not random bytes, because
// real logs compress like templated text, and Kafka's lz4 and gRPC's gzip
// would otherwise report unrealistic bytes/sec.
var Templates = []string{
	"GET {route} completed status={status} duration_ms={ms}",
	"POST {route} completed status={status} duration_ms={ms} bytes={bytes}",
	"user {user} added item {item} to cart quantity={qty}",
	"order {order} placed for user {user} total={amount} currency=USD",
	"payment {order} authorized provider=mock latency_ms={ms}",
	"cache miss key={item} region={region} fallback=db",
	"db query on table products took {ms} ms rows={qty}",
	"retrying call to {peer} attempt={qty} after error: connection reset",
	"failed to reach {peer}: context deadline exceeded after {ms} ms",
	"recommendations computed for user {user} count={qty}",
	"shipping quote {order} computed zip={zip} cost={amount}",
	"health check ok uptime_s={bytes} goroutines={qty}",
}

var (
	routes   = []string{"/api/cart", "/api/products", "/api/checkout", "/api/recommendations", "/api/shipping", "/api/currency"}
	peers    = []string{"cart:7070", "productcatalog:3550", "payment:50051", "shipping:50050", "currency:7001"}
	regions  = []string{"us-east-1", "eu-west-1", "ap-south-1"}
	statuses = []string{"200", "200", "200", "200", "201", "404", "500"}
	attrKeys = []string{"http.method", "http.route", "http.status_code", "user.id", "session.id", "region", "peer.service", "thread.name", "code.function", "app.order.id"}
)

// Severity mix: mostly INFO, as in real services.
var severities = []struct {
	n    logsv1.SeverityNumber
	text string
	w    int
}{
	{logsv1.SeverityNumber_SEVERITY_NUMBER_DEBUG, "DEBUG", 15},
	{logsv1.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO", 70},
	{logsv1.SeverityNumber_SEVERITY_NUMBER_WARN, "WARN", 10},
	{logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR", 5},
}

func str(v string) *commonv1.AnyValue {
	return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}
}

type gen struct{ r *rand.Rand }

func (g gen) pick(xs []string) string { return xs[g.r.IntN(len(xs))] }
func (g gen) between(lo, hi int) int  { return lo + g.r.IntN(hi-lo+1) }

func (g gen) fill(tmpl string) string {
	repl := strings.NewReplacer(
		"{route}", g.pick(routes),
		"{status}", g.pick(statuses),
		"{ms}", fmt.Sprint(g.between(1, 2500)),
		"{bytes}", fmt.Sprint(g.between(100, 90000)),
		"{user}", fmt.Sprintf("u%06d", g.r.IntN(50000)),
		"{item}", fmt.Sprintf("SKU-%05d", g.r.IntN(20000)),
		"{qty}", fmt.Sprint(g.between(1, 9)),
		"{order}", fmt.Sprintf("ord-%08x", g.r.Uint32()),
		"{amount}", fmt.Sprintf("%d.%02d", g.between(1, 900), g.r.IntN(100)),
		"{region}", g.pick(regions),
		"{peer}", g.pick(peers),
		"{zip}", fmt.Sprintf("%05d", g.r.IntN(100000)),
	)
	return repl.Replace(tmpl)
}

// body builds a templated body of exactly n bytes: pad with a variable
// trace-ish suffix (still structured text) or truncate.
func (g gen) body(n int) string {
	b := g.fill(Templates[g.r.IntN(len(Templates))])
	for len(b) < n {
		b += fmt.Sprintf(" req=%016x", g.r.Uint64())
	}
	return b[:n]
}

func (g gen) attrs(n int) []*commonv1.KeyValue {
	keys := g.r.Perm(len(attrKeys))[:n]
	kv := make([]*commonv1.KeyValue, n)
	for i, k := range keys {
		var v string
		switch attrKeys[k] {
		case "http.method":
			v = g.pick([]string{"GET", "POST", "PUT"})
		case "http.route":
			v = g.pick(routes)
		case "http.status_code":
			v = g.pick(statuses)
		case "region":
			v = g.pick(regions)
		case "peer.service":
			v = g.pick(peers)
		default:
			v = fmt.Sprintf("%x", g.r.Uint64())
		}
		kv[i] = &commonv1.KeyValue{Key: attrKeys[k], Value: str(v)}
	}
	return kv
}

func (g gen) severity() (logsv1.SeverityNumber, string) {
	x := g.r.IntN(100)
	for _, s := range severities {
		if x < s.w {
			return s.n, s.text
		}
		x -= s.w
	}
	return severities[1].n, severities[1].text
}

// LogPool pre-generates k distinct requests (§6). Sending them in rotation
// costs nothing per send, while still giving compressors and the
// partitioner realistic variety. Timestamps are fixed at generation time;
// Stamp (stamp.go) moves them to send time per send when -stamp is on.
//
// Request j carries ResourcesPerRequest() resources. Their services rotate
// with j, so across the pool every service appears equally often (k is
// rounded up by PoolSizeFor), and the records of one request are split as
// evenly as possible between them. The returned pool's length is the
// effective pool size.
func LogPool(shape LogShape, k int, runID string, seed uint64) ([]*collogs.ExportLogsServiceRequest, error) {
	if err := shape.Validate(); err != nil {
		return nil, err
	}
	if k < 1 {
		return nil, fmt.Errorf("pool size must be >= 1")
	}
	k = shape.PoolSizeFor(k)
	g := gen{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
	ts := uint64(time.Now().UnixNano())
	nres := shape.ResourcesPerRequest()
	pool := make([]*collogs.ExportLogsServiceRequest, k)
	for j := range pool {
		req := &collogs.ExportLogsServiceRequest{}
		for ri := 0; ri < nres; ri++ {
			svc := (j*nres + ri) % shape.Services
			n := shape.BatchSize / nres
			if ri < shape.BatchSize%nres {
				n++
			}
			recs := make([]*logsv1.LogRecord, n)
			for x := range recs {
				sevN, sevT := g.severity()
				recs[x] = &logsv1.LogRecord{
					TimeUnixNano:         ts,
					ObservedTimeUnixNano: ts,
					SeverityNumber:       sevN,
					SeverityText:         sevT,
					Body:                 str(g.body(g.between(shape.BodyMin, shape.BodyMax))),
					Attributes:           g.attrs(g.between(shape.AttrsMin, shape.AttrsMax)),
				}
			}
			req.ResourceLogs = append(req.ResourceLogs, &logsv1.ResourceLogs{
				Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
					{Key: "service.name", Value: str(ServiceName(svc))},
					{Key: RunIDAttr, Value: str(runID)},
				}},
				ScopeLogs: []*logsv1.ScopeLogs{{
					Scope:      &commonv1.InstrumentationScope{Name: "drishti.loadgen"},
					LogRecords: recs,
				}},
			})
		}
		pool[j] = req
	}
	return pool, nil
}

// CountLogRecords is the number of events in a request (§1).
func CountLogRecords(req *collogs.ExportLogsServiceRequest) int64 {
	var n int64
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			n += int64(len(sl.GetLogRecords()))
		}
	}
	return n
}
