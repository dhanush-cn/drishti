package detect

import "math"

// ring holds per-bucket values for the last len(vals) buckets, addressed by
// absolute bucket number. A slot whose stamp isn't the bucket asked for is
// empty: series don't need to be rolled forward when nothing happens to them.
type ring struct {
	vals  []float64
	stamp []int64
}

func newRing(n int) ring {
	r := ring{vals: make([]float64, n), stamp: make([]int64, n)}
	for i := range r.stamp {
		r.stamp[i] = -1
	}
	return r
}

func (r *ring) add(b int64, v float64) {
	i := int(b % int64(len(r.vals)))
	if r.stamp[i] != b {
		r.stamp[i], r.vals[i] = b, 0
	}
	r.vals[i] += v
}

func (r *ring) get(b int64) float64 {
	i := int(b % int64(len(r.vals)))
	if r.stamp[i] != b {
		return 0
	}
	return r.vals[i]
}

// sum over buckets (end-n, end].
func (r *ring) sum(end int64, n int) float64 {
	s := 0.0
	for b := end - int64(n) + 1; b <= end; b++ {
		s += r.get(b)
	}
	return s
}

// ewma is an exponentially weighted per-bucket mean with bias correction:
// starting from zero, a plain EWMA under-reads for the first half-lives, and
// a new series would look quieter than it is (the weight w tracks how much of
// the average is real data; mean = v / w).
type ewma struct {
	v, w float64
}

func (e *ewma) update(x, decay float64) {
	e.v = e.v*decay + x*(1-decay)
	e.w = e.w*decay + (1 - decay)
}

func (e ewma) mean() float64 {
	if e.w == 0 {
		return 0
	}
	return e.v / e.w
}

// decayFor returns the per-bucket decay factor for a half-life.
func decayFor(bucketsPerHalfLife float64) float64 {
	return math.Pow(0.5, 1/bucketsPerHalfLife)
}

// anomaly is one rule's lifecycle on one series.
type anomaly struct {
	id         string
	opened     int64 // bucket at which it first fired
	lastFire   int64
	lastUpdate int64
	best       Evidence
	window     [2]int64 // buckets of the window behind best
}

// seriesKind decides which rules apply.
type seriesKind uint8

const (
	kindTemplate seriesKind = iota // one Drain template of one service and severity
	kindErrorLog                   // WARN+ lines of one service
	kindSpan                       // one span (service, name, kind): calls, errors, latency
	kindMemory                     // one container's memory usage
)

// series is everything the detector keeps about one signal.
type series struct {
	kind    seriesKind
	key     string            // unique within the shard
	service string            // owner key (service, or container:name)
	labels  map[string]string // the event's "series" field
	topic   string            // where its records come from (for Source)
	lastB   int64             // last bucket with data
	created int64             // bucket of its first data

	// counts (template, error log, span calls)
	count ring
	base  ewma
	base2 ewma // of the squared bucket count: with base, the series' dispersion

	// template
	template   string
	severity   sevClass
	newTmpl    bool // created after its service's warm-up
	occurrence int64

	// span
	active  ewma // share of baseline buckets with any calls (traffic-drop guard)
	errors  ring
	baseErr ewma
	// Overdispersion of the error and slow counts against a binomial at the
	// baseline ratio: ewma of (k - p n)^2 and of n p (1-p) per bucket.
	errRes, errVar   ewma
	slowRes, slowVar ewma
	hist             []ring // per histogram bucket
	baseH            []ewma
	bounds           []float64
	seenCalls        float64 // calls folded into the baseline so far (latency rule needs MinSlowCalls)

	// memory
	mem   []memSample
	limit float64

	open map[string]*anomaly // rule -> open anomaly

	// Bucket ranges [from, to] that belonged to an anomaly: they never enter
	// the baseline. Ranges whose end has left the ring are dropped.
	taints [][2]int64
}

type memSample struct {
	t float64 // seconds
	v float64 // bytes
}
