package detect

import "math"

// The one statistical primitive (docs/detector.md, section 3): how surprising
// is an observed count, given what the baseline expects? Everything is
// returned as log10 of the tail probability, because the interesting values
// (1e-6 and below) underflow comparisons that are easy to get wrong in
// linear space, and a log10 reads directly as "how many nines".

// log10PoissonUpper returns log10 P(X >= k) for X ~ Poisson(mu).
//
// Only the upper tail where k > mu matters (a count at or below its mean is
// never an anomaly), and there the terms P(X = i) shrink with every i: the
// ratio of term i+1 to term i is mu/(i+1) < 1. So the sum starts at k and
// stops once a term no longer changes it.
func log10PoissonUpper(k int, mu float64) float64 {
	if k <= 0 {
		return 0
	}
	if mu <= 0 {
		return math.Inf(-1) // any count at all is impossible under a zero rate
	}
	if float64(k) <= mu {
		return 0 // not in the upper tail: probability is near 1, call it 1
	}
	lg, _ := math.Lgamma(float64(k) + 1)
	logTerm := -mu + float64(k)*math.Log(mu) - lg // ln P(X = k)
	sum, term := 1.0, 1.0
	for i := k + 1; i < k+10000; i++ {
		term *= mu / float64(i)
		sum += term
		if term < 1e-17*sum {
			break
		}
	}
	return (logTerm + math.Log(sum)) / math.Ln10
}

// log10PoissonZero returns log10 P(X = 0) for X ~ Poisson(mu): the "it went
// silent" test for a series that should have had traffic.
func log10PoissonZero(mu float64) float64 { return -mu / math.Ln10 }

// log10BinomialUpper returns log10 P(X >= k) for X ~ Binomial(n, p).
//
// Errors and slow calls are tested this way, as a share of calls, rather than
// as a raw count: a doubling of traffic doubles errors at an unchanged error
// rate, and a Poisson test on the error count alone would call that an
// incident.
func log10BinomialUpper(k, n int, p float64) float64 {
	switch {
	case k <= 0:
		return 0
	case k > n:
		return math.Inf(-1)
	case p <= 0:
		return math.Inf(-1)
	case p >= 1:
		return 0
	}
	if float64(k) <= float64(n)*p {
		return 0
	}
	lnC := lnChoose(n, k)
	logTerm := lnC + float64(k)*math.Log(p) + float64(n-k)*math.Log1p(-p)
	sum, term := 1.0, 1.0
	r := p / (1 - p)
	for i := k; i < n; i++ {
		term *= float64(n-i) / float64(i+1) * r
		sum += term
		if term < 1e-17*sum {
			break
		}
	}
	return (logTerm + math.Log(sum)) / math.Ln10
}

func lnChoose(n, k int) float64 {
	a, _ := math.Lgamma(float64(n) + 1)
	b, _ := math.Lgamma(float64(k) + 1)
	c, _ := math.Lgamma(float64(n-k) + 1)
	return a - b - c
}
