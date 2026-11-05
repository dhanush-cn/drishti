package detect

import (
	"math"
	"testing"
)

// Reference values: mpmath at 50-60 digits, summing the probability mass of
// the tail directly.
func TestLog10PoissonUpper(t *testing.T) {
	for _, c := range []struct {
		k    int
		mu   float64
		want float64
	}{
		{3, 0.05, -4.697506866330},
		{5, 0.083, -7.513799667466},
		{10, 2.0, -4.332565026179},
		{30, 10.0, -6.600334721990},
		// 1 - CDF rounds to 0 in float64. (The first reference, 1 - CDF at 50
		// digits, was itself off by 4e-6 here: it keeps only ~5 digits of a
		// 1e-45 tail. Recomputed as the direct sum of P(X=i), i=200..599.)
		{200, 60.0, -45.170715786497},
		{4, 3.9, -0.262208634811},
	} {
		if got := log10PoissonUpper(c.k, c.mu); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("P(X>=%d | mu=%g): log10 = %.12f, want %.12f", c.k, c.mu, got, c.want)
		}
	}
	if got := log10PoissonUpper(2, 5); got != 0 {
		t.Errorf("k below the mean: %v, want 0 (not in the upper tail)", got)
	}
	if got := log10PoissonUpper(0, 5); got != 0 {
		t.Errorf("k=0: %v, want 0", got)
	}
	if got := log10PoissonUpper(1, 0); !math.IsInf(got, -1) {
		t.Errorf("mu=0, k=1: %v, want -Inf", got)
	}
}

func TestLog10BinomialUpper(t *testing.T) {
	for _, c := range []struct {
		k, n int
		p    float64
		want float64
	}{
		{4, 8, 0.005, -7.365982836786},
		{5, 10, 0.005, -9.112813967238},
		{20, 400, 0.01, -8.126613698610},
		{3, 3, 0.5, -0.903089986992},
		{90, 1000, 0.05, -7.011264271086},
		{2, 5, 0.3, -0.326260473950},
	} {
		if got := log10BinomialUpper(c.k, c.n, c.p); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("P(X>=%d | n=%d p=%g): log10 = %.12f, want %.12f", c.k, c.n, c.p, got, c.want)
		}
	}
	for _, c := range []struct {
		k, n int
		p    float64
		want float64
	}{
		{1, 10, 0.5, 0},             // below the mean
		{0, 10, 0.1, 0},             // no events
		{11, 10, 0.1, math.Inf(-1)}, // more events than trials
		{1, 10, 0, math.Inf(-1)},    // impossible at p=0
		{5, 10, 1, 0},               // certain at p=1
	} {
		if got := log10BinomialUpper(c.k, c.n, c.p); got != c.want {
			t.Errorf("edge k=%d n=%d p=%g: %v, want %v", c.k, c.n, c.p, got, c.want)
		}
	}
}

func TestLog10PoissonZero(t *testing.T) {
	// P(0 | mu=13.8155) = 1e-6: a silent series is significant at alpha=1e-6
	// once ~13.8 events were expected in the window.
	if got := log10PoissonZero(13.815510557964274); math.Abs(got+6) > 1e-12 {
		t.Fatalf("got %v, want -6", got)
	}
}
