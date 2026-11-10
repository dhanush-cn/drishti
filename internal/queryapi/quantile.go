package queryapi

import "math"

// Quantile estimates the q-quantile (0..1) of an explicit-bucket histogram:
// counts[i] holds observations in (bounds[i-1], bounds[i]], the last bucket
// everything above the last bound. Linear interpolation inside the bucket
// that holds the rank, like Prometheus' histogram_quantile; the first bucket
// interpolates from 0, the open last bucket returns its lower bound (there is
// no upper edge to interpolate to). NaN when there are no observations.
func Quantile(q float64, bounds []float64, counts []uint64) float64 {
	var total uint64
	for _, c := range counts {
		total += c
	}
	if total == 0 || len(counts) != len(bounds)+1 {
		return math.NaN()
	}
	rank := q * float64(total)
	var cum float64
	for i, c := range counts {
		next := cum + float64(c)
		if next >= rank && c > 0 {
			if i == len(bounds) {
				return bounds[len(bounds)-1]
			}
			lo := 0.0
			if i > 0 {
				lo = bounds[i-1]
			}
			return lo + (bounds[i]-lo)*(rank-cum)/float64(c)
		}
		cum = next
	}
	return bounds[len(bounds)-1]
}
