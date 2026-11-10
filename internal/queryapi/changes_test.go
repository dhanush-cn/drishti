package queryapi

import (
	"strings"
	"testing"
)

func f(x float64) *float64 { return &x }

// The CPU rule on the scale docker_stats actually reports (percent): ad's
// adHighCpu values from capture 2 must be listed, normal wobble must not.
func TestChanges_CPUInDockerStatsPercent(t *testing.T) {
	hot := &ServiceOverview{Service: "ad", CPU: Stat{Window: f(3.16), Baseline: f(0.347)}}
	if c := changes(hot); len(c) != 1 || !strings.HasPrefix(c[0], "CPU utilization 3.2") {
		t.Fatalf("adHighCpu-sized jump: changes = %v, want the CPU line", c)
	}
	for _, s := range []Stat{{Window: f(2.549), Baseline: f(1.757)}, {Window: f(0.9), Baseline: f(0.3)}} {
		if c := changes(&ServiceOverview{Service: "x", CPU: s}); len(c) != 0 {
			t.Errorf("CPU %v/%v listed as a change: %v", *s.Window, *s.Baseline, c)
		}
	}
}
