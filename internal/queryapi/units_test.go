package queryapi

import (
	"math"
	"testing"
)

// The lines found in the recordings (mask.go), and lines that must survive.
func TestMasker_RealLeakLines(t *testing.T) {
	m := NewMasker(DemoFlags)
	for _, c := range []struct {
		body  string
		attrs map[string]string
		want  bool
	}{
		{"Feature Flag adManualGc enabled, performing a manual gc now", nil, true},
		{"High CPU-Load problempattern enabled", nil, true},
		{"API request failed", map[string]string{"exception.message": "13 INTERNAL: Error: Product Catalog Fail Feature Flag Enabled"}, true},
		{"", map[string]string{"feature_flag_key": "intlShippingSlowdown", "feature_flag_variant": "off"}, true},
		{"", map[string]string{"feature_flag.provider_name": "flagd"}, true},
		{`[2026-11-03T16:23:06Z] "POST /flagservice/flagd.evaluation.v1.Service/ResolveAll HTTP/1.1" 200`, nil, true},
		{"cartfailure is ON", nil, true}, // case-insensitive
		{"Payment request failed. Invalid token. demo.user_context.loyalty_level=gold", nil, false},
		{"Wasn't able to connect to redis", map[string]string{"exception.message": "timeout"}, false},
		{"GET /api/recommendations 200", map[string]string{"http.route": "/api/recommendations"}, false},
	} {
		if got := m.Line(c.body, c.attrs); got != c.want {
			t.Errorf("Line(%q, %v) = %v, want %v", c.body, c.attrs, got, c.want)
		}
	}
}

func TestNames_MapsContainersAndHides(t *testing.T) {
	n := NewNames([]string{"email", "payment", "flagd"}, DefaultHidden)
	for owner, want := range map[string]string{
		"email": "email", "container:email": "email", "container:valkey-cart": "container:valkey-cart",
		"host:docker-desktop": "host:docker-desktop", "flagd": "", "container:flagd": "", "container:flagd-ui": "",
		"container:load-generator": "", "load-generator": "", "container:drishti-kafka": "", "otelcol-contrib": "",
	} {
		if got := n.Show(owner); got != want {
			t.Errorf("Show(%q) = %q, want %q", owner, got, want)
		}
	}
	if o := n.Owners("email"); len(o) != 2 || o[1] != "container:email" {
		t.Errorf("Owners(email) = %v", o)
	}
	if n.Owners("flagd") != nil || n.Owners("container:drishti-storage-1") != nil {
		t.Error("a hidden service is queryable")
	}
	if k := n.Known(); len(k) != 2 || k[0] != "email" || k[1] != "payment" {
		t.Errorf("Known = %v (flagd must not be listed)", k)
	}
}

func TestQuantile(t *testing.T) {
	bounds := []float64{10, 20, 50}
	for _, c := range []struct {
		q      float64
		counts []uint64
		want   float64
	}{
		{0.5, []uint64{0, 10, 0, 0}, 15},  // middle of (10, 20]
		{0.95, []uint64{90, 10, 0, 0}, 15}, // rank 95 is the 5th of 10 in (10, 20]
		{0.5, []uint64{10, 0, 0, 0}, 5},    // first bucket from 0
		{0.95, []uint64{0, 0, 0, 3}, 50},   // open last bucket: its lower bound
	} {
		if got := Quantile(c.q, bounds, c.counts); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("Quantile(%v, %v) = %v, want %v", c.q, c.counts, got, c.want)
		}
	}
	if !math.IsNaN(Quantile(0.95, bounds, []uint64{0, 0, 0, 0})) {
		t.Error("no observations must be NaN (shown as null), not 0")
	}
}
