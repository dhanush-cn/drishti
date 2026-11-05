package drain

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/dhanush-cn/drishti/internal/loadgen"
)

// groupingAccuracy is the Loghub benchmark's metric (Zhu et al., ICSE-SEIP
// 2019): a line is parsed correctly when the set of lines in its cluster is
// exactly the set of lines of its true template. GA = correct / total.
func groupingAccuracy(truth []string, clusters []int) float64 {
	byTruth := map[string]map[int]int{} // truth -> cluster -> lines
	byCluster := map[int]map[string]int{}
	for i := range truth {
		if byTruth[truth[i]] == nil {
			byTruth[truth[i]] = map[int]int{}
		}
		byTruth[truth[i]][clusters[i]]++
		if byCluster[clusters[i]] == nil {
			byCluster[clusters[i]] = map[string]int{}
		}
		byCluster[clusters[i]][truth[i]]++
	}
	correct := 0
	for t, cs := range byTruth {
		if len(cs) != 1 {
			continue // the template was split
		}
		for c, n := range cs {
			if len(byCluster[c]) == 1 { // and its cluster holds nothing else
				correct += n
			}
			_ = t
		}
	}
	return float64(correct) / float64(len(truth))
}

// fill instantiates a load generator template with values of the same kinds
// the generator uses (internal/loadgen: routes, peers and regions from small
// sets, everything else numbers or IDs with digits).
func fill(r *rand.Rand, tmpl string) string {
	pick := func(xs ...string) string { return xs[r.IntN(len(xs))] }
	return strings.NewReplacer(
		"{route}", pick("/api/cart", "/api/products", "/api/checkout", "/api/recommendations", "/api/shipping", "/api/currency"),
		"{status}", pick("200", "201", "404", "500"),
		"{ms}", fmt.Sprint(1+r.IntN(2500)),
		"{bytes}", fmt.Sprint(100+r.IntN(90000)),
		"{user}", fmt.Sprintf("u%06d", r.IntN(50000)),
		"{item}", fmt.Sprintf("SKU-%05d", r.IntN(20000)),
		"{qty}", fmt.Sprint(1+r.IntN(9)),
		"{order}", fmt.Sprintf("ord-%08x", r.Uint32()|1), // |1: never all letters
		"{amount}", fmt.Sprintf("%d.%02d", 1+r.IntN(900), r.IntN(100)),
		"{region}", pick("us-east-1", "eu-west-1", "ap-south-1"),
		"{peer}", pick("cart:7070", "productcatalog:3550", "payment:50051"),
		"{zip}", fmt.Sprintf("%05d", r.IntN(100000)),
	).Replace(tmpl)
}

// On the load generator's 12 body templates: no cluster ever mixes two
// templates (purity 1: merging two would hide one of them from the
// detector), keys stay in the template ("status=<#>"), and the only splits
// are the known ones: the route in second position isn't generalized at
// depth 4, so GET and POST split into one cluster per route (6 each). That
// gives 10 + 2x6 = 22 clusters, plus a rare stray when an order ID happens
// to be all letters (hex a-f, no digit to mask) in second position.
func TestParser_LoadGeneratorTemplates(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	p := New(DefaultConfig())
	members := map[int]map[string]bool{} // cluster -> templates seen in it
	for i := 0; i < 6000; i++ {
		tmpl := loadgen.Templates[r.IntN(len(loadgen.Templates))]
		c, _ := p.Add(fill(r, tmpl))
		if members[c.ID] == nil {
			members[c.ID] = map[string]bool{}
		}
		members[c.ID][tmpl] = true
	}
	for id, ts := range members {
		if len(ts) != 1 {
			t.Errorf("cluster %d (%q) mixes %d templates: %v", id, p.Clusters()[id].Template(), len(ts), ts)
		}
	}
	if n := p.Len(); n < 22 || n > 25 {
		for _, c := range p.Clusters() {
			t.Logf("%5d  %s", c.Count, c.Template())
		}
		t.Fatalf("%d clusters, want 22 (+ at most 3 strays)", n)
	}
	want := "GET /api/cart completed status=<#> duration_ms=<#>"
	found := false
	for _, c := range p.Clusters() {
		if c.Template() == want {
			found = true
		}
	}
	if !found {
		t.Errorf("no cluster %q among %v", want, templates(p))
	}
	// At depth 3 the route is generalized and the 12 templates come back
	// exactly (grouping accuracy 1): the cost of depth 4 here is the split.
	cfg := DefaultConfig()
	cfg.Depth = 3
	p3 := New(cfg)
	var truth []string
	var got []int
	r = rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 6000; i++ {
		tmpl := loadgen.Templates[r.IntN(len(loadgen.Templates))]
		c, _ := p3.Add(fill(r, tmpl))
		truth, got = append(truth, tmpl), append(got, c.ID)
	}
	if ga := groupingAccuracy(truth, got); ga != 1 || p3.Len() != 12 {
		t.Errorf("depth 3: grouping accuracy %.4f, %d clusters; want 1 and 12", ga, p3.Len())
	}
}

func templates(p *Parser) []string {
	var out []string
	for _, c := range p.Clusters() {
		out = append(out, c.Template())
	}
	return out
}

// "new" is true exactly once per cluster: on the line that created it.
func TestParser_NewOnlyOnTheFirstLineOfATemplate(t *testing.T) {
	p := New(DefaultConfig())
	lines := []struct {
		line string
		new  bool
	}{
		{"Payment request failed. Invalid token. id=17", true},
		{"Payment request failed. Invalid token. id=23", false},
		{"Charge request received.", true},
		{"Payment request failed. Invalid token. id=99", false},
		{"Transaction complete.", true},
	}
	for _, l := range lines {
		if _, isNew := p.Add(l.line); isNew != l.new {
			t.Fatalf("%q: new = %v, want %v", l.line, isNew, l.new)
		}
	}
}

// Tokens with digits are variables; words are kept until lines disagree.
func TestMask(t *testing.T) {
	for in, want := range map[string]string{
		"completed": "completed", "status=200": "status=<#>", "duration_ms=12": "duration_ms=<#>",
		"=5": "<#>", "u000123": "<#>", "a1=2": "<#>", "key=value": "key=value", "": "",
		"ord-cbdcfeeb": "ord-cbdcfeeb", // no digit: left for the similarity step
	} {
		if got := mask(in); got != want {
			t.Errorf("mask(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParser_MasksTokensWithDigits(t *testing.T) {
	p := New(DefaultConfig())
	c, _ := p.Add(`[2026-11-03T16:33:05.123Z] "GET /api/products/OLJCESPC7Z HTTP/1.1" 200 via_upstream`)
	want := `<#> "GET <#> <#> <#> via_upstream`
	if c.Template() != want {
		t.Fatalf("template %q, want %q", c.Template(), want)
	}
}

// Lines below the similarity threshold start their own cluster; at or above
// it they merge and the differing positions become wildcards.
func TestParser_SimilarityThreshold(t *testing.T) {
	p := New(DefaultConfig())
	a, _ := p.Add("cache miss key alpha region east fallback db") // 8 tokens
	b, _ := p.Add("cache miss key beta region west fallback db")  // 6/8 same: merge
	if a != b || a.Template() != "cache miss key <*> region <*> fallback db" {
		t.Fatalf("merge: %q / %q", a.Template(), b.Template())
	}
	// Same length and first two tokens (same leaf), 3/8 of the template's
	// non-wildcard tokens in place: 0.375 < 0.4, a new cluster.
	c, isNew := p.Add("cache miss lookup gamma table north retry later")
	if !isNew || c == a {
		t.Fatalf("below threshold joined %q", a.Template())
	}
}

// Past MaxChildren, unseen leading tokens share the wildcard branch, so the
// tree's fan-out stays bounded (random first words can't explode it).
func TestParser_MaxChildrenRoutesToWildcard(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxChildren = 3
	p := New(cfg)
	for _, w := range []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"} {
		p.Add(w + " started worker pool now")
	}
	root := p.byLen[5]
	if n := len(root.children); n > cfg.MaxChildren+1 {
		t.Fatalf("root has %d children, want at most %d (+ wildcard)", n, cfg.MaxChildren)
	}
	if root.children[Wildcard] == nil {
		t.Fatal("no wildcard child after the limit")
	}
}

// Past MaxClusters, unmatched lines go to the overflow cluster (ID -1).
func TestParser_MaxClustersOverflows(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxClusters = 5
	p := New(cfg)
	for i := 0; i < 50; i++ {
		// Distinct words of distinct lengths: nothing merges.
		p.Add(strings.Repeat("w ", i%40+1) + strings.Repeat("x", i+1))
	}
	if p.Len() != 5 {
		t.Fatalf("%d clusters, want 5", p.Len())
	}
	cs := p.Clusters()
	if last := cs[len(cs)-1]; last.ID != -1 || last.Count == 0 {
		t.Fatalf("overflow cluster = %+v", last)
	}
}

func TestParser_EmptyAndWhitespaceLinesShareOneCluster(t *testing.T) {
	p := New(DefaultConfig())
	a, _ := p.Add("")
	b, _ := p.Add("   \t ")
	if a != b || p.Len() != 1 {
		t.Fatalf("empty lines made %d clusters", p.Len())
	}
}

func TestParser_LongLinesAreCut(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxTokens = 8
	p := New(cfg)
	c, _ := p.Add(strings.Repeat("at frame ", 500))
	if len(c.Tokens) != 8 {
		t.Fatalf("%d tokens, want 8", len(c.Tokens))
	}
}

func BenchmarkParser_Add(b *testing.B) {
	r := rand.New(rand.NewPCG(3, 4))
	lines := make([]string, 4096)
	for i := range lines {
		lines[i] = fill(r, loadgen.Templates[r.IntN(len(loadgen.Templates))])
	}
	p := New(DefaultConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		p.Add(lines[i%len(lines)])
	}
}
