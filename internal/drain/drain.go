// Package drain groups log lines into templates with the Drain algorithm
// (He, Zhu, Zheng, Lyu: "Drain: An Online Log Parsing Approach with Fixed
// Depth Tree", ICWS 2017), so the detector can count "how often did this
// kind of line appear" instead of comparing raw strings (docs/detector.md,
// section 3).
//
// One line is handled in three steps:
//
//  1. Tokenize on whitespace and mask variables: a token containing a digit
//     becomes "<#>" ("key=value" keeps its key: "status=<#>"). IDs, counts,
//     durations, timestamps, IPs, hex and UUIDs nearly always contain a
//     digit, so one rule replaces the per-dataset regexes of the paper.
//     Masked tokens are equal to each other, like Drain3's "<NUM>": two
//     lines that differ only in numbers are identical after masking.
//     Tokens without digits (words, routes like /api/cart) stay as they
//     are; Drain's similarity step generalizes those into "<*>".
//  2. Walk a tree of fixed depth: first by token count, then by the first
//     few tokens (a token with a digit, or one past the node's child limit,
//     goes down the "<*>" branch). This bounds the clusters each line is
//     compared with to one leaf.
//  3. In the leaf, pick the cluster whose template shares the most tokens at
//     the same positions. At or above the similarity threshold the line
//     joins it (positions that differ become "<*>"); below it the line
//     starts a new cluster.
//
// A Parser is per service: services never share templates. It is not safe
// for concurrent use; the detector keeps one per service in one partition.
package drain

import (
	"strings"
	"unicode"
)

// Wildcard marks a position generalized by merging: lines disagreed there.
const Wildcard = "<*>"

// Masked replaces a token (or a key=value's value) that contains a digit.
// Unlike Wildcard it counts as a match in the similarity: it is a token
// class, not a disagreement.
const Masked = "<#>"

// Config holds Drain's parameters. Defaults are the paper's and Drain3's.
type Config struct {
	// Depth of the tree including the root and the length level: Depth-2
	// leading tokens route a line. 4 (two tokens), as in the paper and
	// Drain3. The trade-off, measured: with two routing tokens, a variable
	// without a digit in second position (the load generator's route in
	// "GET /api/cart …") is never generalized, so that template splits into
	// one cluster per route (6). Routing on one token (depth 3) fixes that
	// but over-merges real logs: Loghub mean grouping accuracy 0.762 at
	// depth 3 vs 0.821 at depth 4 (OpenStack 0.298 vs 0.807). Splitting a
	// template is cheap for the detector; merging two hides one of them.
	Depth int
	// SimThreshold: the share of positions a line must share with a
	// template to join it. 0.4 in Drain3.
	SimThreshold float64
	// MaxChildren per internal node; lines whose token isn't among the
	// first MaxChildren seen go down the "<*>" branch. 100 in Drain3.
	MaxChildren int
	// MaxClusters bounds memory per parser. Past it, a line that matches no
	// cluster is counted in the overflow cluster (ID -1) instead of creating
	// one: random text must not grow the detector without bound.
	MaxClusters int
	// MaxTokens: lines are cut to this many tokens before parsing (a
	// stack trace in one record must not make one huge template).
	MaxTokens int
}

func DefaultConfig() Config {
	return Config{Depth: 4, SimThreshold: 0.4, MaxChildren: 100, MaxClusters: 2000, MaxTokens: 64}
}

// Cluster is one template. ID is stable for the parser's lifetime; the
// template text can gain wildcards as lines merge into it.
type Cluster struct {
	ID       int
	Tokens   []string
	Count    int64
	template string // cached Template(); reset when Tokens change
}

// Template is the template as one string.
func (c *Cluster) Template() string {
	if c.template == "" {
		c.template = strings.Join(c.Tokens, " ")
	}
	return c.template
}

type node struct {
	children map[string]*node
	clusters []*Cluster // leaves only
}

// Parser holds one service's tree.
type Parser struct {
	cfg      Config
	byLen    map[int]*node
	clusters []*Cluster
	overflow *Cluster
	tok      []string // reused per line
}

func New(cfg Config) *Parser {
	if cfg.Depth < 3 {
		cfg.Depth = 3
	}
	return &Parser{cfg: cfg, byLen: map[int]*node{},
		overflow: &Cluster{ID: -1, Tokens: []string{"<overflow>"}}}
}

// Clusters returns the clusters in creation order (the overflow cluster
// last, if it was used).
func (p *Parser) Clusters() []*Cluster {
	if p.overflow.Count > 0 {
		return append(p.clusters[:len(p.clusters):len(p.clusters)], p.overflow)
	}
	return p.clusters
}

// Len is the number of clusters (without overflow).
func (p *Parser) Len() int { return len(p.clusters) }

// Add parses one line and returns its cluster, and whether the cluster was
// created by this line (the "new template" signal).
func (p *Parser) Add(line string) (*Cluster, bool) {
	toks := p.tokenize(line)
	n := len(toks)
	root := p.byLen[n]
	if root == nil {
		root = &node{children: map[string]*node{}}
		p.byLen[n] = root
	}
	leaf := p.leaf(root, toks)
	if c := p.match(leaf.clusters, toks); c != nil {
		c.Count++
		if merge(c, toks) {
			c.template = ""
		}
		return c, false
	}
	if len(p.clusters) >= p.cfg.MaxClusters {
		p.overflow.Count++
		return p.overflow, false
	}
	// Clone: a token is a substring of the line, and keeping it would keep
	// the whole log body alive for as long as the cluster exists.
	own := make([]string, len(toks))
	for i, t := range toks {
		own[i] = strings.Clone(t)
	}
	c := &Cluster{ID: len(p.clusters), Tokens: own, Count: 1}
	leaf.clusters = append(leaf.clusters, c)
	p.clusters = append(p.clusters, c)
	return c, true
}

// Match returns the cluster a line would join, without changing anything
// (for tests and inspection).
func (p *Parser) Match(line string) *Cluster {
	toks := p.tokenize(line)
	root := p.byLen[len(toks)]
	if root == nil {
		return nil
	}
	n := root
	for i := 0; i < p.cfg.Depth-2 && i < len(toks); i++ {
		next := n.children[toks[i]]
		if next == nil {
			next = n.children[Wildcard]
		}
		if next == nil {
			return nil
		}
		n = next
	}
	return p.match(n.clusters, toks)
}

func (p *Parser) tokenize(line string) []string {
	toks := p.tok[:0]
	start := -1
	for i, r := range line {
		if unicode.IsSpace(r) {
			if start >= 0 {
				toks = append(toks, mask(line[start:i]))
				start = -1
				if len(toks) == p.cfg.MaxTokens {
					break
				}
			}
			continue
		}
		if start < 0 {
			start = i
		}
	}
	if start >= 0 && len(toks) < p.cfg.MaxTokens {
		toks = append(toks, mask(line[start:]))
	}
	p.tok = toks
	return toks
}

// mask replaces a token that contains a digit with Masked; in "key=value"
// with a digit-free key only the value is replaced, so the key stays part of
// the template ("status=<#>", not "<#>").
func mask(t string) string {
	eq := -1
	digit := false
	for i := 0; i < len(t); i++ {
		c := t[i]
		if c == '=' && eq < 0 {
			eq = i
		}
		if c >= '0' && c <= '9' {
			if eq < 0 {
				return Masked // a digit before any '=': the whole token varies
			}
			digit = true
		}
	}
	if !digit {
		return t
	}
	if eq == 0 {
		return Masked
	}
	return t[:eq+1] + Masked
}

// leaf walks (and grows) the tree to the leaf for toks.
func (p *Parser) leaf(n *node, toks []string) *node {
	for i := 0; i < p.cfg.Depth-2 && i < len(toks); i++ {
		k := toks[i]
		next := n.children[k]
		if next == nil {
			// A new child only while the node has room; past that, everything
			// unseen shares the wildcard branch (paper, section III-C).
			if k != Wildcard && len(n.children) >= p.cfg.MaxChildren {
				k = Wildcard
				next = n.children[k]
			}
			if next == nil {
				next = &node{children: map[string]*node{}}
				n.children[k] = next
			}
		}
		n = next
	}
	return n
}

// match returns the most similar cluster at or above the threshold. On equal
// similarity the more general template (more wildcards) wins, then the older
// cluster: a leaf is a slice, so the result never depends on map order.
func (p *Parser) match(cs []*Cluster, toks []string) *Cluster {
	var best *Cluster
	bestSim, bestWild := -1.0, 0
	for _, c := range cs {
		same, wild := 0, 0
		for i, t := range c.Tokens {
			switch {
			case t == Wildcard:
				wild++
			case t == toks[i]:
				same++
			}
		}
		// Similarity counts exact matches only (Drain3 default): a template
		// of nothing but wildcards must not swallow every line of its length.
		sim := 1.0 // an empty line matches the empty template
		if len(toks) > 0 {
			sim = float64(same) / float64(len(toks))
		}
		if sim > bestSim || (sim == bestSim && wild > bestWild) {
			best, bestSim, bestWild = c, sim, wild
		}
	}
	if best == nil || bestSim < p.cfg.SimThreshold {
		return nil
	}
	return best
}

// merge turns positions where the line differs from the template into
// wildcards; reports whether the template changed.
func merge(c *Cluster, toks []string) bool {
	changed := false
	for i, t := range c.Tokens {
		if t != Wildcard && t != toks[i] {
			c.Tokens[i] = Wildcard
			changed = true
		}
	}
	return changed
}
