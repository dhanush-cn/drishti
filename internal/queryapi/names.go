package queryapi

import (
	"slices"
	"strings"
)

// Names maps storage owner keys to the service names the agent sees, and
// hides what it must not see.
//
// Storage keeps the receiver's key: service.name, else "container:<name>",
// else "host:<name>" (docker_stats resources have no service.name). Logs and
// spans say "email", docker_stats says "container:email". The agent sees one
// name: "container:X" is shown as "X" when X is a known service (one with
// spans or logs), else as is (infrastructure such as valkey-cart keeps its
// container: prefix so it isn't mistaken for an instrumented service).
//
// Hidden (never returned, never queryable), Week 5 design E7:
//   - the flag system and the load generator: their telemetry describes the
//     fault injection and the test traffic, not the system under diagnosis;
//   - the observability stack (Drishti's own containers, the collector): not
//     part of the system under diagnosis.
type Names struct {
	known  map[string]bool // instrumented services
	hidden map[string]bool
}

// DefaultHidden are the demo's flag system, load generator and observability
// stack, by service name or container name.
var DefaultHidden = []string{"flagd", "flagd-ui", "load-generator", "otel-collector", "otelcol-contrib", "telemetry-docs"}

func NewNames(known []string, hidden []string) *Names {
	n := &Names{known: map[string]bool{}, hidden: map[string]bool{}}
	for _, h := range hidden {
		n.hidden[h] = true
	}
	for _, k := range known {
		n.known[k] = true
	}
	return n
}

// Hidden reports whether an owner key belongs to something the agent never sees.
func (n *Names) Hidden(owner string) bool {
	base := owner
	if b, ok := strings.CutPrefix(owner, "container:"); ok {
		base = b
		if strings.HasPrefix(b, "drishti-") || strings.HasPrefix(b, "drishti_") {
			return true // Drishti's own containers
		}
	}
	return n.hidden[base]
}

// Show is the name the agent sees for an owner key ("" = hidden).
func (n *Names) Show(owner string) string {
	if n.Hidden(owner) {
		return ""
	}
	if b, ok := strings.CutPrefix(owner, "container:"); ok && n.known[b] {
		return b
	}
	return owner
}

// Owners are the storage keys behind a name the agent asked for (nil =
// hidden or unknown).
func (n *Names) Owners(name string) []string {
	if name == "" || n.Hidden(name) {
		return nil
	}
	if strings.HasPrefix(name, "container:") || strings.HasPrefix(name, "host:") {
		return []string{name}
	}
	return []string{name, "container:" + name}
}

// Known lists instrumented, visible services, sorted.
func (n *Names) Known() []string {
	var out []string
	for k := range n.known {
		if !n.Hidden(k) {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}
