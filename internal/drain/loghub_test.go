package drain

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// TestLoghub measures grouping accuracy on Loghub's 2k-line samples with
// hand-labelled templates (He et al., "Loghub", ISSRE 2023; metric from Zhu
// et al., ICSE-SEIP 2019). The data isn't vendored: clone
// https://github.com/logpai/loghub and point DRISHTI_LOGHUB at it. Skipped
// otherwise.
//
// It reports, it doesn't gate on the paper's numbers: those used a
// hand-written regex set per dataset, this parser uses one generic rule
// (mask tokens with digits). The floor below only catches a broken parser.
func TestLoghub(t *testing.T) {
	dir := os.Getenv("DRISHTI_LOGHUB")
	if dir == "" {
		t.Skip("DRISHTI_LOGHUB not set (clone github.com/logpai/loghub)")
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*_2k.log_structured.csv"))
	if len(files) == 0 {
		t.Fatalf("no *_2k.log_structured.csv under %s", dir)
	}
	sort.Strings(files)
	var lines []string
	sum := map[int]float64{}
	for _, f := range files {
		contents, truth := readLoghub(t, f)
		name := strings.TrimSuffix(filepath.Base(f), "_2k.log_structured.csv")
		row := fmt.Sprintf("| %-11s |", name)
		for _, depth := range []int{4, 3} {
			cfg := DefaultConfig()
			cfg.Depth = depth
			p := New(cfg)
			got := make([]int, len(contents))
			for i, c := range contents {
				cl, _ := p.Add(c)
				got[i] = cl.ID
			}
			ga := groupingAccuracy(truth, got)
			sum[depth] += ga
			row += fmt.Sprintf(" %.3f (%d clusters) |", ga, p.Len())
		}
		lines = append(lines, row)
	}
	t.Logf("\n| Dataset | depth 4 (default, paper) | depth 3 |\n|---|---|---|\n%s\n| mean | %.3f | %.3f |",
		strings.Join(lines, "\n"), sum[4]/float64(len(files)), sum[3]/float64(len(files)))
	if mean := sum[4] / float64(len(files)); mean < 0.7 {
		t.Fatalf("mean grouping accuracy %.3f at the default depth: parser broken?", mean)
	}
}

func readLoghub(t *testing.T, path string) (contents, truth []string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	recs, err := r.ReadAll()
	if err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	ci, ei := -1, -1
	for i, h := range recs[0] {
		switch h {
		case "Content":
			ci = i
		case "EventId":
			ei = i
		}
	}
	if ci < 0 || ei < 0 {
		t.Fatalf("%s: no Content/EventId columns in %v", path, recs[0])
	}
	for _, rec := range recs[1:] {
		contents = append(contents, rec[ci])
		truth = append(truth, rec[ei])
	}
	return contents, truth
}
