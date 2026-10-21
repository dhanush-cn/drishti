package loadgen

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// Provenance ties a run to the exact code that produced it, even for
// internal Pass 1 runs (docs/loadgen.md scope). Injected at build time.
type Provenance struct {
	GitCommit string `json:"git_commit"`
	GitDirty  bool   `json:"git_dirty"`
}

// GeneratorCPU is the generator's own cost, so a run can show whether the
// generator was the bottleneck regardless of how it is deployed (§8).
type GeneratorCPU struct {
	UserSec    float64 `json:"user_sec"`
	SystemSec  float64 `json:"system_sec"`
	GOMAXPROCS int     `json:"gomaxprocs"`
	NumGC      uint32  `json:"num_gc"`
}

// Report is run.json.
type Report struct {
	RunID      string            `json:"run_id"`
	Signal     string            `json:"signal"`
	Pass       string            `json:"pass"` // "1": internal, never quoted (scope)
	Provenance Provenance        `json:"provenance"`
	Flags      map[string]string `json:"flags"`
	Shape      ShapeReport       `json:"shape"`
	Config     ConfigReport      `json:"config"`
	Thresholds Thresholds        `json:"thresholds"`
	Summary    Summary           `json:"summary"`
	Result     *Result           `json:"result"`
	CPU        GeneratorCPU      `json:"generator_cpu"`
	Verdict    Verdict           `json:"verdict"`
}

// ShapeReport is the full payload shape, reported with every number (§1).
type ShapeReport struct {
	LogShape
	ResourcesPerRequest int    `json:"resources_per_request"`
	PoolSize            int    `json:"pool_size"`
	Compression         string `json:"compression"`
	Conns               int    `json:"conns"`
	BatchSizeSource     string `json:"batch_size_source"`
	MeanRequestBytes    int    `json:"mean_request_bytes_uncompressed"`
	MeanRecordBytes     int    `json:"mean_record_bytes_uncompressed"`
}

type ConfigReport struct {
	RateRecordsPerSec float64 `json:"rate_records_per_sec"`
	WarmupSec         float64 `json:"warmup_sec"`
	DurationSec       float64 `json:"steady_duration_sec"`
	TimeoutSec        float64 `json:"timeout_sec"`
	Inflight          int     `json:"inflight"`
	Target            string  `json:"target"`
	BurstRate         float64 `json:"burst_records_per_sec,omitempty"`
	BurstAtSec        float64 `json:"burst_at_sec,omitempty"`
	BurstForSec       float64 `json:"burst_for_sec,omitempty"`
}

// DirName is bench/runs/<UTC-timestamp>-<run_id>/ (§9).
func DirName(start time.Time, runID string) string {
	return start.UTC().Format("20060102T150405Z") + "-" + runID
}

// Write saves run.json, latency.hlog (the decodable source of truth),
// latency.hgrm (for humans) and warmup.hlog into dir.
func Write(dir string, rep *Report) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "run.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	if err := writeHlog(filepath.Join(dir, "latency.hlog"), rep.Result.Steady.Hist); err != nil {
		return err
	}
	if err := writeHlog(filepath.Join(dir, "warmup.hlog"), rep.Result.Warmup.Hist); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "latency.hgrm"))
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprintln(f, "# steady-state latency from due time, microseconds; decode latency.hlog to recompute")
	// valueScale 1000 prints milliseconds in the Value column.
	_, err = rep.Result.Steady.Hist.PercentilesPrint(f, 5, 1000)
	return err
}

// writeHlog stores the histogram in HdrHistogram's compressed base64 form,
// so percentiles can be recomputed later without rerunning (§9).
func writeHlog(path string, h *hdrhistogram.Histogram) error {
	enc, err := h.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(enc, '\n'), 0o644)
}

// ReadHlog decodes a saved histogram.
func ReadHlog(path string) (*hdrhistogram.Histogram, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return hdrhistogram.Decode(b)
}
