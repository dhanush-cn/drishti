package evalschema

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dhanush-cn/drishti/internal/chtest"
)

func schemaFiles(t *testing.T) []string {
	fs, err := filepath.Glob(filepath.Join(chtest.RepoRoot(), "clickhouse", "schema", "*.sql"))
	if err != nil || len(fs) < 3 {
		t.Fatalf("schema files: %v %v", fs, err)
	}
	return fs
}

func TestTransform_RenamesAndDropsEveryTTL(t *testing.T) {
	for _, f := range schemaFiles(t) {
		b, _ := os.ReadFile(f)
		out, err := Transform(string(b), "drishti_eval")
		if err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		code := stripComments(out)
		if strings.Contains(code, "drishti.") {
			t.Errorf("%s: a drishti. reference survived", f)
		}
		if strings.Contains(strings.ToUpper(code), "TTL") {
			t.Errorf("%s: TTL survived", f)
		}
		if strings.Count(stripComments(string(b)), "CREATE") != strings.Count(code, "CREATE") {
			t.Errorf("%s: statements lost", f)
		}
	}
	if _, err := Transform("x", "drishti"); err == nil {
		t.Error("db drishti accepted: the eval schema must never target production")
	}
	if _, err := Transform("CREATE TABLE drishti.t (a UInt8) ENGINE = MergeTree ORDER BY a TTL a + 1", "e"); err == nil {
		t.Error("an inline TTL was not caught")
	}
}

// The trap and the fix, on a real ClickHouse: a row received a week ago is
// dropped on insert by drishti.logs (TTL 7 days on received_at) and kept by
// the eval schema.
func TestEvalSchema_KeepsRowsAProductionTTLWouldDropOnInsert(t *testing.T) {
	s := chtest.New(t, "001_logs.sql", "003_metrics.sql")
	eval := s.DB + "_eval"
	s.Exec("CREATE DATABASE " + eval)
	t.Cleanup(func() { s.Exec("DROP DATABASE IF EXISTS " + eval) })
	for _, f := range schemaFiles(t) {
		b, _ := os.ReadFile(f)
		out, err := Transform(string(b), eval)
		if err != nil {
			t.Fatal(err)
		}
		for _, st := range chtest.Statements(out) {
			s.Exec(st)
		}
	}
	for _, db := range []string{s.DB, eval} {
		s.Exec("INSERT INTO " + db + ".logs (ts, observed_ts, received_at, service, body, kafka_partition, kafka_offset) VALUES " +
			"(now64(9) - INTERVAL 30 DAY, now64(9) - INTERVAL 30 DAY, now64(3) - INTERVAL 30 DAY, 'old', 'x', 0, 1), " +
			"(now64(9), now64(9), now64(3), 'new', 'y', 0, 2)")
	}
	prod := strings.TrimSpace(s.MustQuery("SELECT groupArray(service) FROM (SELECT service FROM " + s.DB + ".logs ORDER BY service) FORMAT TabSeparated"))
	ev := strings.TrimSpace(s.MustQuery("SELECT groupArray(service) FROM (SELECT service FROM " + eval + ".logs ORDER BY service) FORMAT TabSeparated"))
	if prod != "['new']" {
		t.Fatalf("production schema kept %s: the trap this guards against isn't reproduced", prod)
	}
	if ev != "['new','old']" {
		t.Fatalf("eval schema kept %s, want both rows", ev)
	}
}
