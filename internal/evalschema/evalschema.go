// Package evalschema derives the eval database's schema from the production
// schema files (clickhouse/schema/*.sql), so the two can't drift: same
// tables, same sort keys, same codecs, in another database, without TTL.
//
// Why no TTL (Week 5 design, change 1): evals replay recordings with their
// original timestamps. received_at is days old, and ClickHouse drops rows
// whose TTL has already expired on insert, not only on merge (measured in
// the 13 Oct lab: 1000 of 2000 rows gone before any merge). A week-old
// capture loaded into drishti.logs would silently vanish.
package evalschema

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	ttlLine = regexp.MustCompile(`(?m)^[ \t]*TTL\b.*\n?`)
	dbRef   = regexp.MustCompile(`\bdrishti\.`)
	ident   = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
)

// Transform rewrites one schema file for database db: every "drishti." table
// reference becomes "<db>.", and every table-level TTL clause (they are one
// line each in these files) is removed. Comments are left alone.
func Transform(sql, db string) (string, error) {
	if !ident.MatchString(db) || db == "drishti" {
		return "", fmt.Errorf("eval database %q: want a lower-case identifier other than drishti", db)
	}
	out := ttlLine.ReplaceAllString(sql, "")
	out = dbRef.ReplaceAllString(out, db+".")
	if strings.Contains(strings.ToUpper(stripComments(out)), "TTL ") {
		return "", fmt.Errorf("a TTL clause survived the transform (not on its own line?)")
	}
	return out, nil
}

var comment = regexp.MustCompile(`--.*`)

func stripComments(s string) string { return comment.ReplaceAllString(s, "") }
