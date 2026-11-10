// Command evalschema prints the eval database's schema: the production
// schema files with "drishti." renamed and the TTL clauses removed
// (internal/evalschema). scripts/create-eval.sh pipes it into
// clickhouse-client.
//
//	go run ./cmd/evalschema -db drishti_eval clickhouse/schema/*.sql
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dhanush-cn/drishti/internal/evalschema"
)

func main() {
	db := flag.String("db", "drishti_eval", "eval database name")
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "usage: evalschema [-db drishti_eval] FILE.sql...")
		os.Exit(2)
	}
	fmt.Printf("CREATE DATABASE IF NOT EXISTS %s;\n", *db)
	for _, f := range flag.Args() {
		b, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, "evalschema:", err)
			os.Exit(1)
		}
		out, err := evalschema.Transform(string(b), *db)
		if err != nil {
			fmt.Fprintf(os.Stderr, "evalschema: %s: %v\n", f, err)
			os.Exit(1)
		}
		fmt.Printf("-- from %s\n%s\n", f, out)
	}
}
