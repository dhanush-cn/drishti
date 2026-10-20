# CLAUDE.md — working rules for Drishti

Drishti: observability platform. Go OTLP receiver → Kafka → ClickHouse storage consumer (exactly-once), load generator, later a streaming detector and a Python LangGraph RCA agent. Design records live in `docs/` (receiver.md, storage.md, loadgen.md, test-harness.md). Read the relevant one before changing a component.

## How Dhanush works (non-negotiable)
- One concern per change. No drive-by refactors.
- Claude never runs git (no commit, push, branch, PR). Deliver patches (`git format-patch` style or plain diffs) plus a driver script; Dhanush reviews and commits every diff himself.
- Explain the *why* behind every design decision: he must defend every line in an interview.
- Revert-proof every correctness claim: break the mechanism → test goes red → revert → green.
- Never put unmeasured numbers in docs, README or commit messages. Benchmark numbers come only from laptop runs and are marked internal/provisional until §13 thresholds and Pass 2 metadata exist.

## Environment limits on Claude's side
- No access to `proxy.golang.org`; machine Go may be older than `go.mod` (1.27). Build/test only with the staged `go-toolchain.tgz` + `gomodcache.tgz` (`GOMODCACHE=<staged>`, `GOPROXY=off`, `GOTOOLCHAIN=local`). If go.mod gains a dependency, ask for a refreshed `gomodcache.tgz`.
- No Docker stack for benchmarks. Validate ClickHouse DDL/SQL/RowBinary with `chdb`; end-to-end pipeline tests with `kfake` + an HTTP shim over chdb. These are correctness checks, never benchmark numbers.
- Every change must pass `go vet ./...` and `go test -race ./...` (run ×3 for concurrency code).

## Code conventions
- Every setting is a flag with a `DRISHTI_*` env fallback.
- franz-go for Kafka; never serve `http.DefaultServeMux` (pprof goes through `internal/pprofsrv`).
- Storage consumer invariants: one insert = one Kafka partition offset range + one `received_at` day = one part; commit after insert; resume skip from the `ingest_progress` projection; `async_insert=0` sent explicitly. Don't break these without a test proving the replacement.
- Topic names never contain `_`.

## Benchmarks (laptop only)
- Demo stopped, `unset COMPOSE_FILE`, other stacks (Sankalp, FundKit) stopped.
- Repeats are mandatory (×3); single runs aren't results. Record hardware/power state with every run.
- Receiver-only sweeps: `docker compose stop storage`, loadgen `-stamp=false`. Storage runs: `-stamp`.

## State
Current progress and next steps: `PROGRESS.md` (kept in the claude.ai Project, copy in Downloads).
