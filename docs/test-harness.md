# Test harness

## OpenTelemetry Demo

- Version: **3.1.0**, pinned in two places:
  - Repo cloned at tag `3.1.0` into `~/otel-demo`
  - `.env.override`: `DEMO_VERSION=3.1.0` (image tags) and `IMAGE_VERSION=3.1.0`
    (`service.version` in telemetry; `.env` in the 3.1.0 tag still says 3.0.0)
- Mode: `make start-minimal-no-o11y`
  - No observability backends: Drishti replaces them
  - No demo Kafka: avoids a second Kafka; loses `kafkaQueueProblems`
- Idle footprint: ~1.1 GB across 20 containers
- Idle telemetry rate: ~100 events/sec (realism, not throughput; volume comes from the synthetic generator)

## Wiring the demo to Drishti

- After cloning, copy `deploy/otel-demo/otelcol-config-extras.yml` (tracked in this repo) to
  `~/otel-demo/src/otel-collector/`, then `docker restart otel-collector`.
- The receiver joins the demo's network only through the override file `docker-compose.demo.yml`.
  The network is external (the demo creates it, and its `make stop` deletes it), so order matters
  **for demo runs only**:
  1. `cd ~/otel-demo && make start-minimal-no-o11y`
  2. `cd ~/drishti && export COMPOSE_FILE=docker-compose.yml:docker-compose.demo.yml`
  3. `docker compose up -d` (every compose command in this shell now uses both files)
- Benchmarks: `unset COMPOSE_FILE`, stop the demo, then `docker compose up -d` from the base file
  alone. The receiver is then on Drishti's default network only, and needs no demo.
- **Edit the collector config files in place only.** The collector bind-mounts single files
  (e.g. `otelcol-config-extras.yml`). Tools that *replace* a file instead of rewriting it
  (`sed -i`, `mv`, some editors' "safe write") give the path a new inode, the mount source
  dangles, and `docker restart otel-collector` fails with `error mounting ... no such file or
  directory`. Write with `cat new > file` (or nano, which writes in place). If it already
  broke: `docker rm -f otel-collector && make start-minimal-no-o11y` recreates it. (13 Oct)
- **The collector samples its own log output** (default: first 10 of each message per 10s,
  then 1 in 100), so `debug` exporter lines are not a complete record of batches. Measurements
  that count them need `service.telemetry.logs.sampling.enabled: false`
  (`docs/loadgen.md` §13). (13 Oct)

## Before every eval run

1. `git -C ~/otel-demo diff src/flagd/demo.flagd.json` must be empty (all flags off)
2. flagd-ui scheduler must be stopped (it starts stopped: `running: false` in `init`)
3. After each scenario: `git -C ~/otel-demo checkout src/flagd/demo.flagd.json`

## Baseline noise (no flags on)

Measured 13 Oct, Locust at 5 users, `minimal-no-o11y`: about 9% of Locust requests fail with
no flag on. Nearly all are `POST /prompt` (58 of 58 failed, `Name or service not known`): the
agentic AI endpoint, whose service is not part of minimal mode. A few `connection refused`
errors appear while services start. The detector's baseline must treat these as normal, and an
eval must not attribute them to a scenario.

## Scenarios (13 in scope)

| Flag | Category | Expected root cause |
|---|---|---|
| adFailure | Local errors | ad service errors |
| cartFailure | Local errors | cart service errors |
| paymentFailure | Distant symptom, look-alike pair | payment returns errors |
| paymentUnreachable | Distant symptom, look-alike pair | payment unreachable |
| adHighCpu | Same service, different cause; latency only | ad CPU saturation |
| adManualGc | Same service, different cause | ad GC pauses |
| emailMemoryLeak | Slow trend | email memory leak |
| recommendationCacheFailure | Slow trend (verify on first run) | recommendation cache / memory |
| productCatalogLockContention | Latency only | product-catalog DB lock contention |
| imageSlowLoad | Latency only | image-provider slow responses |
| productCatalogFailure | Subset only | product-catalog fails one product |
| intlShippingSlowdown | Subset only | shipping slow for international orders |
| loadGeneratorFloodHomepage | No bug | traffic increase, no faulty service |

Out of scope: aiRunawayAgent, aiSlowResponse (agentic mode), kafkaQueueProblems (minimal mode),
emitRawPii (not a failure), failedReadinessProbe (Kubernetes probe; untested under Docker).

