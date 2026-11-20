# Symptoms and where they point

General operations knowledge, the same for every service. Each section is one symptom: what it usually means, and what
to look at next with the query tools.

## connection refused or unavailable
A caller's CLIENT spans to one callee fail fast (milliseconds), often with "unavailable" or "connection refused" in its
logs. Usually the callee is down, restarting, or the caller is dialing a wrong address. If the callee's own SERVER spans
show no errors and fewer calls, the calls never reached it: look at the caller's side of that connection.

## timeouts and deadline exceeded
A caller's CLIENT spans to one callee end at a fixed duration with "deadline exceeded" or "timeout". The callee is slow or
overloaded: check its SERVER p95, its CPU, and whether its own dependencies are slow. If the callee looks healthy, look at
the network path or the caller's timeout settings.

## errors on one operation only
A service's errors are concentrated on one SERVER operation while its others are fine. The fault is in that operation's own
code path or in the dependency only that operation uses: compare the operation's CLIENT spans to their callees.

## errors on every operation
All of a service's SERVER operations fail at once. Something the whole service depends on broke: its process, its
configuration, a shared datastore, or a dependency every request touches.

## new error log template
A log message appears that was not there before. Read the template: it often names the dependency or the operation that
fails. A new template on a service whose SERVER spans are healthy may be about a call it makes, so check its CLIENT spans.

## errors only at the edge
The reverse proxy or the web frontend returns errors while the services behind them look healthy. Follow the failing route
upstream: which backend serves it, and do that backend's SERVER spans show the same errors?

## slow responses with low CPU
p95 rises on a service while its CPU stays flat. It is waiting, not working: on a slow dependency (check its CLIENT spans),
on a lock or a pool shared between requests, or on a datastore. Throughput often drops at the same time.

## slow responses with high CPU
p95 rises together with CPU on the same service. It is busy itself: a hot loop, heavy computation, or garbage collection
(check memory as well). Every operation of the service tends to slow down, not just one.

## steady memory climb
Memory rises in a straight line over minutes and does not level off. A leak or an unbounded buffer or cache: it ends in an
out-of-memory restart if nothing changes. Garbage collection gets more frequent as it grows, which can add latency and CPU.

## memory high but flat
Memory sits at a high, steady level. Runtimes with a fixed heap reserve memory up front; a flat level is not a leak. Compare
with the service's own baseline rather than with other services.

## CPU saturation
A container's CPU is several times its baseline. Requests queue and every operation slows. Check whether it is serving more
calls than before (traffic) or the same calls more expensively (the service itself).

## garbage collection pauses
Short bursts of latency on a managed runtime, with CPU spikes at the same moments. Often a symptom of memory pressure: check
the memory trend. Forced or very frequent collections slow the whole service.

## traffic surge
A service receives several times its usual calls with nothing failing. The cause is whoever sends the extra calls: follow
the callers up to the edge. Its dependencies see more calls too, and may slow down or fail under the load.

## traffic drop
A service receives far fewer calls than before. Its callers stopped sending: they are failing earlier in the request, or
the path to this service changed. Look upstream, not at the quiet service.

## datastore unreachable or slow
A service's requests fail or slow down together with log lines that name its datastore (cache, database). The datastore or
the connection to it is at fault; the service is the first victim, and its callers the next.

## one victim, many symptoms
A service with errors or latency in many places at once is often in the middle of the call chain. Find the deepest service
in the chain whose own SERVER spans, CPU or memory changed first: that is usually the cause, the rest are victims.
