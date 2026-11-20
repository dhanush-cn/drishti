# frontend

## relations
frontend serves the web store's pages and its API. It calls ad, cart, currency, checkout, product-catalog, recommendation, shipping. It is called by frontend-proxy.

## error
Look at frontend's SERVER spans: is the error ratio up on its own operations, on one or on all of them? If its errors are on CLIENT spans to ad, cart, currency, checkout, product-catalog, recommendation, shipping, the failing callee is the next suspect, not frontend. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare frontend's SERVER p95 with its baseline, operation by operation. If its CLIENT spans to ad, cart, currency, checkout, product-catalog, recommendation, shipping slowed down first, follow the slowest callee. If not, check frontend's CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare frontend's container CPU with its baseline. CPU up together with SERVER p95 up means frontend is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from frontend-proxy.

## memory
Look at frontend's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare frontend's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (frontend-proxy) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When frontend is at fault, its callers (frontend-proxy) show CLIENT errors or latency towards frontend, starting at or after
frontend's own change. When frontend is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans to ad, cart, currency, checkout, product-catalog, recommendation, shipping.
