# recommendation

## relations
recommendation suggests other products based on what is in the cart. It calls product-catalog. It is called by frontend.

## error
Look at recommendation's SERVER spans: is the error ratio up on its own operations, on one or on all of them? If its errors are on CLIENT spans to product-catalog, the failing callee is the next suspect, not recommendation. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare recommendation's SERVER p95 with its baseline, operation by operation. If its CLIENT spans to product-catalog slowed down first, follow the slowest callee. If not, check recommendation's CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare recommendation's container CPU with its baseline. CPU up together with SERVER p95 up means recommendation is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from frontend.

## memory
Look at recommendation's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare recommendation's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (frontend) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When recommendation is at fault, its callers (frontend) show CLIENT errors or latency towards recommendation, starting at or after
recommendation's own change. When recommendation is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans to product-catalog.
