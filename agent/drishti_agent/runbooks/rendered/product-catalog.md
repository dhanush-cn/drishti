# product-catalog

## relations
product-catalog lists, searches and returns products from its data store. It calls its database. It is called by checkout, frontend, recommendation.

## error
Look at product-catalog's SERVER spans: is the error ratio up on its own operations, on one or on all of them? If its errors are on CLIENT spans to its database, the failing callee is the next suspect, not product-catalog. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare product-catalog's SERVER p95 with its baseline, operation by operation. If its CLIENT spans to its database slowed down first, follow the slowest callee. If not, check product-catalog's CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare product-catalog's container CPU with its baseline. CPU up together with SERVER p95 up means product-catalog is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from checkout, frontend, recommendation.

## memory
Look at product-catalog's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare product-catalog's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (checkout, frontend, recommendation) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When product-catalog is at fault, its callers (checkout, frontend, recommendation) show CLIENT errors or latency towards product-catalog, starting at or after
product-catalog's own change. When product-catalog is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans to its database.
