# currency

## relations
currency converts money amounts between currencies; the busiest service by request rate. It calls nothing (no other service). It is called by checkout, frontend.

## error
Look at currency's SERVER spans: is the error ratio up on its own operations, on one or on all of them? currency calls no other service, so errors on its SERVER spans are its own. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare currency's SERVER p95 with its baseline, operation by operation. currency calls no other service, so slowness on its SERVER spans comes from currency itself: check its CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare currency's container CPU with its baseline. CPU up together with SERVER p95 up means currency is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from checkout, frontend.

## memory
Look at currency's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare currency's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (checkout, frontend) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When currency is at fault, its callers (checkout, frontend) show CLIENT errors or latency towards currency, starting at or after
currency's own change. When currency is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans.
