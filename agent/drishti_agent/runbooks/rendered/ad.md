# ad

## relations
ad serves short text ads for the page a user is viewing. It calls nothing (no other service). It is called by frontend.

## error
Look at ad's SERVER spans: is the error ratio up on its own operations, on one or on all of them? ad calls no other service, so errors on its SERVER spans are its own. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare ad's SERVER p95 with its baseline, operation by operation. ad calls no other service, so slowness on its SERVER spans comes from ad itself: check its CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare ad's container CPU with its baseline. CPU up together with SERVER p95 up means ad is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from frontend.

## memory
Look at ad's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare ad's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (frontend) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When ad is at fault, its callers (frontend) show CLIENT errors or latency towards ad, starting at or after
ad's own change. When ad is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans.
