# quote

## relations
quote computes a shipping cost from the number of items. It calls nothing (no other service). It is called by shipping.

## error
Look at quote's SERVER spans: is the error ratio up on its own operations, on one or on all of them? quote calls no other service, so errors on its SERVER spans are its own. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare quote's SERVER p95 with its baseline, operation by operation. quote calls no other service, so slowness on its SERVER spans comes from quote itself: check its CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare quote's container CPU with its baseline. CPU up together with SERVER p95 up means quote is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from shipping.

## memory
Look at quote's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare quote's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (shipping) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When quote is at fault, its callers (shipping) show CLIENT errors or latency towards quote, starting at or after
quote's own change. When quote is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans.
