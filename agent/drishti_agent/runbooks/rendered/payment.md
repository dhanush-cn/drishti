# payment

## relations
payment charges a card for an order (a mock) and returns a transaction id. It calls nothing (no other service). It is called by checkout.

## error
Look at payment's SERVER spans: is the error ratio up on its own operations, on one or on all of them? payment calls no other service, so errors on its SERVER spans are its own. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare payment's SERVER p95 with its baseline, operation by operation. payment calls no other service, so slowness on its SERVER spans comes from payment itself: check its CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare payment's container CPU with its baseline. CPU up together with SERVER p95 up means payment is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from checkout.

## memory
Look at payment's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare payment's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (checkout) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When payment is at fault, its callers (checkout) show CLIENT errors or latency towards payment, starting at or after
payment's own change. When payment is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans.
