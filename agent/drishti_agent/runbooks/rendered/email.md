# email

## relations
email sends order confirmation emails (a mock). It calls nothing (no other service). It is called by checkout.

## error
Look at email's SERVER spans: is the error ratio up on its own operations, on one or on all of them? email calls no other service, so errors on its SERVER spans are its own. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare email's SERVER p95 with its baseline, operation by operation. email calls no other service, so slowness on its SERVER spans comes from email itself: check its CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare email's container CPU with its baseline. CPU up together with SERVER p95 up means email is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from checkout.

## memory
Look at email's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare email's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (checkout) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When email is at fault, its callers (checkout) show CLIENT errors or latency towards email, starting at or after
email's own change. When email is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans.
