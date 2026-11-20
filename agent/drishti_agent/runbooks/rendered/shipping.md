# shipping

## relations
shipping estimates shipping costs and ships orders (a mock). It calls quote. It is called by checkout, frontend.

## error
Look at shipping's SERVER spans: is the error ratio up on its own operations, on one or on all of them? If its errors are on CLIENT spans to quote, the failing callee is the next suspect, not shipping. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare shipping's SERVER p95 with its baseline, operation by operation. If its CLIENT spans to quote slowed down first, follow the slowest callee. If not, check shipping's CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare shipping's container CPU with its baseline. CPU up together with SERVER p95 up means shipping is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from checkout, frontend.

## memory
Look at shipping's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare shipping's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (checkout, frontend) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When shipping is at fault, its callers (checkout, frontend) show CLIENT errors or latency towards shipping, starting at or after
shipping's own change. When shipping is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans to quote.
