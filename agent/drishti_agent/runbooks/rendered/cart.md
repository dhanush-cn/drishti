# cart

## relations
cart stores and returns each user's shopping cart, kept in a key-value store. It calls valkey-cart (key-value store). It is called by checkout, frontend.

## error
Look at cart's SERVER spans: is the error ratio up on its own operations, on one or on all of them? If its errors are on CLIENT spans to valkey-cart (key-value store), the failing callee is the next suspect, not cart. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare cart's SERVER p95 with its baseline, operation by operation. If its CLIENT spans to valkey-cart (key-value store) slowed down first, follow the slowest callee. If not, check cart's CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare cart's container CPU with its baseline. CPU up together with SERVER p95 up means cart is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from checkout, frontend.

## memory
Look at cart's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare cart's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (checkout, frontend) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When cart is at fault, its callers (checkout, frontend) show CLIENT errors or latency towards cart, starting at or after
cart's own change. When cart is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans to valkey-cart (key-value store).
