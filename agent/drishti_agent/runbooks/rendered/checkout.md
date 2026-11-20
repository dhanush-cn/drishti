# checkout

## relations
checkout turns a cart into an order: reads the cart, converts prices, charges the payment, arranges shipping and sends the confirmation email. It calls cart, currency, payment, email, product-catalog, shipping. It is called by frontend.

## error
Look at checkout's SERVER spans: is the error ratio up on its own operations, on one or on all of them? If its errors are on CLIENT spans to cart, currency, payment, email, product-catalog, shipping, the failing callee is the next suspect, not checkout. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare checkout's SERVER p95 with its baseline, operation by operation. If its CLIENT spans to cart, currency, payment, email, product-catalog, shipping slowed down first, follow the slowest callee. If not, check checkout's CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare checkout's container CPU with its baseline. CPU up together with SERVER p95 up means checkout is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from frontend.

## memory
Look at checkout's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare checkout's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (frontend) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When checkout is at fault, its callers (frontend) show CLIENT errors or latency towards checkout, starting at or after
checkout's own change. When checkout is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans to cart, currency, payment, email, product-catalog, shipping.
