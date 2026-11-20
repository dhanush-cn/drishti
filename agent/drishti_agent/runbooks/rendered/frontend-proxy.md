# frontend-proxy

## relations
frontend-proxy reverse proxy in front of the web store: routes requests to the frontend and the image provider. It calls frontend, image-provider. It is called by users and test traffic (external).

## error
Look at frontend-proxy's SERVER spans: is the error ratio up on its own operations, on one or on all of them? If its errors are on CLIENT spans to frontend, image-provider, the failing callee is the next suspect, not frontend-proxy. Read its new or growing warn/error log templates: they often name what fails.

## latency
Compare frontend-proxy's SERVER p95 with its baseline, operation by operation. If its CLIENT spans to frontend, image-provider slowed down first, follow the slowest callee. If not, check frontend-proxy's CPU and memory. Use a timeseries to see when the slowdown started.

## cpu
Compare frontend-proxy's container CPU with its baseline. CPU up together with SERVER p95 up means frontend-proxy is busy itself; CPU up
together with more SERVER calls means more traffic is arriving from users and test traffic (external).

## memory
Look at frontend-proxy's memory trend, not its level: a steady climb over minutes is a leak or an unbounded buffer; a high flat
level is usually normal for its runtime. Check how far it is from its limit and whether latency or CPU rose with it.

## traffic
Compare frontend-proxy's SERVER calls per minute with its baseline. Many more calls with nothing failing is traffic; follow its
callers (users and test traffic (external)) towards the edge to find where it comes from. Far fewer calls means its callers stopped sending.

## cause-or-victim
When frontend-proxy is at fault, its callers (users and test traffic (external)) show CLIENT errors or latency towards frontend-proxy, starting at or after
frontend-proxy's own change. When frontend-proxy is a victim, its own SERVER spans, CPU and memory look normal and the trouble sits on its
CLIENT spans to frontend, image-provider.
