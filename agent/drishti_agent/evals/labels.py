"""Ground truth per demo flag (OTel Demo 3.1.0), fixed before any agent run.

`services`: the root cause an engineer should name. Stricter than the
detector's AFFECTED sets (scripts/detector-eval.py), which list every service
whose telemetry changes; most of those are victims. Two flags accept two
names, because the fault sits between two services and the demo's
telemetry can't separate them:
  - paymentUnreachable: checkout dials a wrong payment address. "checkout
    can't reach payment" is right whichever end is named; payment itself is
    healthy.
  - loadGeneratorFloodHomepage: a traffic surge with nothing broken; it lands
    on frontend through frontend-proxy.

`faults`: accepted fault types (secondary score). recommendationCacheFailure's
cache grows (memory) and its misses slow requests (latency).

`gate`: the symptom that must be visible by flag-off for the scenario to be
scored (design E9): a fault that never showed can't be diagnosed. Checked
against the recording's own no-flag baseline. Kinds:
  errors(service, kind, span, n)  at least n errors on matching spans
  p95(service, x)                 server p95 at least x times the baseline's
  cpu(service, x)                 container CPU at least x times the baseline's
  memory(service, mib_per_min)    memory rising at least this fast
  calls(service, x)               server calls/min at least x times the baseline's
A gate passes if any of its checks passes.
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Check:
    kind: str
    service: str
    value: float
    span_kind: str = ""
    span: str = ""


@dataclass(frozen=True)
class Label:
    services: frozenset
    faults: frozenset
    gate: tuple = field(default_factory=tuple)


def L(services, faults, *gate):
    return Label(frozenset(services), frozenset(faults), tuple(gate))


LABELS = {
    "adFailure": L({"ad"}, {"error"},
                   Check("errors", "frontend", 3, "CLIENT", "oteldemo.AdService/GetAds"), Check("errors", "ad", 3, "SERVER")),
    "cartFailure": L({"cart"}, {"error"},
                     Check("errors", "checkout", 3, "CLIENT", "oteldemo.CartService/EmptyCart"), Check("errors", "cart", 3, "SERVER")),
    "paymentFailure": L({"payment"}, {"error"}, Check("errors", "payment", 3, "SERVER")),
    "paymentUnreachable": L({"checkout", "payment"}, {"error"},
                            Check("errors", "checkout", 3, "CLIENT", "oteldemo.PaymentService/Charge")),
    "productCatalogFailure": L({"product-catalog"}, {"error"}, Check("errors", "product-catalog", 3, "SERVER")),
    "adHighCpu": L({"ad"}, {"cpu", "latency"}, Check("cpu", "ad", 1.5)),
    "adManualGc": L({"ad"}, {"latency", "cpu"}, Check("p95", "ad", 1.5), Check("cpu", "ad", 1.5)),
    "productCatalogLockContention": L({"product-catalog"}, {"latency"}, Check("p95", "product-catalog", 1.5)),
    "imageSlowLoad": L({"frontend-proxy"}, {"latency"}, Check("p95", "frontend-proxy", 1.5)),
    "intlShippingSlowdown": L({"shipping"}, {"latency"}, Check("p95", "shipping", 1.5)),
    "loadGeneratorFloodHomepage": L({"frontend", "frontend-proxy"}, {"traffic"}, Check("calls", "frontend", 2.0)),
    "recommendationCacheFailure": L({"recommendation"}, {"memory", "latency"},
                                    Check("memory", "recommendation", 0.5), Check("p95", "recommendation", 1.5)),
    "emailMemoryLeak": L({"email"}, {"memory"}, Check("memory", "email", 0.5)),
}
