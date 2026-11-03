#!/usr/bin/env python3
"""demo-flag.py: set an OTel Demo feature flag and wait until flagd's file has it.

  scripts/demo-flag.py cartFailure 50%
  scripts/demo-flag.py cartFailure off
  scripts/demo-flag.py --show              # every flag's current variant

Uses the demo's own flag API (flagd-ui, /feature/api/read and /write), the way
the demo's Cypress tests flip flags. The API replaces the whole flags file
asynchronously, so a successful POST doesn't mean flagd has it yet: this script
polls the file flagd reads (src/flagd/demo.flagd.json) until the variant is
there, and fails if it isn't within the timeout.

productCatalogFailure is special: it is switched by its targeting rule (which
product fails), not by defaultVariant (the demo UI does the same).

The API drops the file's "$schema" key and reformats it, so after a run
`git -C ~/otel-demo checkout src/flagd/demo.flagd.json` restores it
(docs/test-harness.md, eval hygiene).

Prints one JSON line: {"flag", "variant", "set_at", "on_disk_at", "wait_ms"}.
"""
import json
import os
import sys
import time
import urllib.request

DEMO = os.environ.get("DEMO_URL", "http://localhost:8080")
FLAG_FILE = os.path.expanduser(os.environ.get("FLAG_FILE", "~/otel-demo/src/flagd/demo.flagd.json"))
TIMEOUT_S = float(os.environ.get("FLAG_TIMEOUT_S", "20"))


def http(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(DEMO + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read() or b"{}")


def current(flags, name):
    f = flags[name]
    if name == "productCatalogFailure":
        return f["targeting"]["if"][1]
    return f["defaultVariant"]


def on_disk(name):
    try:
        with open(FLAG_FILE) as f:
            return current(json.load(f)["flags"], name)
    except (OSError, ValueError, KeyError):
        return None  # mid-rename or not written yet: poll again


def utc(t):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)) + ".%03dZ" % int(t % 1 * 1000)


def main():
    if len(sys.argv) == 2 and sys.argv[1] == "--show":
        flags = http("GET", "/feature/api/read")["flags"]
        for n in sorted(flags):
            print(n, current(flags, n))
        return 0
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    name, variant = sys.argv[1], sys.argv[2]
    body = http("GET", "/feature/api/read")
    flags = body["flags"]
    if name not in flags:
        print(f"unknown flag {name!r}; known: {', '.join(sorted(flags))}", file=sys.stderr)
        return 1
    if variant not in flags[name]["variants"]:
        print(f"flag {name} has no variant {variant!r}; variants: {', '.join(flags[name]['variants'])}", file=sys.stderr)
        return 1
    if name == "productCatalogFailure":
        flags[name]["targeting"]["if"][1] = variant
    else:
        flags[name]["defaultVariant"] = variant
    set_at = time.time()
    http("POST", "/feature/api/write", {"data": body})
    deadline = set_at + TIMEOUT_S
    while on_disk(name) != variant:
        if time.time() > deadline:
            print(f"{name} is still {on_disk(name)!r} in {FLAG_FILE} after {TIMEOUT_S:.0f} s (wanted {variant!r})", file=sys.stderr)
            return 1
        time.sleep(0.05)
    seen = time.time()
    print(json.dumps({"flag": name, "variant": variant, "set_at": utc(set_at), "on_disk_at": utc(seen),
                      "wait_ms": round((seen - set_at) * 1000)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
