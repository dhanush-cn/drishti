#!/usr/bin/env python3
"""Headless check of the dashboard page (driven by TestPageE2E).

  page_e2e.py <url> <screenshot.png>

Protocol on stdout, one line each: READY once the page has its init; then,
for every anomaly id the test announces on stdin ("WAIT <id> <event>"), a JSON
line with how long after its detected_at the page had it rendered.
"""
import json, sys, time
from playwright.sync_api import sync_playwright

url, shot = sys.argv[1], sys.argv[2]
with sync_playwright() as p:
    b = p.chromium.launch()
    pg = b.new_page(viewport={"width": 1280, "height": 800}, device_scale_factor=1)
    errors = []
    pg.on("pageerror", lambda e: errors.append(str(e)))
    pg.goto(url)
    pg.wait_for_function("document.getElementById('connText').textContent === 'live'", timeout=10000)
    print("READY", flush=True)
    for line in sys.stdin:
        cmd = line.split()
        if not cmd:
            continue
        if cmd[0] == "WAIT":
            aid, kind = cmd[1], cmd[2]
            sel = f'.anom[data-id="{aid}"][data-event="{kind}"]'
            pg.wait_for_selector(sel, state="attached", timeout=10000)
            r = pg.evaluate("""([id, kind]) => {
                const x = window.__drishti.received.find(r => r.id === id && r.event === kind);
                return x ? {received_ms: x.receivedAt - x.detectedAt} : null }""", [aid, kind])
            rendered = int(time.time() * 1000)
            det = pg.evaluate("([id]) => Date.parse(window.__drishti.anomalies.get(id).detected_at)", [aid])
            text = pg.inner_text(sel)
            print(json.dumps({"id": aid, "event": kind, "received_ms": r and r["received_ms"],
                              "rendered_ms": rendered - det, "text": text, "errors": errors}), flush=True)
        elif cmd[0] == "ROWS":
            pg.wait_for_function("document.querySelectorAll('#svcs td.svc').length > 0", timeout=10000)
            print(json.dumps({"rows": pg.inner_text("#svcs"), "fresh": pg.inner_text("#fresh"), "errors": errors}), flush=True)
        elif cmd[0] == "SHOT":
            pg.evaluate("document.getElementById('resolvedBox').open = true")
            pg.screenshot(path=shot, full_page=True)
            print(json.dumps({"shot": shot}), flush=True)
        elif cmd[0] == "QUIT":
            break
    b.close()
