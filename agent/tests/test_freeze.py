"""The Week 7 freeze (patch 80): the variant set, the code hash, and what --held-out refuses. No network, no model."""
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from drishti_agent.evals import freeze, run, variants


class Variants(unittest.TestCase):
    def test_every_variant_is_distinct_and_says_what_it_isolates(self):
        sigs = variants.signatures()
        self.assertEqual(len({json.dumps(s, sort_keys=True) for s in sigs.values()}), len(sigs), "no two variants alike")
        g = sigs["graph"]
        self.assertEqual((g["recheck_minutes"], g["runbooks"], g["memory_from"], g["f4"], g["budget"]), (2.0, True, "", False, 12))
        self.assertEqual((sigs["no-recheck"]["recheck_minutes"], sigs["no-recheck"]["columns"]), (None, "end-to-end"))
        self.assertEqual(sigs["recheck0"]["recheck_minutes"], 0.0)
        self.assertFalse(sigs["no-rb"]["runbooks"])
        self.assertEqual(sigs["mem"]["memory_from"], "capture-2,capture-b")
        self.assertTrue(sigs["floor"]["floor"])
        self.assertEqual((sigs["v0"]["budget"], sigs["b20"]["budget"]), (12, 20))

    def test_the_command_line_list(self):
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            variants.main(["--set", "first"])
        names = [l.split()[0] for l in buf.getvalue().splitlines()]
        self.assertEqual(names, ["graph", "v0", "baseline", "floor", "no-recheck", "no-rb"])


class CodeHash(unittest.TestCase):
    def test_stable_and_changed_by_any_code_file(self):
        a = run.code_hash()
        self.assertEqual(a, run.code_hash())
        extra = Path(run.__file__).resolve().parents[1] / "zz_freeze_probe.txt"
        try:
            extra.write_text("x")
            self.assertNotEqual(run.code_hash(), a)
        finally:
            extra.unlink()
        self.assertEqual(run.code_hash(), a)


class HeldOut(unittest.TestCase):
    """--held-out against a version-2 freeze, over the fake call graph."""

    def setUp(self):
        from test_harness import Harness
        self.h = Harness("run_")
        self.h.setUp()
        self.frozen = Path(self.h.d) / "FROZEN.json"
        p = mock.patch.object(run, "FROZEN", self.frozen)
        p.start()
        self.addCleanup(p.stop)
        self.addCleanup(self.h.tearDown)

    def write(self, **over):
        from drishti_agent.prompts import PROMPT_VERSION
        from drishti_agent.prompts_graph import GRAPH_PROMPT_VERSION
        f = {"version": 2, "prompt_version": PROMPT_VERSION, "graph_prompt_version": GRAPH_PROMPT_VERSION,
             "code_hash": run.code_hash(), "model": "by-content", "variants": variants.signatures()}
        f.update(over)
        self.frozen.write_text(json.dumps(f))

    def go(self, *flags):
        from test_durable import ByContent
        out = os.path.join(self.h.d, f"o{len(os.listdir(self.h.d))}")
        with mock.patch.object(run, "from_args", lambda *a, **k: ByContent(tempfile.mktemp())):
            return run.main(["--api", self.h.s.url, "--timeline", self.h.tl, "--recording", "fake", "--provider", "ollama",
                             "--model", "m", "--runs", "1", "--out", out, "--held-out", *flags])

    def test_a_frozen_variant_runs(self):
        self.write()
        self.assertEqual(self.go("--systems", "graph", "--label", "no-rb"), 0)

    def test_settings_outside_the_variant_set_are_refused(self):
        self.write()
        self.assertEqual(self.go("--systems", "graph", "--label", "no-rb", "--budget", "13"), 2)
        self.assertEqual(self.go("--systems", "graph", "--label", "no-rb", "--recheck-minutes", "3"), 2)
        self.assertEqual(self.go("--systems", "graph", "--label", "something"), 2)

    def test_changed_code_another_model_or_a_week5_freeze_is_refused(self):
        self.write(code_hash="0" * 16)
        self.assertEqual(self.go("--systems", "graph", "--label", "no-rb"), 2)
        self.write(model="openai-compat:other")
        self.assertEqual(self.go("--systems", "graph", "--label", "no-rb"), 2)
        self.write(version=None)
        self.assertEqual(self.go("--systems", "graph", "--label", "no-rb"), 2)

    def test_the_floor_needs_no_model_match(self):
        self.write(model="openai-compat:gemini")
        self.assertEqual(self.go("--systems", "agent", "--floor"), 0)


if __name__ == "__main__":
    unittest.main()
