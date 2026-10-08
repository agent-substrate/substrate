# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for generate_sweperf_templates.py:
python3 benchmarking/workloads/manifests/sweperf/test_generate_sweperf_templates.py
"""

import json
import pathlib
import tempfile
import unittest

import generate_sweperf_templates as gen

PINNED = "example.com/sweperf@sha256:" + "0" * 64


class GenerateTest(unittest.TestCase):

    def test_generated_matches_catalog(self):
        # Fails when sweperf-images.json or the .j2 changed without regenerating.
        rendered = gen.render(gen.load_catalog(gen.HERE / "sweperf-images.json"))
        self.assertEqual(gen.check(rendered, gen.HERE / "generated"), [],
                         "run generate_sweperf_templates.py '*'")

    def test_render_fills_task_values(self):
        out = gen.render([{"name": "django-11099", "image": PINNED, "steps": 15}])
        content = out["sweperf-django-11099-template.yaml.tmpl"]
        self.assertIn("name: sweperf-django-11099", content)
        self.assertIn(f'image: "{PINNED}"', content)
        self.assertIn('value: "15"', content)
        # deploy.sh fills these, so they must survive rendering.
        self.assertIn("${BUCKET_NAME}", content)

    def test_check_reports_missing_stale_and_orphaned_files(self):
        rendered = gen.render([{"name": n, "image": PINNED, "steps": 1}
                               for n in ("fresh-1", "stale-1", "missing-1")])
        with tempfile.TemporaryDirectory() as d:
            d = pathlib.Path(d)
            (d / "sweperf-fresh-1-template.yaml.tmpl").write_text(
                rendered["sweperf-fresh-1-template.yaml.tmpl"])
            (d / "sweperf-stale-1-template.yaml.tmpl").write_text("old")
            (d / "sweperf-gone-1-template.yaml.tmpl").write_text("old")
            problems = gen.check(rendered, d)
        kinds = sorted(p.split(":")[0] for p in problems)
        self.assertEqual(kinds, ["missing", "not in catalog", "stale"], problems)

    def test_catalog_rejects_bad_entries(self):
        cases = {
            "missing key": [{"name": "a", "image": PINNED}],
            "zero steps": [{"name": "a", "image": PINNED, "steps": 0}],
            "unpinned image": [{"name": "a", "image": "example.com/sweperf:a", "steps": 1}],
        }
        for label, catalog in cases.items():
            with self.subTest(label), tempfile.NamedTemporaryFile("w", suffix=".json") as f:
                json.dump(catalog, f)
                f.flush()
                with self.assertRaises(ValueError):
                    gen.load_catalog(pathlib.Path(f.name))


if __name__ == "__main__":
    unittest.main()
