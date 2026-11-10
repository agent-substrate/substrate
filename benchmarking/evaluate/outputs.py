# Copyright 2026 The Agent Substrate Authors
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

"""The outputs of an evaluation: evaluation.json, and summary rows for stats.jsonl.

evaluation.json holds the Report as JSON: per evaluation its status, each
condition with its CEL expression and verdict, each variable with all its
values, and the evaluation as loaded. The stats.jsonl rows reuse the file's
five keys (timestamp, tag, test_name, metric, measurements) so the existing
pipeline picks them up: one `evaluation_<name>` row per evaluation and one
`evaluations` row for the summary, with string measurements like the others.
"""

import dataclasses
import json
from pathlib import Path
from typing import Any, Optional

from .evaluator import FAIL, INVALID, Report

# The summary row's metric, which no evaluation_<name> can equal.
SUMMARY_METRIC = "evaluations"


def write_evaluation_json(report: Report, path: Path) -> None:
    text = json.dumps(dataclasses.asdict(report), indent=2, allow_nan=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text + "\n", encoding="utf-8")


def _str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer():
        return str(int(v)) if abs(v) < 1e15 else repr(v)
    return str(v)


def stats_rows(report: Report, timestamp: str, tag: str, test_name: str) -> list[dict[str, Any]]:
    """stats.jsonl rows for report: one per evaluation, then the summary.

    An evaluation's row has its status, passed, reason, failed_conditions and
    invalid_conditions, and each value of each variable as <variable>__<value
    name>, so a dashboard reads the numbers next to the verdict. Variable names
    come from the config, and value names follow the query hook's rule
    (sources/base.py), so the keys stay bounded. A variable name has no "__"
    and doesn't end in "_" (config.py), so no two values share a key. A report
    whose evaluations failed to load gets only the summary row, carrying the
    error.
    """
    def row(metric: str, measurements: dict[str, Any]) -> dict[str, Any]:
        return {"timestamp": timestamp, "tag": tag, "test_name": test_name, "metric": metric,
                "measurements": {k: _str(v) for k, v in measurements.items()}}

    rows = []
    for ev in report.evaluations:
        m: dict[str, Any] = {
            "status": ev.status, "passed": ev.passed, "reason": ev.reason,
            "failed_conditions": ",".join(c.name for c in ev.conditions if c.status == FAIL),
            "invalid_conditions": ",".join(c.name for c in ev.conditions if c.status == INVALID),
        }
        for var in ev.variables:
            for name, value in var.values.items():
                m[f"{var.name}__{name}"] = value
        rows.append(row(f"evaluation_{ev.name}", m))
    s = report.summary
    rows.append(row(SUMMARY_METRIC, {"status": s.status, "passed": s.passed, "failed": ",".join(s.failed),
                                     "invalid": ",".join(s.invalid), "error": report.error or ""}))
    return rows


def append_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    lines = "".join(json.dumps(r) + "\n" for r in rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(lines)


def summary_lines(report: Report) -> list[str]:
    """One line per evaluation, with failed or invalid condition reasons, for logs."""
    if report.error:
        return [f"evaluations not run: {report.error}"]
    lines = []
    for ev in report.evaluations:
        line = f"evaluation {ev.name}: {ev.status}"
        if ev.reason:
            line += f" ({ev.reason})"
        details = list(dict.fromkeys(c.reason for c in ev.conditions if c.reason))
        lines.append(line + "".join(f"\n  {d}" for d in details))
    lines.append(f"summary: {report.summary.status} ({len(report.evaluations)} evaluations)")
    return lines
