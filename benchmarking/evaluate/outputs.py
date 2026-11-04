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

"""The outputs of an evaluation: evaluation.json, and summary rows for stats.jsonl.

evaluation.json holds everything: per evaluation its status, each condition
with the value and limit it compared, each variable with all its values, and
the evaluation as loaded. The stats.jsonl rows reuse the file's five keys
(timestamp, tag, test_name, metric, measurements) so the existing pipeline
picks them up: one `evaluation_<name>` row per evaluation and one
`evaluation_summary` row, with string measurements like the others.
"""

import json
from pathlib import Path
from typing import Any, Optional

SUMMARY_METRIC = "evaluation_summary"


def write_evaluation_json(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, allow_nan=False)
        f.write("\n")


def _str(v: Any) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer():
        return str(int(v)) if abs(v) < 1e15 else repr(v)
    return str(v)


def stats_rows(report: dict[str, Any], timestamp: str, tag: str, test_name: str) -> list[dict[str, Any]]:
    """stats.jsonl rows for report: one per evaluation, then the summary.

    Every value of every variable is flattened as <variable>__<value>, so a
    dashboard reads the numbers next to the verdict without parsing
    evaluation.json. A report whose evaluations failed to load gets only the
    summary row, carrying the error.
    """
    def row(metric: str, measurements: dict[str, Any]) -> dict[str, Any]:
        return {"timestamp": timestamp, "tag": tag, "test_name": test_name, "metric": metric,
                "measurements": {k: _str(v) for k, v in measurements.items()}}

    rows = []
    for ev in report.get("evaluations", []):
        m: dict[str, Any] = {
            "status": ev["status"], "passed": ev["passed"], "reason": ev["reason"],
            "failed_conditions": ",".join(c["name"] for c in ev["conditions"] if c["status"] == "fail"),
            "invalid_conditions": ",".join(c["name"] for c in ev["conditions"] if c["status"] == "invalid"),
        }
        for var in ev["variables"]:
            if var["invalid_reason"]:
                m[f"{var['name']}__invalid_reason"] = var["invalid_reason"]
            for name, value in var["values"].items():
                m[f"{var['name']}__{name}"] = value
        rows.append(row(f"evaluation_{ev['name']}", m))
    s = report.get("summary") or {}
    rows.append(row(SUMMARY_METRIC, {
        "status": s.get("status"), "passed": s.get("passed", False), "failed": ",".join(s.get("failed", [])),
        "invalid": ",".join(s.get("invalid", [])), "error": report.get("error") or ""}))
    return rows


def append_jsonl(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.writelines(json.dumps(r, allow_nan=False) + "\n" for r in rows)


def summary_lines(report: dict[str, Any]) -> list[str]:
    """One line per evaluation, with the reason of each condition that didn't pass, for logs."""
    if report.get("error"):
        return [f"evaluations not run: {report['error']}"]
    lines = []
    for ev in report["evaluations"]:
        line = f"evaluation {ev['name']}: {ev['status']}"
        if ev["reason"]:
            line += f" ({ev['reason']})"
        details = list(dict.fromkeys(c["reason"] for c in ev["conditions"]))
        lines.append(line + "".join(f"\n  {d}" for d in details))
    s = report.get("summary") or {}
    lines.append(f"summary: {s.get('status')} ({len(report['evaluations'])} evaluations)")
    return lines
