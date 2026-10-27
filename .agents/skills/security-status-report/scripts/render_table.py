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

"""Renders final.json as a markdown table, highest priority first."""

import json
import os
import statistics
import sys

PRIORITY_ORDER = ["Critical", "High", "Medium", "Low"]


def sort_key(item):
    p = item.get("priority", "")
    rank = PRIORITY_ORDER.index(p) if p in PRIORITY_ORDER else len(PRIORITY_ORDER)
    tid = item.get("threat_id", "")
    if tid.startswith("T-") and tid[2:].isdigit():
        return (rank, 0, int(tid[2:]), "")
    return (rank, 1, 0, tid)


def cell(value):
    return str(value).replace("|", "/").replace("\n", " ").strip()


def render_table(data):
    rows = sorted(data, key=sort_key)
    scored = [t["quality"] for t in rows if "error" not in t and "quality" in t]
    lines = []
    if scored:
        lines += [f"Mean score: {statistics.mean(scored):.2f}", ""]
    lines += [
        "| Threat | Priority | Score | Mitigation summary | Remaining gap |",
        "| :---- | :---- | :---- | :---- | :---- |",
    ]
    for t in rows:
        if "error" in t:
            score, mitigation, gap = "ERROR", "", cell(t["error"])
        else:
            score = f"{float(t.get('quality', 0.0)):.2f}"
            mitigation = cell(t.get("mitigation_summary", ""))
            gap = cell(t.get("remaining_gap", ""))
        lines.append(f"| {cell(t.get('threat_id', ''))} | {cell(t.get('priority', ''))} | {score} | {mitigation} | {gap} |")
    return "\n".join(lines) + "\n"


if __name__ == '__main__':
    report_file = sys.argv[1] if len(sys.argv) > 1 else ".agents/scratch/security-status-report/final.json"
    output_md = sys.argv[2] if len(sys.argv) > 2 else ".agents/scratch/security-status-report/report.md"
    with open(report_file, 'r') as f:
        table = render_table(json.load(f))
    output_dir = os.path.dirname(output_md)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_md, 'w') as f:
        f.write(table)
    print(f"Table rendered successfully to {output_md}")
