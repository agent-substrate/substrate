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

import sys
import json
import os

# Set backend to non-interactive before importing pyplot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def render_chart(final_report_path, output_png_path):
    with open(final_report_path, 'r') as f:
        data = json.load(f)

    # Sort data by threat_id (e.g. T-01, T-02, ...)
    def sort_key(item):
        tid = item.get("threat_id", "")
        if tid.startswith("T-") and tid[2:].isdigit():
            return int(tid[2:])
        return tid

    sorted_data = sorted(data, key=sort_key)

    threat_ids = [item.get("threat_id", f"T-{i+1:02d}") for i, item in enumerate(sorted_data)]
    
    scores = []
    colors = []
    errors = []

    for item in sorted_data:
        raw_score = item.get("quality", 0.0)
        is_error = "error" in item or raw_score in ("Error", "Timeout/Missing")
        errors.append(is_error)

        try:
            score = float(raw_score) if not is_error else 0.0
        except (ValueError, TypeError):
            score = 0.0
        score = max(0.0, min(1.0, score))
        scores.append(score)

        if is_error:
            colors.append("#dc2626")  # Red for error / timeout
        elif score >= 0.8:
            colors.append("#16a34a")  # Green
        elif score >= 0.5:
            colors.append("#d97706")  # Orange
        else:
            colors.append("#dc2626")  # Red

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar(threat_ids, scores, color=colors, width=0.6)

    # Annotate errors with vertical "ERROR" text above x-axis
    for i, err in enumerate(errors):
        if err:
            ax.text(i, 0.02, "ERROR", rotation=90, ha='center', va='bottom', fontsize=8, fontweight='bold', color='#dc2626')

    ax.set_ylim(0.0, 1.0)
    ax.set_ylabel("Quality Score (0.0 - 1.0)", fontsize=12, fontweight='bold')
    ax.set_xlabel("Threat ID", fontsize=12, fontweight='bold')
    ax.set_title("Substrate Security Threat Posture Scores", fontsize=16, fontweight='bold', pad=15)

    # Standard matplotlib way to rotate tick labels cleanly
    plt.xticks(rotation=90, fontsize=9, fontweight='bold')
    ax.grid(axis='y', linestyle='--', alpha=0.5)

    plt.tight_layout()
    os.makedirs(os.path.dirname(output_png_path), exist_ok=True)
    plt.savefig(output_png_path, dpi=150)
    plt.close()
    print(f"Chart rendered successfully to {output_png_path}")

if __name__ == '__main__':
    report_file = sys.argv[1] if len(sys.argv) > 1 else ".agents/scratch/security-status-report/final.json"
    output_png = sys.argv[2] if len(sys.argv) > 2 else ".agents/scratch/security-status-report/chart.png"
    render_chart(report_file, output_png)
