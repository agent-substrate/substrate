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

def compile_report(threats_json_path, results_dir, output_path):
    with open(threats_json_path, 'r') as f:
        threats = json.load(f)
        
    final_report = []
    succeeded_count = 0
    failed_ids = []

    for t in threats:
        threat_id = t.get("threat_id", "unknown")
        result_file = os.path.join(results_dir, f"{threat_id}.json")
        
        if os.path.exists(result_file):
            try:
                with open(result_file, 'r') as rf:
                    res = json.load(rf)

                # Check threat_id matching
                if not res.get("threat_id"):
                    t["error"] = f"Missing threat_id in result file"
                elif res.get("threat_id") and res.get("threat_id") != threat_id:
                    t["error"] = f"Mismatched threat_id in result file: expected {threat_id}, got {res.get('threat_id')}"
                else:
                    # Validate quality score presence, type, and range
                    if "quality" not in res:
                        t["error"] = "Result JSON missing 'quality' score"
                    elif isinstance(res["quality"], bool):
                      t["error"] = f"Invalid boolean quality score: {res['quality']}"
                    else:
                        try:
                            score = float(res["quality"])
                            if not (0.0 <= score <= 1.0):
                                t["error"] = f"Quality score out of bounds [0.0, 1.0]: {res['quality']}"
                            else:
                                t["quality"] = score
                        except (ValueError, TypeError):
                            t["error"] = f"Invalid non-numeric quality score: {res['quality']}"

                    # Copy strengths and weaknesses if no quality error
                    if "error" not in t:
                        if "strengths" in res:
                            t["strengths"] = str(res["strengths"])
                        if "weaknesses" in res:
                            t["weaknesses"] = str(res["weaknesses"])

                        # Validate and normalize citations schema
                        if "citations" in res:
                            c = res["citations"]
                            if isinstance(c, list):
                                t["citations"] = [str(item) for item in c]
                            elif isinstance(c, str):
                                t["citations"] = [c]
                            else:
                                t["citations"] = []
            except Exception as e:
                t["error"] = f"Failed to parse agent JSON: {e}"
        else:
            t["error"] = "Missing result file. The evaluation sub-agent may have timed out, failed to produce a valid JSON, or written to the wrong location."

        if "error" in t:
            failed_ids.append(threat_id)
        else:
            succeeded_count += 1

        final_report.append(t)
            
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(final_report, f, indent=2)

    total_count = len(final_report)
    print(f"Report compiled successfully to {output_path}")
    print(f"Summary: {total_count} total threats | {succeeded_count} succeeded | {len(failed_ids)} failed")

    if failed_ids:
        print(f"Warning: The following {len(failed_ids)} threat(s) failed evaluation: {', '.join(failed_ids)}", file=sys.stderr)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <threats_json_path> <results_dir> <output_path>", file=sys.stderr)
        sys.exit(1)
    compile_report(sys.argv[1], sys.argv[2], sys.argv[3])
