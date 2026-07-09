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
    for t in threats:
        threat_id = t.get("threat_id", "unknown")
        result_file = os.path.join(results_dir, f"{threat_id}.json")
        
        if os.path.exists(result_file):
            try:
                with open(result_file, 'r') as rf:
                    res = json.load(rf)
                t.update(res)
            except Exception as e:
                t.update({"error": f"Failed to parse agent JSON: {e}"})
        else:
            t.update({"error": "The evaluation sub-agent timed out or failed to produce a valid JSON."})
        final_report.append(t)
            
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(final_report, f, indent=2)
    print(f"Report compiled successfully to {output_path}")

if __name__ == '__main__':
    compile_report(sys.argv[1], sys.argv[2], sys.argv[3])
