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

"""
Template script for formatting threats into agent dispatch specifications.
The main agent should complete this script to output a valid JSON array
matching the 'Subagents' parameter of its invoke_subagent tool.
"""
import sys
import json
import os

def generate_dispatch_payload(output_path):
    with open('docs/threats.json', 'r') as f:
        threats = json.load(f)
        
    subagents = []
    
    for t in threats:
        prompt = f'''You are a security reviewer evaluating the following specific threat:

{json.dumps(t, indent=2)}

- Focus on this threat only.
- Review the entire repo.
- Score against the mitigating invariants. The suggested mitigations are options, not a checklist.
- Don't penalize items the threat puts outside Substrate's scope. Do penalize gaps in Substrate's own code, manifests, docs, and tooling.
- Read the Trust Boundaries section of docs/security/threat-model.md. Don't penalize anything the threat's notes accept with a documented reason, such as a permission the sandbox runtime needs. Do penalize anything the notes don't accept.
- Produce a quality score for the current security posture of the repo with respect to that threat, from 0.0 (not mitigated) to 1.0 (fully mitigated).
- Output your results using the following schema, by writing them to .agents/scratch/security-status-report/{t["threat_id"]}.json,
  where {t["threat_id"]} matches the id in the threat json you were initially provided.

```json
{{
  "threat_id": "<threat_id from input>",
  "threat": "<threat text from input>",
  "quality": <Decimal between 0 (no effective mitigation) and 1 (perfectly mitigated).>,
  "strengths": "<Specific positive code/design mechanisms responsible for the score.>",
  "weaknesses": "<Specific negative code/design mechanisms responsible for the score.>",
  "mitigation_summary": "<Up to 15 words naming the main mechanisms behind the strengths.>",
  "remaining_gap": "<Up to 15 words naming the most important remaining gap, or None.>",
  "citations": ["<repo-relative/path/to/file1.go>"]
}}
```'''
        # TODO: Agent, update this part to ensure it matches the correct schema for you to invoke sub-agents via a tool call.
        subagents.append({
            "Prompt": prompt,
            "Role": "Security Reviewer",
            "TypeName": "self",
            "Workspace": "inherit"
        })


    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(subagents, f, indent=2)
    print(f"Dispatch payload written successfully to {output_path}")

if __name__ == '__main__':
    output_path = sys.argv[1] if len(sys.argv) > 1 else ".agents/scratch/security-status-report/subagents.json"
    generate_dispatch_payload(output_path)
