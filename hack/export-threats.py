#!/usr/bin/env python3
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys
import json
import argparse
import re

def unescape_md(text):
    if not text:
        return text
    return re.sub(r'\\(.)', r'\1', text)

def extract(md_path):
    with open(md_path, 'r') as f:
        lines = f.readlines()

    threats = []
    headers = {}
    in_table = False
    for line in lines:
        line = line.strip()
        if not line.startswith('|'):
            in_table = False
            continue
            
        line_escaped = line.replace('\\|', '\x00PIPE\x00')
        cells = [c.strip().replace('\x00PIPE\x00', '\\|') for c in line_escaped.split('|')[1:-1]]
        if not in_table:
            lower_cells = [unescape_md(c.lower()) for c in cells]
            if 'threat' in lower_cells:
                in_table = True
                headers = {}
                for i, col in enumerate(lower_cells):
                    if col == 'threat': headers['threat'] = i
                    elif col == 'threat id': headers['threat_id'] = i
                    elif col == 'priority': headers['priority'] = i
                    elif col == 'mitigating invariants': headers['mitigating_invariants'] = i
                    elif col == 'suggested concrete mitigations': headers['suggested_concrete_mitigations'] = i
                    elif col == 'notes': headers['notes'] = i
            continue
            
        if all(c.replace('-', '').replace(':', '') == '' for c in cells):
            continue
            
        if in_table and 'threat' in headers and len(cells) > headers['threat']:
            threat = unescape_md(cells[headers['threat']])
            if threat: 
                t_obj = {}
                if 'threat_id' in headers and len(cells) > headers['threat_id']:
                    t_obj['threat_id'] = unescape_md(cells[headers['threat_id']])
                if 'priority' in headers and len(cells) > headers['priority']:
                    t_obj['priority'] = unescape_md(cells[headers['priority']])
                
                t_obj['threat'] = threat
                
                if 'mitigating_invariants' in headers and len(cells) > headers['mitigating_invariants']:
                    t_obj['mitigating_invariants'] = unescape_md(cells[headers['mitigating_invariants']])
                if 'suggested_concrete_mitigations' in headers and len(cells) > headers['suggested_concrete_mitigations']:
                    t_obj['suggested_concrete_mitigations'] = unescape_md(cells[headers['suggested_concrete_mitigations']])
                if 'notes' in headers and len(cells) > headers['notes']:
                    t_obj['notes'] = unescape_md(cells[headers['notes']])
                    
                threats.append(t_obj)

    seen_ids = set()
    for t in threats:
        tid = t.get("threat_id")
        if not tid or not re.match(r"^T-\d+$", tid):
            raise ValueError(f"Invalid threat_id format '{tid}': must match 'T-NN' (e.g. T-01)")
        if tid in seen_ids:
            raise ValueError(f"Duplicate threat_id found in Markdown table: {tid}")
        seen_ids.add(tid)

    return threats

def main():
    parser = argparse.ArgumentParser(description="Export threats from threat-model.md to JSON")
    parser.add_argument("--md", type=str, default="docs/threat-model.md", help="Path to threat-model.md")
    parser.add_argument("--out", type=str, default="docs/threats.json", help="Output JSON path")
    args = parser.parse_args()

    if not os.path.exists(args.md):
        print(f"Error: {args.md} not found.", file=sys.stderr)
        sys.exit(1)

    threats = extract(args.md)
    with open(args.out, 'w') as f:
        json.dump(threats, f, indent=2)
        f.write('\n')
    print(f"Extracted {len(threats)} threats from {args.md} to {args.out}")

if __name__ == '__main__':
    main()
