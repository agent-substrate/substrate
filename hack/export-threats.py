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
            
        cells = [c.strip() for c in line.split('|')[1:-1]]
        if not in_table:
            lower_cells = [c.lower() for c in cells]
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
            threat = cells[headers['threat']]
            if threat: 
                t_obj = {}
                if 'threat_id' in headers and len(cells) > headers['threat_id']:
                    t_obj['threat_id'] = cells[headers['threat_id']]
                if 'priority' in headers and len(cells) > headers['priority']:
                    t_obj['priority'] = cells[headers['priority']]
                
                t_obj['threat'] = threat
                
                if 'mitigating_invariants' in headers and len(cells) > headers['mitigating_invariants']:
                    t_obj['mitigating_invariants'] = cells[headers['mitigating_invariants']]
                if 'suggested_concrete_mitigations' in headers and len(cells) > headers['suggested_concrete_mitigations']:
                    t_obj['suggested_concrete_mitigations'] = cells[headers['suggested_concrete_mitigations']]
                if 'notes' in headers and len(cells) > headers['notes']:
                    t_obj['notes'] = cells[headers['notes']]
                    
                threats.append(t_obj)
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
