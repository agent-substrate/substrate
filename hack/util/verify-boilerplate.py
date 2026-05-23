#!/usr/bin/env python3

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

import os
import sys
import subprocess

import datetime
import re

current_year = datetime.date.today().year


ASLV2_HEADER = [
    "Licensed under the Apache License, Version 2.0 (the \"License\");",
    "you may not use this file except in compliance with the License.",
    "You may obtain a copy of the License at",
    "http://www.apache.org/licenses/LICENSE-2.0",
    "Unless required by applicable law or agreed to in writing, software",
    "distributed under the License is distributed on an \"AS IS\" BASIS,",
    "WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.",
    "See the License for the specific language governing permissions and",
    "limitations under the License."
]

def match_copyright(line):
    match = re.search(r"Copyright\s+(\d{4})\s+Google LLC", line)
    if not match:
        return False
    year = int(match.group(1))
    return 2026 <= year <= current_year


def clean_line(line):
    """Removes leading comment markers and whitespace."""
    line = line.strip()
    if line.startswith('//'):
        line = line[2:]
    elif line.startswith('#'):
        line = line[1:]
    return line.strip()


def verify_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return False

    # Clean the lines to extract text
    cleaned_lines = [clean_line(line) for line in lines[:30]]
    cleaned_lines = [line for line in cleaned_lines if line]  # remove empty lines

    # We just check if the keywords appear in order
    # to be resilient to variations in whitespace or layout
    found_copyright = False
    header_idx = 0
    for line in cleaned_lines:
        if not found_copyright:
            if match_copyright(line):
                found_copyright = True
            continue

        if header_idx < len(ASLV2_HEADER):
            target = ASLV2_HEADER[header_idx].strip()
            if target in line:
                header_idx += 1

    if not found_copyright or header_idx < len(ASLV2_HEADER):
        print(f"Missing License Header: {filepath}")
        return False

    return True


def main():
    # Get tracked files via git ls-files
    try:
        output = subprocess.check_output(['git', 'ls-files', '--others', '--cached', '--exclude-standard'], text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running git ls-files: {e}")
        sys.exit(1)

    files = output.splitlines()
    failed_files = []

    for filepath in files:
        _, ext = os.path.splitext(filepath)
        filename = os.path.basename(filepath)

        # Skip non-source-code files
        if ext in ['.md', '.txt', '.png', '.jpg', '.jpeg', '.gif', '.mp4', '.json', '.pdf', '.ico', '.woff', '.woff2', '.ttf', '.otf']:
            continue
        if filename in ['LICENSE', 'NOTICE', 'CODEOWNERS', '.gitignore', 'go.mod', 'go.sum']:
            continue

        # exclude third_party files, which should have their OWN LICENSE
        # TODO: verify LICENSE exists in each third_party directory?
        if "third_party" in filepath:
            continue

        # exclude top-level LICENSES directory
        if filepath.startswith("LICENSES/"):
            continue

        # exclude github config
        if filepath in [".github/dependabot.yml"]:
            continue

        if not verify_file(filepath):
            failed_files.append(filepath)

    if failed_files:
        print(f"\nVerification failed. {len(failed_files)} file(s) missing license headers.")
        sys.exit(1)

    sys.exit(0)

if __name__ == '__main__':
    main()
