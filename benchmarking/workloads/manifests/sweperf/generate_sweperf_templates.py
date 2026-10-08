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

"""Renders sweperf ActorTemplates from sweperf-images.json.

Usage:
  generate_sweperf_templates.py '*'                    # every task
  generate_sweperf_templates.py astropy-7336           # one task
  generate_sweperf_templates.py 'django*' -o /tmp/out  # glob, custom output directory
  generate_sweperf_templates.py --check                # fail if generated/ is stale

Writes sweperf-<name>-template.yaml.tmpl per task into generated/ next to
this script, where deploy.sh looks for every sweperf-* template. The
generated files are checked in: regenerate after editing the catalog or the
.j2, and use --check to find files that are missing or stale. Images in
sweperf-images.json must be pinned by digest.
"""

import argparse
import fnmatch
import json
import pathlib
import sys

import jinja2

HERE = pathlib.Path(__file__).resolve().parent


def load_catalog(path: pathlib.Path) -> list[dict]:
    """Reads the catalog and rejects entries deploy.sh or the client can't use."""
    tasks = json.loads(path.read_text())
    for t in tasks:
        missing = {"name", "image", "steps"} - t.keys()
        if missing:
            raise ValueError(f"{path}: entry {t} is missing {sorted(missing)}")
        if not isinstance(t["steps"], int) or t["steps"] <= 0:
            raise ValueError(f"{path}: {t['name']}: steps must be a positive integer")
        if "@sha256:" not in t["image"]:
            raise ValueError(f"{path}: {t['name']}: image must be pinned by digest (repo@sha256:...)")
    return tasks


def render(tasks: list[dict]) -> dict[str, str]:
    """Returns the generated file name and contents for each task."""
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(HERE),
        undefined=jinja2.StrictUndefined,
        keep_trailing_newline=True,
    ).get_template("sweperf-template.yaml.j2")
    out = {}
    for t in tasks:
        name = f"sweperf-{t['name']}"
        out[f"{name}-template.yaml.tmpl"] = template.render(
            name=name,
            container=t["name"].split("-")[0],  # astropy-7336 -> astropy
            image=t["image"],
            steps=t["steps"],
        )
    return out


def check(rendered: dict[str, str], directory: pathlib.Path) -> list[str]:
    """Returns how directory differs from what the catalog renders."""
    problems = []
    for fname, content in rendered.items():
        path = directory / fname
        if not path.exists():
            problems.append(f"missing: {path}")
        elif path.read_text() != content:
            problems.append(f"stale: {path}")
    for path in sorted(directory.glob("*-template.yaml.tmpl")):
        if path.name not in rendered:
            problems.append(f"not in catalog: {path}")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("task", nargs="?", help="task name to render, or a glob such as '*'")
    parser.add_argument("-o", "--out", type=pathlib.Path, default=HERE / "generated",
                        help="output directory (default: %(default)s)")
    parser.add_argument("--catalog", type=pathlib.Path, default=HERE / "sweperf-images.json")
    parser.add_argument("--check", action="store_true",
                        help="fail if the output directory is missing, stale or has extra templates")
    args = parser.parse_args()

    try:
        tasks = load_catalog(args.catalog)
    except ValueError as e:
        print(e, file=sys.stderr)
        return 1

    if args.check:
        problems = check(render(tasks), args.out)
        for p in problems:
            print(p, file=sys.stderr)
        if problems:
            print(f"regenerate with: {sys.argv[0]} '*'", file=sys.stderr)
            return 1
        return 0

    if not args.task:
        parser.error("give a task name or glob, or --check")
    selected = [t for t in tasks if fnmatch.fnmatch(t["name"], args.task)]
    if not selected:
        names = ", ".join(t["name"] for t in tasks)
        print(f"no task matches {args.task!r}; known: {names}", file=sys.stderr)
        return 1

    args.out.mkdir(parents=True, exist_ok=True)
    for fname, content in render(selected).items():
        path = args.out / fname
        path.write_text(content)
        print(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
