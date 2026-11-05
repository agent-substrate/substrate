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

"""Evaluates stored benchmark runs with the code the runner uses, read-only on the runs.

  python -m evaluate RUN... (--evaluations JSON|@FILE | --tests-file FILE --test-name NAME | --from-run)
                     [--out DIR]
  python -m evaluate --check-config (--evaluations JSON|@FILE | --tests-file FILE [--test-name NAME])

RUN is a run directory or gs://.../runs/<test>/run_date=.../run_ts=.../run_tag=.../
(copied to a temporary directory). A RUN stored in the runner's layout has the
paired runs the runner would give it, under the same <dest>; any other RUN has
none. --from-run re-reads the evaluations a run was evaluated with (its
evaluation.json); give new ones to re-evaluate it with new thresholds. Writes
evaluation.json and evaluation_rows.jsonl (the rows the runner appends to
stats.jsonl) to --out, by default ./evaluate-out/<test>/run_ts=<ts>/; never
into the stored run.

Exit status: 0 evaluated (whatever the verdict), 2 config error, 1 I/O error.
"""

import argparse
import json
import os
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any, Optional

from . import config, rundir, summary_lines, write
from .evaluator import Evaluator
from .sources import BenchmarkRunContext


class UsageError(Exception):
    pass


def read_json_arg(value: str) -> Any:
    """JSON given inline or as @FILE."""
    text = Path(value[1:]).read_text(encoding="utf-8") if value.startswith("@") else value
    return json.loads(text)


def tests_file_entries(path: str) -> list[dict[str, Any]]:
    try:
        import yaml  # pylint: disable=import-outside-toplevel
    except ImportError as e:
        raise UsageError("--tests-file needs PyYAML (pip install pyyaml)") from e
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f) or {}
    tests = doc.get("tests") if isinstance(doc, dict) else None
    if not isinstance(tests, list):
        raise config.ConfigError(f"{path}: no top-level tests list")
    return [t for t in tests if isinstance(t, dict)]


def tests_file_entry(path: str, name: str) -> dict[str, Any]:
    for t in tests_file_entries(path):
        if t.get("name") == name:
            return t
    raise config.ConfigError(f"{path}: no test named {name!r}")


def open_run(uri: str, tmp: Path) -> tuple[BenchmarkRunContext, dict[str, str]]:
    """The run at uri, with the runner's paired runs when it is stored in the runner's layout, and its identity."""
    run = rundir.open_run(uri, tmp / "run")
    # Identity comes from the path as given, unresolved: a link placed in a runs/ tree is named by its own path.
    identity = rundir.identity_from_uri(uri if uri.startswith("gs://") else os.path.abspath(uri))
    dest = rundir.dest_of(identity["uri"])
    if dest is not None:
        paired_run = rundir.paired_run_finder(dest, identity["tag"], int(identity["run_ts"]), run.files,
                                              tmp / "paired")
        run = replace(run, paired_run=paired_run)
    return run, identity


def from_run_evaluations(run: BenchmarkRunContext, uri: str) -> Any:
    path = run.file("evaluation.json")
    if path is None:
        raise config.ConfigError(f"--from-run: {uri} has no evaluation.json")
    report = json.loads(path.read_text(encoding="utf-8"))
    if report.get("error"):
        raise config.ConfigError(f"--from-run: the run's evaluations did not load: {report['error']}")
    return [ev["config"] for ev in report.get("evaluations", [])]


def check_config(a: argparse.Namespace) -> int:
    if a.evaluations is not None:
        blocks = [("--evaluations", read_json_arg(a.evaluations))]
    elif a.tests_file and a.test_name:
        blocks = [(a.test_name, tests_file_entry(a.tests_file, a.test_name).get("evaluations", []))]
    elif a.tests_file:
        blocks = [(t.get("name"), t["evaluations"]) for t in tests_file_entries(a.tests_file) if "evaluations" in t]
    else:
        raise UsageError("--check-config needs --evaluations or --tests-file")
    bad = 0
    for name, block in blocks:
        try:
            n = len(config.load(block))
            print(f"{name}: ok ({n} evaluations)")
        except config.ConfigError as e:
            bad += 1
            print(f"{name}: {e}", file=sys.stderr)
        except Exception as e:  # pylint: disable=broad-except  # a shape no check expected
            bad += 1
            print(f"{name}: {type(e).__name__}: {e}", file=sys.stderr)
    return 2 if bad else 0


def out_dir(a: argparse.Namespace, identity: dict[str, str], several: bool) -> Path:
    leaf = Path(identity.get("test_name") or Path(identity.get("uri", "run")).name) / \
        f"run_ts={identity.get('run_ts', 'unknown')}"
    if a.out and not several:
        return Path(a.out)
    return Path(a.out or "evaluate-out") / leaf


def evaluate_runs(a: argparse.Namespace) -> int:
    with tempfile.TemporaryDirectory() as tmp:
        raw_evaluations = None
        if a.evaluations is not None:
            raw_evaluations = read_json_arg(a.evaluations)
        elif a.tests_file:
            if not a.test_name:
                raise UsageError("--tests-file needs --test-name")
            raw_evaluations = tests_file_entry(a.tests_file, a.test_name).get("evaluations", [])
        elif not a.from_run:
            raise UsageError("give --evaluations, --tests-file with --test-name, or --from-run")
        evaluations = config.load(raw_evaluations) if raw_evaluations is not None else None
        for i, uri in enumerate(a.runs):
            run, identity = open_run(uri, Path(tmp) / f"r{i}")
            evals = evaluations if evaluations is not None else config.load(from_run_evaluations(run, uri))
            report = Evaluator(evals, run).evaluate(identity)
            out = out_dir(a, identity, len(a.runs) > 1)
            # The rows carry the run's own data timestamp, as the runner's do.
            stamp = rundir.data_timestamp(run.files) or report["evaluated_at"]
            (out / "evaluation_rows.jsonl").unlink(missing_ok=True)
            write(report, out / "evaluation.json", out / "evaluation_rows.jsonl", stamp, identity.get("tag", ""),
                  identity.get("test_name", ""))
            print(f"== {uri}")
            for line in summary_lines(report):
                print(line)
            print(f"wrote {out / 'evaluation.json'}", flush=True)
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="python -m evaluate", description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("runs", nargs="*", metavar="RUN", help="run directory or gs:// run path")
    src = p.add_mutually_exclusive_group()
    src.add_argument("--evaluations", help="the evaluations block as JSON, or @FILE")
    src.add_argument("--tests-file", help="a tests.yaml; with --test-name, that entry's evaluations")
    src.add_argument("--from-run", action="store_true", help="the evaluations in the run's evaluation.json")
    p.add_argument("--test-name", help="the tests-file entry")
    p.add_argument("--out", help="output directory (default ./evaluate-out/<test>/run_ts=<ts>/)")
    p.add_argument("--check-config", action="store_true", help="validate the evaluations only")
    a = p.parse_args(argv)
    try:
        if a.check_config:
            return check_config(a)
        if not a.runs:
            raise UsageError("give at least one RUN")
        return evaluate_runs(a)
    except UsageError as e:
        p.error(str(e))
    except (config.ConfigError, json.JSONDecodeError) as e:
        print(f"config error: {e}", file=sys.stderr)
        return 2
    except OSError as e:
        print(f"I/O error: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
