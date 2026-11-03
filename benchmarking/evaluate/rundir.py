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

"""Where runs are stored: opening stored runs, and finding the run another run is paired with.

The runner stores each run under
<dest>/runs/<test>/run_date=<YYYY-MM-DD>/run_ts=<unix s>/run_tag=<commit>/, with
<dest> a local directory or gs://bucket/path. This module is the only one that
knows that layout or touches gs://: it copies a stored run's files to a local
directory (read-only on the store) and hands sources a BenchmarkRunContext of local
paths. GCS is read through the google-cloud-storage client when it is
installed (the runner image), else through `gcloud storage`.
"""

import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping

from .sources.base import BenchmarkRunContext
from .sources.run_results import read_stats

# The files sources read.
RUN_FILES = ("logs.txt", "stats.jsonl", "server_summary.json", "stats_history.csv")
RUN_PATH = re.compile(r"^(?P<root>.*)/runs/(?P<test>[^/]+)/run_date=(?P<date>[^/]+)/run_ts=(?P<ts>\d+)"
                      r"/run_tag=(?P<tag>[^/]+)/?$")
DEFAULT_MAX_AGE_S = 6 * 3600


def identity_from_uri(uri: str) -> dict[str, str]:
    """test_name, run_date, run_ts, tag and uri of a run stored in the runner's layout; just uri otherwise."""
    uri = uri.rstrip("/")
    m = RUN_PATH.match(uri)
    if not m:
        return {"uri": uri}
    return {"test_name": m["test"], "tag": m["tag"], "run_date": m["date"], "run_ts": m["ts"], "uri": uri}


def run_uri(dest: str, test: str, run_date: str, run_ts: str, tag: str) -> str:
    return f"{dest.rstrip('/')}/runs/{test}/run_date={run_date}/run_ts={run_ts}/run_tag={tag}"


def open_run(uri: str, tmp: Path) -> BenchmarkRunContext:
    """A BenchmarkRunContext for a local run directory, or a gs:// run whose files are copied under tmp."""
    uri = uri.rstrip("/")
    if not uri.startswith("gs://"):
        if not Path(uri).is_dir():
            raise FileNotFoundError(f"no run directory {uri}")
        return BenchmarkRunContext(files={name: Path(uri) / name for name in RUN_FILES}, label=uri)
    tmp.mkdir(parents=True, exist_ok=True)
    local = Path(tempfile.mkdtemp(prefix="run-", dir=tmp))
    for name in RUN_FILES:
        _gcs_copy(f"{uri}/{name}", local / name)
    if not (local / "logs.txt").exists() and not (local / "stats.jsonl").exists():
        raise FileNotFoundError(f"no logs.txt or stats.jsonl under {uri}")
    return BenchmarkRunContext(files={name: local / name for name in RUN_FILES}, label=uri)


def find_paired_run(dest: str, test_name: str, tag: str, before_ts: int, max_age_s: int = DEFAULT_MAX_AGE_S) -> str:
    """The stored run of test_name paired with a run of tag at before_ts; raises LookupError when there is none.

    The pair is the run of test_name under dest with the same tag (commit) and
    the latest run_ts at or before before_ts, at most max_age_s earlier: in a
    suite, the earlier test of the same invocation. It must have a logs.txt or
    a stats.jsonl.
    """
    first = datetime.fromtimestamp(before_ts - max_age_s, tz=timezone.utc).date()
    last = datetime.fromtimestamp(before_ts, tz=timezone.utc).date()
    dates = [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]
    candidates = []
    for date in dates:
        for found in _list_runs(f"{dest.rstrip('/')}/runs/{test_name}/run_date={date}/"):
            other = identity_from_uri(found)
            if other.get("tag") == tag and before_ts - max_age_s <= int(other["run_ts"]) <= before_ts:
                candidates.append((int(other["run_ts"]), found))
    if not candidates:
        raise LookupError(f"no run of {test_name} with tag {tag} under {dest.rstrip('/')}/runs/ in the "
                          f"{max_age_s / 3600:g} h before run_ts {before_ts}")
    return max(candidates)[1]


def cluster_name(files: Mapping[str, Path]) -> str:
    """The cluster a run ran on (trial_summary's metadata_cluster_name); "" when not recorded."""
    trial = read_stats(files.get("stats.jsonl")).get("trial_summary") or {}
    measurements = trial.get("measurements")
    name = measurements.get("metadata_cluster_name") if isinstance(measurements, dict) else None
    return str(name) if name else ""


def paired_run_finder(dest: str, tag: str, run_ts: int, files: Mapping[str, Path], tmp: Path,
                      max_age_s: int = DEFAULT_MAX_AGE_S) -> Callable[[str], BenchmarkRunContext]:
    """BenchmarkRunContext.paired_run for the run of tag at run_ts under dest, whose files are files.

    The paired run of a test is find_paired_run's, opened under tmp and looked
    up once. It must have run on the same cluster as this run when both record
    one, checked on each call because this run's stats.jsonl exists only after
    the run. Any failure (no such run, a listing or permission error, an
    unreadable run) raises LookupError saying why.
    """
    looked_up: dict[str, BenchmarkRunContext | str] = {}

    def paired_run(test: str) -> BenchmarkRunContext:
        if test not in looked_up:
            try:
                looked_up[test] = open_run(find_paired_run(dest, test, tag, run_ts, max_age_s), tmp)
            except Exception as e:  # pylint: disable=broad-except
                # KeyError and IndexError are LookupErrors too, but not ours.
                looked_up[test] = str(e) if type(e) is LookupError else f"{type(e).__name__}: {e}"
        run = looked_up[test]
        if isinstance(run, str):
            raise LookupError(run)
        mine, theirs = cluster_name(files), cluster_name(run.files)
        if mine and theirs and mine != theirs:
            raise LookupError(f"cluster differs ({mine} vs {theirs})")
        return run

    return paired_run


def _list_runs(prefix: str) -> list[str]:
    """Run directories (with a logs.txt or stats.jsonl) under prefix, which ends in run_date=<date>/."""
    marks = ("/logs.txt", "/stats.jsonl")
    if not prefix.startswith("gs://"):
        found = [p for m in marks for p in Path(prefix).glob(f"run_ts=*/run_tag=*{m}")]
        return sorted({str(p.parent) for p in found})
    names = _gcs_list(prefix)
    return sorted({n.rsplit("/", 1)[0] for n in names if n.endswith(marks)})


def _split(uri: str) -> tuple[str, str]:
    bucket, _, path = uri[len("gs://"):].partition("/")
    return bucket, path


def _storage_client():
    """A google-cloud-storage client, or None when the library is not installed."""
    try:
        from google.cloud import storage  # pylint: disable=import-outside-toplevel
    except ImportError:
        return None
    return storage.Client()


def _gcs_list(prefix: str) -> list[str]:
    """gs:// URIs of the objects under prefix. Raises OSError on a listing error."""
    client = _storage_client()
    bucket, path = _split(prefix)
    if client is not None:
        return [f"gs://{bucket}/{b.name}" for b in client.list_blobs(bucket, prefix=path)]
    if not shutil.which("gcloud"):
        raise OSError("reading gs:// needs google-cloud-storage or gcloud")
    res = subprocess.run(["gcloud", "storage", "ls", "-r", prefix], capture_output=True, text=True, check=False)
    if res.returncode != 0:
        if "matched no objects" in res.stderr or "One or more URLs matched no objects" in res.stderr:
            return []
        raise OSError(f"gcloud storage ls {prefix}: {res.stderr.strip()[:300]}")
    return [line.strip() for line in res.stdout.splitlines() if line.strip().startswith("gs://")]


def _gcs_copy(uri: str, dest: Path) -> bool:
    """Copies one object to dest; False when it doesn't exist. Never writes to GCS."""
    client = _storage_client()
    if client is not None:
        bucket, path = _split(uri)
        blob = client.bucket(bucket).blob(path)
        if not blob.exists():
            return False
        blob.download_to_filename(str(dest))
        return True
    if not shutil.which("gcloud"):
        raise OSError("reading gs:// needs google-cloud-storage or gcloud")
    res = subprocess.run(["gcloud", "storage", "cp", "-q", uri, str(dest)], capture_output=True, text=True,
                         check=False)
    return res.returncode == 0 and dest.exists()
