# Copyright 2026 The Agent Substrate Authors
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

"""The runner's storage layout, and its definition of "the same invocation".

The runner stores each run under
<dest>/runs/<test>/run_date=<YYYY-MM-DD>/run_ts=<unix s>/run_tag=<commit>/, with
<dest> a local directory or gs://bucket/path. This module is the only one that
knows that layout or touches gs://: it opens a stored run as a
BenchmarkRunContext whose identity comes from the layout and whose artifacts
are read from the store, a gs:// one downloaded on first use (read-only on the
store), through the google-cloud-storage library.

It also says which run of another test is from the same invocation as a run
(run_of_finder): the latest run of that test under the same dest with the same
tag, at most 6 h older, on the same cluster. The cluster check matters because
many CI jobs write to the same dest with the same tag. The generic evaluator
only sees BenchmarkRunContext.run_of.
"""

import functools
import re
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from .sources.base import ArtifactStore, BenchmarkRunContext, LocalArtifacts, RunIdentity
from .sources.run_results import read_stats

RUN_PATH = re.compile(r"^(?P<root>.*)/runs/(?P<test>[^/]+)/run_date=(?P<date>[^/]+)/run_ts=(?P<ts>\d+)"
                      r"/run_tag=(?P<tag>[^/]+)/?$")
DEFAULT_MAX_AGE_S = 6 * 3600


def identity_from_uri(uri: str) -> RunIdentity:
    """The identity of the run stored at uri: test_name, tag, run_date and run_ts too when it is in the runner's layout."""
    uri = uri.rstrip("/")
    m = RUN_PATH.match(uri)
    if not m:
        return RunIdentity(uri=uri)
    return RunIdentity(uri=uri, test_name=m["test"], tag=m["tag"], run_date=m["date"], run_ts=int(m["ts"]))


class GcsArtifacts:
    """A run's artifacts under a gs:// prefix, each downloaded on first use into tmp. Never writes to GCS."""

    def __init__(self, uri: str, tmp: Path):
        self.uri = uri.rstrip("/")
        self.tmp = tmp
        self._fetched: dict[str, Optional[Path]] = {}

    def fetch(self, name: str) -> Optional[Path]:
        """Raises ValueError for a name outside the run, and google-cloud-storage's exception on a read error."""
        if name.startswith("/") or ".." in name.split("/"):
            raise ValueError(f"artifact name {name!r} is outside the run")
        if name not in self._fetched:
            self._fetched[name] = _gcs_download(f"{self.uri}/{name}", self.tmp / name)
        return self._fetched[name]


def open_run(uri: str, tmp: Path) -> BenchmarkRunContext:
    """A BenchmarkRunContext for a local run directory, or a gs:// run whose artifacts are downloaded under tmp."""
    uri = uri.rstrip("/")
    if not uri.startswith("gs://"):
        if not Path(uri).is_dir():
            raise FileNotFoundError(f"no run directory {uri}")
        return BenchmarkRunContext(identity_from_uri(uri), LocalArtifacts(Path(uri)))
    if not _gcs_list(f"{uri}/"):
        raise FileNotFoundError(f"no objects under {uri}/")
    tmp.mkdir(parents=True, exist_ok=True)
    local = Path(tempfile.mkdtemp(prefix="run-", dir=tmp))
    return BenchmarkRunContext(identity_from_uri(uri), GcsArtifacts(uri, local))


def _run_dates(first_ts: int, last_ts: int) -> list[str]:
    """The run_date=<YYYY-MM-DD> values (UTC) of runs between first_ts and last_ts."""
    first = datetime.fromtimestamp(first_ts, tz=timezone.utc).date()
    last = datetime.fromtimestamp(last_ts, tz=timezone.utc).date()
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def runs_of(dest: str, test: str, tag: str, before_ts: int, max_age_s: int = DEFAULT_MAX_AGE_S) -> list[str]:
    """The uris of the runs of test that can be from the same invocation as a run of tag at before_ts, newest
    first; raises LookupError when there is none.

    Those are the runs of test under dest with the same tag and a run_ts at or
    before before_ts, at most max_age_s earlier: in a suite, the earlier test
    of the same invocation is the newest one on the same cluster. Each has a
    logs.txt or a stats.jsonl.
    """
    first_ts = before_ts - max_age_s
    candidates = []
    for date in _run_dates(first_ts, before_ts):
        for uri in _list_runs(f"{dest.rstrip('/')}/runs/{test}/run_date={date}/"):
            run = identity_from_uri(uri)
            if run.tag == tag and run.run_ts is not None and first_ts <= run.run_ts <= before_ts:
                candidates.append((run.run_ts, uri))
    if not candidates:
        raise LookupError(f"no run of {test} with tag {tag} under {dest.rstrip('/')}/runs/ in the "
                          f"{max_age_s / 3600:g} h before run_ts {before_ts}")
    return [uri for _, uri in sorted(candidates, reverse=True)]


def cluster_name(artifacts: ArtifactStore) -> str:
    """The cluster a run ran on (trial_summary's metadata_cluster_name); "" when not recorded.
    Raises ValueError when stats.jsonl is corrupt, and whatever artifacts raises when it can't be read."""
    trial = read_stats(artifacts.fetch("stats.jsonl")).get("trial_summary") or {}
    measurements = trial.get("measurements")
    name = measurements.get("metadata_cluster_name") if isinstance(measurements, dict) else None
    return str(name) if name else ""


def run_of_finder(dest: str, tag: str, run_ts: int, artifacts: ArtifactStore, tmp: Path,
                  max_age_s: int = DEFAULT_MAX_AGE_S) -> Callable[[str], BenchmarkRunContext]:
    """BenchmarkRunContext.run_of for the run of tag at run_ts under dest, whose artifacts are artifacts.

    The run of a test is the newest of runs_of's that ran on the same cluster
    as this run; a run that records no cluster matches any. Listings and runs
    (opened under tmp) are reused. The cluster is read on each call because
    this run's stats.jsonl exists only after the run. Any failure (no such
    run, a listing or permission error, an unreadable run or stats.jsonl)
    raises LookupError saying why.
    """

    @functools.cache
    def _runs(test: str) -> list[str]:
        return runs_of(dest, test, tag, run_ts, max_age_s)

    @functools.cache
    def _open(uri: str) -> BenchmarkRunContext:
        return open_run(uri, tmp)

    def _clusters(run: BenchmarkRunContext) -> tuple[str, str]:
        try:
            return cluster_name(artifacts), cluster_name(run.artifacts)
        except (ValueError, OSError) as e:
            raise LookupError(f"can't compare the clusters: {e}") from None

    def run_of(test: str) -> BenchmarkRunContext:
        mine, others = "", set()
        try:
            for uri in _runs(test):
                run = _open(uri)
                mine, theirs = _clusters(run)
                if not mine or not theirs or mine == theirs:
                    return run
                others.add(theirs)
        except Exception as e:  # pylint: disable=broad-except
            # Only a plain LookupError means "no such run"; anything else (an I/O or permission
            # error, or a KeyError, which is a LookupError too) says what it was.
            raise LookupError(str(e) if type(e) is LookupError else f"{type(e).__name__}: {e}") from None
        raise LookupError(f"cluster differs: no run of {test} on {mine}, only on {', '.join(sorted(others))}")

    return run_of


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


@functools.cache
def _storage_client():
    """The google-cloud-storage client, created on first use. Raises OSError when the library is not installed."""
    try:
        from google.cloud import storage  # pylint: disable=import-outside-toplevel
    except ImportError as e:
        raise OSError("reading gs:// needs google-cloud-storage (pip install google-cloud-storage)") from e
    return storage.Client()


def _gcs_list(prefix: str) -> list[str]:
    """gs:// URIs of the objects under prefix. A listing error raises google-cloud-storage's exception (a
    google.api_core or google.auth error), as does every gs:// read here."""
    bucket, path = _split(prefix)
    return [f"gs://{bucket}/{b.name}" for b in _storage_client().list_blobs(bucket, prefix=path)]


def _gcs_download(uri: str, dest: Path) -> Optional[Path]:
    """Downloads the object uri to dest; None when there is no such object. Never writes to GCS."""
    bucket, path = _split(uri)
    blob = _storage_client().bucket(bucket).get_blob(path)
    if blob is None:
        return None
    dest.parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(str(dest))
    return dest
