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

"""Unit tests for rundir: stored runs, and the run of another test from the same invocation.

Runs are stand-in directories in the runner's layout; GCS is mocked.
"""

import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from evaluate import rundir  # noqa: E402
from evaluate.sources.base import RunIdentity, no_other_runs  # noqa: E402

T0 = 1791700000  # 2026-10-11T07:06:40Z
TAG = "abc123"


def write_run(d: Path) -> Path:
    d.mkdir(parents=True, exist_ok=True)
    (d / "logs.txt").write_text("[boomer] {}\n")
    return d


def stored(root: Path, test: str, run_ts: int, tag: str = TAG) -> Path:
    date = datetime.fromtimestamp(run_ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return write_run(root / "runs" / test / f"run_date={date}" / f"run_ts={run_ts}" / f"run_tag={tag}")


class IdentityTest(unittest.TestCase):
    def test_identity_from_uri(self):
        self.assertEqual(rundir.identity_from_uri(f"gs://b/p/runs/t1/run_date=2026-10-11/run_ts={T0}/run_tag=x/"),
                         RunIdentity(uri=f"gs://b/p/runs/t1/run_date=2026-10-11/run_ts={T0}/run_tag=x",
                                     test_name="t1", tag="x", run_date="2026-10-11", run_ts=T0))
        self.assertEqual(rundir.identity_from_uri("/tmp/run"), RunIdentity(uri="/tmp/run"))


class OpenRunTest(unittest.TestCase):
    def test_local_run(self):
        with tempfile.TemporaryDirectory() as tmp:
            d = write_run(Path(tmp) / "run")
            run = rundir.open_run(f"{d}/", Path(tmp) / "x")
            self.assertEqual(run.identity, RunIdentity(uri=str(d)))
            self.assertIs(run.run_of, no_other_runs)  # not in the runner's layout
            self.assertEqual(run.artifact("logs.txt"), d / "logs.txt")
            self.assertIsNone(run.artifact("server_summary.json"))
            with self.assertRaisesRegex(FileNotFoundError, "no run directory"):
                rundir.open_run(str(Path(tmp) / "missing"), Path(tmp) / "x")

    def test_gcs_run_downloads_each_artifact_on_first_use(self):
        uri = f"gs://b/p/runs/t1/run_date=2026-10-11/run_ts={T0}/run_tag={TAG}"
        path = uri[len("gs://b/"):]
        stored_names = {f"{path}/logs.txt", f"{path}/custom/extra.json"}
        downloads = []

        class Blob:
            def __init__(self, name):
                self.name = name

            def download_to_filename(self, dest):
                Path(dest).write_text(self.name)

        def get_blob(name):
            downloads.append(name)
            return Blob(name) if name in stored_names else None

        client = mock.Mock()
        client.list_blobs.return_value = [Blob(n) for n in stored_names]
        client.bucket.return_value.get_blob.side_effect = get_blob
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(rundir, "_storage_client", return_value=client):
                run = rundir.open_run(uri, Path(tmp))
                self.assertEqual(run.identity, rundir.identity_from_uri(uri))
                self.assertEqual(downloads, [])  # nothing is read until a source asks
                # A file the framework knows nothing about is there too, for the source that reads it.
                self.assertEqual(run.artifact("custom/extra.json").read_text(), f"{path}/custom/extra.json")
                self.assertEqual(run.artifact("logs.txt").read_text(), f"{path}/logs.txt")
                self.assertEqual(run.artifact("logs.txt").parent.parent, Path(tmp))
                self.assertIsNone(run.artifact("server_summary.json"))
                self.assertIsNone(run.artifact("server_summary.json"))
                with self.assertRaisesRegex(ValueError, "outside the run"):
                    run.artifact("../escape.txt")
            self.assertEqual(downloads, [f"{path}/custom/extra.json", f"{path}/logs.txt",
                                         f"{path}/server_summary.json"])  # each once
            client.list_blobs.return_value = []
            with mock.patch.object(rundir, "_storage_client", return_value=client), \
                 self.assertRaisesRegex(FileNotFoundError, f"no objects under {uri}/"):
                rundir.open_run(uri, Path(tmp))


class SameInvocationTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.uri = str(self.root / f"runs/t150/run_date=2026-10-11/run_ts={T0}/run_tag={TAG}")

    def test_the_run_of_another_test_has_the_same_run_ts(self):
        want = stored(self.root, "t1", T0)
        stored(self.root, "t1", T0 - 600)  # another invocation
        found = rundir.same_invocation(self.uri, self.root / "tmp")("t1")
        self.assertEqual(found.identity, rundir.identity_from_uri(str(want)))
        self.assertEqual(found.artifact("logs.txt"), want / "logs.txt")

    def test_no_run_of_the_test_in_the_invocation(self):
        stored(self.root, "t1", T0 - 600)
        stored(self.root, "t1", T0, tag="other")
        with self.assertRaises(LookupError) as raised:
            rundir.same_invocation(self.uri, self.root / "tmp")("t1")
        self.assertEqual(str(raised.exception),
                         f"no run of t1 at {self.root}/runs/t1/run_date=2026-10-11/run_ts={T0}/run_tag={TAG}")

    def test_a_stored_run_reads_the_runs_of_its_invocation(self):
        stored(self.root, "t1", T0)
        run = rundir.open_run(str(stored(self.root, "t150", T0)), self.root / "tmp")
        self.assertEqual(run.run_of("t1").identity.test_name, "t1")
        self.assertIs(rundir.same_invocation("/tmp/run", self.root / "tmp"), no_other_runs)

    def test_a_found_run_is_reused(self):
        stored(self.root, "t1", T0)
        run_of = rundir.same_invocation(self.uri, self.root / "tmp")
        with mock.patch.object(rundir, "open_run", wraps=rundir.open_run) as opened:
            self.assertIs(run_of("t1"), run_of("t1"))
            for _ in range(2):
                with self.assertRaises(LookupError):
                    run_of("t9")
        self.assertEqual([c.args[0].split("/runs/")[1].split("/")[0] for c in opened.call_args_list],
                         ["t1", "t9", "t9"])  # a miss is looked up again

    def test_a_read_error_is_raised_as_is(self):
        """Only a missing run is a LookupError; the evaluator records any other error as the variable's reason."""
        class Forbidden(Exception):
            pass

        client = mock.Mock()
        client.list_blobs.side_effect = Forbidden("403 no storage.objects.list")
        uri = f"gs://b/p/runs/t150/run_date=2026-10-11/run_ts={T0}/run_tag={TAG}"
        with mock.patch.object(rundir, "_storage_client", return_value=client), \
             self.assertRaisesRegex(Forbidden, "403"):
            rundir.same_invocation(uri, self.root / "tmp")("t1")


if __name__ == "__main__":
    unittest.main()
