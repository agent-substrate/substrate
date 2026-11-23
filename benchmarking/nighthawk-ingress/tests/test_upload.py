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

"""Unit tests for upload() and upload_files() of nighthawk_common/output.py. They use pytest fixtures."""

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from nighthawk_common.output import upload, upload_files


def test_upload_copies_to_a_local_path(tmp_path):
    src = tmp_path / "capacity.json"
    src.write_text("{}")
    dest = tmp_path / "runs" / "name" / "capacity.json"
    upload(src, str(dest))
    assert dest.read_text() == "{}"


def test_upload_sends_a_gs_uri_to_its_bucket(tmp_path):
    src = tmp_path / "capacity.json"
    src.write_text("{}")
    with mock.patch("google.cloud.storage.Client") as client:
        upload(src, "gs://bucket/path/runs/name/capacity.json")
    bucket = client.return_value.bucket
    bucket.assert_called_once_with("bucket")
    bucket.return_value.blob.assert_called_once_with("path/runs/name/capacity.json")
    bucket.return_value.blob.return_value.upload_from_filename.assert_called_once_with(str(src))


def test_upload_files_goes_on_after_a_missing_file_and_a_failure(tmp_path, capsys):
    first, failing, last = (tmp_path / name for name in ("status.json", "results.json", "capacity.json"))
    for src in (first, failing, last):
        src.write_text("{}")
    missing = tmp_path / "output.textproto"

    def fake_upload(src, dest):
        if src == failing:
            raise OSError("bucket gone")

    with mock.patch("nighthawk_common.output.upload", side_effect=fake_upload) as fake:
        failures = upload_files([first, missing, failing, last], "gs://bucket/run")

    assert failures == ["upload of results.json failed: bucket gone"]
    assert [call.args for call in fake.call_args_list] == [
        (first, "gs://bucket/run/status.json"),
        (failing, "gs://bucket/run/results.json"),
        (last, "gs://bucket/run/capacity.json"),
    ]
    assert capsys.readouterr().out.splitlines() == [
        f"Uploaded {first} -> gs://bucket/run/status.json",
        f"Skipping {missing}: not produced",
        f"Upload of {failing} failed: bucket gone",
        f"Uploaded {last} -> gs://bucket/run/capacity.json",
    ]
