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

import obstore
import pytest
from obstore.store import MemoryStore

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from nighthawk_common.output import upload, upload_files


def test_upload_copies_to_a_local_path(tmp_path):
    src = tmp_path / "capacity.json"
    src.write_text("{}")
    dest = tmp_path / "runs" / "name" / "capacity.json"
    upload(src, str(dest))
    assert dest.read_text() == "{}"


@pytest.mark.parametrize("root", ["gs://bucket/run", "s3://bucket/run", "az://container/run"])
def test_upload_files_sends_object_store_urls_to_the_store(tmp_path, monkeypatch, root):
    monkeypatch.chdir(tmp_path)
    src = tmp_path / "capacity.json"
    src.write_text("{}")
    store = MemoryStore()
    with mock.patch("result_store.from_url", return_value=store) as from_url:
        assert upload_files([src], root) == []
    from_url.assert_called_once_with(root)
    assert bytes(obstore.get(store, "capacity.json").bytes()) == b"{}"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["capacity.json"]


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
