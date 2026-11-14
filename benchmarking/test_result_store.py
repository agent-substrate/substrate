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

import subprocess
import sys
from pathlib import Path

import pytest

from result_store import ResultStore, upload


@pytest.mark.parametrize("backend", ["local", "memory"])
def test_round_trip(tmp_path, backend):
    store = ResultStore(str(tmp_path / "results") if backend == "local" else "memory:///")
    source = tmp_path / "source"
    source.write_bytes(b"first\x00\xff")
    path = "runs/test/run_date=2026-11-12/run_ts=123/run_tag=abc/stats.jsonl"
    store.write(path, source)
    store.write("unrelated/status.json", source)
    assert list(store.list("runs/")) == [path]
    assert store.read(path) == source.read_bytes()
    source.write_bytes(b"replacement")
    store.write(path, source)
    assert store.read(path) == b"replacement"
    with pytest.raises(FileNotFoundError):
        store.read("missing")


def test_upload_local_and_failure(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"stats")
    destination = tmp_path / "nested" / "stats.jsonl"
    upload(source, str(destination))
    assert destination.read_bytes() == b"stats"
    with pytest.raises(FileNotFoundError):
        upload(tmp_path / "missing", str(destination))
    with pytest.raises(ValueError, match="Unsupported"):
        upload(source, "https://example.com/stats.jsonl")


def test_cli(tmp_path):
    source = tmp_path / "source"
    source.write_bytes(b"stats")
    root = tmp_path / "results"
    upload(source, f"{root}/runs/test/stats.jsonl")
    script = Path(__file__).with_name("result_store.py")
    def run(*args):
        return subprocess.run([sys.executable, str(script), *map(str, args)], capture_output=True)
    listing = run("list", root, "--match", "runs/*/stats.jsonl")
    assert listing.returncode == 0, listing.stderr
    assert listing.stdout.decode().strip() == f"{root}/runs/test/stats.jsonl"
    assert run("cat", f"{root}/runs/test/stats.jsonl").stdout == b"stats"
    destination = tmp_path / "download"
    result = run("download", root, destination)
    assert result.returncode == 0, result.stderr
    assert (destination / "runs/test/stats.jsonl").read_bytes() == b"stats"
    assert run("cat", f"{root}/missing").returncode != 0


def test_azure_url_selects_container_and_prefix(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT_NAME", "testaccount")
    store = ResultStore("az://results/substrate")
    assert store.store.config["container_name"] == "results"
    assert store.store.prefix == "substrate"
