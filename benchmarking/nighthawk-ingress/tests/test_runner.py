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

import importlib.util
import sys
from pathlib import Path
from unittest import mock

import pytest


@pytest.mark.parametrize("upload_fails", [False, True])
def test_converged_run_requires_successful_uploads(tmp_path, monkeypatch, upload_fails):
    # Actor RPCs are outside this test; their generated clients only exist in
    # the runner image. Keep all workload and network operations mocked.
    actors = mock.Mock()
    spec = importlib.util.spec_from_file_location(
        "nighthawk_runner", Path(__file__).resolve().parents[1] / "runner.py"
    )
    runner = importlib.util.module_from_spec(spec)
    with mock.patch.dict(sys.modules, {"actors": actors}):
        spec.loader.exec_module(runner)
    monkeypatch.setattr(sys, "argv", [
        "runner.py", "--name", "test", "--tag", "test", "--dest", str(tmp_path / "results"),
        "--envoy-cpu", "1",
    ])
    # Place the otherwise timestamp-named work directory under this test.
    monkeypatch.setattr(runner, "Path", lambda _: tmp_path / "work")
    monkeypatch.setattr(runner.spec_mod, "build_spec_dict", lambda **_: {})
    monkeypatch.setattr(runner.spec_mod, "spec_dict_to_textproto", lambda *_: "spec")
    monkeypatch.setattr(runner, "start_service", lambda *_: mock.Mock())
    def adaptive(_spec, output, _logs):
        output.write_text("output")
        return 0
    monkeypatch.setattr(runner, "run_adaptive_client", adaptive)
    monkeypatch.setattr(runner.output_mod, "parse_output_textproto", lambda *_: {})
    monkeypatch.setattr(runner.output_mod, "stage_rows", lambda *_: [{"stage": "testing"}])
    monkeypatch.setattr(runner.output_mod, "stats_records", lambda *_: [])
    monkeypatch.setattr(runner.output_mod, "capacity_summary", lambda *_, **__: {})
    if upload_fails:
        monkeypatch.setattr(runner, "upload", mock.Mock(side_effect=OSError("storage unavailable")))
    with pytest.raises(SystemExit) as exc:
        runner.main()
    assert exc.value.code == (1 if upload_fails else 0)
