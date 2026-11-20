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

"""Unit tests for benchmarking/nighthawk-egress/runner.py."""

import json
from pathlib import Path
import re
import socket
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import runner

ARGS = ["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--envoy-cpu", "1"]
DEST_RE = re.compile(r"gs://b/p/runs/test/run_date=\d{4}-\d{2}-\d{2}/run_ts=\d+/run_tag=tag1/(.+)")
CLIENT_LINE = "[adaptive] stage done\n"


def _stage(attempted_rps: float, achieved_rps: float, failed: str | None = None) -> dict:
    evals = [
        {"metric_id": "nighthawk.builtin/attempted-rps", "metric_value": attempted_rps},
        {"metric_id": "nighthawk.builtin/achieved-rps", "metric_value": achieved_rps},
    ]
    if failed:
        evals.append({"metric_id": f"nighthawk.builtin/{failed}", "metric_value": 1.0, "threshold_score": -1.0})
    # Nighthawk reports the request counter with every evaluation.
    result = {
        "name": "global",
        "execution_duration": "20s",
        "counters": [{"name": "upstream_rq_total", "value": str(int(achieved_rps * 20))}],
    }
    return {"metric_evaluations": evals, "nighthawk_service_output": {"results": [result]}}


SAMPLE_OUTPUT = {
    "session_status": {"code": 0},
    "adjusting_stage_results": [_stage(400.0, 399.0), _stage(800.0, 750.0), _stage(1600.0, 900.0, failed="send-rate")],
    "testing_stage_result": _stage(800.0, 760.0),
}


def _run_main(work_dir: Path, mode: str, *, output=None, client_exit=0, failed_uploads=(), wait_error=None, parse_error=None):
    """Runs runner.main with Nighthawk mocked out. The client writes its
    output file only when output is given. Returns the exit code and the mocks.
    sent holds the text of each file at its upload."""
    proc = MagicMock(stdout=[CLIENT_LINE])
    proc.wait.return_value = client_exit
    if output is not None:
        (work_dir / "output.textproto").write_text("fake output textproto")
    sent = {}

    def fake_upload(src, dest):
        sent[src.name] = src.read_text()
        if src.name in failed_uploads:
            raise OSError("bucket gone")

    with (
        patch("runner.Path", return_value=work_dir),
        patch("nighthawk_common.output.upload", side_effect=fake_upload) as upload,
        patch("subprocess.Popen", return_value=proc) as popen,
        patch("runner.wait_for_tcp", side_effect=wait_error) as wait,
        patch("nighthawk_common.output.parse_output_textproto", return_value=output, side_effect=parse_error),
        patch("runner.spec_dict_to_textproto", return_value="fake textproto") as spec,
    ):
        got = runner.main([*ARGS, "--mode", mode])
    return got, {"upload": upload, "popen": popen, "wait": wait, "spec": spec, "sent": sent}


class TestRunner(unittest.TestCase):
    def test_main_exit_code_and_artifacts(self):
        run_files = {"logs.txt", "spec.textproto", "status.json"}
        all_files = run_files | {"output.textproto", "results.json", "stats.jsonl", "capacity.json"}
        cases = [
            # label, _run_main arguments, want exit code, want failures in status.json, want uploaded files
            ("clean run", {"output": SAMPLE_OUTPUT}, 0, [], all_files),
            (
                "client fails",
                {"client_exit": 3},
                1,
                ["nighthawk_adaptive_load_client exited with code 3"],
                run_files,
            ),
            (
                "invoker never listens",
                {"wait_error": RuntimeError("Timed out waiting for 127.0.0.1:19980")},
                1,
                ["RuntimeError: Timed out waiting for 127.0.0.1:19980"],
                run_files,
            ),
            (
                "output does not parse",
                {"output": SAMPLE_OUTPUT, "parse_error": ValueError("bad textproto")},
                1,
                ["ValueError: bad textproto"],
                run_files | {"output.textproto"},
            ),
            (
                "output has no testing stage",
                {"output": {k: v for k, v in SAMPLE_OUTPUT.items() if k != "testing_stage_result"}},
                1,
                ["the output of nighthawk_adaptive_load_client has no testing stage"],
                all_files,
            ),
            (
                "one upload fails",
                {"output": SAMPLE_OUTPUT, "failed_uploads": {"results.json"}},
                1,
                ["upload of results.json failed: bucket gone"],
                all_files,
            ),
            # status.json is the last upload, so it cannot name its own failure.
            ("status.json upload fails", {"output": SAMPLE_OUTPUT, "failed_uploads": {"status.json"}}, 1, [], all_files),
        ]
        for label, kwargs, want, want_failures, want_uploaded in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                work_dir = Path(tmp)
                got, mocks = _run_main(work_dir, "mitm", **kwargs)
                self.assertEqual(got, want)

                uploaded = {}
                for call in mocks["upload"].call_args_list:
                    src, dest = call.args
                    uploaded[src.name] = DEST_RE.fullmatch(dest).group(1)
                self.assertEqual(set(uploaded), want_uploaded)
                self.assertEqual(set(uploaded.values()), want_uploaded)

                status = json.loads(mocks["sent"]["status.json"])
                self.assertEqual(status["failures"], want_failures)
                if "capacity.json" in want_uploaded:
                    cap = json.loads((work_dir / "capacity.json").read_text())
                    self.assertEqual(status["slo_max_rps"], cap["slo_max_rps"])
                else:
                    self.assertFalse((work_dir / "capacity.json").exists())
                    self.assertIsNone(status["slo_max_rps"])

    def test_main_runs_the_client_through_the_invoker(self):
        with tempfile.TemporaryDirectory() as tmp:
            work_dir = Path(tmp)
            _, mocks = _run_main(work_dir, "http", output=SAMPLE_OUTPUT, client_exit=0)
            logs = (work_dir / "logs.txt").read_text()
            status = json.loads((work_dir / "status.json").read_text())
        mocks["wait"].assert_called_once_with("127.0.0.1", 19980, timeout_s=30.0)
        self.assertEqual(
            mocks["popen"].call_args.args[0],
            [
                "nighthawk_adaptive_load_client",
                "--nighthawk-service-address",
                "127.0.0.1:19980",
                "--spec-file",
                str(work_dir / "spec.textproto"),
                "--output-file",
                str(work_dir / "output.textproto"),
            ],
        )
        self.assertEqual(logs, CLIENT_LINE)
        self.assertEqual(status["adaptive_client_exit_code"], 0)

    def test_capacity_json_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            _run_main(Path(tmp), "http", output=SAMPLE_OUTPUT)
            cap = json.loads((Path(tmp) / "capacity.json").read_text())
        want = {
            "mode": "http",
            "envoy_cpu": 1,
            "actors": 1,
            "client_concurrency": 16,
            "tail_latency_slo_ms": 25.0,
            "slo_max_rps": 750.0,
            "slo_max_stage": "adjusting_001",
            "binding_threshold": ["send-rate"],
            "converged": True,
        }
        self.assertEqual({k: cap.get(k) for k in want}, want)

    def test_main_passthrough_servicedns_ca(self):
        pem = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"
        cases = [
            # label, content of the CA file (None: no file), want failure
            ("bundle mounted", pem, None),
            ("no bundle", None, "ValueError: servicedns CA bundle not found at "),
            ("empty bundle", " \n", "ValueError: servicedns CA bundle at "),
        ]
        for label, content, want_failure in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                ca = Path(tmp) / "ca.crt"
                if content is not None:
                    ca.write_text(content)
                with patch("runner.SERVICEDNS_CA_CERT", str(ca)):
                    got, mocks = _run_main(Path(tmp), "passthrough", output=SAMPLE_OUTPUT)
                status = json.loads((Path(tmp) / "status.json").read_text())
                if want_failure:
                    self.assertEqual(got, 1)
                    self.assertTrue(status["failures"][0].startswith(want_failure), status["failures"])
                    mocks["popen"].assert_not_called()
                    continue
                self.assertEqual(got, 0)
                tpl = mocks["spec"].call_args[0][0]["nighthawk_traffic_template"]
                tls = tpl["transport_socket"]["typed_config"]["common_tls_context"]
                self.assertEqual(tls["validation_context"]["trusted_ca"], {"inline_string": pem})

    def test_wait_for_tcp_returns_when_the_port_listens(self):
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            runner.wait_for_tcp("127.0.0.1", server.getsockname()[1], timeout_s=30.0)

    def test_wait_for_tcp_raises_after_the_limit(self):
        clock = [0.0]

        def sleep(seconds):
            clock[0] += seconds

        with (
            patch("runner.socket.create_connection", side_effect=OSError("refused")),
            patch("runner.time.monotonic", side_effect=lambda: clock[0]),
            patch("runner.time.sleep", side_effect=sleep),
            self.assertRaisesRegex(RuntimeError, "Timed out waiting for 127.0.0.1:19980: refused"),
        ):
            runner.wait_for_tcp("127.0.0.1", 19980, timeout_s=30.0)
        self.assertGreaterEqual(clock[0], 30.0)


if __name__ == "__main__":
    unittest.main()
