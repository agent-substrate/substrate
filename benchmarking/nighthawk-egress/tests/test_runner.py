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

import json
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import runner
from tests.test_output import SAMPLE_OUTPUT, _golden, _stage


ARGS = ["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--envoy-cpu", "1"]
EXT_PROC = 'envoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"}'
IP1 = "# SCRAPED_IPS: 10.0.0.1\n"
IP2 = "# SCRAPED_IPS: 10.0.0.2\n"


def _run_main(work_dir: Path, mode: str, scrapes: list, output: dict | None, extra_args=()):
    """Runs runner.main with Nighthawk mocked out.

    Returns the exit code or the raised exception, the uploaded file names,
    and the spec_dict_to_textproto mock.
    """
    proc = MagicMock(stdout=["[adaptive] stage done\n"])
    proc.wait.return_value = 0
    if output is not None:
        (work_dir / "output.textproto").write_text("fake output textproto")
    with (
        patch("runner.Path", return_value=work_dir),
        patch("runner.scrape_gateway_prometheus", side_effect=scrapes),
        patch("runner.upload") as upload,
        patch("subprocess.Popen", return_value=proc),
        patch("socket.create_connection"),
        patch("runner.parse_output_textproto", return_value=output),
        patch("runner.spec_dict_to_textproto", return_value="fake textproto") as spec,
    ):
        try:
            got = runner.main([*ARGS, "--mode", mode, *extra_args])
        except Exception as exc:
            got = exc
    return got, {c.args[0].name for c in upload.call_args_list}, spec


class TestRunner(unittest.TestCase):
    def test_scrape_gateway_prometheus(self):
        v6 = (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("fd00::2", 15090, 0, 0))
        v4 = (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.1", 15090))
        svc = "atenet-egress-metrics.ate-system.svc"
        cases = [
            # label, getaddrinfo result, URL, scraped IPs, request URL, Host header
            ("IPv6 only", [v6, v6], f"http://{svc}:15090/stats/prometheus",
             "fd00::2", "http://[fd00::2]:15090/stats/prometheus", svc),
            ("IPv6 literal", [v6], "http://[fd00::2]:15090/stats/prometheus",
             "fd00::2", "http://[fd00::2]:15090/stats/prometheus", "[fd00::2]"),
            ("dual stack reads one family", [v6, v4, v4], f"http://{svc}:15090/stats/prometheus",
             "10.0.0.1", "http://10.0.0.1:15090/stats/prometheus", svc),
        ]
        resp = MagicMock()
        resp.read.return_value = b"metric1 100"
        for label, infos, url, want_ips, want_url, want_host in cases:
            with (
                self.subTest(label=label),
                patch("socket.getaddrinfo", return_value=infos),
                patch("urllib.request.urlopen") as urlopen,
            ):
                urlopen.return_value = MagicMock(__enter__=lambda self: resp, __exit__=lambda *args: None)
                got = runner.scrape_gateway_prometheus(url)
                self.assertEqual(got, f"# SCRAPED_IPS: {want_ips}\nmetric1 100")
                urlopen.assert_called_once()
                req = urlopen.call_args[0][0]
                self.assertEqual(req.full_url, want_url)
                self.assertEqual(req.get_header("Host"), want_host)

    def test_main_exit_code_and_artifacts(self):
        mitm = [IP1 + _golden("mitm_before"), IP1 + _golden("mitm_after")]
        failed = _stage(100, 5000, 4000, "0.100s", failed="success-rate")
        no_clean_stage = {
            "session_status": {"code": 0},
            "adjusting_stage_results": [failed],
            "testing_stage_result": failed,
        }
        lost_counters = {
            "gateway_counters": {},
            "tunnels_per_stage": None,
            "ext_proc_invariant_ok": None,
            "mode_exclusivity_ok": None,
            "slo_max_rps": 750.0,
            "testing_stage_completed": True,
        }
        cases = [
            # label, mode, scrapes, Nighthawk output, exit code or (exception, message), capacity.json fields
            ("no Nighthawk output", "http", [IP1 + "metric 1", IP1 + "metric 2"], None, 1, None),
            ("pod IP change", "mitm", [IP1 + f"{EXT_PROC} 100", IP2 + f"{EXT_PROC} 5"], SAMPLE_OUTPUT,
             (RuntimeError, "Pod IP set changed"), {**lost_counters, "gateway_pod_ips_changed": True}),
            ("post-run scrape failure", "mitm", [IP1 + f"{EXT_PROC} 100", OSError("post-run scrape failed")],
             SAMPLE_OUTPUT, (OSError, "post-run scrape failed"),
             {**lost_counters, "gateway_scrape_error": "post-run scrape failed"}),
            ("counter reset", "mitm", [IP1 + f"{EXT_PROC} 100", IP1 + f"{EXT_PROC} 5"], SAMPLE_OUTPUT,
             (RuntimeError, "counters decreased"), {"gateway_counter_reset": True}),
            ("testing stage without a clean stage", "mitm", mitm, no_clean_stage, 1,
             {"testing_stage_completed": True, "slo_max_rps": None}),
            ("gateway checks hold", "mitm", mitm, SAMPLE_OUTPUT, 0,
             {"mode_exclusivity_ok": True, "ext_proc_invariant_ok": True}),
            ("mode exclusivity broken", "http", mitm, SAMPLE_OUTPUT, 1,
             {"mode_exclusivity_ok": False, "ext_proc_invariant_ok": True}),
            ("ext_proc invariant broken", "mitm", [mitm[0], mitm[1] + f"\n{EXT_PROC} 1\n"], SAMPLE_OUTPUT, 1,
             {"mode_exclusivity_ok": True, "ext_proc_invariant_ok": False}),
        ]
        for label, mode, scrapes, output, want, want_cap in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as tmp:
                work_dir = Path(tmp)
                got, uploaded, _ = _run_main(work_dir, mode, scrapes, output)
                if isinstance(want, tuple):
                    self.assertIsInstance(got, want[0])
                    self.assertIn(want[1], str(got))
                else:
                    self.assertEqual(got, want)

                want_uploaded = {"logs.txt", "spec.textproto", "gateway_before.prom", "status.json"}
                if not isinstance(scrapes[1], Exception):
                    want_uploaded.add("gateway_after.prom")
                if output is not None:
                    want_uploaded |= {"output.textproto", "results.json", "stats.jsonl", "capacity.json"}
                self.assertEqual(uploaded, want_uploaded)
                self.assertFalse((work_dir / ".status.json.tmp").exists())
                if want_cap is None:
                    continue
                cap = json.loads((work_dir / "capacity.json").read_text())
                self.assertEqual({k: cap[k] for k in want_cap}, want_cap)
                status = json.loads((work_dir / "status.json").read_text())
                for key in ("testing_stage_completed", "slo_max_rps", "gateway_pod_ips_changed", "gateway_scrape_error"):
                    self.assertEqual(status[key], cap[key], key)

    def test_main_passthrough_servicedns_ca_reaches_spec(self):
        pem = "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"
        with tempfile.TemporaryDirectory() as tmp:
            ca = Path(tmp) / "ca.crt"
            ca.write_text(pem)
            _, _, spec = _run_main(
                Path(tmp), "passthrough", [IP1 + "metric 1", IP1 + "metric 2"], None,
                ["--servicedns-ca-cert", str(ca)],
            )
        tpl = spec.call_args[0][0]["nighthawk_traffic_template"]
        trusted_ca = tpl["transport_socket"]["typed_config"]["common_tls_context"]["validation_context"]["trusted_ca"]
        self.assertEqual(trusted_ca, {"inline_string": pem})


if __name__ == "__main__":
    unittest.main()
