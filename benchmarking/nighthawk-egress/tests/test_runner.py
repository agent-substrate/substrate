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
import os
from pathlib import Path
import socket
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import runner
from tests.test_output import SAMPLE_OUTPUT


class TestScrape(unittest.TestCase):
    @patch("socket.getaddrinfo")
    @patch("urllib.request.urlopen")
    def test_scrape_gateway_prometheus_ipv6_only(self, mock_urlopen, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("fd00::2", 15090, 0, 0)),
            (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("fd00::2", 15090, 0, 0)),
        ]
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"metric1 100"
        mock_urlopen.return_value = MagicMock(__enter__=lambda self: mock_resp, __exit__=lambda *args: None)

        res = runner.scrape_gateway_prometheus(
            "http://atenet-egress-metrics.ate-system.svc:15090/stats/prometheus",
            use_port_forward=False,
        )
        mock_getaddrinfo.assert_called_once_with(
            "atenet-egress-metrics.ate-system.svc",
            15090,
            type=socket.SOCK_STREAM,
        )
        self.assertEqual(res, "# SCRAPED_IPS: fd00::2\nmetric1 100")
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.full_url, "http://[fd00::2]:15090/stats/prometheus")
        self.assertEqual(req.get_header("Host"), "atenet-egress-metrics.ate-system.svc")

        mock_urlopen.reset_mock()
        runner.scrape_gateway_prometheus("http://[fd00::2]:15090/stats/prometheus", use_port_forward=False)
        req_lit = mock_urlopen.call_args[0][0]
        self.assertEqual(req_lit.get_header("Host"), "[fd00::2]")

    @patch("socket.getaddrinfo")
    @patch("urllib.request.urlopen")
    def test_scrape_gateway_prometheus_dual_stack_picks_single_family(self, mock_urlopen, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("fd00::2", 15090, 0, 0)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.1", 15090)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("10.0.0.1", 15090)),
        ]
        mock_resp = MagicMock()
        mock_resp.read.return_value = b"metric1 100"
        mock_urlopen.return_value = MagicMock(__enter__=lambda self: mock_resp, __exit__=lambda *args: None)

        res = runner.scrape_gateway_prometheus(
            "http://localhost:15090/stats/prometheus",
            use_port_forward=False,
        )
        self.assertEqual(res, "# SCRAPED_IPS: 10.0.0.1\nmetric1 100")
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.full_url, "http://10.0.0.1:15090/stats/prometheus")

    @patch("socket.getaddrinfo")
    def test_scrape_gateway_prometheus_no_endpoints(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = []
        with self.assertRaisesRegex(RuntimeError, "no TCP endpoints resolved"):
            runner.scrape_gateway_prometheus("http://127.0.0.1:15090/stats", use_port_forward=False)

    @patch("socket.getaddrinfo")
    def test_scrape_gateway_prometheus_oserror(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = OSError("dns error")
        with self.assertRaises(OSError):
            runner.scrape_gateway_prometheus("http://127.0.0.1:15090/stats", use_port_forward=False)

    @patch("runner.scrape_gateway_prometheus")
    @patch("runner.upload")
    @patch("subprocess.Popen")
    @patch("socket.create_connection")
    @patch("runner.parse_output_textproto")
    @patch("runner.spec_dict_to_textproto")
    def test_main_e2e(self, mock_spec_dict, mock_parse_out, mock_sock, mock_popen, mock_upload, mock_scrape):
        mock_scrape.side_effect = ["# SCRAPED_IPS: 10.0.0.1\nmetric 1", "# SCRAPED_IPS: 10.0.0.1\nmetric 2"]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdout = []
        mock_popen.return_value = mock_proc

        mock_parse_out.return_value = {"results": []}
        mock_spec_dict.return_value = "fake textproto"

        with tempfile.TemporaryDirectory() as tmp:
            with patch("runner.Path", return_value=Path(tmp)), patch("os.replace", wraps=os.replace) as mock_replace:
                res = runner.main(["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "http", "--envoy-cpu", "2"])
                # output.textproto does not exist in tmp, so runner skips parsing and testing_stage_completed stays False.
                self.assertEqual(res, 1)
                self.assertTrue(mock_upload.called)
                mock_replace.assert_called_once_with(Path(tmp) / ".status.json.tmp", Path(tmp) / "status.json")

    @patch("runner.scrape_gateway_prometheus")
    @patch("runner.upload")
    @patch("subprocess.Popen")
    @patch("socket.create_connection")
    @patch("runner.parse_output_textproto")
    @patch("runner.spec_dict_to_textproto")
    def test_main_pod_ip_change_writes_nighthawk_artifacts(
        self, mock_spec_dict, mock_parse_out, mock_sock, mock_popen, mock_upload, mock_scrape
    ):
        mock_scrape.side_effect = [
            '# SCRAPED_IPS: 10.0.0.1\nenvoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"} 100',
            '# SCRAPED_IPS: 10.0.0.2\nenvoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"} 5',
        ]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdout = []
        mock_popen.return_value = mock_proc
        mock_parse_out.return_value = SAMPLE_OUTPUT
        mock_spec_dict.return_value = "fake textproto"

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "output.textproto").write_text("fake output textproto")
            with patch("runner.Path", return_value=tmp_path):
                with self.assertRaisesRegex(RuntimeError, "Pod IP set changed"):
                    runner.main(
                        ["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "mitm", "--envoy-cpu", "2"]
                    )

            self.assertTrue((tmp_path / "results.json").is_file())
            self.assertTrue((tmp_path / "stats.jsonl").is_file())
            self.assertEqual(len((tmp_path / "stats.jsonl").read_text().strip().splitlines()), 4)

            cap = json.loads((tmp_path / "capacity.json").read_text())
            self.assertTrue(cap["gateway_pod_ips_changed"])
            self.assertIsNone(cap["gateway_scrape_error"])
            self.assertEqual(cap["gateway_counters"], {})
            self.assertIsNone(cap["tunnels_per_stage"])
            self.assertIsNone(cap["ext_proc_invariant_ok"])
            self.assertIsNone(cap["mode_exclusivity_ok"])
            self.assertIsNone(cap["forward_proxy_overflow_ok"])
            self.assertFalse(cap["gateway_counter_reset"])
            self.assertAlmostEqual(cap["slo_max_rps"], 750.0)
            self.assertTrue(cap["testing_stage_completed"])

            status = json.loads((tmp_path / "status.json").read_text())
            self.assertTrue(status["gateway_pod_ips_changed"])
            self.assertIsNone(status["gateway_scrape_error"])
            self.assertTrue(status["testing_stage_completed"])
            self.assertAlmostEqual(status["slo_max_rps"], 750.0)

            uploaded_names = {c.args[0].name for c in mock_upload.call_args_list}
            self.assertIn("results.json", uploaded_names)
            self.assertIn("stats.jsonl", uploaded_names)
            self.assertIn("capacity.json", uploaded_names)
            self.assertIn("status.json", uploaded_names)

    @patch("runner.scrape_gateway_prometheus")
    @patch("runner.upload")
    @patch("subprocess.Popen")
    @patch("socket.create_connection")
    @patch("runner.parse_output_textproto")
    @patch("runner.spec_dict_to_textproto")
    def test_main_post_run_scrape_failure_writes_nighthawk_artifacts(
        self, mock_spec_dict, mock_parse_out, mock_sock, mock_popen, mock_upload, mock_scrape
    ):
        mock_scrape.side_effect = [
            '# SCRAPED_IPS: 10.0.0.1\nenvoy_cluster_upstream_rq_total{envoy_cluster_name="ext_proc_server"} 100',
            OSError("post-run scrape failed"),
        ]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdout = ["[adaptive] stage done\n"]
        mock_popen.return_value = mock_proc
        mock_parse_out.return_value = SAMPLE_OUTPUT
        mock_spec_dict.return_value = "fake textproto"

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            (tmp_path / "output.textproto").write_text("fake output textproto")
            with patch("runner.Path", return_value=tmp_path):
                with self.assertRaisesRegex(OSError, "post-run scrape failed"):
                    runner.main(
                        ["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "mitm", "--envoy-cpu", "2"]
                    )

            self.assertTrue((tmp_path / "results.json").is_file())
            self.assertTrue((tmp_path / "stats.jsonl").is_file())
            self.assertTrue((tmp_path / "logs.txt").is_file())
            self.assertFalse((tmp_path / "gateway_after.prom").exists())

            cap = json.loads((tmp_path / "capacity.json").read_text())
            self.assertFalse(cap["gateway_pod_ips_changed"])
            self.assertEqual(cap["gateway_scrape_error"], "post-run scrape failed")
            self.assertEqual(cap["gateway_counters"], {})
            self.assertIsNone(cap["tunnels_per_stage"])
            self.assertIsNone(cap["ext_proc_invariant_ok"])
            self.assertIsNone(cap["mode_exclusivity_ok"])
            self.assertIsNone(cap["forward_proxy_overflow_ok"])
            self.assertAlmostEqual(cap["slo_max_rps"], 750.0)
            self.assertTrue(cap["testing_stage_completed"])

            status = json.loads((tmp_path / "status.json").read_text())
            self.assertFalse(status["gateway_pod_ips_changed"])
            self.assertEqual(status["gateway_scrape_error"], "post-run scrape failed")
            self.assertTrue(status["testing_stage_completed"])
            self.assertAlmostEqual(status["slo_max_rps"], 750.0)

            uploaded_names = {c.args[0].name for c in mock_upload.call_args_list}
            self.assertIn("logs.txt", uploaded_names)
            self.assertIn("results.json", uploaded_names)
            self.assertIn("stats.jsonl", uploaded_names)
            self.assertIn("capacity.json", uploaded_names)
            self.assertIn("gateway_before.prom", uploaded_names)
            self.assertIn("status.json", uploaded_names)
            self.assertNotIn("gateway_after.prom", uploaded_names)

    @patch("runner.scrape_gateway_prometheus")
    @patch("runner.upload")
    @patch("subprocess.Popen")
    @patch("socket.create_connection")
    @patch("runner.parse_output_textproto")
    @patch("runner.spec_dict_to_textproto")
    def test_main_passthrough_servicedns_ca_cert(self, mock_spec_dict, mock_parse_out, mock_sock, mock_popen, mock_upload, mock_scrape):
        mock_scrape.side_effect = ["# SCRAPED_IPS: 10.0.0.1\nmetric 1", "# SCRAPED_IPS: 10.0.0.1\nmetric 2"]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdout = []
        mock_popen.return_value = mock_proc
        mock_parse_out.return_value = {"results": []}
        mock_spec_dict.return_value = "fake textproto"

        with tempfile.TemporaryDirectory() as tmp:
            missing_ca = str(Path(tmp) / "missing-ca.crt")
            with patch("runner.Path", return_value=Path(tmp)):
                with self.assertRaisesRegex(ValueError, "servicedns CA"):
                    runner.main(["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "passthrough", "--envoy-cpu", "1", "--servicedns-ca-cert", missing_ca])

            empty_ca = Path(tmp) / "empty-ca.crt"
            empty_ca.write_text("   \n")
            with patch("runner.Path", return_value=Path(tmp)):
                with self.assertRaisesRegex(ValueError, "servicedns CA"):
                    runner.main(["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "passthrough", "--envoy-cpu", "1", "--servicedns-ca-cert", str(empty_ca)])

            valid_ca = Path(tmp) / "valid-ca.crt"
            valid_ca.write_text("-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n")
            with patch("runner.Path", return_value=Path(tmp)):
                runner.main(["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "passthrough", "--envoy-cpu", "1", "--servicedns-ca-cert", str(valid_ca)])
                spec_passed = mock_spec_dict.call_args[0][0]
                trusted_ca = (
                    spec_passed["nighthawk_traffic_template"]["transport_socket"]
                    ["typed_config"]["common_tls_context"]["validation_context"]["trusted_ca"]
                )
                self.assertEqual(trusted_ca, {"inline_string": valid_ca.read_text()})

    @patch("runner.capacity_summary")
    @patch("runner.scrape_gateway_prometheus")
    @patch("runner.upload")
    @patch("subprocess.Popen")
    @patch("socket.create_connection")
    @patch("runner.parse_output_textproto")
    @patch("runner.spec_dict_to_textproto")
    def test_main_counter_reset(self, mock_spec_dict, mock_parse_out, mock_sock, mock_popen, mock_upload, mock_scrape, mock_cap_sum):
        mock_scrape.side_effect = ["# SCRAPED_IPS: 10.0.0.1\nmetric 1", "# SCRAPED_IPS: 10.0.0.1\nmetric 2"]
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        mock_proc.stdout = []
        mock_popen.return_value = mock_proc
        mock_parse_out.return_value = {"results": []}
        mock_spec_dict.return_value = "fake textproto"
        mock_cap_sum.return_value = {"gateway_counter_reset": True}

        with tempfile.TemporaryDirectory() as tmp:
            with patch("runner.Path", return_value=Path(tmp)):
                with open(Path(tmp) / "output.textproto", "w") as f:
                    f.write("test")
                with self.assertRaises(RuntimeError) as cm:
                    runner.main(["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "http", "--envoy-cpu", "2"])
                self.assertIn("Gateway Prometheus counters decreased", str(cm.exception))
                self.assertTrue(mock_upload.called)


if __name__ == "__main__":
    unittest.main()
