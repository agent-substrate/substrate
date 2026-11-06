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

import os
import unittest
from unittest.mock import patch, MagicMock
import socket
import urllib.request
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import runner

class TestScrape(unittest.TestCase):
    @patch("socket.getaddrinfo")
    @patch("urllib.request.urlopen")
    def test_scrape_gateway_prometheus_multi_ip(self, mock_urlopen, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('10.0.0.1', 80)),
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, '', ('10.0.0.2', 80)),
        ]

        mock_resp1 = MagicMock()
        mock_resp1.read.return_value = b"metric1 100"
        mock_resp2 = MagicMock()
        mock_resp2.read.return_value = b"metric1 200"
        mock_urlopen.side_effect = [
            MagicMock(__enter__=lambda self: mock_resp1, __exit__=lambda *args: None),
            MagicMock(__enter__=lambda self: mock_resp2, __exit__=lambda *args: None),
        ]

        res = runner.scrape_gateway_prometheus("http://my-service/metrics")
        self.assertIn("# SCRAPED_IPS: 10.0.0.1,10.0.0.2", res)
        self.assertIn("metric1 100", res)
        self.assertIn("metric1 200", res)
        self.assertEqual(mock_urlopen.call_count, 2)
        # Check URLs used
        req1 = mock_urlopen.call_args_list[0][0][0]
        self.assertEqual(req1.full_url, "http://10.0.0.1:80/metrics")
        self.assertEqual(req1.get_header("Host"), "my-service")

    @patch("socket.getaddrinfo")
    def test_scrape_gateway_prometheus_oserror(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = OSError("dns error")
        with self.assertRaises(OSError):
            runner.scrape_gateway_prometheus("http://127.0.0.1:15090/stats")



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

        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            with patch("runner.Path", return_value=Path(tmp)), patch("os.replace", wraps=os.replace) as mock_replace:
                res = runner.main(["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "http", "--envoy-cpu", "2"])
                self.assertEqual(res, 1) # Because testing_stage_completed is False since rows is empty
                self.assertTrue(mock_upload.called)
                mock_replace.assert_called_once_with(Path(tmp) / ".status.json.tmp", Path(tmp) / "status.json")

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

        import tempfile
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

        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as tmp:
            with patch("runner.Path", return_value=Path(tmp)):
                # ensure output_path exists so it runs capacity_summary
                with open(Path(tmp) / "output.textproto", "w") as f:
                    f.write("test")
                with self.assertRaises(RuntimeError) as cm:
                    runner.main(["--name", "test", "--tag", "tag1", "--dest", "gs://b/p", "--mode", "http", "--envoy-cpu", "2"])
                self.assertIn("Gateway Prometheus counters decreased", str(cm.exception))
                self.assertTrue(mock_upload.called)


if __name__ == "__main__":
    unittest.main()
