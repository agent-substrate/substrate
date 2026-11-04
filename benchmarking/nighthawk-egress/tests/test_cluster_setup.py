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
import unittest
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import cluster_setup

class TestClusterSetup(unittest.TestCase):
    @patch("cluster_setup.kubectl_json")
    @patch("subprocess.run")
    def test_shape_atenet_egress(self, mock_run, mock_k_json):
        mock_k_json.return_value = {
            "spec": {
                "replicas": 1,
                "template": {"spec": {"containers": [
                    {
                        "name": "envoy",
                        "args": ["--concurrency", "2", "other"],
                        "resources": {"requests": {"cpu": "2000m"}, "limits": {"cpu": "2000m"}}
                    },
                    {
                        "name": "ext-proc",
                        "resources": {"requests": {"cpu": "2000m"}, "limits": {"cpu": "2000m"}}
                    }
                ]}}
            }
        }
        cluster_setup.shape_atenet_egress(4)
        cmds = [c.args[0] for c in mock_run.call_args_list]
        self.assertEqual(cmds[0][:2], ["kubectl", "patch"])
        patch_body = json.loads(cmds[0][cmds[0].index("-p") + 1])
        envoy = next(c for c in patch_body["spec"]["template"]["spec"]["containers"] if c["name"] == "envoy")
        self.assertEqual(envoy["args"], ["other", "--concurrency", "4"])
        self.assertEqual(cmds[1][:3], ["kubectl", "rollout", "status"])

    @patch("cluster_setup.kubectl_json")
    @patch("subprocess.run")
    def test_shape_atenet_egress_matching_still_waits_for_rollout(self, mock_run, mock_k_json):
        mock_k_json.return_value = {
            "spec": {
                "replicas": 1,
                "template": {"spec": {"containers": [
                    {
                        "name": "envoy",
                        "args": ["--concurrency", "4"],
                        "resources": {"requests": {"cpu": "4000m"}, "limits": {"cpu": "4000m"}}
                    },
                    {
                        "name": "ext-proc",
                        "resources": {"requests": {"cpu": "4000m"}, "limits": {"cpu": "4000m"}}
                    }
                ]}}
            }
        }
        cluster_setup.shape_atenet_egress(4)
        cmds = [c.args[0] for c in mock_run.call_args_list]
        self.assertEqual([c[:3] for c in cmds], [["kubectl", "rollout", "status"]])

    @patch("cluster_setup.kubectl_json")
    @patch("subprocess.run")
    def test_authorize_nighthawk_spiffe(self, mock_run, mock_k_json):
        mock_k_json.return_value = {
            "spec": {"template": {"spec": {"containers": [{"name": "atenet-router", "args": ["--ingress-allowed-spiffe-ids=foo"]}]}}}
        }
        cluster_setup.authorize_nighthawk_spiffe()
        mock_run.assert_called()


    @patch("cluster_setup.shape_atenet_egress")
    def test_main_shape(self, mock_shape):
        with patch("sys.argv", ["cluster_setup.py", "shape", "--envoy-cpu", "4"]):
            cluster_setup.main()
        mock_shape.assert_called_once_with(4)


    @patch("cluster_setup.run_orchestration")
    def test_run_dev_skip_deploy(self, mock_run):
        with patch("sys.argv", ["cluster_setup.py", "run-dev", "--name", "n", "--job-name", "j", "--image", "i", "--tag", "t", "--dest", "d", "--mode", "http", "--envoy-cpu", "2", "--tail-slo-ms", "25", "--measuring-period", "20s", "--testing-duration", "60s", "--convergence-deadline", "600s", "--connections", "16", "--max-pending", "32", "--skip-deploy"]):
            cluster_setup.main()
        # skip_deploy is the last param. Check it is True.
        mock_run.assert_called_once()
        self.assertTrue(mock_run.call_args.kwargs["skip_deploy"])

    @patch("cluster_setup.kubectl_json")
    @patch("cluster_setup.shape_atenet_egress")
    @patch("cluster_setup.authorize_nighthawk_spiffe")
    @patch("subprocess.run")
    @patch("builtins.open", create=True)
    @patch("time.monotonic", side_effect=[0, 10])
    @patch("time.sleep", return_value=None)
    def test_run_orchestration_timeout_and_delete(self, mock_sleep, mock_time, mock_open, mock_run, mock_auth, mock_shape, mock_k_json):
        mock_file = MagicMock()
        mock_file.read.return_value = "timeout=$CONVERGENCE_DEADLINE"
        mock_open.return_value.__enter__.return_value = mock_file

        # mock job status returning nothing (timeout)
        mock_k_json.return_value = {"status": {}}


        mock_time.side_effect = [0, 10000]
        with self.assertRaises(SystemExit):
            cluster_setup.run_orchestration("n", "j", "i", "t", "d", "http", "2", "25", "20s", "60s", "600s", "16", "32", True)

        # ensure delete job was called EXACTLY ONCE (before apply), not twice
        delete_calls = [call for call in mock_run.call_args_list if call[0][0][:3] == ["kubectl", "delete", "job"]]
        self.assertEqual(len(delete_calls), 1)

    @patch("cluster_setup.kubectl_json")
    @patch("cluster_setup.shape_atenet_egress")
    @patch("cluster_setup.authorize_nighthawk_spiffe")
    @patch("subprocess.run")
    @patch("builtins.open", create=True)
    @patch("time.monotonic", side_effect=[0, 1, 2, 3])
    @patch("time.sleep", return_value=None)
    def test_run_orchestration_failed(self, mock_sleep, mock_time, mock_open, mock_run, mock_auth, mock_shape, mock_k_json):
        mock_file = MagicMock()
        mock_file.read.return_value = "timeout=$CONVERGENCE_DEADLINE"
        mock_open.return_value.__enter__.return_value = mock_file

        # mock job status returning failed
        mock_k_json.return_value = {"status": {"failed": 1}}

        with self.assertRaises(SystemExit):
            cluster_setup.run_orchestration("n", "j", "i", "t", "d", "http", "2", "25", "20s", "60s", "600s", "16", "32", True)

        # ensure delete job was called TWICE (before apply, and after because res="failed")
        delete_calls = [call for call in mock_run.call_args_list if call[0][0][:3] == ["kubectl", "delete", "job"]]
        self.assertEqual(len(delete_calls), 2)


    def test_main_bad_args(self):
        with patch("sys.argv", ["cluster_setup.py", "badcmd"]):
            with self.assertRaises(SystemExit):
                cluster_setup.main()

    def test_egress_policy_exact_hostnames(self):
        policy_path = Path(__file__).resolve().parent.parent / "egress-policy.yaml"
        text = policy_path.read_text()
        self.assertNotIn('"*"', text)
        self.assertEqual(text.count('"nighthawk-server.nighthawk.svc"'), 3)
        self.assertEqual(text.count('"nighthawk-server.nighthawk.svc.cluster.local"'), 3)

if __name__ == "__main__":
    unittest.main()
