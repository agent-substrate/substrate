# Copyright 2026 Google LLC
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

"""Unit tests for benchmarking/monitoring.yaml and benchmarking/workloads/deploy.sh."""

from pathlib import Path
import re
import subprocess
import unittest

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
MONITORING_YAML = REPO_ROOT / "benchmarking" / "monitoring.yaml"
DEPLOY_SH = REPO_ROOT / "benchmarking" / "workloads" / "deploy.sh"


def load_monitoring_docs() -> list[dict]:
    """Loads all Kubernetes manifest documents from benchmarking/monitoring.yaml."""
    with open(MONITORING_YAML, encoding="utf-8") as f:
        return [d for d in yaml.safe_load_all(f) if d]


def load_prometheus_config() -> dict:
    """Extracts and parses the embedded prometheus.yml ConfigMap payload."""
    for doc in load_monitoring_docs():
        if (
            doc.get("kind") == "ConfigMap"
            and doc.get("metadata", {}).get("name") == "prometheus-config"
        ):
            return yaml.safe_load(doc["data"]["prometheus.yml"])
    raise AssertionError("prometheus-config ConfigMap not found in monitoring.yaml")


def apply_metric_relabel_configs(
    labels: dict[str, str], rules: list[dict]
) -> dict[str, str] | None:
    """Simulates Prometheus metric_relabel_configs evaluation (RE2 fullmatch)."""
    current = dict(labels)
    for rule in rules:
        action = rule.get("action", "replace")
        sep = rule.get("separator", ";")
        src = sep.join(current.get(k, "") for k in rule.get("source_labels", []))
        regex = rule.get("regex", "(.*)")
        m = re.fullmatch(regex, src)
        if action == "keep":
            if not m:
                return None
        elif action == "drop":
            if m:
                return None
        elif action == "replace":
            if m:
                replacement = rule.get("replacement", "$1")
                for idx in range(len(m.groups()), 0, -1):
                    replacement = replacement.replace(f"${idx}", m.group(idx) or "")
                current[rule["target_label"]] = replacement
    return current


class MonitoringConfigTest(unittest.TestCase):
    """Verifies Prometheus scrape jobs, deployment flags, and cAdvisor relabel rules."""

    def test_prometheus_deployment_enables_admin_api_and_retention(self):
        docs = load_monitoring_docs()
        deploy = next(
            d
            for d in docs
            if d.get("kind") == "Deployment"
            and d.get("metadata", {}).get("name") == "prometheus"
        )
        container = deploy["spec"]["template"]["spec"]["containers"][0]
        args = container["args"]
        self.assertIn("--web.enable-admin-api", args)
        self.assertIn("--storage.tsdb.retention.time=24h", args)
        self.assertEqual(container["resources"]["requests"]["memory"], "8Gi")
        self.assertEqual(container["resources"]["limits"]["memory"], "16Gi")

    def test_scrape_jobs_include_ate_system_and_apiservers(self):
        cfg = load_prometheus_config()
        jobs = {j["job_name"]: j for j in cfg["scrape_configs"]}
        self.assertIn("kubernetes-pods", jobs)
        self.assertIn("kubernetes-pods-8080", jobs)
        self.assertIn("kubernetes-apiservers", jobs)
        self.assertIn("kubernetes-cadvisor", jobs)

        pods_ns = jobs["kubernetes-pods"]["kubernetes_sd_configs"][0]["namespaces"]["names"]
        self.assertIn("ate-system", pods_ns)
        self.assertIn("benchmarking", pods_ns)

        pods_8080_rules = jobs["kubernetes-pods-8080"]["relabel_configs"]
        self.assertIsNotNone(
            apply_metric_relabel_configs(
                {
                    "__meta_kubernetes_pod_container_port_name": "metrics",
                    "__meta_kubernetes_pod_container_port_number": "8080",
                    "__address__": "10.0.0.5:8080",
                },
                pods_8080_rules,
            )
        )
        self.assertIsNone(
            apply_metric_relabel_configs(
                {
                    "__meta_kubernetes_pod_container_port_name": "metrics",
                    "__meta_kubernetes_pod_container_port_number": "9090",
                    "__address__": "10.0.0.6:9090",
                },
                pods_8080_rules,
            )
        )

        apiserver_rules = jobs["kubernetes-apiservers"]["metric_relabel_configs"]
        for kept in (
            "process_cpu_seconds_total",
            "process_resident_memory_bytes",
            "go_memstats_heap_inuse_bytes",
            "apiserver_request_total",
            "apiserver_request_duration_seconds_bucket",
            "apiserver_flowcontrol_current_executing_seats",
            "apiserver_storage_objects",
        ):
            self.assertIsNotNone(
                apply_metric_relabel_configs({"__name__": kept}, apiserver_rules),
                kept,
            )
        self.assertIsNone(
            apply_metric_relabel_configs(
                {"__name__": "etcd_debugging_mvcc_keys_total"}, apiserver_rules
            )
        )

    def test_cadvisor_relabel_drops_actor_subcgroups_and_preserves_oom_and_rollups(self):
        cfg = load_prometheus_config()
        cadvisor = next(
            j for j in cfg["scrape_configs"] if j["job_name"] == "kubernetes-cadvisor"
        )
        self.assertEqual(cadvisor["scrape_interval"], "60s")
        self.assertEqual(cadvisor["scrape_timeout"], "45s")
        rules = cadvisor["metric_relabel_configs"]

        # 1. Host root cgroup (id="/", container="") is relabeled to container="node" and kept.
        node_ws = apply_metric_relabel_configs(
            {"__name__": "container_memory_working_set_bytes", "id": "/", "container": ""},
            rules,
        )
        self.assertIsNotNone(node_ws)
        self.assertEqual(node_ws["container"], "node")

        # 2. Machine capacity series are kept.
        for machine_metric in ("machine_cpu_cores", "machine_memory_bytes"):
            self.assertIsNotNone(
                apply_metric_relabel_configs(
                    {"__name__": machine_metric, "id": "/", "container": ""}, rules
                )
            )

        # 3. Worker container series are kept.
        worker_id = (
            "/kubepods.slice/kubepods-burstable.slice/"
            "kubepods-burstable-pod123.slice/cri-containerd-worker.scope"
        )
        for m_name in (
            "container_memory_working_set_bytes",
            "container_cpu_usage_seconds_total",
            "container_oom_events_total",
        ):
            self.assertIsNotNone(
                apply_metric_relabel_configs(
                    {"__name__": m_name, "id": worker_id, "container": "worker"}, rules
                ),
                m_name,
            )

        # 4. Pod cgroup rollup keeps PSI and network, but drops empty-container memory/CPU/CFS duplicates.
        pod_id = "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice"
        self.assertIsNotNone(
            apply_metric_relabel_configs(
                {
                    "__name__": "container_pressure_cpu_waiting_seconds_total",
                    "id": pod_id,
                    "container": "",
                },
                rules,
            )
        )
        for dup_metric in (
            "container_memory_working_set_bytes",
            "container_memory_rss",
            "container_spec_memory_limit_bytes",
            "container_cpu_usage_seconds_total",
            "container_cpu_cfs_throttled_seconds_total",
        ):
            self.assertIsNone(
                apply_metric_relabel_configs(
                    {"__name__": dup_metric, "id": pod_id, "container": ""}, rules
                ),
                dup_metric,
            )

        # 5. Per-actor sandbox sub-cgroups drop resource metrics but preserve container_oom_events_total.
        for sub_id in (
            f"{worker_id}/sb-actor-001",
            f"{worker_id}/actor-001",
            f"{worker_id}/actor-001-_sandbox",
            f"{pod_id}/cri-containerd-actor001.scope/sandbox",
        ):
            for dropped in (
                "container_memory_working_set_bytes",
                "container_memory_rss",
                "container_cpu_usage_seconds_total",
                "container_pressure_memory_waiting_seconds_total",
                "container_fs_reads_bytes_total",
                "container_network_receive_bytes_total",
            ):
                self.assertIsNone(
                    apply_metric_relabel_configs(
                        {"__name__": dropped, "id": sub_id, "container": ""}, rules
                    ),
                    f"{dropped} on {sub_id}",
                )
            self.assertIsNotNone(
                apply_metric_relabel_configs(
                    {
                        "__name__": "container_oom_events_total",
                        "id": sub_id,
                        "container": "",
                    },
                    rules,
                ),
                f"container_oom_events_total on {sub_id}",
            )


class DeployShScriptTest(unittest.TestCase):
    """Verifies bash template substitution and timeout defaults in deploy.sh."""

    def test_substitute_renders_valid_workerpool_with_memory_and_special_chars(self):
        script = f"""
set -euo pipefail
source <(sed -n '/^cpu_millicores() {{/,/^}}/p; /^memory_bytes() {{/,/^}}/p; /^substitute() {{/,/^}}/p' "{DEPLOY_SH}")
BUCKET_NAME=test-bucket
WORKER_COUNT=4
SANDBOX_CLASS=microvm
OTLP_ENDPOINT="http://otel:4317?a=1&b=2|pipe"
ACTOR_MEMORY=256Mi
ACTOR_CPU=250m
ACTOR_CPU_MILLICORES="$(cpu_millicores "${{ACTOR_CPU}}")"
ACTOR_MEMORY_BYTES="$(memory_bytes "${{ACTOR_MEMORY}}")"
WORKER_MEMORY=12Gi
STORAGE_CLASS_NAME=""
substitute "{REPO_ROOT}/benchmarking/workloads/manifests/workloads.yaml.tmpl"
"""
        res = subprocess.run(
            ["bash", "-c", script],
            check=True,
            capture_output=True,
            text=True,
        )
        docs = [d for d in yaml.safe_load_all(res.stdout) if d]
        pool = next(d for d in docs if d.get("kind") == "WorkerPool")
        tmpl = pool["spec"]["template"]
        self.assertEqual(
            tmpl["resources"],
            {"requests": {"memory": "12Gi"}, "limits": {"memory": "12Gi"}},
        )

        # Also verify glt/template manifest substitution preserves '&' and '|' in OTLP_ENDPOINT.
        tmpl_script = f"""
set -euo pipefail
source <(sed -n '/^cpu_millicores() {{/,/^}}/p; /^memory_bytes() {{/,/^}}/p; /^substitute() {{/,/^}}/p' "{DEPLOY_SH}")
BUCKET_NAME=test-bucket
WORKER_COUNT=4
SANDBOX_CLASS=microvm
OTLP_ENDPOINT="http://otel:4317?a=1&b=2|pipe"
ACTOR_MEMORY=256Mi
ACTOR_CPU=250m
ACTOR_CPU_MILLICORES="$(cpu_millicores "${{ACTOR_CPU}}")"
ACTOR_MEMORY_BYTES="$(memory_bytes "${{ACTOR_MEMORY}}")"
WORKER_MEMORY=""
STORAGE_CLASS_NAME=""
substitute "{REPO_ROOT}/benchmarking/workloads/manifests/glutton-template.yaml.tmpl"
"""
        tmpl_res = subprocess.run(
            ["bash", "-c", tmpl_script],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("http://otel:4317?a=1&b=2|pipe", tmpl_res.stdout)

    def test_wait_timeout_defaults_by_cluster_size(self):
        script = f'source <(sed -n "/^if \\[\\[ -z \\"\\${{WAIT_TIMEOUT_SECS:-}}\\"/,/^fi$/p" "{DEPLOY_SH}"); echo "$WAIT_TIMEOUT_SECS"'
        default_res = subprocess.run(
            ["bash", "-c", script],
            env={"PATH": "/usr/bin:/bin"},
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertEqual(default_res.stdout.strip(), "300")

        for size in ("size10", "size100"):
            size_res = subprocess.run(
                ["bash", "-c", script],
                env={"PATH": "/usr/bin:/bin", "ATE_INSTALL_CLUSTER_SIZE": size},
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual(size_res.stdout.strip(), "900")


if __name__ == "__main__":
    unittest.main()
