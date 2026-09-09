// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package steps

import (
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
)

// The emitted cluster must reference its TLS material through SDS: an inline
// tls_certificates entry never picks up kubelet's certificate rotation.
func TestEmitAdditionalEgressExtprocCluster(t *testing.T) {
	out := emitAdditionalEgressExtprocCluster("foo.ate-system.svc.cluster.local", "50051", "foo.ate-system.svc")

	var clusters []map[string]any
	if err := yaml.Unmarshal([]byte(out), &clusters); err != nil {
		t.Fatalf("emitted cluster block is not valid YAML: %v\n%s", err, out)
	}
	if len(clusters) != 1 {
		t.Fatalf("expected 1 cluster, got %d", len(clusters))
	}
	if got := clusters[0]["name"]; got != additionalEgressExtprocCluster {
		t.Errorf("cluster name = %v, want %s", got, additionalEgressExtprocCluster)
	}

	for _, want := range []string{
		"tls_certificate_sds_secret_configs",
		"path: /etc/envoy/sds-podidentity-cert.yaml",
		"combined_validation_context",
		"path: /etc/envoy/sds-servicedns-validation.yaml",
		"exact: foo.ate-system.svc",
		"port_value: 50051",
		"address: foo.ate-system.svc.cluster.local",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("emitted cluster block is missing %q", want)
		}
	}
	if strings.Contains(out, "tls_certificates:") {
		t.Error("emitted cluster block still carries an inline tls_certificates entry")
	}
	// watched_directory belongs in the SDS resource files, not here: on an
	// inline entry Envoy silently ignores it.
	if strings.Contains(out, "watched_directory") {
		t.Error("emitted cluster block contains watched_directory")
	}
}

// Splices the real sdsmint manifest and re-parses the result. CI deploys the
// sdsmint variant but never with the extproc flag, so this is the only
// automated check on the injected cluster.
func TestPatchAtenetEgressManifest(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	env := &Env{Cfg: &config.Config{
		Root:                           root,
		ExperimentalUseSDSMint:         true,
		AdditionalEgressExtprocService: "ate-system/foo:50051",
	}}

	patched, err := env.patchAtenetEgressManifest()
	if err != nil {
		t.Fatalf("patchAtenetEgressManifest failed: %v", err)
	}
	// Prose that merely mentions a marker survives on purpose; only lines
	// that start with one are splice targets.
	for _, line := range strings.Split(string(patched), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#ATE_MITM_EXTPROC") {
			t.Errorf("patched manifest still contains an unreplaced marker line: %q", line)
		}
	}

	// Find the atenet-egress ConfigMap among the manifest's documents.
	var data map[string]string
	for _, doc := range strings.Split(string(patched), "\n---\n") {
		var obj struct {
			Kind string            `json:"kind"`
			Data map[string]string `json:"data"`
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("patched manifest document is not valid YAML: %v", err)
		}
		if obj.Kind == "ConfigMap" {
			data = obj.Data
			break
		}
	}
	if data == nil {
		t.Fatal("no ConfigMap found in the patched manifest")
	}

	// Every file Envoy will read from /etc/envoy must be present and parse.
	for _, key := range []string{
		"envoy.yaml",
		"sds-servicedns-cert.yaml",
		"sds-podidentity-cert.yaml",
		"sds-servicedns-validation.yaml",
	} {
		content, ok := data[key]
		if !ok {
			t.Errorf("ConfigMap is missing key %q", key)
			continue
		}
		var parsed map[string]any
		if err := yaml.Unmarshal([]byte(content), &parsed); err != nil {
			t.Errorf("ConfigMap key %q is not valid YAML: %v", key, err)
		}
	}

	// The spliced cluster must reference the SDS files the ConfigMap ships.
	envoyYaml := data["envoy.yaml"]
	for _, want := range []string{
		"path: /etc/envoy/sds-podidentity-cert.yaml",
		"path: /etc/envoy/sds-servicedns-validation.yaml",
		"cluster_name: " + additionalEgressExtprocCluster,
	} {
		if !strings.Contains(envoyYaml, want) {
			t.Errorf("patched envoy.yaml is missing %q", want)
		}
	}
	// watched_directory must live only in the SDS resource files; on an
	// inline entry in the bootstrap Envoy silently ignores it. Comment
	// lines may mention it.
	for _, line := range strings.Split(envoyYaml, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") && strings.Contains(line, "watched_directory") {
			t.Errorf("patched envoy.yaml contains watched_directory outside the SDS resource files: %q", line)
		}
	}
	for _, key := range []string{"sds-servicedns-cert.yaml", "sds-podidentity-cert.yaml", "sds-servicedns-validation.yaml"} {
		if !strings.Contains(data[key], "watched_directory") {
			t.Errorf("SDS resource %q is missing watched_directory; rotation would be silently broken", key)
		}
	}
}
