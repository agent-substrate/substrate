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
	"context"
	"fmt"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kyaml "sigs.k8s.io/kustomize/kyaml/yaml"
	"sigs.k8s.io/yaml"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// Splicing the provider selection into the real egress manifest replaces the
// marker: with the provider flags for a selected provider, with nothing when
// injection is off. CI deploys the bundled provider on envoy, but only this
// pins the spliced flags themselves and the off shape.
func TestPatchAtenetEgressInject(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	env := &Env{Cfg: &config.Config{Root: root}}
	raw, err := os.ReadFile(env.atenetEgressManifestPath())
	if err != nil {
		t.Fatalf("reading egress manifest: %v", err)
	}

	for _, tc := range []struct {
		name     string
		provider config.CredentialProvider
		want     []string
	}{
		{
			name:     "kubernetes",
			provider: config.CredentialProvider{Name: config.K8sCredentialProviderName, Address: config.K8sCredentialProviderAddress},
			want: []string{
				"--credential-provider-name=k8s.io",
				"--credential-provider-address=k8s-credential-provider.ate-system.svc:50051",
				"--credential-provider-server-name=k8s-credential-provider.ate-system.svc",
				"--credential-provider-ca-file=",
				"--credential-provider-client-cert=",
			},
		},
		{
			name:     "another provider",
			provider: config.CredentialProvider{Name: "vault.example.com", Address: "vault.ate-system.svc:8200"},
			want: []string{
				"--credential-provider-name=vault.example.com",
				"--credential-provider-address=vault.ate-system.svc:8200",
				"--credential-provider-server-name=vault.ate-system.svc",
			},
		},
		{name: "off"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patched, err := env.patchAtenetEgressInject(raw, tc.provider)
			if err != nil {
				t.Fatalf("patchAtenetEgressInject failed: %v", err)
			}
			for _, line := range strings.Split(string(patched), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "#ATE_EGRESS_INJECT_FLAGS") {
					t.Errorf("patched manifest still contains an unreplaced marker: %q", line)
				}
			}
			for _, want := range tc.want {
				if !strings.Contains(string(patched), want) {
					t.Errorf("patched manifest is missing spliced flag %q", want)
				}
			}
			if !tc.provider.Enabled() && strings.Contains(string(patched), "- --credential-provider-") {
				t.Error("patched manifest carries provider flags with injection off")
			}
			assertEgressManifestParses(t, patched)
		})
	}

	// The marker is what the splice keys on; a manifest without it is a
	// broken install, not a silent no-op.
	if _, err := env.patchAtenetEgressInject([]byte("kind: ConfigMap\n"), config.CredentialProvider{}); err == nil {
		t.Error("patchAtenetEgressInject accepted a manifest without the marker")
	}
}

func TestPatchAtenetEgressStatsFlushOnAdmin(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	env := &Env{Cfg: &config.Config{Root: root}}
	raw, err := os.ReadFile(env.atenetEgressManifestPath())
	if err != nil {
		t.Fatalf("reading egress manifest: %v", err)
	}

	for _, tc := range []struct {
		name       string
		e2eTesting bool
		want       bool
	}{
		{name: "enabled", e2eTesting: true, want: true},
		{name: "disabled", e2eTesting: false, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := &Env{Cfg: &config.Config{Root: root, E2ETesting: tc.e2eTesting}}
			patched, err := env.patchAtenetEgressStatsFlushOnAdmin(raw)
			if err != nil {
				t.Fatalf("patchAtenetEgressStatsFlushOnAdmin failed: %v", err)
			}
			for _, line := range strings.Split(string(patched), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "#ATE_EGRESS_STATS_FLUSH_ON_ADMIN") {
					t.Errorf("patched manifest still contains an unreplaced marker: %q", line)
				}
			}
			got := strings.Contains(string(patched), "stats_flush_on_admin: true")
			if got != tc.want {
				t.Errorf("patched manifest contains stats_flush_on_admin: true = %v, want %v", got, tc.want)
			}
			assertEgressManifestParses(t, patched)
		})
	}

	if _, err := env.patchAtenetEgressStatsFlushOnAdmin([]byte("kind: ConfigMap\n")); err == nil {
		t.Error("patchAtenetEgressStatsFlushOnAdmin accepted a manifest without the marker")
	}
}

// assertEgressManifestParses checks that every document is still YAML and
// that the atenet-egress ConfigMap's envoy.yaml re-parses.
func assertEgressManifestParses(t *testing.T, manifest []byte) {
	t.Helper()
	for _, doc := range strings.Split(string(manifest), "\n---\n") {
		var obj struct {
			Kind string            `json:"kind"`
			Data map[string]string `json:"data"`
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("patched manifest document is not valid YAML: %v", err)
		}
		if obj.Kind == "ConfigMap" {
			var parsed map[string]any
			if err := yaml.Unmarshal([]byte(obj.Data["envoy.yaml"]), &parsed); err != nil {
				t.Errorf("patched envoy.yaml is not valid YAML: %v", err)
			}
			return
		}
	}
}

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

// Splices the real egress manifest and re-parses the result. CI never deploys
// with the extproc flag, so this is the only automated check on the injected
// cluster.
func TestPatchAtenetEgressManifest(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatalf("resolving repo root: %v", err)
	}
	env := &Env{Cfg: &config.Config{
		Root:                           root,
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

	// The additional processor is spliced in below the policy ext_proc on
	// both HTTP chains, so it sees only requests the policy allowed.
	spliced := 0
	for _, filters := range httpFilterChains(t, envoyYaml) {
		policyAt, additionalAt := -1, -1
		for i, f := range filters {
			switch extProcCluster(f) {
			case "ext_proc_server":
				policyAt = i
			case additionalEgressExtprocCluster:
				additionalAt = i
			}
		}
		if additionalAt < 0 {
			continue
		}
		spliced++
		if policyAt < 0 || policyAt > additionalAt {
			t.Errorf("additional ext_proc at http_filters[%d] runs before the policy ext_proc at [%d]", additionalAt, policyAt)
		}
	}
	if spliced != 2 {
		t.Errorf("additional ext_proc spliced into %d HTTP chains, want 2", spliced)
	}
}

// httpFilterChains returns the http_filters of every HTTP connection manager
// in the bootstrap's static listeners.
func httpFilterChains(t *testing.T, envoyYaml string) [][]map[string]any {
	t.Helper()
	var bootstrap struct {
		StaticResources struct {
			Listeners []struct {
				FilterChains []struct {
					Filters []struct {
						Name        string `json:"name"`
						TypedConfig struct {
							HTTPFilters []map[string]any `json:"http_filters"`
						} `json:"typed_config"`
					} `json:"filters"`
				} `json:"filter_chains"`
			} `json:"listeners"`
		} `json:"static_resources"`
	}
	if err := yaml.Unmarshal([]byte(envoyYaml), &bootstrap); err != nil {
		t.Fatalf("patched envoy.yaml does not parse as a bootstrap: %v", err)
	}
	var chains [][]map[string]any
	for _, l := range bootstrap.StaticResources.Listeners {
		for _, fc := range l.FilterChains {
			for _, f := range fc.Filters {
				if f.Name == "envoy.filters.network.http_connection_manager" {
					chains = append(chains, f.TypedConfig.HTTPFilters)
				}
			}
		}
	}
	return chains
}

// extProcCluster returns the cluster an ext_proc filter dials, or "" for any
// other filter.
func extProcCluster(filter map[string]any) string {
	if filter["name"] == "envoy.filters.http.composite" {
		typedConfig, _ := filter["typed_config"].(map[string]any)
		matcher, _ := typedConfig["matcher"].(map[string]any)
		matcherList, _ := matcher["matcher_list"].(map[string]any)
		matchers, _ := matcherList["matchers"].([]any)
		if len(matchers) > 0 {
			m, _ := matchers[0].(map[string]any)
			onMatch, _ := m["on_match"].(map[string]any)
			action, _ := onMatch["action"].(map[string]any)
			actionConfig, _ := action["typed_config"].(map[string]any)
			inner, _ := actionConfig["typed_config"].(map[string]any)
			filter = inner
		}
	}
	if filter["name"] != "envoy.filters.http.ext_proc" {
		return ""
	}
	typedConfig, _ := filter["typed_config"].(map[string]any)
	grpcService, _ := typedConfig["grpc_service"].(map[string]any)
	envoyGRPC, _ := grpcService["envoy_grpc"].(map[string]any)
	name, _ := envoyGRPC["cluster_name"].(string)
	return name
}

// The plain GKE install renders the base kustomization, not the raw
// directory: the directory would re-apply pod-certificate-controller.yaml and
// undo the size10 flags and the WORKERS_PER_SIGNER value.
func TestSystemOverlayDefaultIsBase(t *testing.T) {
	if got := SystemOverlay(&config.Config{Router: config.RouterEnvoy}); got != installDir+"/base" {
		t.Errorf("SystemOverlay(envoy, GKE) = %q, want %s/base", got, installDir)
	}
}

// pinnedWorkloads returns the Deployment and StatefulSet names in a rendered
// manifest that carry the cordon-control-plane node pinning, mapped to the
// pool each one selects, and every workload name seen. Only the shared pool
// spreads its replicas, and no pool requires anti-affinity: that would need a
// node per pod.
func pinnedWorkloads(t *testing.T, manifest []byte) (pinned map[string]string, all []string) {
	t.Helper()
	pinned = map[string]string{}
	for _, doc := range strings.Split(string(manifest), "\n---\n") {
		var obj struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Spec struct {
				Template struct {
					Metadata struct {
						Labels map[string]string `json:"labels"`
					} `json:"metadata"`
					Spec struct {
						NodeSelector              map[string]string `json:"nodeSelector"`
						Tolerations               []map[string]any  `json:"tolerations"`
						Affinity                  map[string]any    `json:"affinity"`
						TopologySpreadConstraints []map[string]any  `json:"topologySpreadConstraints"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("rendered document is not valid YAML: %v", err)
		}
		if obj.Kind != "Deployment" && obj.Kind != "StatefulSet" {
			continue
		}
		all = append(all, obj.Metadata.Name)
		podSpec := obj.Spec.Template.Spec
		pool := podSpec.NodeSelector["ate.dev/workloadType"]
		if pool == "" {
			continue
		}
		if !slices.ContainsFunc(podSpec.Tolerations, func(tol map[string]any) bool {
			return tol["key"] == "ate.dev/workloadType" && tol["value"] == pool
		}) {
			t.Errorf("workload %s selects pool %s but does not tolerate its taint", obj.Metadata.Name, pool)
		}
		if podSpec.Affinity["podAntiAffinity"] != nil {
			t.Errorf("workload %s has pod anti-affinity", obj.Metadata.Name)
		}
		if spread := len(podSpec.TopologySpreadConstraints) > 0; spread != (pool == "ate-control-plane") {
			t.Errorf("workload %s in pool %s: topology spread = %v", obj.Metadata.Name, pool, spread)
		}
		// The spread narrows on the app label; without it the workload would
		// spread against the whole pool instead of its own replicas.
		if pool == "ate-control-plane" && obj.Spec.Template.Metadata.Labels["app"] == "" {
			t.Errorf("workload %s has no app label to spread its replicas by", obj.Metadata.Name)
		}
		pinned[obj.Metadata.Name] = pool
	}
	return pinned, all
}

// Under --cordon-control-plane every control plane apply path has to carry the
// pinning, since each workload reaches the cluster through a different one:
// the system bundle, the lone redeploy files, the podcert overlay, the
// postgres file, the egress variants, and the bundled credential provider. postgres gets a pool of its own;
// every other control plane workload shares one.
func TestRenderCordonControlPlane(t *testing.T) {
	root := repoRoot(t)
	for _, tc := range []struct {
		name string
		cfg  config.Config
		path func(e *Env) string
		want []string
	}{
		{
			name: "base bundle",
			cfg:  config.Config{Router: config.RouterEnvoy},
			path: func(e *Env) string { return e.Cfg.Path(SystemOverlay(e.Cfg)) },
			want: []string{"ate-api-server", "ate-controller", "atenet-router"},
		},
		{
			name: "kind bundle",
			cfg:  config.Config{Router: config.RouterEnvoy, Kind: true},
			path: func(e *Env) string { return e.Cfg.Path(SystemOverlay(e.Cfg)) },
			want: []string{"ate-api-server", "ate-controller", "atenet-router"},
		},
		{
			name: "agentgateway bundle",
			cfg:  config.Config{Router: config.RouterAgentgateway},
			path: func(e *Env) string { return e.Cfg.Path(SystemOverlay(e.Cfg)) },
			want: []string{"ate-api-server", "ate-controller", "atenet-router"},
		},
		{
			name: "api server file",
			path: func(e *Env) string { return e.Cfg.Manifest("ate-api-server.yaml") },
			want: []string{"ate-api-server"},
		},
		{
			name: "podcert file",
			path: func(e *Env) string { return e.Cfg.Manifest("pod-certificate-controller.yaml") },
			want: []string{"podcertificate-controller"},
		},
		{
			name: "podcert size10 overlay",
			cfg:  config.Config{ClusterSize: config.ClusterSizeSize10},
			path: func(e *Env) string { return e.Cfg.Manifest("podcert-size10") },
			want: []string{"podcertificate-controller"},
		},
		{
			name: "postgres file",
			path: func(e *Env) string { return e.postgresManifestPath() },
			want: []string{"postgres"},
		},
		{
			name: "egress file",
			path: func(e *Env) string { return e.atenetEgressManifestPath() },
			want: []string{"atenet-egress"},
		},
		{
			name: "agentgateway egress overlay",
			cfg:  config.Config{Router: config.RouterAgentgateway},
			path: func(e *Env) string { return e.Cfg.Path(installDir + "/agentgateway-egress") },
			want: []string{"atenet-egress"},
		},
		{
			name: "credential provider file",
			path: func(e *Env) string { return e.k8sCredentialProviderPath(k8sCredentialProviderManifest) },
			want: []string{k8sCredentialProviderDeployment},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.Root = root
			cfg.CordonControlPlane = true
			e := &Env{Cfg: &cfg}

			rendered, err := e.render(tc.path(e))
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			pinned, all := pinnedWorkloads(t, rendered)
			for _, name := range tc.want {
				if !slices.Contains(all, name) {
					t.Errorf("rendered manifest has no workload %s (found %v)", name, all)
				}
				want := "ate-control-plane"
				if name == "postgres" {
					want = "ate-postgres"
				}
				if got := pinned[name]; got != want {
					t.Errorf("workload %s pinned to pool %q, want %q", name, got, want)
				}
			}
			// The DaemonSet-shaped and demo workloads stay off the pools; only
			// the named control plane workloads are pinned.
			for name := range pinned {
				if !slices.Contains(tc.want, name) {
					t.Errorf("workload %s is pinned but is not a control plane workload", name)
				}
			}
		})
	}
}

// The extproc-patched egress manifest arrives as bytes, and the pinning has to
// reach it too.
func TestRenderBytesCordonControlPlane(t *testing.T) {
	e := &Env{Cfg: &config.Config{
		Root:                           repoRoot(t),
		CordonControlPlane:             true,
		AdditionalEgressExtprocService: "ate-system/foo:50051",
	}}
	patched, err := e.patchAtenetEgressManifest()
	if err != nil {
		t.Fatalf("patchAtenetEgressManifest: %v", err)
	}
	rendered, err := e.renderBytes(patched)
	if err != nil {
		t.Fatalf("renderBytes: %v", err)
	}
	pinned, _ := pinnedWorkloads(t, rendered)
	if pinned["atenet-egress"] != "ate-control-plane" {
		t.Errorf("atenet-egress is not pinned in the composed extproc manifest (pinned: %v)", pinned)
	}
	if !strings.Contains(string(rendered), additionalEgressExtprocCluster) {
		t.Error("composition dropped the spliced extproc cluster")
	}
}

// Without the flag, render is a plain read or build and adds nothing.
func TestRenderWithoutCordonLeavesManifestsAlone(t *testing.T) {
	e := &Env{Cfg: &config.Config{Root: repoRoot(t)}}
	rendered, err := e.render(e.Cfg.Manifest("ate-api-server.yaml"))
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if pinned, _ := pinnedWorkloads(t, rendered); len(pinned) != 0 {
		t.Errorf("render without --cordon-control-plane pinned %v", pinned)
	}
	raw, err := os.ReadFile(e.Cfg.Manifest("ate-api-server.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(rendered) != string(raw) {
		t.Error("render of a plain file without --cordon-control-plane is not the file's bytes")
	}
}

// The agentgateway egress overlay mounts the CA pool Secret
// EnsureEgressMITMCAPoolSecret generates; without it atenet-egress waits on a
// Secret nobody creates.
func TestAgentgatewayEgressOverlay(t *testing.T) {
	cfg := &config.Config{
		Root:   repoRoot(t),
		Router: config.RouterAgentgateway,
	}
	e := &Env{Cfg: cfg, Kube: fakeKube(t)}

	built, err := e.Kustomize(installDir + "/agentgateway-egress")
	if err != nil {
		t.Fatalf("Kustomize(agentgateway-egress) = %v", err)
	}
	if !strings.Contains(string(built), SecretEgressMITMCAPool) {
		t.Errorf("the MITM overlay does not mount the %s Secret", SecretEgressMITMCAPool)
	}
	foundConfig := false
	for _, doc := range strings.Split(string(built), "\n---\n") {
		var cm corev1.ConfigMap
		if err := yaml.Unmarshal([]byte(doc), &cm); err != nil || cm.Name != "atenet-egress-agentgateway-substrate-config" {
			continue
		}
		foundConfig = true
		var gateway struct {
			Binds []struct {
				Mode      string `json:"mode"`
				Protocol  string `json:"protocol"`
				Listeners []struct {
					Protocol string `json:"protocol"`
				} `json:"listeners"`
			} `json:"binds"`
		}
		if err := yaml.Unmarshal([]byte(cm.Data["config.yaml"]), &gateway); err != nil {
			t.Fatalf("parsing agentgateway config: %v", err)
		}
		var protocols []string
		for _, bind := range gateway.Binds {
			if bind.Mode != "internal" {
				continue
			}
			if bind.Protocol != "AUTO" {
				t.Errorf("internal bind protocol = %q, want AUTO", bind.Protocol)
			}
			for _, listener := range bind.Listeners {
				protocols = append(protocols, listener.Protocol)
			}
		}
		slices.Sort(protocols)
		if !slices.Equal(protocols, []string{"HTTP", "HTTPS", "TLS"}) {
			t.Errorf("egress listeners = %v, want HTTP, HTTPS interception, and TLS passthrough", protocols)
		}
	}
	if !foundConfig {
		t.Fatal("agentgateway egress ConfigMap is missing")
	}

	if err := e.EnsureEgressMITMCAPoolSecret(t.Context()); err != nil {
		t.Fatalf("EnsureEgressMITMCAPoolSecret() error = %v", err)
	}
	exists, err := e.Kube.SecretExists(t.Context(), NamespaceAteSystem, SecretEgressMITMCAPool)
	if err != nil {
		t.Fatalf("SecretExists() error = %v", err)
	}
	if !exists {
		t.Errorf("no %s Secret was generated for the agentgateway dataplane", SecretEgressMITMCAPool)
	}
}

func TestRenderAgentgatewayCredentialProvider(t *testing.T) {
	for _, tc := range []struct {
		name     string
		provider config.CredentialProvider
	}{
		{name: "disabled"},
		{name: "kubernetes", provider: config.CredentialProvider{Name: config.K8sCredentialProviderName, Address: config.K8sCredentialProviderAddress}},
		{name: "custom", provider: config.CredentialProvider{Name: "vault.example.com", Address: "vault.ate-system.svc:8200"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"}
			e := &Env{
				Cfg: &config.Config{Root: repoRoot(t), Router: config.RouterAgentgateway, Images: src},
				resolver: images.NewPrebuilt(src, func(context.Context, string) (string, error) {
					return "sha256:" + strings.Repeat("2", 64), nil
				}),
			}
			built, err := e.renderAtenetEgressManifest(t.Context(), tc.provider)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(built), "#ATE_AGENTGATEWAY_CREDENTIAL_PROVIDERS") {
				t.Fatal("unresolved credential provider marker")
			}
			found := false
			for _, doc := range strings.Split(string(built), "\n---\n") {
				var cm corev1.ConfigMap
				if err := yaml.Unmarshal([]byte(doc), &cm); err != nil || cm.Name != "atenet-egress-agentgateway-substrate-config" {
					continue
				}
				found = true
				resolved, err := yaml.YAMLToJSON([]byte(cm.Data["config.yaml"]))
				if err != nil {
					t.Fatal(err)
				}
				gateway, err := kyaml.Parse(string(resolved))
				if err != nil {
					t.Fatal(err)
				}
				for _, protocol := range []string{"HTTP", "HTTPS"} {
					policy, err := gateway.Pipe(kyaml.Lookup("binds", "[mode=internal]", "listeners", "[protocol="+protocol+"]", "routes", "0", "policies", "substrateEgress"))
					if err != nil || policy == nil {
						t.Fatalf("%s egress policy missing: %v", protocol, err)
					}
					var got struct {
						Providers []map[string]any `yaml:"credentialProviders"`
					}
					if err := policy.YNode().Decode(&got); err != nil {
						t.Fatal(err)
					}
					if !tc.provider.Enabled() {
						if len(got.Providers) != 0 {
							t.Errorf("%s unexpectedly has credential providers: %v", protocol, got.Providers)
						}
						continue
					}
					want := []map[string]any{{
						"uriAuthority": tc.provider.Name,
						"target": map[string]any{
							"host": tc.provider.Address,
							"policies": map[string]any{"backendTLS": map[string]any{
								"hostname": tc.provider.ServerName(),
								"cert":     "/run/podidentity.podcert.ate.dev/credential-bundle.pem",
								"key":      "/run/podidentity.podcert.ate.dev/credential-bundle.pem",
								"root":     "/run/servicedns-ca/trust-bundle.pem",
							}},
						},
					}}
					if !reflect.DeepEqual(got.Providers, want) {
						t.Errorf("credential providers = %v, want %v", got.Providers, want)
					}
				}
			}
			if !found {
				t.Fatal("agentgateway egress ConfigMap is missing")
			}
		})
	}
}

func TestPatchAgentgatewayCredentialProviderRequiresOneMarker(t *testing.T) {
	for _, raw := range []string{"", "#ATE_AGENTGATEWAY_CREDENTIAL_PROVIDERS\n#ATE_AGENTGATEWAY_CREDENTIAL_PROVIDERS\n"} {
		if _, err := patchAgentgatewayEgressInject([]byte(raw), config.CredentialProvider{}); err == nil {
			t.Errorf("accepted a manifest without exactly one marker: %q", raw)
		}
	}
}

// otelConfig seeds the ConfigMap every component reads its collector address
// from.
func otelConfig(endpoint string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: NamespaceAteSystem, Name: otelConfigMap},
		Data:       map[string]string{otelEndpointKey: endpoint},
	}
}

// restartedAt reports whether a workload's pod template carries the restart
// annotation.
func restartedAt(t *testing.T, e *Env, kind, name string) bool {
	t.Helper()
	var annotations map[string]string
	switch kind {
	case "deployment":
		dep, err := e.Kube.GetDeployment(t.Context(), NamespaceAteSystem, name)
		if err != nil || dep == nil {
			t.Fatalf("GetDeployment(%s) = %v, %v", name, dep, err)
		}
		annotations = dep.Spec.Template.Annotations
	case "daemonset":
		ds, err := e.Kube.Typed.AppsV1().DaemonSets(NamespaceAteSystem).Get(t.Context(), name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("getting daemonset %s: %v", name, err)
		}
		annotations = ds.Spec.Template.Annotations
	}
	_, ok := annotations["kubectl.kubernetes.io/restartedAt"]
	return ok
}

func TestApplyOtelEndpointOverride(t *testing.T) {
	const endpoint = "http://collector.benchmark.svc:4317"

	t.Run("no endpoint configured is a no-op", func(t *testing.T) {
		e := &Env{Cfg: &config.Config{}, Kube: fakeKube(t, otelConfig("http://default:4317"))}
		if err := e.applyOtelEndpointOverride(t.Context()); err != nil {
			t.Fatalf("applyOtelEndpointOverride() error = %v", err)
		}
		cm, _ := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, otelConfigMap)
		if cm.Data[otelEndpointKey] != "http://default:4317" {
			t.Errorf("%s = %q, want the cluster default untouched", otelEndpointKey, cm.Data[otelEndpointKey])
		}
	})

	// Restarting when nothing changed makes the restart race the rollout the
	// caller is about to wait on, and `rollout status` then times out.
	t.Run("already correct restarts nothing", func(t *testing.T) {
		e := &Env{
			Cfg:  &config.Config{OtlpEndpoint: endpoint},
			Kube: fakeKube(t, otelConfig(endpoint), apiServerDeployment()),
		}
		if err := e.applyOtelEndpointOverride(t.Context()); err != nil {
			t.Fatalf("applyOtelEndpointOverride() error = %v", err)
		}
		if restartedAt(t, e, "deployment", "ate-api-server") {
			t.Error("ate-api-server was restarted even though the endpoint was unchanged")
		}
	})

	t.Run("patches and restarts the consumers", func(t *testing.T) {
		// The atelet DaemonSet name carries a substrate version suffix, so it
		// can only be found by label.
		atelet := &appsv1.DaemonSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: NamespaceAteSystem,
				Name:      "atelet-v1-2-3",
				Labels:    map[string]string{"app": "atelet"},
			},
		}
		e := &Env{
			Cfg:  &config.Config{OtlpEndpoint: endpoint},
			Kube: fakeKube(t, otelConfig("http://default:4317"), apiServerDeployment(), atelet),
		}

		if err := e.applyOtelEndpointOverride(t.Context()); err != nil {
			t.Fatalf("applyOtelEndpointOverride() error = %v", err)
		}

		cm, _ := e.Kube.GetConfigMap(t.Context(), NamespaceAteSystem, otelConfigMap)
		if cm.Data[otelEndpointKey] != endpoint {
			t.Errorf("%s = %q, want %q", otelEndpointKey, cm.Data[otelEndpointKey], endpoint)
		}
		// ate-controller and atenet-router are absent here: a deploy of one
		// component has only that component, which is not an error.
		if !restartedAt(t, e, "deployment", "ate-api-server") {
			t.Error("ate-api-server was not restarted")
		}
		if !restartedAt(t, e, "daemonset", "atelet-v1-2-3") {
			t.Error("the atelet DaemonSet was not restarted")
		}
	})
}

// A pre-built install renders the envoy egress manifest without building
// anything: envoy-dataplane is pinned from the release like every ko image, so
// neither docker nor KO_DOCKER_REPO is needed.
func TestRenderAtenetEgressManifestPrebuilt(t *testing.T) {
	const digest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	src := images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"}
	var looked []string
	e := &Env{
		Cfg: &config.Config{Root: repoRoot(t), Router: config.RouterEnvoy, Images: src},
		resolver: images.NewPrebuilt(src, func(_ context.Context, ref string) (string, error) {
			looked = append(looked, ref)
			return digest, nil
		}),
	}

	out, err := e.renderAtenetEgressManifest(t.Context(), config.CredentialProvider{})
	if err != nil {
		t.Fatalf("renderAtenetEgressManifest() error = %v", err)
	}
	want := "example.com/substrate/envoy-dataplane:v1.2.3@" + digest
	if !strings.Contains(string(out), "image: "+want) {
		t.Errorf("rendered manifest does not install %s:\n%s", want, out)
	}
	for _, leftover := range []string{"${ENVOY_DATAPLANE_IMAGE}", "ko://"} {
		if strings.Contains(string(out), leftover) {
			t.Errorf("rendered manifest still contains %q", leftover)
		}
	}
	if !slices.Contains(looked, "example.com/substrate/envoy-dataplane:v1.2.3") {
		t.Errorf("registry lookups = %v, want one for envoy-dataplane", looked)
	}
}

// A release that did not publish envoy-dataplane fails the install with a
// message naming the image and the target that publishes it, rather than a bare
// registry error.
func TestDockerfileImagePrebuiltNotPublished(t *testing.T) {
	src := images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"}
	e := &Env{
		Cfg: &config.Config{Images: src},
		resolver: images.NewPrebuilt(src, func(_ context.Context, ref string) (string, error) {
			return "", fmt.Errorf("resolving %s to a digest: MANIFEST_UNKNOWN", ref)
		}),
	}

	_, err := e.dockerfileImage(t.Context(), envoyDataplaneImage, envoyDataplaneDockefile)
	if err == nil {
		t.Fatal("dockerfileImage() error = nil, want one")
	}
	for _, want := range []string{
		"make build-envoy-dataplane",
		"resolving example.com/substrate/envoy-dataplane:v1.2.3 to a digest: MANIFEST_UNKNOWN",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error is missing %q:\n%v", want, err)
		}
	}
}

func TestPatchEnvoyDataplaneImage(t *testing.T) {
	e := &Env{}
	raw := []byte("containers:\n- name: envoy\n  image: ${ENVOY_DATAPLANE_IMAGE}\n")
	want := "containers:\n- name: envoy\n  image: gcr.io/example/envoy-dataplane@sha256:abc123\n"
	got := string(e.patchEnvoyDataplaneImage(raw, "gcr.io/example/envoy-dataplane@sha256:abc123"))
	if got != want {
		t.Errorf("patchEnvoyDataplaneImage() = %q, want %q", got, want)
	}
}
