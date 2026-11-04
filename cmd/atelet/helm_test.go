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

package main

import (
	"os"
	"os/exec"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

func TestHelmImageCredentialProvider(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	const providerConfig = `apiVersion: kubelet.config.k8s.io/v1
kind: CredentialProviderConfig
providers: []`
	for _, tc := range []struct {
		name, config, configYAML, binDir string
		wantError                        string
	}{
		{name: "public registries"},
		{name: "node provider", config: "/etc/srv/kubernetes/cri_auth_config.yaml", binDir: "/home/kubernetes/bin"},
		{name: "chart provided config", configYAML: providerConfig, binDir: "/home/kubernetes/bin"},
		{name: "missing bin directory", config: "/etc/provider.yaml", wantError: "must be set together"},
		{name: "chart provided config without bin directory", configYAML: providerConfig, wantError: "must be set together"},
		{name: "missing config", binDir: "/opt/providers", wantError: "must be set together"},
		{
			name:       "both config sources",
			config:     "/etc/provider.yaml",
			configYAML: providerConfig,
			binDir:     "/opt/providers",
			wantError:  "set only one of",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := exec.CommandContext(t.Context(), "helm", "template", "substrate", "../../charts/substrate",
				"--show-only", "templates/atelet.yaml",
				"--set-string", "atelet.imageCredentialProviderConfig="+tc.config,
				"--set-string", "atelet.imageCredentialProviderConfigYAML="+tc.configYAML,
				"--set-string", "atelet.imageCredentialProviderBinDir="+tc.binDir).CombinedOutput()
			if tc.wantError != "" {
				if err == nil || !strings.Contains(string(out), tc.wantError) {
					t.Fatalf("render rejected provider config with %q: %v\n%s", tc.wantError, err, out)
				}
				return
			}
			if err != nil {
				t.Fatalf("render: %v\n%s", err, out)
			}
			var pod *corev1.PodSpec
			var configMap *corev1.ConfigMap
			for _, doc := range strings.Split(string(out), "\n---\n") {
				var ds appsv1.DaemonSet
				if err := yaml.Unmarshal([]byte(doc), &ds); err != nil {
					t.Fatal(err)
				}
				if ds.Kind == "DaemonSet" {
					pod = &ds.Spec.Template.Spec
				}
				var cm corev1.ConfigMap
				if err := yaml.Unmarshal([]byte(doc), &cm); err != nil {
					t.Fatal(err)
				}
				if cm.Kind == "ConfigMap" {
					configMap = &cm
				}
			}
			if (configMap != nil) != (tc.configYAML != "") {
				t.Fatalf("ConfigMap presence does not match provider configuration: %v", configMap)
			}
			if configMap != nil {
				if got := strings.TrimRight(configMap.Data["config.yaml"], "\n"); got != tc.configYAML {
					t.Errorf("ConfigMap carries %q, want the configured provider config", got)
				}
				if configMap.Name != "atelet-image-credential-provider" {
					t.Errorf("ConfigMap is named %q", configMap.Name)
				}
			}
			if pod == nil || len(pod.Containers) != 1 {
				t.Fatal("expected atelet DaemonSet with one container")
			}
			container := pod.Containers[0]
			for _, arg := range container.Args {
				if strings.HasPrefix(arg, "--gcp-auth-for-image-pulls") {
					t.Fatal("chart passes the removed GCP authentication flag")
				}
			}
			for _, binding := range []struct{ name, flag, mount, host string }{
				{"image-credential-provider-config", "--image-credential-provider-config", "/run/image-credential-provider/config.yaml", tc.config},
				{"image-credential-provider-bin", "--image-credential-provider-bin-dir", "/run/image-credential-provider/bin", tc.binDir},
			} {
				enabled := tc.config != "" || tc.configYAML != ""
				if slices.Contains(container.Args, binding.flag+"="+binding.mount) != enabled {
					t.Errorf("%s flag does not match provider configuration", binding.flag)
				}
				mountIndex := slices.IndexFunc(container.VolumeMounts, func(m corev1.VolumeMount) bool { return m.Name == binding.name })
				volumeIndex := slices.IndexFunc(pod.Volumes, func(v corev1.Volume) bool { return v.Name == binding.name })
				if (mountIndex >= 0) != enabled || (volumeIndex >= 0) != enabled {
					t.Fatalf("%s mount/volume presence does not match provider configuration", binding.name)
				}
				if enabled {
					mount := container.VolumeMounts[mountIndex]
					volume := pod.Volumes[volumeIndex]
					if mount.MountPath != binding.mount || !mount.ReadOnly {
						t.Errorf("%s must mount read-only at the flag's path", binding.name)
					}
					// An empty host path means the chart supplied the config itself.
					if binding.host == "" {
						if volume.ConfigMap == nil || configMap == nil || volume.ConfigMap.Name != configMap.Name || mount.SubPath != "config.yaml" {
							t.Errorf("%s must project the chart's ConfigMap onto the flag's path", binding.name)
						}
					} else if volume.HostPath == nil || volume.HostPath.Path != binding.host || mount.SubPath != "" {
						t.Errorf("%s must mount the configured host path at the flag's path", binding.name)
					}
				}
			}
		})
	}
}

func TestHelmSandboxAssetStorage(t *testing.T) {
	for _, tc := range []struct {
		name, backend string
		rustfs        bool
	}{
		{name: "rustfs", backend: "s3", rustfs: true},
		{name: "external s3", backend: "s3"},
		{name: "gcs", backend: "gcs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"template", "substrate", "../../charts/substrate", "--show-only", "templates/atelet.yaml",
				"--set", "atelet.storageBackend=" + tc.backend,
				"--set", "objectStorePlugin.extraEnv[0].name=AWS_REGION",
				"--set", "objectStorePlugin.extraEnv[0].value=us-west-2"}
			if !tc.rustfs {
				args = append(args, "--set", "rustfs.enabled=false", "--set", "ateApi.snapshotLocation=gs://external-snapshots/")
			}
			out, err := exec.CommandContext(t.Context(), "helm", args...).CombinedOutput()
			if err != nil {
				t.Fatalf("render: %v\n%s", err, out)
			}
			var pod *corev1.PodSpec
			for _, doc := range strings.Split(string(out), "\n---\n") {
				var ds appsv1.DaemonSet
				if err := yaml.Unmarshal([]byte(doc), &ds); err != nil {
					t.Fatal(err)
				}
				if ds.Kind == "DaemonSet" {
					pod = &ds.Spec.Template.Spec
				}
			}
			if pod == nil || len(pod.Containers) != 1 || len(pod.InitContainers) != 1 {
				t.Fatal("expected atelet with one object-store plugin sidecar")
			}
			for _, env := range pod.Containers[0].Env {
				if env.Name == "ATE_STORAGE_BACKEND" || strings.HasPrefix(env.Name, "AWS_") {
					t.Errorf("atelet receives plugin storage setting %s", env.Name)
				}
			}
			plugin := pod.InitContainers[0]
			if !slices.Contains(plugin.Env, corev1.EnvVar{Name: "ATE_STORAGE_BACKEND", Value: tc.backend}) ||
				!slices.Contains(plugin.Env, corev1.EnvVar{Name: "AWS_REGION", Value: "us-west-2"}) {
				t.Errorf("object-store plugin is missing configured storage settings: %v", plugin.Env)
			}
			for _, arg := range []string{"--asset-staging-dir=/var/lib/objectstore-plugin/asset-staging", "--asset-staging-capacity=8Gi"} {
				if !slices.Contains(plugin.Args, arg) {
					t.Errorf("object-store plugin is missing %s", arg)
				}
			}
			mount := corev1.VolumeMount{Name: "asset-staging", MountPath: "/var/lib/objectstore-plugin/asset-staging"}
			if !slices.ContainsFunc(plugin.VolumeMounts, func(m corev1.VolumeMount) bool {
				return m.Name == mount.Name && m.MountPath == mount.MountPath && !m.ReadOnly
			}) {
				t.Error("object-store plugin cannot access asset staging")
			}
			for _, m := range pod.Containers[0].VolumeMounts {
				if m.Name == mount.Name {
					t.Error("atelet can access unverified staged assets")
				}
			}
			i := slices.IndexFunc(pod.Volumes, func(v corev1.Volume) bool { return v.Name == mount.Name })
			if i < 0 || pod.Volumes[i].EmptyDir == nil || pod.Volumes[i].EmptyDir.SizeLimit == nil ||
				pod.Volumes[i].EmptyDir.SizeLimit.Cmp(resource.MustParse("9Gi")) != 0 {
				t.Error("asset staging needs an emptyDir with capacity plus filesystem overhead")
			}
		})
	}
}

func TestHelmGVisorSandboxConfigMatchesManifest(t *testing.T) {
	out, err := exec.CommandContext(t.Context(), "helm", "template", "substrate", "../../charts/substrate",
		"--show-only", "templates/sandboxconfig-gvisor.yaml").CombinedOutput()
	if err != nil {
		t.Fatalf("render: %v\n%s", err, out)
	}
	manifest, err := os.ReadFile("../../manifests/ate-install/sandboxconfig-gvisor.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var got, want atev1.SandboxConfig
	if err := yaml.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if err := yaml.Unmarshal(manifest, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Spec, want.Spec) {
		t.Errorf("Helm sandbox config must match the runtime assets in the upstream manifest: got %+v, want %+v", got.Spec, want.Spec)
	}
}

// helmTemplate returns the rendered manifests without their "# Source:"
// comments, which name the chart. It reads stdout only, because Helm logs the
// substrate-atelet chart's symlinked templates on stderr.
func helmTemplate(t *testing.T, args ...string) string {
	t.Helper()
	var stderr strings.Builder
	cmd := exec.CommandContext(t.Context(), "helm", append([]string{"template"}, args...)...)
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("helm template %v: %v\n%s", args, err, stderr.String())
	}
	return regexp.MustCompile(`(?m)^# Source: .*\n`).ReplaceAllString(string(out), "")
}

func TestHelmAteletChartMatchesSubstrateChart(t *testing.T) {
	for _, tc := range []struct {
		name, release, namespace string
		values                   []string
	}{
		{name: "defaults", release: "substrate", namespace: "ate-system"},
		{
			name:      "shared values",
			release:   "team",
			namespace: "custom",
			values: []string{
				"--set", "preview[0]=ALL",
				"--set", "otel.endpoint=http://collector:4317",
				"--set", "rustfs.enabled=false",
				"--set", "ateApi.snapshotLocation=gs://snapshots/",
				"--set", "atelet.storageBackend=gcs",
				"--set", "image.tag=test",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := helmTemplate(t, append([]string{tc.release, "../../charts/substrate", "-n", tc.namespace,
				"--show-only", "templates/atelet.yaml"}, tc.values...)...)
			got := helmTemplate(t, append([]string{"atelet", "../../charts/substrate-atelet", "-n", tc.namespace,
				"--set", "substrateReleaseName=" + tc.release}, tc.values...)...)
			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("substrate-atelet renders a different atelet than the substrate chart (-substrate +substrate-atelet):\n%s", diff)
			}
		})
	}
}

func TestHelmAteletOptOut(t *testing.T) {
	out := helmTemplate(t, "substrate", "../../charts/substrate", "--set", "atelet.enabled=false")
	for _, doc := range strings.Split(out, "\n---\n") {
		var obj struct {
			Kind     string
			Metadata struct{ Name string }
		}
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatal(err)
		}
		// The NetworkPolicy still guards an atelet installed by substrate-atelet.
		if obj.Kind == "DaemonSet" || obj.Kind == "PriorityClass" ||
			(obj.Kind != "NetworkPolicy" && strings.HasPrefix(obj.Metadata.Name, "atelet")) {
			t.Errorf("atelet.enabled=false still renders %s %s", obj.Kind, obj.Metadata.Name)
		}
	}
}
