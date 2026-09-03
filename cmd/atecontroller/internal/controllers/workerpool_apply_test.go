// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controllers

import (
	"regexp"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/deviceplugin"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

func TestBuildDeploymentApplyConfig(t *testing.T) {
	requiredNodeAffinity := &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "workload",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"substrate"},
				}},
			}},
		},
	}
	preferredNodeAffinity := &corev1.NodeAffinity{
		PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
			Weight: 50,
			Preference: corev1.NodeSelectorTerm{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      "disk",
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{"ssd"},
				}},
			},
		}},
	}
	tolerationSeconds := int64(300)
	toleration := corev1.Toleration{
		Key:               "dedicated",
		Operator:          corev1.TolerationOpEqual,
		Value:             "workerpool",
		Effect:            corev1.TaintEffectNoSchedule,
		TolerationSeconds: &tolerationSeconds,
	}

	tests := []struct {
		name string
		wp   *atev1alpha1.WorkerPool
		want *appsv1ac.DeploymentApplyConfiguration
	}{
		{
			name: "default workerpool",
			wp:   testWorkerPoolApplyConfig(nil),
			want: expectedDeploymentApplyConfig(nil),
		},
		{
			name: "with node selector",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				NodeSelector: map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				},
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithNodeSelector(map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				})
			}),
		},
		{
			name: "with tolerations",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				Tolerations: []corev1.Toleration{toleration},
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{
					*corev1ac.Toleration().
						WithKey("dedicated").
						WithOperator(corev1.TolerationOpEqual).
						WithValue("workerpool").
						WithEffect(corev1.TaintEffectNoSchedule).
						WithTolerationSeconds(300),
				}
			}),
		},
		{
			name: "with node affinity",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				NodeAffinity: requiredNodeAffinity,
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithAffinity(corev1ac.Affinity().WithNodeAffinity(
					corev1ac.NodeAffinity().WithRequiredDuringSchedulingIgnoredDuringExecution(
						corev1ac.NodeSelector().WithNodeSelectorTerms(
							corev1ac.NodeSelectorTerm().WithMatchExpressions(
								corev1ac.NodeSelectorRequirement().
									WithKey("workload").
									WithOperator(corev1.NodeSelectorOpIn).
									WithValues("substrate"),
							),
						),
					),
				))
			}),
		},
		{
			name: "with priority class name",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				PriorityClassName: "interactive-workerpool",
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithPriorityClassName("interactive-workerpool")
			}),
		},
		{
			name: "with resources",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				Resources: &corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.Containers[0].WithResources(corev1ac.ResourceRequirements().
					WithRequests(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					}).
					WithLimits(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					}))
			}),
		},
		{
			name: "with combined scheduling fields",
			wp: testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
				NodeSelector: map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				},
				Tolerations:       []corev1.Toleration{toleration},
				PriorityClassName: "interactive-workerpool",
				NodeAffinity:      preferredNodeAffinity,
			}),
			want: expectedDeploymentApplyConfig(func(podSpecAC *corev1ac.PodSpecApplyConfiguration) {
				podSpecAC.WithNodeSelector(map[string]string{
					"accelerator": "gpu",
					"topology":    "high-mem",
				})
				podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{
					*corev1ac.Toleration().
						WithKey("dedicated").
						WithOperator(corev1.TolerationOpEqual).
						WithValue("workerpool").
						WithEffect(corev1.TaintEffectNoSchedule).
						WithTolerationSeconds(300),
				}
				podSpecAC.WithPriorityClassName("interactive-workerpool")
				podSpecAC.WithAffinity(corev1ac.Affinity().WithNodeAffinity(
					corev1ac.NodeAffinity().WithPreferredDuringSchedulingIgnoredDuringExecution(
						corev1ac.PreferredSchedulingTerm().
							WithWeight(50).
							WithPreference(corev1ac.NodeSelectorTerm().WithMatchExpressions(
								corev1ac.NodeSelectorRequirement().
									WithKey("disk").
									WithOperator(corev1.NodeSelectorOpIn).
									WithValues("ssd"),
							)),
					),
				))
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDeploymentApplyConfig(tt.wp, ateomOTelSettings{})
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("buildDeploymentApplyConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBuildDeploymentApplyConfigMetadata(t *testing.T) {
	wp := testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
		Labels: map[string]atev1alpha1.WorkerPoolLabelValue{
			"project":             "agent-substrate",
			"team":                "compute",
			"ate.dev/worker-pool": "incorrect",
		},
		Annotations: map[string]string{
			"policy.example.com/exemption": "sandbox-host",
		},
	})

	got := buildDeploymentApplyConfig(wp, ateomOTelSettings{})
	wantLabels := map[string]string{
		"project":             "agent-substrate",
		"team":                "compute",
		"ate.dev/worker-pool": wp.Name,
	}
	wantAnnotations := map[string]string{
		"policy.example.com/exemption": "sandbox-host",
	}

	if diff := cmp.Diff(wantLabels, got.Labels); diff != "" {
		t.Errorf("Deployment labels mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantAnnotations, got.Annotations); diff != "" {
		t.Errorf("Deployment annotations mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantLabels, got.Spec.Template.Labels); diff != "" {
		t.Errorf("pod-template labels mismatch (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(wantAnnotations, got.Spec.Template.Annotations); diff != "" {
		t.Errorf("pod-template annotations mismatch (-want +got):\n%s", diff)
	}
}

// TestMicroVMPodShape asserts the micro-VM sandbox class requests the host
// devices as extended resources (served by atelet's device plugin) and
// tolerates the ate.dev/sandboxClass taint; other classes get none of it.
// Placement comes from the device request, so no nodeSelector is added.
func TestMicroVMPodShape(t *testing.T) {
	tests := []struct {
		name        string
		class       atev1alpha1.SandboxClass
		wantMicroVM bool
	}{
		{"gvisor default", "", false},
		{"gvisor explicit", atev1alpha1.SandboxClassGvisor, false},
		{"microvm", atev1alpha1.SandboxClassMicroVM, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wp := testWorkerPoolApplyConfig(nil)
			wp.Spec.SandboxClass = tt.class
			ps := buildDeploymentApplyConfig(wp, ateomOTelSettings{}).Spec.Template.Spec

			// /dev/kvm must come from the device plugin, never a hostPath: a
			// hostPath mount carries no cgroup device allow rule, and the
			// runtime denies /dev/kvm by default. /dev/net/tun is the
			// opposite — allowed by default, so it is bind-mounted.
			for _, v := range ps.Volumes {
				if v.HostPath == nil || v.HostPath.Path == nil || *v.HostPath.Path == tunDevicePath {
					continue
				}
				if strings.HasPrefix(*v.HostPath.Path, "/dev/") {
					t.Errorf("device %q must be requested as a resource, not hostPath-mounted", *v.HostPath.Path)
				}
			}
			hasTunMount := false
			for _, v := range ps.Volumes {
				if v.HostPath == nil || v.HostPath.Path == nil || *v.HostPath.Path != tunDevicePath {
					continue
				}
				hasTunMount = true
				// Without CharDevice, kubelet would happily create a directory
				// there on a node where the tun module has yet to load.
				if v.HostPath.Type == nil || *v.HostPath.Type != corev1.HostPathCharDev {
					t.Errorf("%s hostPath type = %v, want CharDevice", tunDevicePath, v.HostPath.Type)
				}
			}
			if hasTunMount && !containerMountsPath(ps.Containers[0], tunDevicePath) {
				t.Errorf("%s is a pod volume but the ateom container does not mount it", tunDevicePath)
			}

			hasDeviceRequest := true
			for _, name := range []string{deviceplugin.ResourceKVM} {
				qty, ok := deviceLimit(ps.Containers[0], name)
				if !ok {
					hasDeviceRequest = false
					continue
				}
				if qty != "1" {
					t.Errorf("%s limit = %s, want 1", name, qty)
				}
			}

			// The device request handles placement, so the class must not also
			// pin a nodeSelector (which would re-introduce the hand-applied
			// label as a scheduling requirement).
			if _, hasSelector := ps.NodeSelector["ate.dev/sandboxClass"]; hasSelector {
				t.Errorf("nodeSelector on ate.dev/sandboxClass should be gone; placement comes from the device request")
			}
			hasTol := false
			for _, tol := range ps.Tolerations {
				if tol.Key != nil && *tol.Key == "ate.dev/sandboxClass" {
					hasTol = true
				}
			}
			if hasDeviceRequest != tt.wantMicroVM || hasTol != tt.wantMicroVM || hasTunMount != tt.wantMicroVM {
				t.Errorf("microvm shape: deviceRequest=%v toleration=%v tunMount=%v, want all %v",
					hasDeviceRequest, hasTol, hasTunMount, tt.wantMicroVM)
			}
		})
	}
}

// containerMountsPath reports whether c mounts anything at path.
func containerMountsPath(c corev1ac.ContainerApplyConfiguration, path string) bool {
	for _, m := range c.VolumeMounts {
		if m.MountPath != nil && *m.MountPath == path {
			return true
		}
	}
	return false
}

// deviceLimit returns the container's limit for an extended resource.
func deviceLimit(c corev1ac.ContainerApplyConfiguration, name string) (string, bool) {
	if c.Resources == nil || c.Resources.Limits == nil {
		return "", false
	}
	qty, ok := (*c.Resources.Limits)[corev1.ResourceName(name)]
	if !ok {
		return "", false
	}
	return qty.String(), true
}

// A pod template's own resource limits must survive the device requests being
// merged in.
func TestMicroVMDeviceRequestsPreserveTemplateResources(t *testing.T) {
	wp := testWorkerPoolApplyConfig(&atev1alpha1.WorkerPoolPodTemplate{
		Resources: &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
		},
	})
	wp.Spec.SandboxClass = atev1alpha1.SandboxClassMicroVM
	c := buildDeploymentApplyConfig(wp, ateomOTelSettings{}).Spec.Template.Spec.Containers[0]

	if got, ok := deviceLimit(c, string(corev1.ResourceMemory)); !ok || got != "2Gi" {
		t.Errorf("memory limit = %q (present=%v), want 2Gi", got, ok)
	}
	if got, ok := deviceLimit(c, deviceplugin.ResourceKVM); !ok || got != "1" {
		t.Errorf("%s limit = %q (present=%v), want 1", deviceplugin.ResourceKVM, got, ok)
	}
}

// TestAteomSecurityContextByClass asserts no worker runs privileged: every class
// drops ALL capabilities and adds back an explicit set. Only the micro-VM class
// gives up the runtime's default seccomp profile, and only so virtiofsd can keep
// its own sandbox. An empty class defaults to gVisor.
func TestAteomSecurityContextByClass(t *testing.T) {
	tests := []struct {
		name     string
		class    atev1alpha1.SandboxClass
		wantCaps []corev1.Capability
		// wantSeccompUnconfined holds for every class: both runsc's and
		// virtiofsd's sandbox pivot_root(), which the default profile denies.
		wantSeccompUnconfined bool
	}{
		{"gvisor default", "", ateomGvisorCapabilities, true},
		{"gvisor explicit", atev1alpha1.SandboxClassGvisor, ateomGvisorCapabilities, true},
		{"microvm", atev1alpha1.SandboxClassMicroVM, ateomMicroVMCapabilities, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := ateomSecurityContext(tt.class)
			if sc.Privileged == nil || *sc.Privileged {
				t.Errorf("Privileged = %v, want false for every class", sc.Privileged)
			}
			if sc.RunAsUser == nil || *sc.RunAsUser != 0 || sc.RunAsGroup == nil || *sc.RunAsGroup != 0 {
				t.Errorf("RunAsUser/Group = %v/%v, want 0/0", sc.RunAsUser, sc.RunAsGroup)
			}
			if sc.Capabilities == nil {
				t.Fatalf("capabilities must be set so the default set is dropped")
			}
			if len(sc.Capabilities.Drop) != 1 || sc.Capabilities.Drop[0] != "ALL" {
				t.Errorf("capabilities drop = %v, want [ALL]", sc.Capabilities.Drop)
			}
			if diff := cmp.Diff(tt.wantCaps, sc.Capabilities.Add); diff != "" {
				t.Errorf("capabilities add mismatch (-want +got):\n%s", diff)
			}
			// Every class mounts inside the worker, which the default AppArmor
			// profile denies.
			if sc.AppArmorProfile == nil || sc.AppArmorProfile.Type == nil ||
				*sc.AppArmorProfile.Type != corev1.AppArmorProfileTypeUnconfined {
				t.Errorf("AppArmorProfile = %v, want Unconfined", sc.AppArmorProfile)
			}
			// Every class declares Unconfined explicitly so it does not depend on
			// the cluster leaving the seccomp profile unset.
			gotSeccompUnconfined := sc.SeccompProfile != nil && sc.SeccompProfile.Type != nil &&
				*sc.SeccompProfile.Type == corev1.SeccompProfileTypeUnconfined
			if gotSeccompUnconfined != tt.wantSeccompUnconfined {
				t.Errorf("seccomp Unconfined = %v, want %v", gotSeccompUnconfined, tt.wantSeccompUnconfined)
			}
		})
	}
}

// TestTerminationGracePeriodSeconds asserts the pod's grace period is hardcoded to 3600s.
func TestTerminationGracePeriodSeconds(t *testing.T) {
	wp := testWorkerPoolApplyConfig(nil)
	ps := buildDeploymentApplyConfig(wp, ateomOTelSettings{}).Spec.Template.Spec
	if ps.TerminationGracePeriodSeconds == nil {
		t.Fatalf("TerminationGracePeriodSeconds not set")
	}
	if *ps.TerminationGracePeriodSeconds != 3600 {
		t.Errorf("TerminationGracePeriodSeconds = %d, want 3600", *ps.TerminationGracePeriodSeconds)
	}
}

// TestBuildDeploymentApplyConfigOTelEndpoint asserts the OTLP endpoint and the
// resource identity are set on the ateom container only when an endpoint is
// configured, and that every ref the value substitutes is declared ahead of it.
func TestBuildDeploymentApplyConfigOTelEndpoint(t *testing.T) {
	const endpoint = "http://collector.otel-system.svc:4317"
	tests := []struct {
		name          string
		endpoint      string
		wantTelemetry bool
	}{
		{"endpoint empty", "", false},
		{"endpoint set", endpoint, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := buildDeploymentApplyConfig(testWorkerPoolApplyConfig(nil), ateomOTelSettings{Endpoint: tt.endpoint}).
				Spec.Template.Spec.Containers[0]
			env := envByName(c.Env)

			if _, ok := env["POD_UID"]; !ok {
				t.Error("POD_UID must always be set")
			}

			if !tt.wantTelemetry {
				for _, k := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_RESOURCE_ATTRIBUTES", "OTEL_METRIC_EXPORT_INTERVAL", "OTEL_METRIC_EXPORT_TIMEOUT", "POD_NAME", "POD_NAMESPACE", "NODE_NAME"} {
					if _, ok := env[k]; ok {
						t.Errorf("%s must be absent without an OTLP endpoint", k)
					}
				}
				return
			}

			if got := env["OTEL_EXPORTER_OTLP_ENDPOINT"].value; got != endpoint {
				t.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT = %q, want %q", got, endpoint)
			}
			resourceAttrs := env["OTEL_RESOURCE_ATTRIBUTES"].value

			wantKeys := []string{"k8s.namespace.name", "k8s.pod.name", "k8s.pod.uid", "k8s.node.name", "service.instance.id"}
			if diff := cmp.Diff(wantKeys, envAttrKeys(resourceAttrs)); diff != "" {
				t.Errorf("OTEL_RESOURCE_ATTRIBUTES keys (-want +got):\n%s", diff)
			}

			refs := envRefs(resourceAttrs)
			if len(refs) == 0 {
				t.Fatalf("OTEL_RESOURCE_ATTRIBUTES %q substitutes nothing, so the ref check below proves nothing", resourceAttrs)
			}
			raIdx := env["OTEL_RESOURCE_ATTRIBUTES"].index
			for _, ref := range refs {
				if _, ok := env[ref]; !ok {
					t.Errorf("%s must be set for OTEL_RESOURCE_ATTRIBUTES substitution", ref)
					continue
				}
				if env[ref].index > raIdx {
					t.Errorf("%s (index %d) must precede OTEL_RESOURCE_ATTRIBUTES (index %d)", ref, env[ref].index, raIdx)
				}
			}
		})
	}
}

// TestBuildDeploymentApplyConfigMetricExportTuning asserts the export interval and
// per-export timeout reach the ateom container only when each is set alongside an
// endpoint. ateom is invisible to the collector until its first successful export
// tick, so the kind stack shortens the SDK's 60s interval to keep that gap inside
// the e2e budget, and its 30s timeout so a failing tick cannot swallow three
// shortened intervals.
func TestBuildDeploymentApplyConfigMetricExportTuning(t *testing.T) {
	const endpoint = "http://collector.otel-system.svc:4317"
	tests := []struct {
		name string
		otel ateomOTelSettings
		want map[string]string // env name -> value; absent key means must not be set
	}{
		{
			name: "unset keeps SDK defaults",
			otel: ateomOTelSettings{Endpoint: endpoint},
			want: nil,
		},
		{
			name: "both set with endpoint",
			otel: ateomOTelSettings{Endpoint: endpoint, MetricExportInterval: "10000", MetricExportTimeout: "10000"},
			want: map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "10000", "OTEL_METRIC_EXPORT_TIMEOUT": "10000"},
		},
		{
			name: "interval alone",
			otel: ateomOTelSettings{Endpoint: endpoint, MetricExportInterval: "10000"},
			want: map[string]string{"OTEL_METRIC_EXPORT_INTERVAL": "10000"},
		},
		{
			name: "timeout alone",
			otel: ateomOTelSettings{Endpoint: endpoint, MetricExportTimeout: "10000"},
			want: map[string]string{"OTEL_METRIC_EXPORT_TIMEOUT": "10000"},
		},
		{
			name: "ignored without endpoint",
			otel: ateomOTelSettings{MetricExportInterval: "10000", MetricExportTimeout: "10000"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := buildDeploymentApplyConfig(testWorkerPoolApplyConfig(nil), tt.otel).
				Spec.Template.Spec.Containers[0]
			env := envByName(c.Env)
			for _, k := range []string{"OTEL_METRIC_EXPORT_INTERVAL", "OTEL_METRIC_EXPORT_TIMEOUT"} {
				got, ok := env[k]
				want, wantSet := tt.want[k]
				if ok != wantSet {
					t.Errorf("%s present = %v, want %v", k, ok, wantSet)
					continue
				}
				if ok && got.value != want {
					t.Errorf("%s = %q, want %q", k, got.value, want)
				}
			}
		})
	}
}

// TestBuildDeploymentApplyConfigTracesSamplerPropagation asserts the sampler
// env pair reaches the ateom container only alongside an endpoint, and the arg
// only alongside a sampler: an arg without a sampler name is dead config the
// SDK ignores.
func TestBuildDeploymentApplyConfigTracesSamplerPropagation(t *testing.T) {
	const endpoint = "http://collector.otel-system.svc:4317"
	tests := []struct {
		name string
		otel ateomOTelSettings
		want map[string]string // value by env name; absent key means must not be set
	}{
		{
			name: "unset keeps binary default",
			otel: ateomOTelSettings{Endpoint: endpoint},
			want: nil,
		},
		{
			name: "sampler and arg with endpoint",
			otel: ateomOTelSettings{Endpoint: endpoint, TracesSampler: "parentbased_traceidratio", TracesSamplerArg: "0.25"},
			want: map[string]string{"OTEL_TRACES_SAMPLER": "parentbased_traceidratio", "OTEL_TRACES_SAMPLER_ARG": "0.25"},
		},
		{
			name: "sampler alone",
			otel: ateomOTelSettings{Endpoint: endpoint, TracesSampler: "parentbased_always_on"},
			want: map[string]string{"OTEL_TRACES_SAMPLER": "parentbased_always_on"},
		},
		{
			name: "arg alone stays unset",
			otel: ateomOTelSettings{Endpoint: endpoint, TracesSamplerArg: "0.25"},
			want: nil,
		},
		{
			name: "ignored without endpoint",
			otel: ateomOTelSettings{TracesSampler: "parentbased_traceidratio", TracesSamplerArg: "0.25"},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := buildDeploymentApplyConfig(testWorkerPoolApplyConfig(nil), tt.otel).
				Spec.Template.Spec.Containers[0]
			env := envByName(c.Env)
			for _, k := range []string{"OTEL_TRACES_SAMPLER", "OTEL_TRACES_SAMPLER_ARG"} {
				got, ok := env[k]
				want, wantSet := tt.want[k]
				if ok != wantSet {
					t.Errorf("%s present = %v, want %v", k, ok, wantSet)
					continue
				}
				if ok && got.value != want {
					t.Errorf("%s = %q, want %q", k, got.value, want)
				}
			}
		})
	}
}

type envInfo struct {
	index int
	value string
}

var envRefPattern = regexp.MustCompile(`\$\(([A-Za-z_][A-Za-z0-9_]*)\)`)

// envRefs returns the distinct variables a value substitutes, first use first.
func envRefs(value string) []string {
	var refs []string
	seen := make(map[string]bool)
	for _, m := range envRefPattern.FindAllStringSubmatch(value, -1) {
		if seen[m[1]] {
			continue
		}
		seen[m[1]] = true
		refs = append(refs, m[1])
	}
	return refs
}

// envAttrKeys returns the attribute keys of an OTEL_RESOURCE_ATTRIBUTES value.
func envAttrKeys(value string) []string {
	var keys []string
	for _, pair := range strings.Split(value, ",") {
		if k, _, ok := strings.Cut(pair, "="); ok {
			keys = append(keys, k)
		}
	}
	return keys
}

func envByName(env []corev1ac.EnvVarApplyConfiguration) map[string]envInfo {
	m := make(map[string]envInfo, len(env))
	for i, e := range env {
		if e.Name == nil {
			continue
		}
		info := envInfo{index: i}
		if e.Value != nil {
			info.value = *e.Value
		}
		m[*e.Name] = info
	}
	return m
}

func testWorkerPoolApplyConfig(tmpl *atev1alpha1.WorkerPoolPodTemplate) *atev1alpha1.WorkerPool {
	return &atev1alpha1.WorkerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool", Namespace: "default", UID: "uid"},
		Spec: atev1alpha1.WorkerPoolSpec{
			Replicas:    2,
			WorkerImage: "ateom:v1",
			Template:    tmpl,
		},
	}
}

func expectedDeploymentApplyConfig(mutatePodSpec func(*corev1ac.PodSpecApplyConfiguration)) *appsv1ac.DeploymentApplyConfiguration {
	wp := testWorkerPoolApplyConfig(nil)

	podSpecAC := corev1ac.PodSpec().
		WithSecurityContext(corev1ac.PodSecurityContext().
			WithRunAsUser(0).
			WithRunAsGroup(0)).
		WithVolumes(
			corev1ac.Volume().
				WithName("run-ateom").
				WithHostPath(corev1ac.HostPathVolumeSource().
					WithPath(ateompath.BasePath).
					WithType(corev1.HostPathDirectoryOrCreate)),
			corev1ac.Volume().
				WithName(atunnelIdentityVolume).
				WithProjected(corev1ac.ProjectedVolumeSource().
					WithSources(
						corev1ac.VolumeProjection().
							WithPodCertificate(corev1ac.PodCertificateProjection().
								WithSignerName("podidentity.podcert.ate.dev/identity").
								WithKeyType("ECDSAP256").
								WithCredentialBundlePath("credential-bundle.pem")),
						corev1ac.VolumeProjection().
							WithClusterTrustBundle(corev1ac.ClusterTrustBundleProjection().
								WithSignerName("podidentity.podcert.ate.dev/identity").
								WithLabelSelector(metav1ac.LabelSelector().
									WithMatchLabels(map[string]string{"podcert.ate.dev/canarying": "live"})).
								WithPath("trust-bundle.pem")),
					),
				),
			corev1ac.Volume().
				WithName(atunnelEgressTrustVolume).
				WithProjected(corev1ac.ProjectedVolumeSource().
					WithSources(
						corev1ac.VolumeProjection().
							WithClusterTrustBundle(corev1ac.ClusterTrustBundleProjection().
								WithSignerName("servicedns.podcert.ate.dev/identity").
								WithLabelSelector(metav1ac.LabelSelector().
									WithMatchLabels(map[string]string{"podcert.ate.dev/canarying": "live"})).
								WithPath("trust-bundle.pem")),
					),
				),
		).
		WithContainers(corev1ac.Container().
			WithName("ateom").
			WithImage(wp.Spec.WorkerImage).
			WithArgs(
				"--pod-uid=$(POD_UID)",
				"--atunnel-listen-address=:443",
				"--atunnel-connect-listen-address=:8443",
				"--atunnel-credential-bundle="+atunnelIdentityMountPath+"/credential-bundle.pem",
				"--atunnel-trust-bundle="+atunnelIdentityMountPath+"/trust-bundle.pem",
				"--atunnel-egress-listen-address=0.0.0.0:15001",
				"--atunnel-egress-trust-bundle="+atunnelEgressTrustMountPath+"/trust-bundle.pem",
			).
			WithPorts(corev1ac.ContainerPort().
				WithName("https").
				WithContainerPort(443).
				WithProtocol(corev1.ProtocolTCP),
				corev1ac.ContainerPort().
					WithName("connect").
					WithContainerPort(8443).
					WithProtocol(corev1.ProtocolTCP),
				corev1ac.ContainerPort().
					WithName("readyz").
					WithContainerPort(8080).
					WithProtocol(corev1.ProtocolTCP)).
			WithReadinessProbe(corev1ac.Probe().
				WithHTTPGet(corev1ac.HTTPGetAction().
					WithPath("/readyz").
					WithPort(intstr.FromString("readyz")))).
			WithSecurityContext(corev1ac.SecurityContext().
				WithRunAsUser(0).
				WithRunAsGroup(0).
				WithPrivileged(false).
				WithCapabilities(corev1ac.Capabilities().
					WithDrop("ALL").
					WithAdd(ateomGvisorCapabilities...)).
				WithAppArmorProfile(corev1ac.AppArmorProfile().
					WithType(corev1.AppArmorProfileTypeUnconfined)).
				WithSeccompProfile(corev1ac.SeccompProfile().
					WithType(corev1.SeccompProfileTypeUnconfined))).
			WithEnv(
				corev1ac.EnvVar().
					WithName("POD_UID").
					WithValueFrom(corev1ac.EnvVarSource().
						WithFieldRef(corev1ac.ObjectFieldSelector().
							WithFieldPath("metadata.uid"))),
			).
			WithVolumeMounts(
				corev1ac.VolumeMount().
					WithName("run-ateom").
					WithMountPath(ateompath.BasePath).
					WithMountPropagation(corev1.MountPropagationHostToContainer),
				corev1ac.VolumeMount().
					WithName(atunnelIdentityVolume).
					WithMountPath(atunnelIdentityMountPath).
					WithReadOnly(true),
				corev1ac.VolumeMount().
					WithName(atunnelEgressTrustVolume).
					WithMountPath(atunnelEgressTrustMountPath).
					WithReadOnly(true),
			).
			WithResources(corev1ac.ResourceRequirements()))

	podSpecAC.NodeSelector = map[string]string{}
	podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{}
	podSpecAC.WithPriorityClassName("")
	podSpecAC.WithAffinity(corev1ac.Affinity())
	podSpecAC.WithTerminationGracePeriodSeconds(workerTerminationGracePeriodSeconds)
	if mutatePodSpec != nil {
		mutatePodSpec(podSpecAC)
	}

	return appsv1ac.Deployment(wp.Name, wp.Namespace).
		WithLabels(map[string]string{"ate.dev/worker-pool": wp.Name}).
		WithOwnerReferences(metav1ac.OwnerReference().
			WithAPIVersion(atev1alpha1.GroupVersion.String()).
			WithKind("WorkerPool").
			WithName(wp.Name).
			WithUID(wp.UID).
			WithController(true).
			WithBlockOwnerDeletion(true)).
		WithSpec(appsv1ac.DeploymentSpec().
			WithReplicas(wp.Spec.Replicas).
			WithSelector(metav1ac.LabelSelector().
				WithMatchLabels(map[string]string{"ate.dev/worker-pool": wp.Name})).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithLabels(map[string]string{"ate.dev/worker-pool": wp.Name}).
				WithSpec(podSpecAC)))
}
