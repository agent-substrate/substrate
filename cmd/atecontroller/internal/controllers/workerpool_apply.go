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
	"os"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	appsv1ac "k8s.io/client-go/applyconfigurations/apps/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"

	"github.com/agent-substrate/substrate/internal/ateompath"
	"github.com/agent-substrate/substrate/internal/deviceplugin"
	atev1alpha1 "github.com/agent-substrate/substrate/pkg/api/v1alpha1"
)

// ateomOTelResourceAttributes mirrors atelet.yaml; service.instance.id is the pod
// uid so each worker pod is a distinct telemetry source.
const ateomOTelResourceAttributes = "k8s.namespace.name=$(POD_NAMESPACE),k8s.pod.name=$(POD_NAME),k8s.pod.uid=$(POD_UID),service.instance.id=$(POD_UID)"

// workerTerminationGracePeriodSeconds is the hardcoded pod termination grace
// period for worker pods (60 minutes).
const workerTerminationGracePeriodSeconds int64 = 3600

// ateomOTelSettings is the telemetry configuration propagated to ateom worker
// pods. A zero value leaves the pods without telemetry env.
type ateomOTelSettings struct {
	// Endpoint is the OTLP collector address. Empty disables ateom telemetry
	// entirely, so the other fields are ignored.
	Endpoint string
	// MetricExportInterval overrides the SDK's 60s PeriodicReader interval. It is
	// the raw OTEL_METRIC_EXPORT_INTERVAL value, i.e. whole milliseconds; the SDK
	// falls back to its default when it does not parse. Empty keeps the default.
	//
	// ateom registers no instruments of its own, so its only telemetry is
	// otelgrpc's rpc.server.* and it stays invisible to the collector until the
	// first export tick fires. Shortening the interval is what keeps that
	// startup gap inside an e2e budget; production leaves it unset.
	MetricExportInterval string
	// MetricExportTimeout overrides the SDK's  per-export timeout, in the same
	// whole-millisecond form as MetricExportInterval. Empty keeps the default.
	MetricExportTimeout string
	// TracesSampler and TracesSamplerArg are the raw OTEL_TRACES_SAMPLER and
	// OTEL_TRACES_SAMPLER_ARG values, passed through untouched: ateom's own
	// serverboot resolution validates them. Empty sampler keeps the worker's
	// default and drops the arg, which is dead config on its own.
	TracesSampler    string
	TracesSamplerArg string
}

const (
	atunnelIdentityVolume       = "atunnel-identity"
	atunnelIdentityMountPath    = "/run/podidentity.podcert.ate.dev"
	atunnelEgressTrustVolume    = "atunnel-egress-trust"
	atunnelEgressTrustMountPath = "/run/servicedns.podcert.ate.dev"
)

// buildDeploymentApplyConfig constructs the SSA apply configuration for the
// Deployment managed by a WorkerPool. Only fields owned by this controller
// are declared here. otel, when it carries an endpoint, is propagated to the
// ateom container so it pushes telemetry to that collector.
func buildDeploymentApplyConfig(wp *atev1alpha1.WorkerPool, otel ateomOTelSettings) *appsv1ac.DeploymentApplyConfiguration {
	labels := map[string]string{}
	annotations := map[string]string{}
	if wp.Spec.Template != nil {
		for key, value := range wp.Spec.Template.Labels {
			labels[key] = string(value)
		}
		for key, value := range wp.Spec.Template.Annotations {
			annotations[key] = value
		}
	}
	labels["ate.dev/worker-pool"] = wp.Name

	containerAC := corev1ac.Container().
		WithName("ateom").
		WithImage(wp.Spec.AteomImage).
		WithArgs(
			"--pod-uid=$(POD_UID)",
			"--atunnel-listen-address=0.0.0.0:443",
			"--atunnel-connect-listen-address=0.0.0.0:444",
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
				WithContainerPort(444).
				WithProtocol(corev1.ProtocolTCP),
			corev1ac.ContainerPort().
				WithName("readyz").
				WithContainerPort(8080).
				WithProtocol(corev1.ProtocolTCP)).
		WithReadinessProbe(corev1ac.Probe().
			WithHTTPGet(corev1ac.HTTPGetAction().
				WithPath("/readyz").
				WithPort(intstr.FromString("readyz")))).
		WithSecurityContext(ateomSecurityContext(wp.Spec.SandboxClass)).
		WithEnv(ateomContainerEnv(otel)...).
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
		)

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
		)

	applyWorkerPoolPodTemplate(podSpecAC, containerAC, wp.Spec.Template)
	maybeApplyMicroVMPodShape(podSpecAC, containerAC, wp.Spec.SandboxClass)
	maybeApplyGPUPodShape(podSpecAC, containerAC, wp.Spec.Template, wp.Spec.SandboxClass)
	podSpecAC.WithContainers(containerAC)
	podSpecAC.WithTerminationGracePeriodSeconds(workerTerminationGracePeriodSeconds)

	return appsv1ac.Deployment(wp.Name, wp.Namespace).
		WithLabels(labels).
		WithAnnotations(annotations).
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
				WithLabels(labels).
				WithAnnotations(annotations).
				WithSpec(podSpecAC)))
}

// ateomContainerEnv adds the OTLP endpoint and resource identity only when
// telemetry is configured. POD_* refs precede OTEL_RESOURCE_ATTRIBUTES so its
// $(POD_*) substitutions resolve.
func ateomContainerEnv(otel ateomOTelSettings) []*corev1ac.EnvVarApplyConfiguration {
	envs := []*corev1ac.EnvVarApplyConfiguration{
		fieldRefEnv("POD_UID", "metadata.uid"),
	}
	if otel.Endpoint == "" {
		return envs
	}
	envs = append(envs,
		fieldRefEnv("POD_NAME", "metadata.name"),
		fieldRefEnv("POD_NAMESPACE", "metadata.namespace"),
		corev1ac.EnvVar().WithName("OTEL_EXPORTER_OTLP_ENDPOINT").WithValue(otel.Endpoint),
		corev1ac.EnvVar().WithName("OTEL_RESOURCE_ATTRIBUTES").WithValue(ateomOTelResourceAttributes),
	)
	if otel.MetricExportInterval != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_METRIC_EXPORT_INTERVAL").
			WithValue(otel.MetricExportInterval))
	}
	if otel.MetricExportTimeout != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_METRIC_EXPORT_TIMEOUT").
			WithValue(otel.MetricExportTimeout))
	}
	if otel.TracesSampler != "" {
		envs = append(envs, corev1ac.EnvVar().
			WithName("OTEL_TRACES_SAMPLER").
			WithValue(otel.TracesSampler))
		if otel.TracesSamplerArg != "" {
			envs = append(envs, corev1ac.EnvVar().
				WithName("OTEL_TRACES_SAMPLER_ARG").
				WithValue(otel.TracesSamplerArg))
		}
	}
	return envs
}

func fieldRefEnv(name, fieldPath string) *corev1ac.EnvVarApplyConfiguration {
	return corev1ac.EnvVar().
		WithName(name).
		WithValueFrom(corev1ac.EnvVarSource().
			WithFieldRef(corev1ac.ObjectFieldSelector().
				WithFieldPath(fieldPath)))
}

// ateomGvisorCapabilities is the capability set an unprivileged gVisor worker
// needs. runsc's gofer maps a full-range identity in a user namespace
// (SETUID/SETGID/SETPCAP/SETFCAP), the sandbox pivots root and traces the
// application (SYS_ADMIN/SYS_CHROOT/SYS_PTRACE), actor networking programs the
// veth and nftables rules (NET_ADMIN/NET_RAW), and the OCI rootfs is unpacked
// and device nodes created as root over image-owned trees
// (DAC_OVERRIDE/FOWNER/CHOWN/MKNOD). This replaces the former privileged worker;
// seccomp stays at the runtime default, but AppArmor must be Unconfined (see
// ateomSecurityContext) since runsc's own mounts trip the default profile.
var ateomGvisorCapabilities = []corev1.Capability{
	"NET_ADMIN", "SYS_ADMIN", "SYS_CHROOT", "SYS_PTRACE",
	"SETUID", "SETGID", "SETPCAP", "DAC_OVERRIDE",
	"FOWNER", "CHOWN", "MKNOD", "NET_RAW", "SETFCAP",
}

// ateomMicroVMCapabilities is the capability set an unprivileged micro-VM worker
// needs: the gVisor set, which covers the same worker-side work, plus FSETID and
// DAC_READ_SEARCH for virtiofsd. virtiofsd re-applies a fixed set to its
// sandboxed child, and the kernel refuses to raise a capability the bounding set
// omits, so without those two it exits with "can't apply the child capabilities"
// and the VM never gets its virtio-fs device.
//
// The hypervisor devices are not capabilities: they come from atelet's device
// plugin (see maybeApplyMicroVMPodShape).
var ateomMicroVMCapabilities = slices.Concat(ateomGvisorCapabilities, []corev1.Capability{
	"FSETID", "DAC_READ_SEARCH",
})

// ateomSecurityContext returns the ateom container security context for a sandbox
// class. Neither class runs privileged; they differ in their capability set and
// in seccomp. An empty class defaults to gVisor.
func ateomSecurityContext(class atev1alpha1.SandboxClass) *corev1ac.SecurityContextApplyConfiguration {
	// Both runtimes mount inside the worker — runsc pivots root and the worker
	// remounts /sys/fs/cgroup to nest per-actor cgroups; the micro-VM worker
	// shares /run/kata-containers and remounts /proc/sys — and the default
	// AppArmor profile denies mount (enforced on GKE COS and Ubuntu, a no-op on
	// nodes that do not load AppArmor). A privileged worker got this implicitly;
	// unprivileged must request it. Replacing Unconfined with a tailored profile
	// per runtime is a follow-up.
	sc := corev1ac.SecurityContext().
		WithRunAsUser(0).
		WithRunAsGroup(0).
		WithPrivileged(false).
		WithAppArmorProfile(corev1ac.AppArmorProfile().
			WithType(corev1.AppArmorProfileTypeUnconfined))

	if class == atev1alpha1.SandboxClassMicroVM {
		// Give up the default seccomp profile so virtiofsd keeps its own sandbox,
		// which pivot_root()s — a syscall the profile denies whatever capabilities
		// the worker holds. The trade favors containing the guest: virtiofsd parses
		// requests from it, so its namespaces are part of the guest-to-host
		// boundary, while the pod-wide profile mostly confines ateom, which we
		// trust. Both guest-facing processes confine themselves anyway, virtiofsd
		// with its own seccomp filter and nine capabilities, cloud-hypervisor with
		// per-thread filters.
		return sc.
			WithCapabilities(corev1ac.Capabilities().
				WithDrop("ALL").
				WithAdd(ateomMicroVMCapabilities...)).
			WithSeccompProfile(corev1ac.SeccompProfile().
				WithType(corev1.SeccompProfileTypeUnconfined))
	}
	// gVisor needs no such relaxation and keeps the runtime default.
	return sc.
		WithCapabilities(corev1ac.Capabilities().
			WithDrop("ALL").
			WithAdd(ateomGvisorCapabilities...))
}

// maybeApplyMicroVMPodShape adds the /dev/kvm device and node placement a
// micro-VM (kata + cloud-hypervisor) worker pool needs, on top of any
// pod-template settings. No-op unless sandboxClass is the micro-VM class.
//
// TODO: this hardcodes one sandbox class's pod requirements in the controller.
// Consider making it generic so a sandbox class can declare its own pod shape
// (e.g. required devices/mounts + node placement on the SandboxConfig spec)
// instead of branching on SandboxClass here, so new classes don't need a
// controller change.
func maybeApplyMicroVMPodShape(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	sandboxClass atev1alpha1.SandboxClass,
) {
	if sandboxClass != atev1alpha1.SandboxClassMicroVM {
		return
	}

	// The micro-VM runtime needs /dev/kvm to create the VM. Request it as an
	// extended resource served by atelet's device plugin rather than
	// hostPath-mounting it: a container's device access is gated by the cgroup
	// device controller, which denies /dev/kvm by default, and kubelet's device
	// manager is what adds the matching allow rule. A hostPath mount alone
	// yields EPERM unless the pod is privileged, which would hand over every
	// other device on the node too.
	//
	// atelet advertises it only on nodes where the device exists, so the
	// request also keeps the pod off nodes that cannot run a micro-VM.
	addDeviceResourceLimits(containerAC, deviceplugin.ResourceKVM)

	// The runtime also opens /dev/net/tun to build the guest's tap, but that
	// one needs no grant: it is in the runtime's default device allow-list, so
	// a bind mount is enough and creating the tap is a capability question
	// (CAP_NET_ADMIN, above) rather than a device one. That default is a
	// compatibility guarantee rather than an accident — runc dropped the rule
	// once, restored it when it broke users, and crun matched.
	//
	// Advertising tun as a resource would only add a placement constraint that
	// can fail: it is not scarce, and a node where the tun module has yet to
	// load would refuse a worker that needs nothing from it.
	containerAC.WithVolumeMounts(corev1ac.VolumeMount().
		WithName(tunDeviceVolume).
		WithMountPath(tunDevicePath))
	podSpecAC.WithVolumes(corev1ac.Volume().
		WithName(tunDeviceVolume).
		WithHostPath(corev1ac.HostPathVolumeSource().
			WithPath(tunDevicePath).
			WithType(corev1.HostPathCharDev)))

	// Placement onto KVM-capable nodes comes from the device request above: the
	// scheduler only picks nodes advertising the resource, and atelet advertises
	// it only where the device exists. That is derived from the node itself,
	// unlike an ate.dev/sandboxClass label, which is a hand-applied convention
	// that can be wrong in both directions (a labeled node without KVM takes
	// pods that then fail at runtime; an unlabeled KVM node is invisible
	// capacity). Note this makes the pool depend on atelet having registered:
	// with no node advertising the resource, workers stay Pending rather than
	// landing somewhere they cannot run.
	//
	// The toleration stays: extended resources constrain where this pod fits but
	// repel nothing, so a cluster reserving nested-virt nodes with a taint still
	// needs it. Additive on top of the WorkerPool's configurable scheduling
	// fields (spec.template nodeSelector/tolerations/affinity, added in #247) —
	// merge, don't overwrite.
	podSpecAC.WithTolerations(corev1ac.Toleration().
		WithKey("ate.dev/sandboxClass").
		WithOperator(corev1.TolerationOpEqual).
		WithValue(string(atev1alpha1.SandboxClassMicroVM)).
		WithEffect(corev1.TaintEffectNoSchedule))
}

// The tun device node a micro-VM worker bind-mounts to build the guest's tap.
const (
	tunDeviceVolume = "dev-net-tun"
	tunDevicePath   = "/dev/net/tun"
)

// nvidiaToolkitContainerPath is where the host toolkit is mounted inside the
// worker; ateom-gvisor's toolkitDir must match this. It sits outside
// /usr/local/nvidia because the GPU device plugin mounts that tree into the
// container read-only, and a mount cannot create its own mountpoint there, so
// mounting under it only works when the toolkit happens to live inside the
// directory the plugin mounts.
const nvidiaToolkitContainerPath = "/opt/nvidia-toolkit"

// defaultNvidiaToolkitHostPath is where gpu-operator installs the toolkit
// (toolkit.installDir defaults to /usr/local/nvidia). It is deliberately not the
// container path above: the two are independent, since where the node keeps the
// toolkit says nothing about where we can mount it.
const defaultNvidiaToolkitHostPath = "/usr/local/nvidia/toolkit"

// nvidiaDriverRootEnv names the directory the GPU device plugin mounts the driver
// into a pod at. ateom derives the driver library and binary paths from it, both of
// which nvidia-ctk needs to generate a CDI spec. Only set it when the cluster's
// device plugin does not use the /usr/local/nvidia convention; the controller
// forwards its own value onto GPU worker pods.
const nvidiaDriverRootEnv = "ATE_NVIDIA_DRIVER_ROOT"

// nvidiaToolkitHostPath is where the NVIDIA container toolkit lives on the node.
// It is platform-specific: gpu-operator and EKS install it at
// /usr/local/nvidia/toolkit, while GKE keeps NVIDIA assets under
// /home/kubernetes/bin/nvidia, so it is overridable via the
// ATE_NVIDIA_TOOLKIT_HOST_PATH env var on the controller. We mount it read-only
// so nvidia-ctk / nvidia-cdi-hook match whatever toolkit/driver the cluster runs.
var nvidiaToolkitHostPath = envOrDefault("ATE_NVIDIA_TOOLKIT_HOST_PATH", defaultNvidiaToolkitHostPath)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// maybeApplyGPUPodShape shapes a gVisor worker pod that requests a GPU so ateom
// can inject the GPU into actors via CDI. It mounts the host NVIDIA toolkit
// (version-matched to the node) read-only, for the glibc-based ateom image to run
// directly. The pod keeps the same security posture as any other gVisor worker.
// No-op for non-GPU pools and non-gVisor classes; an empty class defaults to
// gVisor (WorkerPoolSpec kubebuilder default).
func maybeApplyGPUPodShape(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	tmpl *atev1alpha1.WorkerPoolPodTemplate,
	sandboxClass atev1alpha1.SandboxClass,
) {
	if sandboxClass != atev1alpha1.SandboxClassGvisor && sandboxClass != "" {
		return
	}
	if !templateRequestsGPU(tmpl) {
		return
	}
	// Mount the host NVIDIA toolkit (version-matched to the node) read-only.
	containerAC.WithVolumeMounts(corev1ac.VolumeMount().
		WithName("nvidia-toolkit").
		WithMountPath(nvidiaToolkitContainerPath).
		WithReadOnly(true))
	podSpecAC.WithVolumes(corev1ac.Volume().
		WithName("nvidia-toolkit").
		WithHostPath(corev1ac.HostPathVolumeSource().
			WithPath(nvidiaToolkitHostPath).
			WithType(corev1.HostPathDirectory)))
	// Only propagated when set, so a default deployment adds no env to worker pods.
	if root := os.Getenv(nvidiaDriverRootEnv); root != "" {
		containerAC.WithEnv(corev1ac.EnvVar().WithName(nvidiaDriverRootEnv).WithValue(root))
	}
}

// templateRequestsGPU reports whether the pool template requests one or more
// nvidia.com/gpu devices (limits or requests).
func templateRequestsGPU(tmpl *atev1alpha1.WorkerPoolPodTemplate) bool {
	if tmpl == nil || tmpl.Resources == nil {
		return false
	}
	const gpu = corev1.ResourceName("nvidia.com/gpu")
	if q, ok := tmpl.Resources.Limits[gpu]; ok && !q.IsZero() {
		return true
	}
	if q, ok := tmpl.Resources.Requests[gpu]; ok && !q.IsZero() {
		return true
	}
	return false
}

// addDeviceResourceLimits requests one unit of each named extended resource,
// merging into whatever limits the pod template already set.
func addDeviceResourceLimits(containerAC *corev1ac.ContainerApplyConfiguration, resourceNames ...string) {
	if containerAC.Resources == nil {
		containerAC.WithResources(corev1ac.ResourceRequirements())
	}
	limits := corev1.ResourceList{}
	if containerAC.Resources.Limits != nil {
		limits = *containerAC.Resources.Limits
	}
	for _, name := range resourceNames {
		limits[corev1.ResourceName(name)] = resource.MustParse("1")
	}
	containerAC.Resources.WithLimits(limits)
}

func applyWorkerPoolPodTemplate(
	podSpecAC *corev1ac.PodSpecApplyConfiguration,
	containerAC *corev1ac.ContainerApplyConfiguration,
	tmpl *atev1alpha1.WorkerPoolPodTemplate,
) {
	podSpecAC.NodeSelector = map[string]string{}
	podSpecAC.Tolerations = []corev1ac.TolerationApplyConfiguration{}
	podSpecAC.WithPriorityClassName("")
	podSpecAC.WithAffinity(corev1ac.Affinity())
	resourcesAC := corev1ac.ResourceRequirements()
	containerAC.WithResources(resourcesAC)

	if tmpl == nil {
		return
	}

	if tmpl.NodeSelector != nil {
		podSpecAC.WithNodeSelector(tmpl.NodeSelector)
	}
	podSpecAC.Tolerations = tolerationApplyValues(tolerationsToApply(tmpl.Tolerations))
	podSpecAC.WithPriorityClassName(tmpl.PriorityClassName)

	if tmpl.NodeAffinity != nil {
		podSpecAC.WithAffinity(corev1ac.Affinity().WithNodeAffinity(nodeAffinityToApply(tmpl.NodeAffinity)))
	}

	if tmpl.Resources != nil {
		if tmpl.Resources.Requests != nil {
			resourcesAC.WithRequests(tmpl.Resources.Requests)
		}
		if tmpl.Resources.Limits != nil {
			resourcesAC.WithLimits(tmpl.Resources.Limits)
		}
	}
}

func tolerationApplyValues(tolerations []*corev1ac.TolerationApplyConfiguration) []corev1ac.TolerationApplyConfiguration {
	out := make([]corev1ac.TolerationApplyConfiguration, 0, len(tolerations))
	for _, toleration := range tolerations {
		out = append(out, *toleration)
	}
	return out
}

func tolerationsToApply(tolerations []corev1.Toleration) []*corev1ac.TolerationApplyConfiguration {
	out := make([]*corev1ac.TolerationApplyConfiguration, 0, len(tolerations))
	for i := range tolerations {
		t := &tolerations[i]
		ac := corev1ac.Toleration()
		if t.Key != "" {
			ac.WithKey(t.Key)
		}
		if t.Operator != "" {
			ac.WithOperator(t.Operator)
		}
		if t.Value != "" {
			ac.WithValue(t.Value)
		}
		if t.Effect != "" {
			ac.WithEffect(t.Effect)
		}
		if t.TolerationSeconds != nil {
			ac.WithTolerationSeconds(*t.TolerationSeconds)
		}
		out = append(out, ac)
	}
	return out
}

func nodeAffinityToApply(na *corev1.NodeAffinity) *corev1ac.NodeAffinityApplyConfiguration {
	ac := corev1ac.NodeAffinity()
	if na.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		ac.WithRequiredDuringSchedulingIgnoredDuringExecution(nodeSelectorToApply(na.RequiredDuringSchedulingIgnoredDuringExecution))
	}
	for i := range na.PreferredDuringSchedulingIgnoredDuringExecution {
		term := &na.PreferredDuringSchedulingIgnoredDuringExecution[i]
		ac.WithPreferredDuringSchedulingIgnoredDuringExecution(preferredSchedulingTermToApply(term))
	}
	return ac
}

func nodeSelectorToApply(ns *corev1.NodeSelector) *corev1ac.NodeSelectorApplyConfiguration {
	ac := corev1ac.NodeSelector()
	for i := range ns.NodeSelectorTerms {
		ac.WithNodeSelectorTerms(nodeSelectorTermToApply(&ns.NodeSelectorTerms[i]))
	}
	return ac
}

func preferredSchedulingTermToApply(term *corev1.PreferredSchedulingTerm) *corev1ac.PreferredSchedulingTermApplyConfiguration {
	return corev1ac.PreferredSchedulingTerm().
		WithWeight(term.Weight).
		WithPreference(nodeSelectorTermToApply(&term.Preference))
}

func nodeSelectorTermToApply(term *corev1.NodeSelectorTerm) *corev1ac.NodeSelectorTermApplyConfiguration {
	ac := corev1ac.NodeSelectorTerm()
	for i := range term.MatchExpressions {
		ac.WithMatchExpressions(nodeSelectorRequirementToApply(&term.MatchExpressions[i]))
	}
	for i := range term.MatchFields {
		ac.WithMatchFields(nodeSelectorRequirementToApply(&term.MatchFields[i]))
	}
	return ac
}

func nodeSelectorRequirementToApply(req *corev1.NodeSelectorRequirement) *corev1ac.NodeSelectorRequirementApplyConfiguration {
	ac := corev1ac.NodeSelectorRequirement().WithKey(req.Key).WithOperator(req.Operator)
	if len(req.Values) > 0 {
		ac.WithValues(req.Values...)
	}
	return ac
}
