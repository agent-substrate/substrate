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
	"maps"
	"path/filepath"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kustomize"
)

// systemPod is one ate-system workload's pod template, as the NetworkPolicies
// see it.
type systemPod struct {
	workload string // kind/name, for messages
	labels   map[string]string
	ports    []corev1.ContainerPort
}

// systemInstall is everything an install of one configuration puts in
// ate-system: the system overlay plus the manifests the installer applies on
// their own (postgres, the egress gateway, the bundled credential provider).
type systemInstall struct {
	pods     []systemPod
	policies []networkingv1.NetworkPolicy
}

// unadmittedPorts are the container ports no NetworkPolicy may admit, keyed
// by the pod's app label and the port's name. Every other container port of an
// ate-system pod must be admitted by some policy, so a new port fails
// TestSystemNetworkPoliciesCoverEveryPort until someone decides which list it
// belongs on, rather than being silently dropped by the default deny.
var unadmittedPorts = map[string][]string{
	// /statusz exposes cross-tenant request history (#2275); reach it with a
	// port-forward.
	"atenet-router": {"status", "extproc"},
	// ext_proc is served to the envoy container on loopback.
	"atenet-egress": {"extproc"},
	// The provider ships its own policy admitting only the egress gateway's
	// gRPC, and docs/egress-credential-injection.md promises nothing else
	// gets in; its probes come from the node.
	"k8s-credential-provider": {"statusz", "metrics", "health"},
	// kind only: the rustfs web console.
	"rustfs": {"console"},
}

// installConfigs are the configurations whose ate-system the policies must
// cover: both dataplanes, on GKE and on kind, each with the real and the fake
// data plane.
func installConfigs() []*config.Config {
	var out []*config.Config
	for _, kind := range []bool{false, true} {
		for _, router := range []string{config.RouterEnvoy, config.RouterAgentgateway} {
			for _, fake := range []bool{false, true} {
				out = append(out, &config.Config{Kind: kind, Router: router, FakeDataPlane: fake})
			}
		}
	}
	return out
}

func installName(cfg *config.Config) string {
	env := "gke"
	if cfg.Kind {
		env = "kind"
	}
	name := env + "-" + cfg.Router
	if cfg.FakeDataPlane {
		name += "-fake"
	}
	return name
}

func loadSystemInstall(t *testing.T, cfg *config.Config) systemInstall {
	t.Helper()
	root := repoRoot(t)
	cfg.Root = root
	e := &Env{Cfg: cfg}

	var objs []*unstructured.Unstructured
	add := func(what string, rendered []byte, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("rendering %s: %v", what, err)
		}
		decoded, err := kube.DecodeManifestBytes(rendered)
		if err != nil {
			t.Fatalf("decoding %s: %v", what, err)
		}
		objs = append(objs, decoded...)
	}
	system := filepath.Join(root, SystemOverlay(cfg))
	rendered, err := e.render(system)
	add(system, rendered, err)
	rendered, err = e.render(e.postgresManifestPath())
	add(e.postgresManifestPath(), rendered, err)
	if cfg.Router == config.RouterAgentgateway {
		egress := filepath.Join(root, installDir, "agentgateway-egress")
		rendered, err = kustomize.Build(egress)
		add(egress, rendered, err)
	} else {
		loaded, err := kube.LoadPath(e.atenetEgressManifestPath())
		if err != nil {
			t.Fatalf("loading %s: %v", e.atenetEgressManifestPath(), err)
		}
		objs = append(objs, loaded...)
	}
	provider, err := kube.LoadPath(e.k8sCredentialProviderPath(k8sCredentialProviderManifest))
	if err != nil {
		t.Fatalf("loading the credential provider manifest: %v", err)
	}
	objs = append(objs, provider...)

	var install systemInstall
	for _, obj := range objs {
		if obj.GetNamespace() != NamespaceAteSystem {
			continue
		}
		switch obj.GetKind() {
		case "NetworkPolicy":
			var np networkingv1.NetworkPolicy
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, &np); err != nil {
				t.Fatalf("converting networkpolicy/%s: %v", obj.GetName(), err)
			}
			install.policies = append(install.policies, np)
		case "Deployment", "DaemonSet", "StatefulSet", "Job":
			raw, ok, _ := unstructured.NestedMap(obj.Object, "spec", "template")
			if !ok {
				t.Fatalf("%s/%s has no pod template", obj.GetKind(), obj.GetName())
			}
			var tmpl corev1.PodTemplateSpec
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(raw, &tmpl); err != nil {
				t.Fatalf("converting the pod template of %s/%s: %v", obj.GetKind(), obj.GetName(), err)
			}
			pod := systemPod{workload: obj.GetKind() + "/" + obj.GetName(), labels: tmpl.Labels}
			if obj.GetKind() == "Job" {
				// The Job controller adds this label to its pods.
				pod.labels = map[string]string{"batch.kubernetes.io/job-name": obj.GetName()}
				for k, v := range tmpl.Labels {
					pod.labels[k] = v
				}
			}
			for _, c := range slices.Concat(tmpl.Spec.InitContainers, tmpl.Spec.Containers) {
				pod.ports = append(pod.ports, c.Ports...)
			}
			install.pods = append(install.pods, pod)
		}
	}
	if len(install.pods) == 0 {
		t.Fatal("found no ate-system workloads; the manifests changed shape and this test is checking nothing")
	}
	return install
}

func mustSelector(t *testing.T, sel *metav1.LabelSelector) labels.Selector {
	t.Helper()
	s, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		t.Fatalf("invalid label selector %v: %v", sel, err)
	}
	return s
}

// policiesFor returns the policies that select pod for ingress.
func (in systemInstall) policiesFor(t *testing.T, pod systemPod) []networkingv1.NetworkPolicy {
	t.Helper()
	var out []networkingv1.NetworkPolicy
	for _, np := range in.policies {
		if !slices.Contains(np.Spec.PolicyTypes, networkingv1.PolicyTypeIngress) {
			continue
		}
		if mustSelector(t, &np.Spec.PodSelector).Matches(labels.Set(pod.labels)) {
			out = append(out, np)
		}
	}
	return out
}

// portMatches reports whether a policy port admits the container port.
func portMatches(pp networkingv1.NetworkPolicyPort, cp corev1.ContainerPort) bool {
	proto := corev1.ProtocolTCP
	if pp.Protocol != nil {
		proto = *pp.Protocol
	}
	cproto := cp.Protocol
	if cproto == "" {
		cproto = corev1.ProtocolTCP
	}
	if proto != cproto {
		return false
	}
	if pp.Port == nil {
		return true
	}
	if pp.Port.StrVal != "" {
		return cp.Name != "" && pp.Port.StrVal == cp.Name
	}
	return pp.Port.IntVal == cp.ContainerPort
}

// admittingRules returns the ingress rules, across every policy selecting pod,
// that admit the container port.
func (in systemInstall) admittingRules(t *testing.T, pod systemPod, cp corev1.ContainerPort) []networkingv1.NetworkPolicyIngressRule {
	t.Helper()
	var out []networkingv1.NetworkPolicyIngressRule
	for _, np := range in.policiesFor(t, pod) {
		for _, rule := range np.Spec.Ingress {
			if len(rule.Ports) == 0 {
				out = append(out, rule)
				continue
			}
			for _, pp := range rule.Ports {
				if portMatches(pp, cp) {
					out = append(out, rule)
					break
				}
			}
		}
	}
	return out
}

func (in systemInstall) pod(t *testing.T, app string) systemPod {
	t.Helper()
	for _, p := range in.pods {
		if p.labels["app"] == app {
			return p
		}
	}
	t.Fatalf("no ate-system workload with app=%s", app)
	return systemPod{}
}

func namedPort(t *testing.T, pod systemPod, name string) corev1.ContainerPort {
	t.Helper()
	for _, p := range pod.ports {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("%s has no container port named %q", pod.workload, name)
	return corev1.ContainerPort{}
}

func numberedPort(t *testing.T, pod systemPod, n int32) corev1.ContainerPort {
	t.Helper()
	for _, p := range pod.ports {
		if p.ContainerPort == n {
			return p
		}
	}
	t.Fatalf("%s has no container port %d", pod.workload, n)
	return corev1.ContainerPort{}
}

// requireOnlyFromApps fails unless every rule admitting the port names only
// ate-system pods whose app label is one of apps.
func requireOnlyFromApps(t *testing.T, in systemInstall, pod systemPod, cp corev1.ContainerPort, apps ...string) {
	t.Helper()
	rules := in.admittingRules(t, pod, cp)
	if len(rules) == 0 {
		t.Errorf("%s port %d (%s) is not admitted from anywhere, want from app in %v", pod.workload, cp.ContainerPort, cp.Name, apps)
	}
	for _, rule := range rules {
		if len(rule.From) == 0 {
			t.Errorf("%s port %d (%s) is admitted from any source, want only app in %v", pod.workload, cp.ContainerPort, cp.Name, apps)
			continue
		}
		for _, peer := range rule.From {
			if peer.NamespaceSelector != nil || peer.IPBlock != nil || peer.PodSelector == nil {
				t.Errorf("%s port %d (%s) admits peer %+v, want only ate-system pods with app in %v", pod.workload, cp.ContainerPort, cp.Name, peer, apps)
				continue
			}
			sel := mustSelector(t, peer.PodSelector)
			if sel.Empty() {
				t.Errorf("%s port %d (%s) admits every ate-system pod, want only app in %v", pod.workload, cp.ContainerPort, cp.Name, apps)
				continue
			}
			// The peer must not select a pod outside apps.
			for _, other := range in.pods {
				if sel.Matches(labels.Set(other.labels)) && !slices.Contains(apps, other.labels["app"]) {
					t.Errorf("%s port %d (%s) admits %s, want only app in %v", pod.workload, cp.ContainerPort, cp.Name, other.workload, apps)
				}
			}
		}
	}
}

// admitsFrom reports whether a rule admitting the port lets in the ate-system
// pod client.
func (in systemInstall) admitsFrom(t *testing.T, pod systemPod, cp corev1.ContainerPort, client systemPod) bool {
	t.Helper()
	for _, rule := range in.admittingRules(t, pod, cp) {
		if len(rule.From) == 0 {
			return true
		}
		for _, peer := range rule.From {
			if peer.PodSelector != nil && peer.NamespaceSelector == nil && peer.IPBlock == nil &&
				mustSelector(t, peer.PodSelector).Matches(labels.Set(client.labels)) {
				return true
			}
		}
	}
	return false
}

// TestSystemNetworkPoliciesAdmitFakeDataPlane requires the fake data plane's
// own flows to get through the default deny: fake-workersync to
// ate-api-server, which it calls in ate-controller's place, and to the relay
// on each fake-atelet.
func TestSystemNetworkPoliciesAdmitFakeDataPlane(t *testing.T) {
	for _, cfg := range installConfigs() {
		if !cfg.FakeDataPlane {
			continue
		}
		t.Run(installName(cfg), func(t *testing.T) {
			in := loadSystemInstall(t, cfg)
			workersync := in.pod(t, "fake-workersync")
			api := in.pod(t, "ate-api-server")
			if !in.admitsFrom(t, api, numberedPort(t, api, 443), workersync) {
				t.Error("ate-api-server port 443 does not admit fake-workersync")
			}
			atelet := in.pod(t, "atelet")
			if !in.admitsFrom(t, atelet, namedPort(t, atelet, "relay"), workersync) {
				t.Error("fake-atelet's relay port does not admit fake-workersync")
			}
		})
	}
}

// TestSystemNetworkPoliciesDefaultDeny requires every install to carry a
// policy that selects every ate-system pod for ingress and admits nothing.
func TestSystemNetworkPoliciesDefaultDeny(t *testing.T) {
	for _, cfg := range installConfigs() {
		t.Run(installName(cfg), func(t *testing.T) {
			in := loadSystemInstall(t, cfg)
			for _, np := range in.policies {
				if len(np.Spec.PodSelector.MatchLabels) == 0 && len(np.Spec.PodSelector.MatchExpressions) == 0 &&
					slices.Equal(np.Spec.PolicyTypes, []networkingv1.PolicyType{networkingv1.PolicyTypeIngress}) &&
					len(np.Spec.Ingress) == 0 {
					return
				}
			}
			t.Error("no NetworkPolicy denies ingress to every ate-system pod by default")
		})
	}
}

// TestSystemNetworkPoliciesCoverEveryPort requires each container port of an
// ate-system pod to be either admitted by a policy or listed in
// unadmittedPorts, and the listed ones to be admitted by none.
func TestSystemNetworkPoliciesCoverEveryPort(t *testing.T) {
	for _, cfg := range installConfigs() {
		t.Run(installName(cfg), func(t *testing.T) {
			in := loadSystemInstall(t, cfg)
			for _, pod := range in.pods {
				for _, cp := range pod.ports {
					denied := slices.Contains(unadmittedPorts[pod.labels["app"]], cp.Name)
					admitted := len(in.admittingRules(t, pod, cp)) > 0
					switch {
					case denied && admitted:
						t.Errorf("%s port %d (%s) must not be admitted by any NetworkPolicy", pod.workload, cp.ContainerPort, cp.Name)
					case !denied && !admitted:
						t.Errorf("%s port %d (%s) is admitted by no NetworkPolicy, so the default deny drops it; admit it in manifests/ate-install/ate-system-networkpolicies.yaml or add it to unadmittedPorts", pod.workload, cp.ContainerPort, cp.Name)
					}
				}
			}
		})
	}
}

// TestSystemNetworkPoliciesNamedPortsResolve catches a policy port name that
// no selected pod declares, which Kubernetes accepts and silently matches
// nothing. Port names differ between the dataplanes (agentgateway has "stats",
// envoy "envoy-metrics"), so a name only has to resolve in one install.
func TestSystemNetworkPoliciesNamedPortsResolve(t *testing.T) {
	type policyPort struct{ policy, port string }
	referenced := map[policyPort]bool{}
	resolved := map[policyPort]bool{}
	for _, cfg := range installConfigs() {
		in := loadSystemInstall(t, cfg)
		for _, np := range in.policies {
			sel := mustSelector(t, &np.Spec.PodSelector)
			for _, rule := range np.Spec.Ingress {
				for _, pp := range rule.Ports {
					if pp.Port == nil || pp.Port.StrVal == "" {
						continue
					}
					key := policyPort{np.Name, pp.Port.StrVal}
					referenced[key] = true
					for _, pod := range in.pods {
						if !sel.Matches(labels.Set(pod.labels)) {
							continue
						}
						for _, cp := range pod.ports {
							if cp.Name == pp.Port.StrVal {
								resolved[key] = true
							}
						}
					}
				}
			}
		}
	}
	if len(referenced) == 0 {
		t.Fatal("no NetworkPolicy names a port; the manifests changed shape and this test is checking nothing")
	}
	for key := range referenced {
		if !resolved[key] {
			t.Errorf("networkpolicy/%s names port %q, which no pod it selects declares in any install", key.policy, key.port)
		}
	}
}

// TestSystemNetworkPoliciesLeastPrivilege pins who may reach the sensitive
// ports.
func TestSystemNetworkPoliciesLeastPrivilege(t *testing.T) {
	for _, cfg := range installConfigs() {
		t.Run(installName(cfg), func(t *testing.T) {
			in := loadSystemInstall(t, cfg)

			postgres := in.pod(t, "postgres")
			requireOnlyFromApps(t, in, postgres, namedPort(t, postgres, "postgres"), "ate-api-server")

			atelet := in.pod(t, "atelet")
			requireOnlyFromApps(t, in, atelet, namedPort(t, atelet, "grpc"), "ate-api-server")
			if cfg.FakeDataPlane {
				requireOnlyFromApps(t, in, atelet, namedPort(t, atelet, "relay"), "fake-workersync")
			}

			// The API admits the control plane and opted-in namespaces only.
			api := in.pod(t, "ate-api-server")
			for _, rule := range in.admittingRules(t, api, numberedPort(t, api, 443)) {
				if len(rule.From) == 0 {
					t.Error("ate-api-server port 443 is admitted from any source")
				}
				for _, peer := range rule.From {
					if peer.IPBlock != nil {
						t.Errorf("ate-api-server port 443 admits ipBlock %v", peer.IPBlock)
					}
					if peer.NamespaceSelector != nil {
						if want := map[string]string{"ate.dev/api-access": "true"}; !maps.Equal(peer.NamespaceSelector.MatchLabels, want) || len(peer.NamespaceSelector.MatchExpressions) != 0 {
							t.Errorf("ate-api-server port 443 admits namespaces %v, want only those labeled %v", peer.NamespaceSelector, want)
						}
						// An opted-in namespace may host a WorkerPool; its worker
						// pods never call the API and must stay out.
						if peer.PodSelector == nil {
							t.Errorf("ate-api-server port 443 admits every pod of an opted-in namespace, including worker pods")
							continue
						}
						sel := mustSelector(t, peer.PodSelector)
						if sel.Matches(labels.Set{workerPoolLabelForTest: "any"}) {
							t.Errorf("ate-api-server port 443 admits worker pods (%s) of opted-in namespaces", workerPoolLabelForTest)
						}
						if !sel.Matches(labels.Set{"app": "dashboard"}) {
							t.Errorf("ate-api-server port 443 namespace peer %v does not admit ordinary pods of opted-in namespaces", peer.PodSelector)
						}
						continue
					}
					if peer.PodSelector == nil || mustSelector(t, peer.PodSelector).Empty() {
						t.Errorf("ate-api-server port 443 admits every ate-system pod (%+v)", peer)
					}
				}
			}

			// The egress gateway admits worker pods only.
			egress := in.pod(t, "atenet-egress")
			rules := in.admittingRules(t, egress, namedPort(t, egress, "https"))
			if len(rules) == 0 {
				t.Error("atenet-egress https is admitted from nowhere; worker pods cannot reach the egress gateway")
			}
			for _, rule := range rules {
				if len(rule.From) == 0 {
					t.Error("atenet-egress https is admitted from any source, want worker pods only")
				}
				for _, peer := range rule.From {
					// Workers live in their WorkerPool's namespace, so the peer
					// has to reach across every namespace, not just ate-system.
					if peer.NamespaceSelector == nil || !mustSelector(t, peer.NamespaceSelector).Empty() {
						t.Errorf("atenet-egress https admits peer %+v, want worker pods from every namespace (namespaceSelector: {})", peer)
					}
					if peer.PodSelector == nil || !mustSelector(t, peer.PodSelector).Matches(labels.Set{workerPoolLabelForTest: "any"}) ||
						mustSelector(t, peer.PodSelector).Matches(labels.Set{"app": "probe"}) {
						t.Errorf("atenet-egress https admits peer %+v, want pods labeled %s in any namespace", peer, workerPoolLabelForTest)
					}
				}
			}
		})
	}
}

// workerPoolLabelForTest is the label the WorkerPool controller puts on
// worker pods (cmd/atecontroller/internal/controllers/workerpool_apply.go).
const workerPoolLabelForTest = "ate.dev/worker-pool"
