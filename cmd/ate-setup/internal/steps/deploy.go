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
	"slices"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// crdGVK is the kind of a CustomResourceDefinition.
var crdGVK = schema.GroupVersionKind{
	Group:   "apiextensions.k8s.io",
	Version: "v1",
	Kind:    "CustomResourceDefinition",
}

// DeployOptions carries the per-invocation choices for DeployAteSystem.
type DeployOptions struct {
	// SetupCSI additionally installs the CSI driver (nfs, hostpath, both, none).
	// Kind only. The hostpath driver is Kind only.
	SetupCSI string
}

// Validate checks the options that can be checked without configuration or a
// cluster, so the command can reject them while cobra is still parsing.
func (o DeployOptions) Validate() error {
	return ValidateCSIDriver(o.SetupCSI)
}

// DeployAteSystem installs the whole control plane: CRDs, RBAC, the
// podcertificate controller, the store, ateapi, the controller, atenet, and
// atelet.
func (e *Env) DeployAteSystem(ctx context.Context, opts DeployOptions) error {
	log.Step("deploy_ate_system")

	// This step applies the checked-in manifests, so it refuses a relocated
	// namespace before creating anything.
	if err := e.RequireCanonicalNamespace("deploy ate-system"); err != nil {
		return err
	}
	// Fail fast on an unusable credential provider selection before touching
	// the cluster.
	provider, err := e.Cfg.CredentialProvider()
	if err != nil {
		return err
	}
	// Likewise the CSI request, even though it is only acted on partway
	// through: a driver this cluster cannot run is worth knowing before the
	// control plane goes up, not after.
	if err := e.CheckCSIDriver(opts.SetupCSI); err != nil {
		return err
	}
	if err := e.requireNoWorkerPods(ctx); err != nil {
		return err
	}

	// The namespace has to exist before RBAC or CRDs are applied.
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}

	// DeployCRDs, not EnsureCRDs: an existence check would skip upgrades,
	// stranding stale CRD schemas and RBAC (role.yaml has no other apply
	// path).
	if err := e.DeployCRDs(ctx); err != nil {
		return err
	}

	if err := e.EnsureAPIServerPrerequisites(ctx); err != nil {
		return err
	}

	// The podcertificate controller goes first so it starts signing and
	// publishing trust bundles immediately.
	if err := e.DeployPodCertificateController(ctx); err != nil {
		return err
	}
	if err := e.SetupCSI(ctx, opts.SetupCSI); err != nil {
		return err
	}

	if err := e.DeploySandboxConfig(ctx); err != nil {
		return err
	}

	// Ahead of the bundle below, for the same reason as the namespace: every
	// workload pulls this ConfigMap in via envFrom, and a container whose
	// envFrom target is missing will not start.
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}

	// Resolved before the bundle apply: adopting a Cloud SQL instance reads
	// the ConfigMap that EnsureAPIServerPrerequisites has already rewritten,
	// and the answer decides both the apply and the rollout wait below. The
	// bundled database is rendered at its final size before the apply, so the
	// rollout wait covers the one pod that will actually serve.
	postgres, err := e.planPostgres(ctx)
	if err != nil {
		return err
	}
	if err := e.deployPostgres(ctx, postgres); err != nil {
		return err
	}

	manifests, err := e.renderSystemManifests(ctx)
	if err != nil {
		return err
	}
	// Before the bundle, which starts ate-controller on the real data plane.
	if err := e.removeFakeWorkersync(ctx); err != nil {
		return err
	}
	if err := e.applyWithAPIServerEnvHash(ctx, manifests); err != nil {
		return err
	}

	// After the bundle, which resets the pod template to the sidecar-free base.
	if err := e.reconcileCloudSQLProxySidecar(ctx); err != nil {
		return err
	}

	// Deploy egress gateway explicitly so kind and experimental modes are applied.
	if err := e.EnsureEgressMITMCAPoolSecret(ctx); err != nil {
		return err
	}
	// After the podcertificate controller: the provider serves with a
	// projected pod certificate.
	if provider.Kubernetes() {
		err = e.deployK8sCredentialProvider(ctx)
	} else {
		err = e.removeK8sCredentialProvider(ctx)
	}
	if err != nil {
		return err
	}
	if err := e.applyAtenetEgress(ctx, provider); err != nil {
		return err
	}

	log.Step("Waiting for ATE system components to be ready...")
	type rollout struct{ kind, name string }
	var waits []rollout
	waits = append(waits,
		rollout{kube.KindDeployment, "ate-api-server"},
		rollout{kube.KindDeployment, "ate-controller"},
		rollout{kube.KindDeployment, "atenet-router"},
		rollout{kube.KindDeployment, "atenet-egress"},
		rollout{kube.KindDaemonSet, "atelet"},
	)
	if provider.Kubernetes() {
		waits = append(waits, rollout{kube.KindDeployment, k8sCredentialProviderDeployment})
	}
	for _, w := range waits {
		if err := e.Kube.RolloutStatus(ctx, w.kind, e.Namespace(), w.name, e.Cfg.RolloutTimeout); err != nil {
			return err
		}
	}
	if err := e.waitFakeWorkersync(ctx); err != nil {
		return err
	}
	return e.applyOtelEndpointOverride(ctx)
}

// DeployPodCertificateController creates any missing signer CA pools, applies
// the podcertificate controller, and waits until it is rolling and has
// published its trust bundles.
//
// size10 clusters render the podcert-size10 overlay instead of the base file.
// It appends --kube-api-qps=100 / --kube-api-burst=200 so the controller keeps
// up with the request volume the size10 postgres profile enables. An overlay
// rather than a post-apply patch, so that no later apply of the base file can
// reconcile the flags away; the base kustomization leaves the file out for the
// same reason.
//
// --podcert-workers-per-signer is set on the rendered objects for the same
// reason: one apply means one rollout, and the rollout wait sees the pod that
// will actually serve.
func (e *Env) DeployPodCertificateController(ctx context.Context) error {
	// The controller mounts every pool, so one that an older release did not
	// create keeps the new pod in ContainerCreating.
	if err := e.EnsurePodCertificateCAs(ctx); err != nil {
		return err
	}
	path := e.Cfg.Manifest("pod-certificate-controller.yaml")
	if e.Cfg.Size10() {
		path = e.Cfg.Manifest("podcert-size10")
	}
	manifest, err := e.renderResolve(ctx, path)
	if err != nil {
		return err
	}
	objs, err := kube.DecodeManifestBytes(manifest)
	if err != nil {
		return err
	}
	if e.Cfg.PodcertWorkersPerSigner > 0 {
		log.Infof("Setting WORKERS_PER_SIGNER to %d", e.Cfg.PodcertWorkersPerSigner)
		if err := setPodcertWorkersPerSigner(objs, e.Cfg.PodcertWorkersPerSigner); err != nil {
			return err
		}
	}
	if err := e.Kube.Apply(ctx, objs); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, NamespacePodCert, "podcertificate-controller", e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	return e.WaitForPodCertificateTrustBundles(ctx)
}

// setPodcertWorkersPerSigner sets WORKERS_PER_SIGNER on the
// podcertificate-controller container in objs, the variable its
// --workers-per-signer argument expands.
func setPodcertWorkersPerSigner(objs []*unstructured.Unstructured, workers int) error {
	var dep *unstructured.Unstructured
	for _, obj := range objs {
		if obj.GetKind() == "Deployment" && obj.GetNamespace() == NamespacePodCert && obj.GetName() == "podcertificate-controller" {
			dep = obj
			break
		}
	}
	if dep == nil {
		return fmt.Errorf("the podcertificate-controller manifest has no deployment/podcertificate-controller")
	}

	containers, found, err := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		return fmt.Errorf("%s has no containers", kube.Describe(dep))
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return fmt.Errorf("%s: container 0 is not an object", kube.Describe(dep))
	}
	env, _, err := unstructured.NestedSlice(container, "env")
	if err != nil {
		return fmt.Errorf("%s: %w", kube.Describe(dep), err)
	}
	value := strconv.Itoa(workers)
	found = false
	for i, v := range env {
		envVar, ok := v.(map[string]any)
		if ok && envVar["name"] == "WORKERS_PER_SIGNER" {
			env[i] = map[string]any{"name": "WORKERS_PER_SIGNER", "value": value}
			found = true
			break
		}
	}
	if !found {
		env = append(env, map[string]any{"name": "WORKERS_PER_SIGNER", "value": value})
	}
	container["env"] = env
	containers[0] = container
	return unstructured.SetNestedSlice(dep.Object, containers, "spec", "template", "spec", "containers")
}

// DeployAteAPIServer redeploys only ate-api-server.
func (e *Env) DeployAteAPIServer(ctx context.Context) error {
	log.Step("deploy_ate_apiserver")

	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if err := e.EnsureAPIServerPrerequisites(ctx); err != nil {
		return err
	}
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}
	if err := e.applyOtelEndpointOverride(ctx); err != nil {
		return err
	}
	postgres, err := e.planPostgres(ctx)
	if err != nil {
		return err
	}
	if postgres.bundled {
		if err := e.waitAndSetupBundledPostgres(ctx); err != nil {
			return err
		}
	}
	manifest, err := e.renderResolve(ctx, e.apiServerManifestPath())
	if err != nil {
		return err
	}
	if err := e.applyWithAPIServerEnvHash(ctx, manifest); err != nil {
		return err
	}
	// After the manifest, which resets the pod template to the sidecar-free base.
	if err := e.reconcileCloudSQLProxySidecar(ctx); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), "ate-api-server", e.Cfg.RolloutTimeout)
}

// apiServerManifestPath is the ate-api-server manifest for the environment.
func (e *Env) apiServerManifestPath() string {
	if e.Cfg.Kind {
		return e.Cfg.Manifest("kind", "ate-api-server")
	}
	return e.Cfg.Manifest("ate-api-server.yaml")
}

// DeployAteController redeploys only ate-controller.
func (e *Env) DeployAteController(ctx context.Context) error {
	log.Step("deploy_ate_controller")

	if err := e.requireInstalledDataPlane(ctx, "deploy ate-controller"); err != nil {
		return err
	}
	if err := e.DeployCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}
	manifest, err := e.render(e.Cfg.Manifest("ate-controller.yaml"))
	if err != nil {
		return err
	}
	if err := e.ResolveAndApplyBytes(ctx, manifest); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), "ate-controller", e.Cfg.RolloutTimeout); err != nil {
		return err
	}
	return e.waitFakeWorkersync(ctx)
}

// fakeWorkersyncDeployment is the Deployment fakeDataPlaneComponent adds.
const fakeWorkersyncDeployment = "fake-workersync"

// fakeWorkersyncPath is fakeDataPlaneComponent's fake-workersync manifest.
func (e *Env) fakeWorkersyncPath() string {
	return e.Cfg.Manifest("components", "fake-data-plane", "fake-workersync.yaml")
}

// requireInstalledDataPlane refuses a single-component redeploy that asks for
// a data plane other than the installed one. atelet and ate-controller switch
// together: fake-atelet beside a running ate-controller would have its fake
// Workers deleted, and the real atelet beside a stopped one would have no
// WorkerPool served. deploy ate-system switches both.
func (e *Env) requireInstalledDataPlane(ctx context.Context, step string) error {
	installed, err := e.Kube.DeploymentExists(ctx, e.Namespace(), fakeWorkersyncDeployment)
	if err != nil {
		return err
	}
	switch {
	case e.Cfg.FakeDataPlane && !installed:
		return fmt.Errorf("%s --fake-data-plane needs the fake data plane installed; install it with deploy ate-system --fake-data-plane", step)
	case !e.Cfg.FakeDataPlane && installed:
		return fmt.Errorf("the cluster has the fake data plane, which %s alone would half replace; pass --fake-data-plane, or switch back with deploy ate-system", step)
	}
	return nil
}

// workerPoolLabel is the label ate-controller puts on each WorkerPool's
// worker pods, naming the pool.
const workerPoolLabel = "ate.dev/worker-pool"

// requireNoWorkerPods refuses to install the fake data plane while worker
// pods exist, terminating ones included. Their WorkerPools own them, so
// stopping ate-controller would leave them running, or leave the Workers of
// those terminating unregistered, taking actors fake-workersync knows nothing
// of, beside a fake atelet. A pod that has finished, as an evicted one has,
// holds no Worker, so a pool scaled to zero does not stand in the way.
func (e *Env) requireNoWorkerPods(ctx context.Context) error {
	if !e.Cfg.FakeDataPlane {
		return nil
	}
	list, err := e.Kube.Typed.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{LabelSelector: workerPoolLabel})
	if err != nil {
		return fmt.Errorf("while listing worker pods: %w", err)
	}
	names := make([]string, 0, len(list.Items))
	for _, p := range list.Items {
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		names = append(names, p.Namespace+"/"+p.Labels[workerPoolLabel])
	}
	if len(names) == 0 {
		return nil
	}
	slices.Sort(names)
	return fmt.Errorf("--fake-data-plane cannot replace the real data plane while worker pods exist; delete these WorkerPools and wait for their pods to go: %s", strings.Join(slices.Compact(names), ", "))
}

// stopFakeWorkersync deletes fake-workersync's Deployment, if any, and waits
// until its pod is gone.
func (e *Env) stopFakeWorkersync(ctx context.Context) error {
	running, err := e.Kube.DeploymentExists(ctx, e.Namespace(), fakeWorkersyncDeployment)
	if err != nil || !running {
		return err
	}
	log.Step("remove_fake_workersync")
	return e.Kube.DeleteDeploymentAndWait(ctx, e.Namespace(), fakeWorkersyncDeployment, e.Cfg.RolloutTimeout)
}

// removeFakeWorkersync removes what an earlier fake install left of
// fake-workersync, on an install of the real data plane. It runs before the
// bundle, so fake-workersync is gone before ate-controller starts;
// ate-controller's startup sweep then deletes the fake Workers, which no pod
// backs. The rest of its manifest is deleted on every real install, as
// missing objects are skipped, so a run that failed partway leaves nothing
// behind for long.
func (e *Env) removeFakeWorkersync(ctx context.Context) error {
	if e.Cfg.FakeDataPlane {
		return nil
	}
	if err := e.stopFakeWorkersync(ctx); err != nil {
		return err
	}
	return e.Kube.DeletePath(ctx, e.fakeWorkersyncPath())
}

// waitFakeWorkersync waits for fake-workersync's rollout under
// --fake-data-plane, after every apply that carries atelet or ate-controller.
func (e *Env) waitFakeWorkersync(ctx context.Context) error {
	if !e.Cfg.FakeDataPlane {
		return nil
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), fakeWorkersyncDeployment, e.Cfg.RolloutTimeout)
}

// DeployAtelet redeploys only the atelet DaemonSet.
func (e *Env) DeployAtelet(ctx context.Context) error {
	log.Step("deploy_atelet")

	if err := e.requireInstalledDataPlane(ctx, "deploy atelet"); err != nil {
		return err
	}
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}
	if err := e.applyOtelEndpointOverride(ctx); err != nil {
		return err
	}

	manifest, err := e.render(e.ateletManifestPath())
	if err != nil {
		return err
	}
	manifest, err = e.ResolveManifestBytes(ctx, manifest)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, manifest); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDaemonSet, e.Namespace(), "atelet", e.Cfg.RolloutTimeout); err != nil {
		return err
	}
	return e.waitFakeWorkersync(ctx)
}

// ateletManifestPath is the atelet manifest for the environment.
func (e *Env) ateletManifestPath() string {
	if e.Cfg.Kind {
		return e.Cfg.Manifest("kind", "atelet")
	}
	return e.Cfg.Manifest("atelet.yaml")
}

// DeployAtenet redeploys the atenet dataplane: router and egress.
func (e *Env) DeployAtenet(ctx context.Context) error {
	log.Step("deploy_atenet")

	provider, err := e.Cfg.CredentialProvider()
	if err != nil {
		return err
	}
	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}
	if err := e.applyOtelEndpointOverride(ctx); err != nil {
		return err
	}

	routerManifest, err := e.renderAtenetRouterManifest(ctx)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, routerManifest); err != nil {
		return err
	}
	if err := e.EnsureEgressMITMCAPoolSecret(ctx); err != nil {
		return err
	}
	if provider.Kubernetes() {
		err = e.deployK8sCredentialProvider(ctx)
	} else {
		err = e.removeK8sCredentialProvider(ctx)
	}
	if err != nil {
		return err
	}
	if err := e.applyAtenetEgress(ctx, provider); err != nil {
		return err
	}

	deployments := []string{"atenet-router", "atenet-egress"}
	if provider.Kubernetes() {
		deployments = append(deployments, k8sCredentialProviderDeployment)
	}
	for _, name := range deployments {
		if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), name, e.Cfg.RolloutTimeout); err != nil {
			return err
		}
	}
	return nil
}

// DeploySandboxConfig applies the SandboxConfig admission policy, then the
// default gVisor SandboxConfig.
func (e *Env) DeploySandboxConfig(ctx context.Context) error {
	log.Step("deploy_sandboxconfig")

	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}

	// Enforce per-class SandboxConfig asset requirements. This is applied
	// before any SandboxConfig so the config below is validated too.
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-validation.yaml")); err != nil {
		return err
	}

	// Install the cluster-wide sandbox config. Sandbox binaries live on
	// cluster-scoped SandboxConfigs each ActorTemplate names via
	// sandboxConfig.configName; gVisor templates name this one unless they
	// create their own SandboxConfig.
	return e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-gvisor.yaml"))
}

// EnsureCRDs creates the generated CRDs that are missing. Existing CRDs and
// RBAC change only through DeployCRDs, so a component redeploy cannot move them.
func (e *Env) EnsureCRDs(ctx context.Context) error {
	log.Step("ensure_crds")

	crds, err := e.generatedCRDs()
	if err != nil {
		return err
	}
	missing, err := missingCRDs(crds, func(name string) (bool, error) {
		return e.Kube.Exists(ctx, crdGVK, "", name)
	})
	if err != nil {
		return err
	}
	if len(missing) == 0 {
		return nil
	}
	if err := e.Kube.Apply(ctx, missing); err != nil {
		return err
	}
	return e.waitCRDsEstablished(ctx, missing)
}

// generatedCRDs loads the generated CRDs, leaving out the RBAC beside them.
func (e *Env) generatedCRDs() ([]*unstructured.Unstructured, error) {
	objs, err := kube.LoadPath(e.Cfg.Manifest("generated"))
	if err != nil {
		return nil, err
	}
	var crds []*unstructured.Unstructured
	for _, obj := range objs {
		if obj.GroupVersionKind() == crdGVK {
			crds = append(crds, obj)
		}
	}
	return crds, nil
}

// missingCRDs returns the CRDs that exists reports absent.
func missingCRDs(crds []*unstructured.Unstructured, exists func(name string) (bool, error)) ([]*unstructured.Unstructured, error) {
	var missing []*unstructured.Unstructured
	for _, crd := range crds {
		present, err := exists(crd.GetName())
		if err != nil {
			return nil, err
		}
		if !present {
			missing = append(missing, crd)
		}
	}
	return missing, nil
}

// DeployCRDs applies the generated CRDs and RBAC, waiting for them to reach Established condition.
func (e *Env) DeployCRDs(ctx context.Context) error {
	log.Step("deploy_crds")
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("generated")); err != nil {
		return err
	}
	crds, err := e.generatedCRDs()
	if err != nil {
		return err
	}
	return e.waitCRDsEstablished(ctx, crds)
}

// waitCRDsEstablished waits until the CRDs are served.
func (e *Env) waitCRDsEstablished(ctx context.Context, crds []*unstructured.Unstructured) error {
	for _, crd := range crds {
		if err := e.Kube.WaitCondition(ctx, crdGVK, "", crd.GetName(), "Established", 30*time.Second); err != nil {
			return err
		}
	}
	// Later steps create SandboxConfigs and other custom resources; discovery
	// has to see the kinds that were just installed.
	e.Kube.InvalidateDiscovery()
	return nil
}
