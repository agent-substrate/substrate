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
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/kube"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// crdGVK identifies the CRDs whose presence EnsureCRDs checks for.
var crdGVK = schema.GroupVersionKind{
	Group:   "apiextensions.k8s.io",
	Version: "v1",
	Kind:    "CustomResourceDefinition",
}

// ateCRDs are the custom resources the control plane and demos depend on.
var ateCRDs = []string{
	"workerpools.ate.dev",
	"sandboxconfigs.ate.dev",
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
	// Fail fast on an unusable build version before touching the cluster.
	if _, _, err := e.SubstrateVersion(); err != nil {
		return err
	}
	// Likewise the CSI request, even though it is only acted on partway
	// through: a driver this cluster cannot run is worth knowing before the
	// control plane goes up, not after.
	if err := e.CheckCSIDriver(opts.SetupCSI); err != nil {
		return err
	}

	// The namespace has to exist before RBAC or CRDs are applied.
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}

	// Before the bundle: the atelet DaemonSet applied below and the demo
	// WorkerPools' version-pinned pods schedule only to version-labeled nodes.
	if err := e.LabelNodesSubstrateVersion(ctx); err != nil {
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

	// Enforce per-class SandboxConfig asset requirements. This is applied
	// before any SandboxConfig so the config below is validated too.
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-validation.yaml")); err != nil {
		return err
	}

	// Install the cluster-wide sandbox config. Sandbox binaries live on
	// cluster-scoped SandboxConfigs each ActorTemplate names via
	// sandboxConfig.configName; gVisor templates name this one unless they
	// create their own SandboxConfig.
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-gvisor.yaml")); err != nil {
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
	if err := e.applyBundledPostgres(ctx, postgres); err != nil {
		return err
	}

	manifests, err := e.renderSystemManifests(ctx)
	if err != nil {
		return err
	}
	// The atelet DaemonSet in the bundle is version-keyed; fill its
	// placeholders after the render (kustomize and ko pass them through).
	manifests, err = e.SubstituteVersion(manifests)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, manifests); err != nil {
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
	if err := e.applyAtenetEgress(ctx); err != nil {
		return err
	}

	ateletName, err := e.AteletDaemonSetName()
	if err != nil {
		return err
	}
	log.Step("Waiting for ATE system components to be ready...")
	type rollout struct{ kind, name string }
	var waits []rollout
	if postgres.bundled {
		waits = append(waits, rollout{kube.KindStatefulSet, "postgres"})
	}
	waits = append(waits,
		rollout{kube.KindDeployment, "ate-api-server"},
		rollout{kube.KindDeployment, "ate-controller"},
		rollout{kube.KindDeployment, "atenet-router"},
		rollout{kube.KindDeployment, "atenet-egress"},
		rollout{kube.KindDaemonSet, ateletName},
	)
	for _, w := range waits {
		if err := e.Kube.RolloutStatus(ctx, w.kind, e.Namespace(), w.name, e.Cfg.RolloutTimeout); err != nil {
			return err
		}
	}
	return e.applyOtelEndpointOverride(ctx)
}

// DeployPodCertificateController applies the podcertificate controller and
// waits until it is rolling and has published its trust bundles.
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
	if err := e.renderResolveApply(ctx, e.Cfg.Manifest("ate-api-server.yaml")); err != nil {
		return err
	}
	// After the manifest, which resets the pod template to the sidecar-free base.
	if err := e.reconcileCloudSQLProxySidecar(ctx); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), "ate-api-server", e.Cfg.RolloutTimeout)
}

// DeployAteController redeploys only ate-controller.
func (e *Env) DeployAteController(ctx context.Context) error {
	log.Step("deploy_ate_controller")

	if err := e.DeployCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}
	if err := e.renderResolveApply(ctx, e.Cfg.Manifest("ate-controller.yaml")); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), "ate-controller", e.Cfg.RolloutTimeout)
}

// DeployAtelet redeploys only the atelet DaemonSet.
func (e *Env) DeployAtelet(ctx context.Context) error {
	log.Step("deploy_atelet")

	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}
	if err := e.EnsureAteSystemNamespace(ctx); err != nil {
		return err
	}
	if err := e.LabelNodesSubstrateVersion(ctx); err != nil {
		return err
	}
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}
	if err := e.applyOtelEndpointOverride(ctx); err != nil {
		return err
	}

	var manifest []byte
	var err error
	if e.Cfg.Kind {
		// The kind overlay patches the DaemonSet for the local node layout.
		manifest, err = e.KustomizeResolve(ctx, installDir+"/kind/atelet")
	} else {
		manifest, err = e.ResolveManifest(ctx, e.Cfg.Manifest("atelet.yaml"))
	}
	if err != nil {
		return err
	}
	manifest, err = e.SubstituteVersion(manifest)
	if err != nil {
		return err
	}
	if err := e.Kube.ApplyBytes(ctx, manifest); err != nil {
		return err
	}
	ateletName, err := e.AteletDaemonSetName()
	if err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDaemonSet, e.Namespace(), ateletName, e.Cfg.RolloutTimeout)
}

// DeployAtenet redeploys the atenet dataplane: router and egress.
func (e *Env) DeployAtenet(ctx context.Context) error {
	log.Step("deploy_atenet")

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
	if err := e.applyAtenetEgress(ctx); err != nil {
		return err
	}

	for _, name := range []string{"atenet-router", "atenet-egress"} {
		if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, e.Namespace(), name, e.Cfg.RolloutTimeout); err != nil {
			return err
		}
	}
	return nil
}

// EnsureCRDs installs the CRDs only if they are missing. Component redeploys
// use this so they do not pay for a full CRD apply on every run.
func (e *Env) EnsureCRDs(ctx context.Context) error {
	log.Step("ensure_crds")

	allPresent := true
	for _, name := range ateCRDs {
		present, err := e.Kube.Exists(ctx, crdGVK, "", name)
		if err != nil {
			return err
		}
		if !present {
			allPresent = false
			break
		}
	}
	if allPresent {
		return nil
	}
	return e.DeployCRDs(ctx)
}

// DeployCRDs applies the generated CRDs and RBAC, waiting for them to reach Established condition.
func (e *Env) DeployCRDs(ctx context.Context) error {
	log.Step("deploy_crds")
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("generated")); err != nil {
		return err
	}
	for _, name := range ateCRDs {
		if err := e.Kube.WaitCondition(ctx, crdGVK, "", name, "Established", 30*time.Second); err != nil {
			return err
		}
	}
	// Later steps create SandboxConfigs and other custom resources; discovery
	// has to see the kinds that were just installed.
	e.Kube.InvalidateDiscovery()
	return nil
}
