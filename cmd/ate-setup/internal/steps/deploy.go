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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// DeployAteSystem installs the whole control plane: CRDs, RBAC, the
// podcertificate controller, the store, ateapi, the controller, atenet, and
// atelet.
func (e *Env) DeployAteSystem(ctx context.Context, opts DeployOptions) error {
	log.Step("deploy_ate_system")

	// Fail fast on an unusable build version before touching the cluster.
	if _, _, err := e.SubstrateVersion(); err != nil {
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
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("pod-certificate-controller.yaml")); err != nil {
		return err
	}
	if err := e.applyPodcertWorkersOverride(ctx); err != nil {
		return err
	}
	if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, NamespacePodCert, "podcertificate-controller", e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	if err := e.WaitForPodCertificateTrustBundles(ctx); err != nil {
		return err
	}
	if err := e.SetupCSI(ctx, opts.SetupCSI); err != nil {
		return err
	}

	// Enforce per-class SandboxConfig asset requirements. This is applied
	// before any SandboxConfig so the default below is validated too.
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-validation.yaml")); err != nil {
		return err
	}

	// Install the cluster-wide default sandbox config. Sandbox binaries live
	// on cluster-scoped SandboxConfigs resolved via each WorkerPool's
	// SandboxClass, decoupled from ActorTemplate; gVisor pools resolve to this
	// default unless they name their own SandboxConfig.
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-gvisor.yaml")); err != nil {
		return err
	}

	// Ahead of the bundle below, for the same reason as the namespace: every
	// workload pulls this ConfigMap in via envFrom, and a container whose
	// envFrom target is missing will not start.
	if err := e.applyOtelConfig(ctx); err != nil {
		return err
	}

	if err := e.applyBundledPostgres(ctx); err != nil {
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
	// Only when the bundled StatefulSet was applied above; an external
	// database means it never gets deployed, and waiting on it would block
	// until the timeout on an object that will never exist.
	if e.useBundledPostgres() {
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
		if err := e.Kube.RolloutStatus(ctx, w.kind, NamespaceAteSystem, w.name, e.Cfg.RolloutTimeout); err != nil {
			return err
		}
	}
	return nil
}

// applyPodcertWorkersOverride sets WORKERS_PER_SIGNER on podcertificate-controller if configured.
func (e *Env) applyPodcertWorkersOverride(ctx context.Context) error {
	if e.Cfg.PodcertWorkersPerSigner <= 0 {
		return nil
	}
	workers := strconv.Itoa(e.Cfg.PodcertWorkersPerSigner)
	dep, err := e.Kube.Typed.AppsV1().Deployments(NamespacePodCert).Get(ctx, "podcertificate-controller", metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting podcertificate-controller deployment: %w", err)
	}

	if len(dep.Spec.Template.Spec.Containers) == 0 {
		return nil
	}
	found := false
	for i, envVar := range dep.Spec.Template.Spec.Containers[0].Env {
		if envVar.Name == "WORKERS_PER_SIGNER" {
			if envVar.Value == workers {
				return nil
			}
			dep.Spec.Template.Spec.Containers[0].Env[i].Value = workers
			found = true
			break
		}
	}
	if !found {
		dep.Spec.Template.Spec.Containers[0].Env = append(dep.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{
			Name:  "WORKERS_PER_SIGNER",
			Value: workers,
		})
	}

	log.Infof("Overriding WORKERS_PER_SIGNER with %s", workers)
	_, err = e.Kube.Typed.AppsV1().Deployments(NamespacePodCert).Update(ctx, dep, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("updating podcertificate-controller WORKERS_PER_SIGNER: %w", err)
	}
	return nil
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
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("ate-api-server.yaml")); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDeployment, NamespaceAteSystem, "ate-api-server", e.Cfg.RolloutTimeout)
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
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("ate-controller.yaml")); err != nil {
		return err
	}
	return e.Kube.RolloutStatus(ctx, kube.KindDeployment, NamespaceAteSystem, "ate-controller", e.Cfg.RolloutTimeout)
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
	return e.Kube.RolloutStatus(ctx, kube.KindDaemonSet, NamespaceAteSystem, ateletName, e.Cfg.RolloutTimeout)
}

// DeployAtenet redeploys the atenet dataplane: router, egress, and DNS.
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
	if err := e.ResolveAndApply(ctx, e.Cfg.Manifest("atenet-dns.yaml")); err != nil {
		return err
	}

	for _, name := range []string{"atenet-router", "atenet-egress", "dns"} {
		if err := e.Kube.RolloutStatus(ctx, kube.KindDeployment, NamespaceAteSystem, name, e.Cfg.RolloutTimeout); err != nil {
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
