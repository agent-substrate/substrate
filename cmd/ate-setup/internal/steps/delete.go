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
	"errors"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/util/retry"

	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/internal/clustertrustbundle"
	atev1alpha1 "go.ate.dev/substrate/pkg/api/v1alpha1"
)

// DeleteAteSystem removes the control plane.
//
// PostgreSQL, the agentgateway ConfigMap, the bundled credential provider,
// fake-workersync, and the CRDs are deleted explicitly because they are not
// part of every rendered bundle: which of them the install created depends on
// the router, credential provider, and data plane that were selected, and
// teardown must not depend on remembering that. The provider goes first so
// that a later failure cannot leave its ClusterRole and binding behind. The
// cluster-scoped trust bundles go last, once the controllers that publish
// them are gone.
func (e *Env) DeleteAteSystem(ctx context.Context) error {
	log.Step("delete_ate_system")

	if err := e.Kube.DeletePath(ctx, e.k8sCredentialProviderPath(k8sCredentialProviderManifest)); err != nil {
		return err
	}

	if e.Cfg.Kind {
		manifest, err := e.Kustomize(installDir + "/kind")
		if err != nil {
			return err
		}
		if err := e.Kube.DeleteBytes(ctx, manifest); err != nil {
			return err
		}
		// Not part of the kind bundle (see its kustomization), so it goes by
		// name. The directory delete below covers it on every other install.
		if err := e.Kube.DeletePath(ctx, e.Cfg.Manifest("pod-certificate-controller.yaml")); err != nil {
			return err
		}
	} else if err := e.Kube.DeletePath(ctx, e.Cfg.Manifest()); err != nil {
		return err
	}

	for _, path := range [][]string{
		{"components", "agentgateway", "configmap.yaml"},
		{"components", "fake-data-plane", "fake-workersync.yaml"},
		{"postgres", "postgres.yaml"},
		{"generated"},
	} {
		if err := e.Kube.DeletePath(ctx, e.Cfg.Manifest(path...)); err != nil {
			return err
		}
	}
	namespace := schema.GroupVersionKind{Version: "v1", Kind: "Namespace"}
	if err := e.Kube.WaitDeleted(ctx, namespace, "", e.Namespace(), e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	// Removing the protection finalizer by hand is safe here. atecontroller,
	// which would remove it, went away with the namespace. Deleting the CRDs
	// above deleted every WorkerPool, so no pool uses a config anymore.
	if err := releaseSandboxConfigs(ctx, e.Kube.Dynamic.Resource(atev1alpha1.GroupVersion.WithResource("sandboxconfigs"))); err != nil {
		return err
	}
	// The podcertificate controller republishes its bundles every few
	// seconds, so they can only be deleted once it is gone.
	if err := e.Kube.WaitDeleted(ctx, namespace, "", NamespacePodCert, e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	return e.DeleteTrustBundles(ctx)
}

// releaseSandboxConfigs removes the WorkerPool protection finalizer from
// every SandboxConfig. Only atecontroller adds and removes this finalizer,
// and atecontroller is gone once its namespace is. Without this step no
// SandboxConfig could finish deleting, and deleting the CRD would hang.
func releaseSandboxConfigs(ctx context.Context, res dynamic.ResourceInterface) error {
	list, err := res.List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while listing SandboxConfigs: %w", err)
	}
	for _, item := range list.Items {
		name := item.GetName()
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			sc, err := res.Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			finalizers := slices.DeleteFunc(sc.GetFinalizers(), func(f string) bool {
				return f == atev1alpha1.SandboxConfigWorkerPoolProtectionFinalizer
			})
			if len(finalizers) == len(sc.GetFinalizers()) {
				return nil
			}
			sc.SetFinalizers(finalizers)
			_, err = res.Update(ctx, sc, metav1.UpdateOptions{})
			return err
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("while releasing SandboxConfig %s: %w", name, err)
		}
	}
	return nil
}

// substrateTrustBundles maps the name of each ClusterTrustBundle Substrate
// publishes to its signer: the podcertificate controller's (see
// trustBundleNames) and atecontroller's egress MITM bundle
// (egressMITMTrustBundleName and egressMITMSignerName in
// cmd/atecontroller/internal/controllers, which ate-setup cannot import).
var substrateTrustBundles = map[string]string{
	"podidentity.podcert.ate.dev:identity:primary-bundle": "podidentity.podcert.ate.dev/identity",
	"servicedns.podcert.ate.dev:identity:primary-bundle":  "servicedns.podcert.ate.dev/identity",
	"postgres.podcert.ate.dev:identity:primary-bundle":    "postgres.podcert.ate.dev/identity",
	"egress-mitm.ate.dev:mitm:primary-bundle":             "egress-mitm.ate.dev/mitm",
}

// DeleteTrustBundles deletes the ClusterTrustBundles in substrateTrustBundles.
// They are cluster-scoped, so deleting the namespaces of the controllers that
// publish them leaves them behind.
//
// A bundle is deleted only when both its name and its signer match. Anyone
// allowed to attest for one of these signers may publish a bundle under a
// different name, so the signer alone does not show that Substrate created it.
func (e *Env) DeleteTrustBundles(ctx context.Context) error {
	bundles, err := clustertrustbundle.NewClient(e.Kube.Typed, nil)
	if errors.Is(err, clustertrustbundle.ErrNotServed) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("while discovering the ClusterTrustBundle API: %w", err)
	}
	list, err := bundles.List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("while listing ClusterTrustBundles: %w", err)
	}
	for _, bundle := range list.Items {
		if signer, ok := substrateTrustBundles[bundle.Name]; !ok || bundle.Spec.SignerName != signer {
			continue
		}
		opts := metav1.DeleteOptions{Preconditions: metav1.NewUIDPreconditions(string(bundle.UID))}
		if err := bundles.Delete(ctx, bundle.Name, opts); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("while deleting ClusterTrustBundle %s: %w", bundle.Name, err)
		}
		log.Infof("Deleted ClusterTrustBundle %s", bundle.Name)
	}
	return nil
}

// DeleteAtenet removes the atenet dataplane and the bundled credential
// provider, deleting the provider first so that a later failure cannot leave
// its ClusterRole and binding behind. The provider's policy ConfigMap stays:
// it holds the operator's allow-list.
func (e *Env) DeleteAtenet(ctx context.Context) error {
	log.Step("delete_atenet")

	if err := e.Kube.DeletePath(ctx, e.k8sCredentialProviderPath(k8sCredentialProviderManifest)); err != nil {
		return err
	}

	for _, path := range [][]string{
		{"atenet-router.yaml"},
		{"components", "agentgateway", "configmap.yaml"},
		{"atenet-egress.yaml"},
	} {
		if err := e.Kube.DeletePath(ctx, e.Cfg.Manifest(path...)); err != nil {
			return err
		}
	}
	return nil
}

// Deleter is the demo teardown DeleteAll drives.
//
// The demos live in their own packages, which import this one for Env, so the
// interface is declared here rather than imported from there.
type Deleter interface {
	Delete(ctx context.Context, e *Env) error
}

// DeleteAll removes every demo, then the microvm SandboxConfig, then the
// control plane.
func (e *Env) DeleteAll(ctx context.Context, demos []Deleter) error {
	log.Step("delete_all")

	if err := e.deleteDemos(ctx, demos); err != nil {
		return err
	}
	return e.DeleteAteSystem(ctx)
}

// deleteDemos removes every demo, then tries to delete the microvm
// SandboxConfig. An earlier `delete benchmarks` or `delete demo` may have
// left the config behind because a pool still used it. After the demos, no
// pool does.
//
// The delete is best effort. The script skips a config that is still in use,
// and a failure is only logged, because DeleteAteSystem deletes the config
// with the CRD anyway.
//
// The gvisor config needs no such step. It is installed and deleted with the
// control plane, and DeleteAteSystem strips its finalizer itself. The microvm
// config is managed by hack/install-microvm-deps.sh, whose `kubectl delete`
// would block while a pool still uses it.
func (e *Env) deleteDemos(ctx context.Context, demos []Deleter) error {
	for _, demo := range demos {
		if err := demo.Delete(ctx, e); err != nil {
			return err
		}
	}
	if err := e.runScript(ctx, installMicrovmDepScript, "--delete"); err != nil {
		log.Warnf("while deleting the microvm SandboxConfig: %v", err)
	}
	return nil
}
