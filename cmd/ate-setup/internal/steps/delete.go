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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/clustertrustbundle"
)

// DeleteAteSystem removes the control plane.
//
// PostgreSQL, the agentgateway ConfigMap, the bundled credential provider, and
// the CRDs are deleted explicitly because they are not part of every rendered
// bundle: which of them the install created depends on the router and
// credential provider that were selected, and teardown must not depend on
// remembering that. The provider goes first so that a later failure cannot
// leave its ClusterRole and binding behind. The cluster-scoped trust bundles
// go last, once the controllers that publish them are gone.
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

	// atelet DaemonSet names carry a version suffix.
	if err := e.Kube.Typed.AppsV1().DaemonSets(e.Namespace()).DeleteCollection(ctx,
		metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "app=atelet"}); err != nil {
		return fmt.Errorf("while deleting atelet daemonsets: %w", err)
	}

	for _, path := range [][]string{
		{"components", "agentgateway", "configmap.yaml"},
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
	// The podcertificate controller republishes its bundles every few
	// seconds, so they can only be deleted once it is gone.
	if err := e.Kube.WaitDeleted(ctx, namespace, "", NamespacePodCert, e.Cfg.WaitTimeout(BootstrapTimeout)); err != nil {
		return err
	}
	if err := e.DeleteTrustBundles(ctx); err != nil {
		return err
	}
	return e.UnlabelNodesSubstrateVersion(ctx)
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

// DeleteAll removes every registered demo and then the control plane.
func (e *Env) DeleteAll(ctx context.Context, demos []Deleter) error {
	log.Step("delete_all")

	for _, demo := range demos {
		if err := demo.Delete(ctx, e); err != nil {
			return err
		}
	}
	return e.DeleteAteSystem(ctx)
}
