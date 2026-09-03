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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
)

// DeleteAteSystem removes the control plane.
//
// PostgreSQL, the agentgateway ConfigMap, and the CRDs are deleted explicitly
// afterwards because they are not part of every rendered bundle: which of them
// the install created depends on the router that was selected, and teardown
// must not depend on remembering that.
func (e *Env) DeleteAteSystem(ctx context.Context) error {
	log.Step("delete_ate_system")

	if e.Cfg.Kind {
		manifest, err := e.Kustomize(installDir + "/kind")
		if err != nil {
			return err
		}
		if err := e.Kube.DeleteBytes(ctx, manifest); err != nil {
			return err
		}
	} else if err := e.Kube.DeletePath(ctx, e.Cfg.Manifest()); err != nil {
		return err
	}

	// atelet DaemonSet names carry a version suffix.
	if err := e.Kube.Typed.AppsV1().DaemonSets(NamespaceAteSystem).DeleteCollection(ctx,
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
	return e.UnlabelNodesSubstrateVersion(ctx)
}

// DeleteAtenet removes the atenet dataplane.
func (e *Env) DeleteAtenet(ctx context.Context) error {
	log.Step("delete_atenet")

	for _, path := range [][]string{
		{"atenet-router.yaml"},
		{"components", "agentgateway", "configmap.yaml"},
		// Both egress variants, not the selected one: teardown has to clean up
		// an install made with --experimental-use-sdsmint whether or not this
		// invocation passes it, and either file may declare resources the
		// other does not.
		{"atenet-egress.yaml"},
		{"atenet-egress-with-sdsmint.yaml"},
		{"atenet-dns.yaml"},
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
