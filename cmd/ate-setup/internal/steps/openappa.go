// Copyright 2026 The Agent Substrate Authors
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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
)

// OpenAPPA's manifests. Like the credential provider's, they sit outside the
// ate-install tree because only installs that select it apply them.
const (
	openAPPADir      = "manifests/openappa"
	openAPPAManifest = "openappa.yaml"
	openAPPAPolicy   = "appa.toml"

	// Object names from the manifest above; openappa_test.go checks them.
	openAPPADeployment      = "openappa"
	openAPPAPolicyConfigMap = "openappa-policy"

	// openAPPAPolicyHashAnnotation carries a digest of the policy on the pod
	// template. The runtime reads the policy at startup, so the digest is what
	// turns a changed policy into a restart.
	openAPPAPolicyHashAnnotation = "ate.dev/openappa-policy-hash"
)

func (e *Env) openAPPAPath(name string) string {
	return e.Cfg.Path(openAPPADir, name)
}

// deployOpenAPPA deploys OpenAPPA. The policy ConfigMap is applied from
// --openappa-policy when it is set. Otherwise the bundled policy is applied
// only when the ConfigMap is absent, so a redeploy keeps an operator's edits.
func (e *Env) deployOpenAPPA(ctx context.Context) error {
	log.Step("deploy_openappa")
	policy, err := e.applyOpenAPPAPolicy(ctx)
	if err != nil {
		return err
	}
	manifest, err := e.renderOpenAPPA(policy)
	if err != nil {
		return err
	}
	return e.Kube.Apply(ctx, manifest)
}

// removeOpenAPPA removes OpenAPPA left by an earlier install. The policy
// ConfigMap and the provider credentials Secret stay: the operator may have
// written both.
func (e *Env) removeOpenAPPA(ctx context.Context) error {
	running, err := e.Kube.DeploymentExists(ctx, e.Namespace(), openAPPADeployment)
	if err != nil || !running {
		return err
	}
	log.Step("remove_openappa")
	return e.Kube.DeletePath(ctx, e.openAPPAPath(openAPPAManifest))
}

// renderOpenAPPA renders the manifest with the configured images and MCP
// servers, and stamps the Deployment with the policy's digest.
func (e *Env) renderOpenAPPA(policy string) ([]*unstructured.Unstructured, error) {
	rendered, err := e.render(e.openAPPAPath(openAPPAManifest))
	if err != nil {
		return nil, err
	}
	objs, err := kube.DecodeManifestBytes(rendered)
	if err != nil {
		return nil, err
	}
	dep := findObject(objs, "Deployment", openAPPADeployment)
	if dep == nil {
		return nil, fmt.Errorf("%s has no deployment/%s", openAPPAManifest, openAPPADeployment)
	}
	containers, _, err := unstructured.NestedSlice(dep.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return nil, fmt.Errorf("%s: %w", kube.Describe(dep), err)
	}
	set := map[string]bool{}
	for i, c := range containers {
		container := c.(map[string]any)
		switch container["name"] {
		case "extproc":
			container["image"] = e.Cfg.OpenAPPAExtprocImage
			args, _, err := unstructured.NestedStringSlice(container, "args")
			if err != nil {
				return nil, fmt.Errorf("%s: %w", kube.Describe(dep), err)
			}
			for _, server := range e.Cfg.OpenAPPAMCPServers {
				args = append(args, "--mcp-server="+server)
			}
			if err := unstructured.SetNestedStringSlice(container, args, "args"); err != nil {
				return nil, fmt.Errorf("%s: %w", kube.Describe(dep), err)
			}
		case "runtime":
			container["image"] = e.Cfg.OpenAPPARuntimeImage
		default:
			continue
		}
		set[container["name"].(string)] = true
		containers[i] = container
	}
	if !set["extproc"] || !set["runtime"] {
		return nil, fmt.Errorf("%s needs extproc and runtime containers", kube.Describe(dep))
	}
	if err := unstructured.SetNestedSlice(dep.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(policy))
	if err := unstructured.SetNestedField(dep.Object, hex.EncodeToString(sum[:]),
		"spec", "template", "metadata", "annotations", openAPPAPolicyHashAnnotation); err != nil {
		return nil, err
	}
	return objs, nil
}

// applyOpenAPPAPolicy reconciles the policy ConfigMap and returns the policy
// it holds.
func (e *Env) applyOpenAPPAPolicy(ctx context.Context) (string, error) {
	path := e.Cfg.OpenAPPAPolicy
	if path == "" {
		cm, err := e.Kube.GetConfigMap(ctx, e.Namespace(), openAPPAPolicyConfigMap)
		if err != nil {
			return "", err
		}
		if cm != nil {
			return cm.Data[openAPPAPolicy], nil
		}
		path = e.openAPPAPath(openAPPAPolicy)
	}
	policy, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("while reading the OpenAPPA policy: %w", err)
	}
	if err := e.Kube.ApplyConfigMap(ctx, e.Namespace(), openAPPAPolicyConfigMap,
		map[string]string{openAPPAPolicy: string(policy)}); err != nil {
		return "", err
	}
	return string(policy), nil
}

// findObject returns the named object of a kind, or nil.
func findObject(objs []*unstructured.Unstructured, kind, name string) *unstructured.Unstructured {
	for _, obj := range objs {
		if obj.GetKind() == kind && obj.GetName() == name {
			return obj
		}
	}
	return nil
}
