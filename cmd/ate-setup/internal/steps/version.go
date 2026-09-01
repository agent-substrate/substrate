// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	"os"
	"regexp"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/ko"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/log"
	"github.com/agent-substrate/substrate/internal/versionlabel"
)

// SubstrateVersion returns the build version and the object-name suffix for atelet.
// The version is the same one ko stamps into the binaries.
//
// With --image-repo nothing is built, so `git describe` would report the
// checkout rather than the images being installed. The image tag is the version
// in that case. It has to be: the version names the atelet DaemonSet and sets
// the node label that partitions nodes across coexisting versions, so it must
// describe the atelet that is actually running, which came from the image.
// VERSION still wins over both, so a tag that is not a valid label value can be
// worked around the same way.
func (e *Env) SubstrateVersion() (version, suffix string, err error) {
	if e.substrateVersion != "" {
		return e.substrateVersion, e.substrateVersionSuffix, nil
	}
	var raw string
	if e.Cfg.Images.IsPrebuilt() {
		raw = os.Getenv("VERSION")
		if raw == "" {
			raw = e.Cfg.Images.Tag
		}
	} else {
		// ko.BuildVersion consults VERSION itself before `git describe`.
		raw = ko.BuildVersion(e.Cfg.Root)
	}
	if v := versionlabel.Value(raw); v != raw {
		return "", "", fmt.Errorf("build version %q is not a valid label value (it would sanitize to %q); pin a label-safe one with the VERSION env var", raw, v)
	}
	e.substrateVersion = raw
	e.substrateVersionSuffix = versionlabel.NameSuffix(raw)
	return e.substrateVersion, e.substrateVersionSuffix, nil
}

func (e *Env) AteletDaemonSetName() (string, error) {
	_, suffix, err := e.SubstrateVersion()
	if err != nil {
		return "", err
	}
	return "atelet-" + suffix, nil
}

// unquotedVersionScalar matches a whole-scalar ${SUBSTRATE_VERSION} value
// kustomize re-emitted unquoted. It is re-quoted before substitution so an
// all-digit version lands as a YAML string, not an integer.
var unquotedVersionScalar = regexp.MustCompile(`(?m)^(\s*[^:\n]+): \$\{SUBSTRATE_VERSION\}$`)

// SubstituteVersion fills the ${SUBSTRATE_VERSION} and
// ${SUBSTRATE_VERSION_SUFFIX} placeholders in a rendered manifest.
func (e *Env) SubstituteVersion(manifest []byte) ([]byte, error) {
	version, suffix, err := e.SubstrateVersion()
	if err != nil {
		return nil, err
	}
	s := unquotedVersionScalar.ReplaceAllString(string(manifest), `$1: "$${SUBSTRATE_VERSION}"`)
	s = strings.ReplaceAll(s, "${SUBSTRATE_VERSION}", version)
	s = strings.ReplaceAll(s, "${SUBSTRATE_VERSION_SUFFIX}", suffix)
	return []byte(s), nil
}

// RestartAteletDaemonSets pod-restarts every atelet DaemonSet by label.
func (e *Env) RestartAteletDaemonSets(ctx context.Context) error {
	list, err := e.Kube.Typed.AppsV1().DaemonSets(NamespaceAteSystem).List(ctx, metav1.ListOptions{
		LabelSelector: "app=atelet",
	})
	if err != nil {
		return fmt.Errorf("while listing atelet daemonsets: %w", err)
	}
	for _, ds := range list.Items {
		if err := e.Kube.RolloutRestart(ctx, NamespaceAteSystem, ds.Name, time.Now()); err != nil {
			return err
		}
	}
	return nil
}

// LabelNodesSubstrateVersion stamps ate.dev/substrate-version on every node
// that does not carry it yet. Nodes that already carry the label keep their
// value: during a rolling upgrade the operator owns the per-node value, and
// an install must not yank nodes across versions behind its back.
func (e *Env) LabelNodesSubstrateVersion(ctx context.Context) error {
	version, _, err := e.SubstrateVersion()
	if err != nil {
		return err
	}
	log.Stepf("label_nodes_substrate_version (%s)", version)
	nodes, err := e.Kube.Typed.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: "!" + versionlabel.Key,
	})
	if err != nil {
		return fmt.Errorf("while listing unlabeled nodes: %w", err)
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:%q}}}`, versionlabel.Key, version)
	for _, node := range nodes.Items {
		if _, err := e.Kube.Typed.CoreV1().Nodes().Patch(ctx, node.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("while labeling node %s: %w", node.Name, err)
		}
	}
	return nil
}

// UnlabelNodesSubstrateVersion clears ate.dev/substrate-version from every
// node that carries it. Uninstall removes the label with the system: the
// install won't relabels a labeled node, so a leftover label would pin the
// next install to the old version world.
func (e *Env) UnlabelNodesSubstrateVersion(ctx context.Context) error {
	log.Step("unlabel_nodes_substrate_version")
	nodes, err := e.Kube.Typed.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: versionlabel.Key,
	})
	if err != nil {
		return fmt.Errorf("while listing labeled nodes: %w", err)
	}
	patch := fmt.Sprintf(`{"metadata":{"labels":{%q:null}}}`, versionlabel.Key)
	for _, node := range nodes.Items {
		if _, err := e.Kube.Typed.CoreV1().Nodes().Patch(ctx, node.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
			return fmt.Errorf("while unlabeling node %s: %w", node.Name, err)
		}
	}
	return nil
}
