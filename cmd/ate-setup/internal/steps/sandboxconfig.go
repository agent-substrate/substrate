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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"

	atev1 "go.ate.dev/substrate/api/infra/k8s/ate/v1"
	"go.ate.dev/substrate/cmd/ate-setup/internal/images"
	"go.ate.dev/substrate/cmd/ate-setup/internal/kube"
	"go.ate.dev/substrate/cmd/ate-setup/internal/log"
	"go.ate.dev/substrate/cmd/ate-setup/internal/render"
)

// DeploySandboxConfig applies the SandboxConfig admission policy, then
// creates this build's gVisor SandboxConfig from sandboxconfig-gvisor.yaml.tmpl
// unless it exists. sandboxConfigName says how the config is named. A config
// it creates gets the class-default annotation when
// createWithDefaultAnnotation is true. An existing config, annotations
// included, is left as it is.
func (e *Env) DeploySandboxConfig(ctx context.Context, createWithDefaultAnnotation bool) error {
	sc, err := e.checkSandboxConfig(ctx)
	if err != nil {
		return err
	}
	return e.deploySandboxConfig(ctx, sc, createWithDefaultAnnotation)
}

// deploySandboxConfig is DeploySandboxConfig for the config that
// checkSandboxConfig returned.
func (e *Env) deploySandboxConfig(ctx context.Context, sc sandboxConfig, createWithDefaultAnnotation bool) error {
	log.Step("deploy_sandboxconfig")

	if err := e.EnsureCRDs(ctx); err != nil {
		return err
	}

	// Enforce per-class SandboxConfig asset requirements. This is applied
	// before any SandboxConfig so the config below is validated too.
	if err := e.Kube.ApplyPath(ctx, e.Cfg.Manifest("sandboxconfig-validation.yaml")); err != nil {
		return err
	}

	if sc.exists {
		// An existing config may have been edited since, so it is left as it
		// is.
		log.Infof("SandboxConfig %s exists; leaving it and its annotations as they are", sc.name)
	} else {
		if err := e.Kube.ApplyBytes(ctx, sc.manifest); err != nil {
			return err
		}
		log.Infof("created SandboxConfig %s", sc.name)
		if createWithDefaultAnnotation {
			patch, err := json.Marshal(map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{atev1.SandboxConfigClassDefaultAnnotation: "true"}},
			})
			if err != nil {
				return fmt.Errorf("while building the patch for SandboxConfig %s: %w", sc.name, err)
			}
			if _, err := e.sandboxConfigs().Patch(ctx, sc.name, types.MergePatchType, patch, metav1.PatchOptions{FieldManager: kube.FieldManager}); err != nil {
				return fmt.Errorf("while annotating SandboxConfig %s: %w", sc.name, err)
			}
			log.Infof("annotated SandboxConfig %s %s=true", sc.name, atev1.SandboxConfigClassDefaultAnnotation)
		}
	}
	// Checked after the create, against the config's real creation time and
	// annotations.
	configs, err := e.listSandboxConfigs(ctx)
	if err != nil {
		return err
	}
	if warning := classDefaultWarning(configs, sc.name, atev1.SandboxClassGvisor); warning != "" {
		log.Warnf("%s", warning)
	}
	return nil
}

// sandboxConfig is this build's gVisor SandboxConfig.
type sandboxConfig struct {
	name string
	// manifest is the config rendered with name.
	manifest []byte
	exists   bool
}

// checkSandboxConfig returns this build's gVisor SandboxConfig. It fails when
// sandboxConfigExists does.
func (e *Env) checkSandboxConfig(ctx context.Context) (sandboxConfig, error) {
	tmpl := e.Cfg.Manifest("sandboxconfig-gvisor.yaml.tmpl")
	unnamed, err := render.Template(tmpl, nil, nil)
	if err != nil {
		return sandboxConfig{}, err
	}
	name, err := sandboxConfigName(e.SubstrateVersion(), atev1.SandboxClassGvisor, unnamed)
	if err != nil {
		return sandboxConfig{}, err
	}
	configs, err := e.listSandboxConfigs(ctx)
	if err != nil {
		return sandboxConfig{}, err
	}
	exists, err := sandboxConfigExists(configs, name)
	if err != nil {
		return sandboxConfig{}, err
	}
	manifest := render.Expand(string(unnamed), map[string]string{"SANDBOXCONFIG_NAME": name}, nil)
	return sandboxConfig{name: name, manifest: manifest, exists: exists}, nil
}

// sandboxConfigExists reports whether configs hold the SandboxConfig named
// name. It fails when that config is being deleted, because applying it would
// only update the terminating object.
func sandboxConfigExists(configs []atev1.SandboxConfig, name string) (bool, error) {
	i := slices.IndexFunc(configs, func(sc atev1.SandboxConfig) bool { return sc.Name == name })
	if i >= 0 && !configs[i].DeletionTimestamp.IsZero() {
		return false, fmt.Errorf("SandboxConfig %s is being deleted; wait until it is gone, then run this again", name)
	}
	return i >= 0, nil
}

// listSandboxConfigs returns the SandboxConfigs in the cluster. A cluster
// without the SandboxConfig CRD has none.
func (e *Env) listSandboxConfigs(ctx context.Context) ([]atev1.SandboxConfig, error) {
	list, err := e.sandboxConfigs().List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("while listing SandboxConfigs: %w", err)
	}
	configs := make([]atev1.SandboxConfig, len(list.Items))
	for i := range list.Items {
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(list.Items[i].Object, &configs[i]); err != nil {
			return nil, fmt.Errorf("while decoding SandboxConfig %s: %w", list.Items[i].GetName(), err)
		}
	}
	return configs, nil
}

func (e *Env) sandboxConfigs() dynamic.ResourceInterface {
	return e.Kube.Dynamic.Resource(atev1.GroupVersion.WithResource("sandboxconfigs"))
}

// sandboxConfigName returns the name of the SandboxConfig of class that
// Substrate version installs. unnamed is the config's manifest rendered
// without its name.
//
//   - A release's config is substrate-<version>-<class>, with the version
//     lower-cased. version is a release when it is a release tag without what
//     `git describe` appends to one.
//   - Any other build's is substrate-dev-<hash>-<class>. hash is the first
//     10 hex digits of the SHA-256 of unnamed without its comment lines, so
//     builds that render the same config share it.
func sandboxConfigName(version string, class atev1.SandboxClass, unnamed []byte) (string, error) {
	var id string
	if images.CheckReleaseVersion(version) == nil && !gitDescribeSuffix.MatchString(version) {
		id = strings.ToLower(version)
	} else {
		h := sha256.New()
		for _, line := range strings.SplitAfter(string(unnamed), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "#") {
				h.Write([]byte(line))
			}
		}
		id = "dev-" + hex.EncodeToString(h.Sum(nil))[:10]
	}
	name := fmt.Sprintf("substrate-%s-%s", id, class)
	if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
		return "", fmt.Errorf("version %q gives the invalid SandboxConfig name %q: %s", version, name, strings.Join(errs, "; "))
	}
	return name, nil
}

// gitDescribeSuffix matches what `git describe --dirty` appends to a tag: the
// number of commits since it and the commit, and -dirty. A release tag
// allows both as a prerelease.
var gitDescribeSuffix = regexp.MustCompile(`(-[0-9]+-g[0-9a-f]+)(-dirty)?$|-dirty$`)

// classDefaultWarning returns a warning about the class default of class, or
// "" for none. configs are the SandboxConfigs in the cluster, and name is
// this build's config.
//
//   - It warns when the config has the class-default annotation but is not
//     the class default, because a newer default exists. That happens when an
//     earlier build's config is deployed again.
//   - It warns when the class has no default, so pools of the class without a
//     configRef resolve none.
func classDefaultWarning(configs []atev1.SandboxConfig, name string, class atev1.SandboxClass) string {
	def := atev1.ClassDefaultSandboxConfig(configs, class)
	if def == nil {
		return fmt.Sprintf("no SandboxConfig of sandbox class %q has the %s annotation, so WorkerPools of the class without a configRef resolve no config",
			class, atev1.SandboxConfigClassDefaultAnnotation)
	}
	i := slices.IndexFunc(configs, func(sc atev1.SandboxConfig) bool { return sc.Name == name })
	if i >= 0 && configs[i].Annotations[atev1.SandboxConfigClassDefaultAnnotation] == "true" && def.Name != name {
		return fmt.Sprintf("SandboxConfig %s has the %s annotation but is not the %s class default, because %s is newer",
			name, atev1.SandboxConfigClassDefaultAnnotation, class, def.Name)
	}
	return ""
}
